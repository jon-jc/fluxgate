// Package aggregator coordinates message delivery, windowed aggregation and
// durable flushing.
//
// The package exists because those three concerns only make sense together.
// The aggregation engine knows about windows but not about messages; the store
// knows about transactions but not about windows; and the correctness argument
// -- that a batch is counted exactly once -- lives in how their lifecycles are
// sequenced, which is here.
package aggregator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/observability"
	"github.com/jon-jc/fluxgate/internal/pubsubx"
	"github.com/jon-jc/fluxgate/internal/store"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

// Store is the persistence the runner needs. It is an interface so the
// coordination logic can be tested without a database.
type Store interface {
	// Flush writes rollups and the contributions that produced them in one
	// transaction.
	Flush(ctx context.Context, rollups []aggregate.Rollup, contributions []store.Contribution) error
	// SeenContributions reports which (batch, window) pairs are already
	// committed.
	SeenContributions(ctx context.Context, batchID string, windows []time.Time) (map[string]bool, error)
}

// Options configures a Runner.
type Options struct {
	// Engine accumulates points into windows.
	Engine *aggregate.Engine
	// Store persists the results.
	Store Store
	// FlushInterval is how often closed windows are drained even when no new
	// data has arrived. Without it, a producer that goes quiet would leave its
	// last window unwritten and its messages unacknowledged indefinitely.
	FlushInterval time.Duration
	// Metrics records flush outcomes. Optional; a nil value disables
	// instrumentation rather than panicking.
	Metrics *observability.Metrics
	// Logger receives lifecycle events.
	Logger *slog.Logger
}

func (o *Options) applyDefaults() {
	if o.FlushInterval <= 0 {
		o.FlushInterval = 15 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Runner consumes batches, aggregates them and flushes the results.
//
// A message is acknowledged only once every window it contributed to has been
// committed. That is the whole design: acknowledging on receipt would mean a
// crash between accepting a point and writing its window silently loses the
// point, with the broker believing it delivered successfully.
type Runner struct {
	engine        *aggregate.Engine
	store         Store
	flushInterval time.Duration
	metrics       *observability.Metrics
	log           *slog.Logger

	mu sync.Mutex
	// inflight tracks messages waiting on windows, keyed by window start.
	inflight map[int64][]*pendingMessage
	// contributions records which batches fed which window, so the flush can
	// write the ledger entries that make redelivery safe.
	contributions map[int64]map[string]store.Contribution
	// claimed is the in-memory half of duplicate suppression: contributions
	// accepted but not yet handed to the store, which by definition are not in
	// the database yet.
	claimed map[string]struct{}
	// flushing holds contributions whose write is in progress. A redelivery
	// arriving now cannot be answered honestly -- the write may still fail --
	// so it is handed back rather than acknowledged as a duplicate.
	flushing map[string]struct{}

	stats Stats
}

type pendingMessage struct {
	delivery pubsubx.Delivery
	// awaiting is the set of window starts this message is still waiting on.
	// A batch that straddles a boundary must not be acknowledged when only one
	// of its windows has been written, so settlement waits for the set to
	// empty rather than for a single flush.
	awaiting map[int64]struct{}
	// settled guards against a double settlement when one of a message's
	// windows fails while another succeeds.
	settled bool
}

// Stats is a snapshot of the runner's counters.
type Stats struct {
	BatchesAccepted  int64
	BatchesDuplicate int64
	FlushesSucceeded int64
	FlushesFailed    int64
	RollupsWritten   int64
	MessagesAcked    int64
	MessagesNacked   int64
}

// New returns a Runner.
func New(opts Options) (*Runner, error) {
	switch {
	case opts.Engine == nil:
		return nil, errors.New("aggregator: engine is required")
	case opts.Store == nil:
		return nil, errors.New("aggregator: store is required")
	}
	opts.applyDefaults()

	return &Runner{
		engine:        opts.Engine,
		store:         opts.Store,
		flushInterval: opts.FlushInterval,
		metrics:       opts.Metrics,
		log:           opts.Logger,
		inflight:      make(map[int64][]*pendingMessage),
		contributions: make(map[int64]map[string]store.Contribution),
		claimed:       make(map[string]struct{}),
		flushing:      make(map[string]struct{}),
	}, nil
}

// Handle processes one delivery. It satisfies pubsubx.Handler.
//
// The handler takes ownership of the message: it returns nil without
// acknowledging, and settlement happens later, when the windows this batch fed
// have been committed. Returning an error hands the message back for
// redelivery in the usual way.
func (r *Runner) Handle(ctx context.Context, d pubsubx.Delivery) error {
	batch := d.Envelope.Batch()
	log := observability.LoggerFromContext(ctx)

	// Work out which windows this batch touches before accumulating anything,
	// so already-committed windows can be excluded rather than double-counted.
	windows := r.windowsFor(batch)
	if len(windows) == 0 {
		// An empty batch has nothing to wait for.
		d.Ack()
		return nil
	}

	committed, err := r.store.SeenContributions(ctx, batch.ID, windows)
	if err != nil {
		// Without knowing what is already committed, accumulating would risk
		// double-counting. Hand the message back and try again.
		return fmt.Errorf("check delivery ledger for batch %s: %w", batch.ID, err)
	}

	skip, inFlight := r.skipSet(batch, windows, committed)
	if inFlight {
		// The outcome of the in-progress write is not known yet. Answering
		// "duplicate" would be a guess, and the wrong guess loses data, so the
		// message goes back and returns once the flush has resolved.
		return ErrFlushInProgress
	}
	if len(skip) == len(windows) {
		// Every window this batch feeds is already accounted for. This is a
		// duplicate delivery, which is expected rather than exceptional.
		r.recordDuplicate()
		log.Debug("skipping duplicate batch", slog.String("batch_id", batch.ID))
		d.Ack()
		return nil
	}

	filtered := filterBatch(batch, skip, r.engine.WindowSize())
	if len(filtered.Points) == 0 {
		r.recordDuplicate()
		d.Ack()
		return nil
	}

	result := r.engine.Ingest(filtered)
	if len(result.Windows) == 0 {
		// Everything was late or shed. There is no window to wait on, and
		// nothing durable will ever be written for it.
		if result.Late > 0 || result.Shed > 0 {
			log.Warn("batch produced no rollups",
				slog.String("batch_id", batch.ID),
				slog.Int("late", result.Late),
				slog.Int("shed", result.Shed))
		}
		d.Ack()
		return nil
	}

	r.track(d, batch, result.Windows)

	log.Debug("batch accumulated",
		slog.String("batch_id", batch.ID),
		slog.Int("accepted", result.Accepted),
		slog.Int("late", result.Late),
		slog.Int("shed", result.Shed),
		slog.Int("windows", len(result.Windows)))

	return nil
}

// windowsFor returns the distinct window starts a batch's points fall into.
func (r *Runner) windowsFor(batch telemetry.Batch) []time.Time {
	size := r.engine.WindowSize()

	seen := make(map[int64]time.Time, 4)
	for _, p := range batch.Points {
		w := aggregate.WindowFor(p.Timestamp, size)
		seen[w.Start.Unix()] = w.Start
	}

	out := make([]time.Time, 0, len(seen))
	for _, start := range seen {
		out = append(out, start)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

// ErrFlushInProgress means a redelivery arrived while the original is being
// written. The message is handed back and will be answered definitively once
// the write has succeeded or failed.
var ErrFlushInProgress = errors.New("a flush for this batch is in progress")

// skipSet returns the window starts this batch must not be accumulated into,
// combining the durable ledger with the in-memory claims. The boolean reports
// that one of the windows is mid-write, which the caller must treat as
// "unknown" rather than as a duplicate.
func (r *Runner) skipSet(
	batch telemetry.Batch, windows []time.Time, committed map[string]bool,
) (map[int64]struct{}, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	skip := make(map[int64]struct{}, len(windows))
	for _, start := range windows {
		key := store.Contribution{BatchID: batch.ID, WindowStart: start}.Key()

		// Committed covers a redelivery after a restart.
		if committed[key] {
			skip[start.Unix()] = struct{}{}
			continue
		}
		// Mid-write: the answer is not knowable yet.
		if _, writing := r.flushing[key]; writing {
			return nil, true
		}
		// Claimed covers a redelivery before the flush, when nothing is in the
		// database yet and only this process knows.
		if _, pending := r.claimed[key]; pending {
			skip[start.Unix()] = struct{}{}
		}
	}
	return skip, false
}

// filterBatch drops the points belonging to windows that must be skipped.
func filterBatch(batch telemetry.Batch, skip map[int64]struct{}, size time.Duration) telemetry.Batch {
	if len(skip) == 0 {
		return batch
	}

	points := make([]telemetry.Point, 0, len(batch.Points))
	for _, p := range batch.Points {
		if _, skipped := skip[aggregate.WindowFor(p.Timestamp, size).Start.Unix()]; skipped {
			continue
		}
		points = append(points, p)
	}

	batch.Points = points
	return batch
}

// track registers a message as waiting on the windows it fed.
func (r *Runner) track(d pubsubx.Delivery, batch telemetry.Batch, windows []aggregate.Window) {
	r.mu.Lock()
	defer r.mu.Unlock()

	pending := &pendingMessage{
		delivery: d,
		awaiting: make(map[int64]struct{}, len(windows)),
	}

	for _, w := range windows {
		pending.awaiting[w.Start.Unix()] = struct{}{}

		key := w.Start.Unix()
		r.inflight[key] = append(r.inflight[key], pending)

		contribution := store.Contribution{
			BatchID:     batch.ID,
			TenantID:    batch.TenantID,
			WindowStart: w.Start,
		}
		if r.contributions[key] == nil {
			r.contributions[key] = make(map[string]store.Contribution)
		}
		r.contributions[key][contribution.Key()] = contribution
		r.claimed[contribution.Key()] = struct{}{}
	}

	r.stats.BatchesAccepted++
}

func (r *Runner) recordDuplicate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stats.BatchesDuplicate++
}

// Flush drains every window the watermark has passed and settles the messages
// that fed them.
//
// Ordering is the point: the rollups and the ledger commit together, and only
// then are messages acknowledged. If the commit fails, the messages are handed
// back instead, and redelivery rebuilds exactly the windows that did not
// commit.
func (r *Runner) Flush(ctx context.Context) error {
	return r.flush(ctx, false)
}

// FlushAll drains every window regardless of the watermark. It runs at
// shutdown, where the alternative is discarding partial windows and forcing
// their messages to be redelivered to another instance.
func (r *Runner) FlushAll(ctx context.Context) error {
	return r.flush(ctx, true)
}

func (r *Runner) flush(ctx context.Context, all bool) error {
	var (
		rollups []aggregate.Rollup
		windows []aggregate.Window
	)
	if all {
		rollups, windows = r.engine.CollectAll()
	} else {
		// A producer that has gone quiet would otherwise strand its last
		// window: the watermark only moves when data arrives.
		if r.engine.AdvanceOnIdle() {
			r.log.Debug("advanced the watermark on idleness")
		}
		rollups, windows = r.engine.Collect()
	}
	if len(windows) == 0 {
		return nil
	}

	contributions, messages := r.detach(windows)
	// Whatever happens next, these contributions stop being in-flight.
	defer r.releaseFlushing(contributions)

	started := time.Now()

	if err := r.store.Flush(ctx, rollups, contributions); err != nil {
		// The engine has already handed over these rollups, so this data now
		// exists nowhere else. Handing the messages back is what recovers it:
		// no ledger entry was written, so redelivery rebuilds exactly these
		// windows and nothing else.
		r.nackAll(messages)

		r.mu.Lock()
		r.stats.FlushesFailed++
		r.mu.Unlock()

		return fmt.Errorf("flush %d windows: %w", len(windows), err)
	}

	// The engine is only told now: before the write is confirmed, a point for
	// one of these windows is not late, it is the raw material for the retry.
	r.engine.MarkFlushed(windows)
	r.metrics.ObserveFlush(len(windows), len(rollups), time.Since(started))
	r.ackAll(messages)

	r.mu.Lock()
	r.stats.FlushesSucceeded++
	r.stats.RollupsWritten += int64(len(rollups))
	r.mu.Unlock()

	r.log.Info("flushed windows",
		slog.Int("windows", len(windows)),
		slog.Int("rollups", len(rollups)),
		slog.Int("messages", len(messages)),
		slog.String("oldest", windows[0].String()))

	return nil
}

// detach removes the bookkeeping for the given windows and returns it.
func (r *Runner) detach(windows []aggregate.Window) ([]store.Contribution, []*pendingMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var (
		contributions []store.Contribution
		messages      []*pendingMessage
		seen          = make(map[*pendingMessage]struct{})
	)

	for _, w := range windows {
		key := w.Start.Unix()

		for _, c := range r.contributions[key] {
			contributions = append(contributions, c)
		}
		delete(r.contributions, key)

		for _, m := range r.inflight[key] {
			if _, dup := seen[m]; dup {
				continue
			}
			seen[m] = struct{}{}
			messages = append(messages, m)
		}
		delete(r.inflight, key)
	}

	// Claims become in-flight rather than being released: a redelivery
	// arriving during the write must be held, not answered.
	for _, c := range contributions {
		delete(r.claimed, c.Key())
		r.flushing[c.Key()] = struct{}{}
	}

	// Cross the flushed windows off each message. One that straddled a
	// boundary keeps waiting until its last window commits.
	for _, m := range messages {
		for _, w := range windows {
			delete(m.awaiting, w.Start.Unix())
		}
	}
	return contributions, messages
}

// releaseFlushing clears the in-flight marks once a write has resolved.
func (r *Runner) releaseFlushing(contributions []store.Contribution) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, c := range contributions {
		delete(r.flushing, c.Key())
	}
}

func (r *Runner) ackAll(messages []*pendingMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, m := range messages {
		// Still waiting on another window: acknowledging now would tell the
		// broker the whole batch is durable when half of it is not.
		if m.settled || len(m.awaiting) > 0 {
			continue
		}
		m.settled = true
		m.delivery.Ack()
		r.stats.MessagesAcked++
	}
}

func (r *Runner) nackAll(messages []*pendingMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, m := range messages {
		if m.settled {
			continue
		}
		m.settled = true
		m.delivery.Nack()
		r.stats.MessagesNacked++
	}
}

// Run flushes on a timer until ctx is cancelled, then drains what is left.
func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return r.drain(ctx)

		case <-ticker.C:
			if err := r.Flush(ctx); err != nil {
				// A failed flush is not fatal: the messages have been handed
				// back, and the next delivery rebuilds the window.
				r.log.Error("flush failed", slog.Any("error", err))
			}
		}
	}
}

// drain writes whatever is still held in memory at shutdown.
//
// It runs on a fresh deadline rather than the cancelled parent: inheriting the
// cancellation would abort the very write that keeps the last window from being
// lost.
func (r *Runner) drain(ctx context.Context) error {
	drainCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	if err := r.FlushAll(drainCtx); err != nil {
		// The messages have been handed back, so the data is not lost -- it
		// will be redelivered to whichever instance is still running.
		r.log.Error("final flush failed; unacknowledged messages will be redelivered",
			slog.Any("error", err))
		return err
	}

	r.log.Info("aggregator drained", slog.String("stats", r.engine.Stats().String()))
	return nil
}

// Stats returns a snapshot of the runner's counters.
func (r *Runner) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats
}

// InflightMessages reports how many messages are waiting on a window, for
// diagnostics and an operational gauge.
func (r *Runner) InflightMessages() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	seen := make(map[*pendingMessage]struct{})
	for _, pending := range r.inflight {
		for _, m := range pending {
			seen[m] = struct{}{}
		}
	}
	return len(seen)
}
