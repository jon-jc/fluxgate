package aggregate

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jon-jc/fluxgate/internal/telemetry"
)

// Window is a half-open interval [Start, End) that observations are grouped
// into.
type Window struct {
	Start time.Time
	End   time.Time
}

// String renders the window for logs.
func (w Window) String() string {
	return w.Start.UTC().Format(time.RFC3339) + "/" + w.End.UTC().Format(time.RFC3339)
}

// WindowFor returns the tumbling window a timestamp belongs to.
//
// Boundaries are anchored to the Unix epoch rather than to process start, so
// every instance in the fleet derives identical windows from identical
// timestamps. Anchoring to start-up would make two replicas disagree about
// where a window begins, and their rollups would never merge.
//
// The arithmetic is explicit rather than time.Truncate, which anchors to the
// zero time -- January 1 of year 1 -- not to the epoch. The two coincide only
// for durations that divide evenly into a day, so a weekly window truncated
// that way lands on a boundary inherited from the proleptic Gregorian calendar
// rather than on the one an operator configuring "168h" would expect.
//
// Floor division rather than truncation toward zero, so timestamps before 1970
// land in the window that contains them instead of the one after it.
func WindowFor(ts time.Time, size time.Duration) Window {
	if size <= 0 {
		// A non-positive window has no meaningful boundary. Returning an empty
		// window here would silently discard data; the engine rejects this at
		// construction, so reaching it means a caller built a Window directly.
		size = time.Minute
	}

	nanos := ts.UTC().UnixNano()
	step := size.Nanoseconds()

	bucket := nanos / step
	if nanos < 0 && nanos%step != 0 {
		bucket--
	}

	start := time.Unix(0, bucket*step).UTC()
	return Window{Start: start, End: start.Add(size)}
}

// Rollup is one series' aggregate over one window, ready to be persisted.
type Rollup struct {
	Window Window
	Key    SeriesKey
	// Labels are carried alongside the hash so a reader never has to reverse
	// it to find out what the series actually is.
	Labels map[string]string
	Acc    *Accumulator
}

// Config tunes the engine.
type Config struct {
	// WindowSize is the tumbling window width.
	WindowSize time.Duration
	// AllowedLateness is how far behind the highest observed event time the
	// watermark trails. It is the engine's entire tolerance for out-of-order
	// arrival: too small and legitimate stragglers are discarded, too large and
	// every rollup is delayed by that much before anyone can read it.
	AllowedLateness time.Duration
	// MaxSeries caps how many distinct series are held across all open
	// windows. Cardinality is the failure mode that kills a metrics system, and
	// a bound that sheds is survivable in a way that an OOM kill is not.
	MaxSeries int
	// IdleTimeout is how long a stream may be silent before the watermark is
	// allowed to advance on processing time instead of event time.
	//
	// Without it, a producer that stops sending strands its last window
	// forever: the watermark only moves when data arrives, so a window needing
	// one more observation to close never gets it, and the rollup is never
	// written. Zero disables the fallback.
	IdleTimeout time.Duration
	// Clock is the source of processing time, used only for the idle timeout.
	// Aggregation itself is driven entirely by event time.
	Clock func() time.Time
}

func (c *Config) applyDefaults() {
	if c.WindowSize <= 0 {
		c.WindowSize = time.Minute
	}
	if c.AllowedLateness < 0 {
		c.AllowedLateness = 0
	}
	if c.MaxSeries <= 0 {
		c.MaxSeries = 100_000
	}
	if c.IdleTimeout < 0 {
		c.IdleTimeout = 0
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
}

// IngestResult reports what happened to one batch.
type IngestResult struct {
	// Accepted is how many points were folded into a window.
	Accepted int
	// Late is how many arrived for a window that had already been flushed.
	Late int
	// Shed is how many were dropped because the series cap was reached.
	Shed int
	// Windows are the windows this batch contributed to. A caller holding a
	// message acknowledgement uses them to know when its data is durable.
	Windows []Window
}

// Stats is a snapshot of the engine's counters, for metrics and logging.
type Stats struct {
	OpenWindows      int
	TrackedSeries    int
	PointsAccepted   int64
	PointsLate       int64
	PointsShed       int64
	WindowsFlushed   int64
	WatermarkUnixSec int64
	// IdleAdvances counts how often the watermark moved on processing time
	// because the stream had gone quiet. A steadily rising count means the
	// producers are idle more often than the window size assumes.
	IdleAdvances int64
}

// Engine accumulates points into tumbling windows and emits rollups once a
// window can no longer receive data.
//
// Progress is driven by event time, not wall-clock time. A watermark trails the
// highest timestamp observed by AllowedLateness, and a window is closed once
// the watermark passes its end. This is what makes replay work: feeding a day
// of historical data through the engine produces exactly the rollups it
// produced live, because nothing depends on when the process happened to run.
type Engine struct {
	cfg Config

	mu sync.Mutex
	// windows is keyed by window start in Unix nanoseconds.
	//
	// Nanoseconds, not seconds: a sub-second window size would map several
	// distinct windows onto one second-resolution key, merging their points
	// into a single rollup whose reconstructed boundaries describe none of
	// them -- silently, with no error and simply wrong totals.
	windows map[int64]map[SeriesKey]*seriesState
	// watermark is the event time below which no further data is expected.
	watermark time.Time
	// flushedThrough is the end of the most recent window already emitted.
	// Anything for a window ending at or before it is late by definition.
	flushedThrough time.Time
	// seriesCount tracks distinct series across all open windows.
	seriesCount int
	// lastIngestAt is processing time, not event time: it answers "has this
	// stream gone quiet", which event time cannot.
	lastIngestAt time.Time
	stats        Stats
}

type seriesState struct {
	labels map[string]string
	acc    *Accumulator
}

// New returns an engine ready to accept points.
func New(cfg Config) *Engine {
	cfg.applyDefaults()
	return &Engine{
		cfg:     cfg,
		windows: make(map[int64]map[SeriesKey]*seriesState),
	}
}

// WindowSize reports the configured window width.
func (e *Engine) WindowSize() time.Duration { return e.cfg.WindowSize }

// Ingest folds a batch into its windows.
func (e *Engine) Ingest(batch telemetry.Batch) IngestResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	var (
		result IngestResult
		seen   = make(map[int64]Window, 4)
	)

	for _, p := range batch.Points {
		// The watermark advances on every observation, including ones that are
		// then rejected as late: the fact that a producer has reached this
		// timestamp is information regardless of what happens to the point.
		if p.Timestamp.After(e.watermark.Add(e.cfg.AllowedLateness)) {
			e.watermark = p.Timestamp.Add(-e.cfg.AllowedLateness)
		}

		window := WindowFor(p.Timestamp, e.cfg.WindowSize)

		// A window that has already been emitted cannot take more data without
		// silently changing a rollup somebody may already have read.
		if !e.flushedThrough.IsZero() && !window.End.After(e.flushedThrough) {
			result.Late++
			e.stats.PointsLate++
			continue
		}

		key := SeriesKeyFor(batch.TenantID, p)
		series, ok := e.windows[window.Start.UnixNano()]
		if !ok {
			series = make(map[SeriesKey]*seriesState)
			e.windows[window.Start.UnixNano()] = series
		}

		state, exists := series[key]
		if !exists {
			if e.seriesCount >= e.cfg.MaxSeries {
				// Shedding a point is recoverable. Running out of memory takes
				// the process down and loses every window it was holding.
				result.Shed++
				e.stats.PointsShed++
				continue
			}
			state = &seriesState{
				labels: copyLabels(p.Labels),
				acc:    NewAccumulator(p.Kind),
			}
			series[key] = state
			e.seriesCount++
		}

		state.acc.Observe(p.Value, p.Timestamp.UnixNano())
		result.Accepted++
		e.stats.PointsAccepted++
		seen[window.Start.UnixNano()] = window
	}

	result.Windows = make([]Window, 0, len(seen))
	for _, w := range seen {
		result.Windows = append(result.Windows, w)
	}
	sort.Slice(result.Windows, func(i, j int) bool {
		return result.Windows[i].Start.Before(result.Windows[j].Start)
	})

	if result.Accepted > 0 || result.Late > 0 {
		e.lastIngestAt = e.cfg.Clock()
	}
	e.stats.WatermarkUnixSec = e.watermark.Unix()
	return result
}

// AdvanceOnIdle lets the watermark move on processing time when the stream has
// gone quiet, and reports whether it did.
//
// Event-time watermarks only advance when data arrives, so a producer that
// stops sending leaves its final window one observation short of closing --
// permanently. This is the standard escape: after a period of silence, trust
// the wall clock instead.
//
// It is a deliberate, contained exception to the rule that aggregation depends
// only on event time. Replaying an idle stream can therefore close its last
// window at a different moment than the live run did, which is the price of
// not stranding the data at all. It is called explicitly rather than folded
// into Collect so that the dependence on wall-clock time is visible at the
// call site.
func (e *Engine) AdvanceOnIdle() bool {
	if e.cfg.IdleTimeout <= 0 {
		return false
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.windows) == 0 {
		return false
	}

	now := e.cfg.Clock()
	// A stream that has never delivered anything is not idle, it is new.
	if e.lastIngestAt.IsZero() {
		e.lastIngestAt = now
		return false
	}
	if now.Sub(e.lastIngestAt) < e.cfg.IdleTimeout {
		return false
	}

	// Advance only as far as the oldest open window needs to close. Jumping
	// the watermark to the wall clock would slam every open window shut at
	// once, including ones a resuming producer could still legitimately fill.
	oldest := time.Time{}
	for start := range e.windows {
		end := time.Unix(0, start).UTC().Add(e.cfg.WindowSize)
		if oldest.IsZero() || end.Before(oldest) {
			oldest = end
		}
	}

	if !oldest.After(e.watermark) {
		return false
	}

	e.watermark = oldest
	e.stats.WatermarkUnixSec = e.watermark.Unix()
	e.stats.IdleAdvances++
	return true
}

// Watermark returns the event time below which the engine expects no more
// data.
func (e *Engine) Watermark() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.watermark
}

// ErrNoClosedWindows means nothing was ready to emit.
var ErrNoClosedWindows = errors.New("no windows are closed")

// Collect removes and returns the rollups for every window the watermark has
// passed.
//
// Removing before persisting is deliberate: the engine's contract is that a
// collected window is gone from memory, and the caller owns the result. A
// caller that fails to persist must decide what to do with data it now holds
// exclusively -- which is a decision it can only make if it knows it holds it.
//
// Collecting does not close the window to further data. That only happens when
// the caller reports success through MarkFlushed, because a window whose write
// failed has to be rebuildable from a redelivery -- and it cannot be, if the
// engine has already decided that anything arriving for it is late.
func (e *Engine) Collect() ([]Rollup, []Window) {
	return e.collect(false)
}

// MarkFlushed records that these windows have been durably written.
//
// Only now does the engine begin treating arrivals for them as late: before
// the write is confirmed, a point for one of these windows is not late, it is
// the raw material for the retry.
func (e *Engine) MarkFlushed(windows []Window) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, w := range windows {
		if w.End.After(e.flushedThrough) {
			e.flushedThrough = w.End
		}
	}
	e.stats.WindowsFlushed += int64(len(windows))
}

// CollectAll removes and returns every window regardless of the watermark. It
// is used at shutdown, where the alternative is discarding partial windows that
// will never be completed by anyone.
func (e *Engine) CollectAll() ([]Rollup, []Window) {
	return e.collect(true)
}

func (e *Engine) collect(all bool) ([]Rollup, []Window) {
	e.mu.Lock()
	defer e.mu.Unlock()

	starts := make([]int64, 0, len(e.windows))
	for start := range e.windows {
		window := Window{
			Start: time.Unix(0, start).UTC(),
			End:   time.Unix(0, start).UTC().Add(e.cfg.WindowSize),
		}
		if all || !window.End.After(e.watermark) {
			starts = append(starts, start)
		}
	}
	if len(starts) == 0 {
		return nil, nil
	}
	// Emit oldest first so that a partial failure leaves a contiguous prefix
	// persisted rather than holes scattered through the timeline.
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })

	var (
		rollups []Rollup
		windows []Window
	)
	for _, start := range starts {
		window := Window{
			Start: time.Unix(0, start).UTC(),
			End:   time.Unix(0, start).UTC().Add(e.cfg.WindowSize),
		}
		for key, state := range e.windows[start] {
			rollups = append(rollups, Rollup{
				Window: window,
				Key:    key,
				Labels: state.labels,
				Acc:    state.acc,
			})
			e.seriesCount--
		}
		delete(e.windows, start)
		windows = append(windows, window)
	}

	return rollups, windows
}

// Stats returns a snapshot of the engine's counters.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()

	s := e.stats
	s.OpenWindows = len(e.windows)
	s.TrackedSeries = e.seriesCount
	return s
}

// PendingWindows returns the open windows, oldest first, for diagnostics.
func (e *Engine) PendingWindows() []Window {
	e.mu.Lock()
	defer e.mu.Unlock()

	starts := make([]int64, 0, len(e.windows))
	for start := range e.windows {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })

	windows := make([]Window, len(starts))
	for i, start := range starts {
		windows[i] = Window{
			Start: time.Unix(0, start).UTC(),
			End:   time.Unix(0, start).UTC().Add(e.cfg.WindowSize),
		}
	}
	return windows
}

// String renders a stats line for logging.
func (s Stats) String() string {
	return fmt.Sprintf(
		"windows=%d series=%d accepted=%d late=%d shed=%d flushed=%d",
		s.OpenWindows, s.TrackedSeries, s.PointsAccepted,
		s.PointsLate, s.PointsShed, s.WindowsFlushed)
}

func copyLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	// The source map belongs to a decoded message that the caller may reuse or
	// mutate; the engine holds this for the lifetime of a window.
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}
