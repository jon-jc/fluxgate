package aggregator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/pubsubx"
	"github.com/jon-jc/fluxgate/internal/store"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

var base = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

// fakeStore records what was committed, standing in for Postgres so the
// coordination logic can be tested without one.
type fakeStore struct {
	mu sync.Mutex

	// committed maps a contribution key to the sum it contributed, so a
	// double-count shows up as a wrong total rather than merely a wrong count.
	committed map[string]struct{}
	// totals maps "metric|window" to the accumulated sum actually written.
	totals map[string]float64

	flushes  int
	failNext int
	failErr  error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		committed: make(map[string]struct{}),
		totals:    make(map[string]float64),
	}
}

func (s *fakeStore) Flush(_ context.Context, rollups []aggregate.Rollup, contributions []store.Contribution) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.flushes++
	if s.failNext > 0 {
		s.failNext--
		if s.failErr != nil {
			return s.failErr
		}
		return errors.New("simulated database failure")
	}

	// The real store commits rollups and the ledger in one transaction, so the
	// fake applies both or neither.
	for i := range rollups {
		r := &rollups[i]
		key := r.Key.Metric + "|" + r.Window.Start.UTC().Format(time.RFC3339)
		s.totals[key] += r.Acc.Sum
	}
	for _, c := range contributions {
		s.committed[c.Key()] = struct{}{}
	}
	return nil
}

func (s *fakeStore) SeenContributions(_ context.Context, batchID string, windows []time.Time) (map[string]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seen := make(map[string]bool, len(windows))
	for _, w := range windows {
		key := store.Contribution{BatchID: batchID, WindowStart: w}.Key()
		if _, ok := s.committed[key]; ok {
			seen[key] = true
		}
	}
	return seen, nil
}

func (s *fakeStore) total(metric string, window time.Time) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totals[metric+"|"+window.UTC().Format(time.RFC3339)]
}

func (s *fakeStore) flushCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushes
}

// deliveryFor builds a delivery whose settlement is observable.
//
// pubsubx.Delivery settles through an unexported message, so a delivery built
// in a test is inert: settlement is asserted through the runner's own counters,
// which is what the production path drives anyway.
func deliveryFor(batchID string, points ...telemetry.Point) pubsubx.Delivery {
	envelope := pubsubx.NewEnvelope(telemetry.Batch{
		ID:         batchID,
		TenantID:   "acme",
		ReceivedAt: base,
		Points:     points,
	})
	return pubsubx.Delivery{Envelope: envelope, MessageID: batchID}
}

func point(metric string, value float64, offset time.Duration) telemetry.Point {
	return telemetry.Point{
		Metric:    metric,
		Kind:      telemetry.KindGauge,
		Value:     value,
		Timestamp: base.Add(offset),
	}
}

func newRunner(t *testing.T, s Store) *Runner {
	t.Helper()

	r, err := New(Options{
		Engine: aggregate.New(aggregate.Config{
			WindowSize: time.Minute,
			MaxSeries:  10_000,
		}),
		Store:  s,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

func TestHandleAccumulatesAndFlushes(t *testing.T) {
	fake := newFakeStore()
	r := newRunner(t, fake)
	ctx := context.Background()

	if err := r.Handle(ctx, deliveryFor("b1",
		point("cpu.util", 10, 0),
		point("cpu.util", 20, 10*time.Second),
	)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// Advance the watermark past the window.
	if err := r.Handle(ctx, deliveryFor("b2", point("cpu.util", 5, 2*time.Minute))); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if err := r.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := fake.total("cpu.util", base); got != 30 {
		t.Errorf("stored sum = %v, want 30", got)
	}
	if s := r.Stats(); s.MessagesAcked != 1 {
		t.Errorf("acked %d messages, want 1 (the second is still waiting)", s.MessagesAcked)
	}
}

// TestMessageIsNotAckedUntilItsDataIsDurable is the property the whole design
// exists for: acknowledging on receipt would let a crash lose the point while
// the broker believes it was delivered.
func TestMessageIsNotAckedUntilItsDataIsDurable(t *testing.T) {
	fake := newFakeStore()
	r := newRunner(t, fake)
	ctx := context.Background()

	if err := r.Handle(ctx, deliveryFor("b1", point("cpu.util", 1, 0))); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if s := r.Stats(); s.MessagesAcked != 0 {
		t.Fatalf("acked %d messages before any flush; the data is still only in memory", s.MessagesAcked)
	}
	if got := r.InflightMessages(); got != 1 {
		t.Errorf("inflight = %d, want 1", got)
	}

	// Close the window and flush.
	if err := r.Handle(ctx, deliveryFor("b2", point("cpu.util", 1, 2*time.Minute))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := r.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if s := r.Stats(); s.MessagesAcked != 1 {
		t.Errorf("acked %d messages after the flush, want 1", s.MessagesAcked)
	}
}

// TestStraddlingMessageWaitsForBothWindows: a batch spanning a boundary must
// not be acknowledged when only half of it is durable.
func TestStraddlingMessageWaitsForBothWindows(t *testing.T) {
	fake := newFakeStore()
	r := newRunner(t, fake)
	ctx := context.Background()

	// One batch with points either side of 12:01.
	if err := r.Handle(ctx, deliveryFor("straddle",
		point("cpu.util", 1, 30*time.Second),
		point("cpu.util", 2, 90*time.Second),
	)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// Advance so only the first window closes.
	if err := r.Handle(ctx, deliveryFor("b2", point("other", 1, 110*time.Second))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := r.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if s := r.Stats(); s.MessagesAcked != 0 {
		t.Errorf("acked %d messages; the second window is still open", s.MessagesAcked)
	}

	// Now close the second window too.
	if err := r.Handle(ctx, deliveryFor("b3", point("other", 1, 5*time.Minute))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := r.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if s := r.Stats(); s.MessagesAcked < 1 {
		t.Errorf("the straddling message was never acknowledged (stats: %+v)", s)
	}
}

// TestRedeliveryBeforeFlushDoesNotDoubleCount covers the in-memory half of
// duplicate suppression: the first delivery is not in the database yet, so only
// the claim set can catch the repeat.
func TestRedeliveryBeforeFlushDoesNotDoubleCount(t *testing.T) {
	fake := newFakeStore()
	r := newRunner(t, fake)
	ctx := context.Background()

	batch := deliveryFor("b1", point("cpu.util", 100, 0))

	if err := r.Handle(ctx, batch); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	// The same message again, before anything has been written.
	if err := r.Handle(ctx, batch); err != nil {
		t.Fatalf("redelivery: %v", err)
	}

	if err := r.Handle(ctx, deliveryFor("b2", point("cpu.util", 1, 2*time.Minute))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := r.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := fake.total("cpu.util", base); got != 100 {
		t.Errorf("stored sum = %v, want 100; the redelivery was counted twice", got)
	}
	if s := r.Stats(); s.BatchesDuplicate != 1 {
		t.Errorf("BatchesDuplicate = %d, want 1", s.BatchesDuplicate)
	}
}

// TestRedeliveryAfterFlushDoesNotDoubleCount covers the durable half: after a
// restart the claim set is empty, and only the ledger can catch the repeat.
func TestRedeliveryAfterFlushDoesNotDoubleCount(t *testing.T) {
	fake := newFakeStore()
	ctx := context.Background()

	first := newRunner(t, fake)
	if err := first.Handle(ctx, deliveryFor("b1", point("cpu.util", 100, 0))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := first.Handle(ctx, deliveryFor("b2", point("cpu.util", 7, 2*time.Minute))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := first.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// A fresh runner, as after a restart: no in-memory state, only the ledger.
	second := newRunner(t, fake)
	if err := second.Handle(ctx, deliveryFor("b1", point("cpu.util", 100, 0))); err != nil {
		t.Fatalf("redelivery after restart: %v", err)
	}
	if err := second.Handle(ctx, deliveryFor("b3", point("cpu.util", 1, 3*time.Minute))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := second.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := fake.total("cpu.util", base); got != 100 {
		t.Errorf("stored sum = %v, want 100; the ledger did not suppress the redelivery", got)
	}
}

// TestPartialRedeliveryRebuildsOnlyTheMissingWindow is why the ledger is keyed
// on (batch, window) rather than on the batch alone.
func TestPartialRedeliveryRebuildsOnlyTheMissingWindow(t *testing.T) {
	fake := newFakeStore()
	ctx := context.Background()

	r := newRunner(t, fake)

	// A batch straddling 12:01.
	straddle := deliveryFor("straddle",
		point("cpu.util", 10, 30*time.Second),
		point("cpu.util", 20, 90*time.Second),
	)
	if err := r.Handle(ctx, straddle); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	// Close only the first window and commit it.
	if err := r.Handle(ctx, deliveryFor("filler", point("other", 1, 110*time.Second))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := r.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := fake.total("cpu.util", base); got != 10 {
		t.Fatalf("first window = %v, want 10", got)
	}

	// The process restarts before the second window was written, so the
	// message is redelivered to a fresh runner.
	restarted := newRunner(t, fake)
	if err := restarted.Handle(ctx, straddle); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if err := restarted.Handle(ctx, deliveryFor("filler2", point("other", 1, 5*time.Minute))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := restarted.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// The committed window must not have been counted again...
	if got := fake.total("cpu.util", base); got != 10 {
		t.Errorf("first window = %v, want 10; the committed part was re-counted", got)
	}
	// ...and the window that never committed must now exist.
	if got := fake.total("cpu.util", base.Add(time.Minute)); got != 20 {
		t.Errorf("second window = %v, want 20; the uncommitted part was lost", got)
	}
}

// TestFlushFailureHandsMessagesBack is the recovery path: the engine has
// already surrendered the rollups, so the data exists nowhere else and the only
// way to get it back is redelivery.
func TestFlushFailureHandsMessagesBack(t *testing.T) {
	fake := newFakeStore()
	fake.failNext = 1

	r := newRunner(t, fake)
	ctx := context.Background()

	if err := r.Handle(ctx, deliveryFor("b1", point("cpu.util", 1, 0))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := r.Handle(ctx, deliveryFor("b2", point("cpu.util", 1, 2*time.Minute))); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if err := r.Flush(ctx); err == nil {
		t.Fatal("Flush returned nil despite the store failing")
	}

	s := r.Stats()
	if s.MessagesNacked == 0 {
		t.Error("no message was handed back after a failed flush")
	}
	if s.MessagesAcked != 0 {
		t.Error("a message was acknowledged despite the flush failing")
	}
	if s.FlushesFailed != 1 {
		t.Errorf("FlushesFailed = %d, want 1", s.FlushesFailed)
	}
}

// TestFailedFlushReleasesItsClaims lets the redelivered message be accumulated
// again; a claim left behind would make the retry look like a duplicate and
// silently discard the data.
func TestFailedFlushReleasesItsClaims(t *testing.T) {
	fake := newFakeStore()
	fake.failNext = 1

	r := newRunner(t, fake)
	ctx := context.Background()

	batch := deliveryFor("b1", point("cpu.util", 42, 0))
	if err := r.Handle(ctx, batch); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := r.Handle(ctx, deliveryFor("b2", point("filler", 1, 2*time.Minute))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := r.Flush(ctx); err == nil {
		t.Fatal("expected the flush to fail")
	}

	// The broker redelivers the nacked message.
	if err := r.Handle(ctx, batch); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if err := r.Handle(ctx, deliveryFor("b3", point("filler", 1, 4*time.Minute))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := r.Flush(ctx); err != nil {
		t.Fatalf("second Flush: %v", err)
	}

	if got := fake.total("cpu.util", base); got != 42 {
		t.Errorf("stored sum = %v, want 42; the retry was mistaken for a duplicate", got)
	}
}

func TestLedgerFailureHandsTheMessageBack(t *testing.T) {
	fake := newFakeStore()
	r := newRunner(t, fake)

	failing := &failingLedger{fakeStore: fake}
	r.store = failing

	// Without a reliable view of what is already committed, accumulating could
	// double-count, so the message must go back rather than be guessed at.
	err := r.Handle(context.Background(), deliveryFor("b1", point("cpu.util", 1, 0)))
	if err == nil {
		t.Fatal("Handle returned nil despite the ledger lookup failing")
	}
	if r.InflightMessages() != 0 {
		t.Error("a message was tracked despite the ledger lookup failing")
	}
}

type failingLedger struct{ *fakeStore }

func (f *failingLedger) SeenContributions(context.Context, string, []time.Time) (map[string]bool, error) {
	return nil, errors.New("database unreachable")
}

func TestFlushAllDrainsOpenWindows(t *testing.T) {
	fake := newFakeStore()
	r := newRunner(t, fake)
	ctx := context.Background()

	if err := r.Handle(ctx, deliveryFor("b1", point("cpu.util", 5, 0))); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// Nothing is closed, so an ordinary flush writes nothing.
	if err := r.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if fake.flushCount() != 0 {
		t.Fatalf("the store was written to with no closed windows")
	}

	// At shutdown the alternative is discarding a partial window.
	if err := r.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	if got := fake.total("cpu.util", base); got != 5 {
		t.Errorf("stored sum = %v, want 5", got)
	}
	if s := r.Stats(); s.MessagesAcked != 1 {
		t.Errorf("acked %d messages, want 1", s.MessagesAcked)
	}
}

func TestEmptyBatchIsAcknowledged(t *testing.T) {
	r := newRunner(t, newFakeStore())

	// Nothing to wait for; leaving it unacknowledged would stall the
	// subscription behind a message that will never become durable.
	if err := r.Handle(context.Background(), deliveryFor("empty")); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if r.InflightMessages() != 0 {
		t.Error("an empty batch was tracked as inflight")
	}
}

func TestLatePointsAreAcknowledgedNotStalled(t *testing.T) {
	fake := newFakeStore()
	r := newRunner(t, fake)
	ctx := context.Background()

	if err := r.Handle(ctx, deliveryFor("b1", point("cpu.util", 1, 0))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := r.Handle(ctx, deliveryFor("b2", point("cpu.util", 1, 5*time.Minute))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := r.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	before := r.InflightMessages()

	// A straggler for a window that has already been written. It can never
	// become durable, so holding its message would stall the subscription.
	if err := r.Handle(ctx, deliveryFor("late", point("cpu.util", 999, 10*time.Second))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if got := r.InflightMessages(); got != before {
		t.Errorf("inflight went from %d to %d; the late message was tracked", before, got)
	}
	if got := fake.total("cpu.util", base); got != 1 {
		t.Errorf("stored sum = %v, want 1; the late point changed a written rollup", got)
	}
}

func TestConcurrentHandlingIsSafe(t *testing.T) {
	fake := newFakeStore()
	r := newRunner(t, fake)
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 25 {
				batchID := string(rune('a'+g)) + string(rune('0'+i%10)) + string(rune('0'+i/10))
				_ = r.Handle(ctx, deliveryFor(batchID,
					point("concurrent.metric", 1, time.Duration(i)*time.Second)))
			}
		}(g)
	}

	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		for range 20 {
			_ = r.Flush(ctx)
			r.Stats()
			r.InflightMessages()
		}
	}()

	wg.Wait()
	<-flushDone

	if err := r.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
}

func TestNewRejectsMissingCollaborators(t *testing.T) {
	if _, err := New(Options{Store: newFakeStore()}); err == nil {
		t.Error("New succeeded with no engine")
	}
	if _, err := New(Options{Engine: aggregate.New(aggregate.Config{})}); err == nil {
		t.Error("New succeeded with no store")
	}
}

func TestRunFlushesOnShutdown(t *testing.T) {
	fake := newFakeStore()
	r := newRunner(t, fake)

	ctx, cancel := context.WithCancel(context.Background())

	if err := r.Handle(ctx, deliveryFor("b1", point("cpu.util", 3, 0))); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	// A partial window at shutdown is written rather than discarded.
	if got := fake.total("cpu.util", base); got != 3 {
		t.Errorf("stored sum = %v, want 3; the final drain lost the open window", got)
	}
}
