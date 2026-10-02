package aggregator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Return an old database snapshot only after the caller releases the gate.
// This models a read that overlaps a commit and whose network response is slow.
type delayedLedger struct {
	*fakeStore
	armed   atomic.Bool
	reads   atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (s *delayedLedger) SeenContributions(ctx context.Context, tenant, batch string, windows []time.Time) (map[string]bool, error) {
	s.reads.Add(1)
	seen, err := s.fakeStore.SeenContributions(ctx, tenant, batch, windows)
	if s.armed.CompareAndSwap(true, false) {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return seen, err
}

func waitAdmissionResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("database lookup blocked unrelated admission or checkpoint")
		return nil
	}
}

func TestSlowLedgerLookupDoesNotBlockCheckpointOrAdmitStaleDuplicate(t *testing.T) {
	s := &delayedLedger{fakeStore: newFakeStore(), entered: make(chan struct{}), release: make(chan struct{})}
	r := newRunner(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d := deliveryFor("overlap", point("metric", 7, 0), point("metric", 11, time.Minute))
	if err := r.Handle(ctx, d); err != nil {
		t.Fatal(err)
	}
	s.armed.Store(true)
	slow := make(chan error, 1)
	go func() { slow <- r.Handle(ctx, d) }()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	other := make(chan error, 1)
	go func() { other <- r.Handle(ctx, deliveryFor("unrelated", point("other", 3, 0))) }()
	if err := waitAdmissionResult(t, other); err != nil {
		t.Fatal(err)
	}
	flushed := make(chan error, 1)
	go func() { flushed <- r.FlushAll(ctx) }()
	if err := waitAdmissionResult(t, flushed); err != nil {
		t.Fatal(err)
	}
	close(s.release)
	if err := waitAdmissionResult(t, slow); err != nil {
		t.Fatal(err)
	}
	if err := r.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if s.reads.Load() != 4 || r.Stats().BatchesDuplicate != 1 {
		t.Fatal("stale ledger snapshot was not refreshed before admission", s.reads.Load(), r.Stats())
	}
	if s.total("metric", base) != 7 || s.total("metric", base.Add(time.Minute)) != 11 || s.total("other", base) != 3 {
		t.Fatal("overlapping lookup/commit duplicated or lost a window")
	}
	if r.Stats().PendingMessages != 0 || r.engine.Stats().TrackedSeries != 0 {
		t.Fatal("duplicate entered the engine after its original committed")
	}
}

func TestDrainDoesNotWaitForUnadmittedLedgerLookup(t *testing.T) {
	s := &delayedLedger{fakeStore: newFakeStore(), entered: make(chan struct{}), release: make(chan struct{})}
	r := newRunner(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Handle(ctx, deliveryFor("accepted", point("metric", 7, 0))); err != nil {
		t.Fatal(err)
	}
	s.armed.Store(true)
	slow := make(chan error, 1)
	go func() { slow <- r.Handle(ctx, deliveryFor("not-admitted", point("metric", 11, 0))) }()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	drained := make(chan error, 1)
	go func() { drained <- r.drain(ctx) }()
	if err := waitAdmissionResult(t, drained); err != nil {
		t.Fatal(err)
	}
	close(s.release)
	if err := waitAdmissionResult(t, slow); err == nil {
		t.Fatal("lookup admitted data after shutdown drained the engine")
	}
	if s.total("metric", base) != 7 || r.Stats().MessagesAcked != 1 || r.Stats().PendingMessages != 0 {
		t.Fatal("drain lost accepted data or acknowledged unadmitted data")
	}
}

type concurrentLedger struct {
	*fakeStore
	entered chan struct{}
	release chan struct{}
}

func (s *concurrentLedger) SeenContributions(ctx context.Context, tenant, batch string, windows []time.Time) (map[string]bool, error) {
	s.entered <- struct{}{}
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.fakeStore.SeenContributions(ctx, tenant, batch, windows)
}

func TestLedgerReadsProceedConcurrentlyAndAdmissionRemainsAtomic(t *testing.T) {
	const count = 16
	s := &concurrentLedger{newFakeStore(), make(chan struct{}, count), make(chan struct{})}
	r := newRunner(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, count)
	for range count {
		go func() { done <- r.Handle(ctx, deliveryFor("duplicate", point("metric", 7, 0))) }()
	}
	for range count {
		select {
		case <-s.entered:
		case <-ctx.Done():
			t.Fatal("ledger reads were serialized")
		}
	}
	close(s.release)
	for range count {
		if err := waitAdmissionResult(t, done); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if s.total("metric", base) != 7 || r.Stats().MessagesAcked != count {
		t.Fatal("concurrent ledger reads weakened duplicate suppression")
	}
}

type invalidatedLedger struct {
	*fakeStore
	invalidate func()
	deadlines  []time.Time
}

func (s *invalidatedLedger) SeenContributions(ctx context.Context, tenant, batch string, windows []time.Time) (map[string]bool, error) {
	deadline, _ := ctx.Deadline()
	s.deadlines = append(s.deadlines, deadline)
	s.invalidate()
	return s.fakeStore.SeenContributions(ctx, tenant, batch, windows)
}

func TestRepeatedLedgerInvalidationKeepsOriginalDeadline(t *testing.T) {
	s := &invalidatedLedger{fakeStore: newFakeStore()}
	r := newRunner(t, s)
	r.storageTimeout = 10 * time.Millisecond
	s.invalidate = func() {
		// Simulate checkpoints completing throughout the read/admit race.
		r.releaseFlushing(nil)
	}
	err := r.Handle(context.Background(), deliveryFor("retrying", point("metric", 7, 0)))
	if !errors.Is(err, context.DeadlineExceeded) || len(s.deadlines) < 2 {
		t.Fatal("stale reads did not stop at the admission deadline", err)
	}
	for _, deadline := range s.deadlines {
		if !deadline.Equal(s.deadlines[0]) {
			t.Fatal("retry reset the storage timeout")
		}
	}
	if r.Stats().PendingMessages != 0 || r.engine.Stats().PointsAccepted != 0 {
		t.Fatal("stale ledger read admitted data")
	}
}

type latencyLedger struct{ *fakeStore }

func (s latencyLedger) SeenContributions(ctx context.Context, tenant, batch string, windows []time.Time) (map[string]bool, error) {
	timer := time.NewTimer(2 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.fakeStore.SeenContributions(ctx, tenant, batch, windows)
}

// Isolates admission's sensitivity to database round-trip latency, not end-to-end
// capacity. Each operation admits 32 unique batches, reconciles and checkpoints.
func BenchmarkLedgerAdmission(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		s := latencyLedger{newFakeStore()}
		r := newRunner(b, s)
		var wg sync.WaitGroup
		for j := range 32 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := r.Handle(ctx, deliveryFor(fmt.Sprint(j), point("metric", 1, 0))); err != nil {
					b.Error(err)
				}
			}()
		}
		wg.Wait()
		if err := r.FlushAll(ctx); err != nil {
			b.Fatal(err)
		}
		if s.total("metric", base) != 32 {
			b.Fatal("incorrect total")
		}
	}
	b.ReportMetric(32, "batches/op")
}
