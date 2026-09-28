package aggregator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
)

func TestConcurrentDuplicateWaitsForCommit(t *testing.T) {
	s := newFakeStore()
	r := newRunner(t, s)
	ctx := context.Background()
	d := deliveryFor("same-batch", point("metric", 10, 0))
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Handle(ctx, d); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if r.InflightMessages() != 32 {
		t.Fatal("duplicate was settled before its data committed")
	}
	if err := r.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s.total("metric", base); got != 10 {
		t.Fatalf("sum=%v, want 10", got)
	}
	if r.Stats().MessagesAcked != 32 {
		t.Fatal("not all deliveries settled after commit")
	}
}

func TestCapacityDoesNotCommitAPartialBatch(t *testing.T) {
	s := newFakeStore()
	r := newRunner(t, s)
	r.engine = aggregate.New(aggregate.Config{WindowSize: time.Minute, MaxSeries: 2})
	ctx := context.Background()
	if err := r.Handle(ctx, deliveryFor("first", point("one", 10, 0))); err != nil {
		t.Fatal(err)
	}
	d := deliveryFor("second", point("two", 20, 0), point("three", 30, 0))
	if err := r.Handle(ctx, d); !errors.Is(err, aggregate.ErrCapacity) {
		t.Fatalf("err=%v", err)
	}
	if r.engine.Stats().TrackedSeries != 1 {
		t.Fatal("part of rejected batch was accumulated")
	}
	if err := r.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Handle(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err := r.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if s.total("two", base) != 20 || s.total("three", base) != 30 {
		t.Fatal("retry lost points")
	}
}

func TestFailedOldWindowCanRetryAfterNewerCommit(t *testing.T) {
	s := newFakeStore()
	r := newRunner(t, s)
	ctx := context.Background()
	d := deliveryFor("old", point("metric", 42, 0))
	if err := r.Handle(ctx, d); err != nil {
		t.Fatal(err)
	}
	s.failNext = 1
	if err := r.FlushAll(ctx); err == nil {
		t.Fatal("expected failure")
	}
	if err := r.Handle(ctx, deliveryFor("new", point("other", 1, 5*time.Minute))); err != nil {
		t.Fatal(err)
	}
	if err := r.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Handle(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err := r.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if s.total("metric", base) != 42 {
		t.Fatal("newer watermark discarded the retry")
	}
}

type blockedFlush struct {
	*fakeStore
	entered chan struct{}
	release chan struct{}
}

func (s *blockedFlush) Flush(ctx context.Context, rollups []aggregate.Rollup, claims []store.Contribution) error {
	close(s.entered)
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.fakeStore.Flush(ctx, rollups, claims)
}

func TestDeliveryDuringFlushIsNotAcknowledgedAsDurable(t *testing.T) {
	s := &blockedFlush{newFakeStore(), make(chan struct{}), make(chan struct{})}
	r := newRunner(t, s)
	ctx := context.Background()
	d := deliveryFor("inflight", point("metric", 5, 0))
	if err := r.Handle(ctx, d); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.FlushAll(ctx) }()
	<-s.entered
	err := r.Handle(ctx, d)
	close(s.release)
	if !errors.Is(err, ErrFlushInProgress) {
		t.Fatalf("err=%v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
