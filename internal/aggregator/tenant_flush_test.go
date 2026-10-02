package aggregator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/pubsubx"
	"github.com/jon-jc/fluxgate/internal/store"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

func tenantDelivery(tenant, batch, metric string) pubsubx.Delivery {
	return pubsubx.Delivery{EncodedBytes: 100, Envelope: pubsubx.NewEnvelope(telemetry.Batch{
		ID: batch, TenantID: tenant, ReceivedAt: base,
		Points: []telemetry.Point{point(metric, 7, 0), point(metric, 11, time.Minute)},
	})}
}

type isolatedTenantStore struct {
	*fakeStore
	entered chan struct{}
	release chan struct{}
	fail    bool
	once    sync.Once
}

func (s *isolatedTenantStore) Flush(ctx context.Context, rows []aggregate.Rollup, claims []store.Contribution) error {
	tenant := claims[0].TenantID
	for _, c := range claims {
		if c.TenantID != tenant {
			return errors.New("checkpoint mixed tenants")
		}
	}
	for i := range rows {
		if rows[i].Key.TenantID != tenant {
			return errors.New("rollup assigned to another tenant")
		}
	}
	if tenant == "blocked" {
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		if s.fail {
			return store.ErrContributionConflict
		}
	}
	return s.fakeStore.Flush(ctx, rows, claims)
}

func TestTenantCheckpointsCommitAndSettleIndependently(t *testing.T) {
	s := &isolatedTenantStore{fakeStore: newFakeStore(), entered: make(chan struct{}), release: make(chan struct{}), fail: true}
	r := newRunner(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	blocked := tenantDelivery("blocked", "same-id", "slow")
	fast := tenantDelivery("healthy", "same-id", "fast")
	for _, d := range []pubsubx.Delivery{blocked, fast, fast} {
		if err := r.Handle(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- r.FlushAll(ctx) }()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("blocked tenant transaction did not start")
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for r.Stats().MessagesAcked != 2 {
		select {
		case <-deadline.C:
			t.Fatal("healthy tenant did not settle while other tenant blocked", r.Stats())
		case <-time.After(time.Millisecond):
		}
	}
	if r.Stats().PendingMessages != 1 || r.Stats().PendingBytes != 100 || s.total("fast", base) != 7 || s.total("fast", base.Add(time.Minute)) != 11 {
		t.Fatal("healthy tenant settlement affected another tenant or duplicated totals")
	}
	if err := r.Handle(ctx, fast); err != nil {
		t.Fatal("healthy replay was blocked by the other transaction", err)
	}
	close(s.release)
	if err := waitAdmissionResult(t, done); !errors.Is(err, store.ErrContributionConflict) {
		t.Fatal("conflicting tenant did not require redelivery", err)
	}
	if r.Stats().MessagesNacked != 1 || r.Stats().PendingMessages != 0 {
		t.Fatal("failure retried a healthy tenant", r.Stats())
	}
	s.fail = false
	if err := r.Handle(ctx, blocked); err != nil {
		t.Fatal(err)
	}
	if err := r.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if s.total("slow", base) != 7 || s.total("slow", base.Add(time.Minute)) != 11 || s.total("fast", base) != 7 {
		t.Fatal("isolated failure/retry changed committed totals")
	}
}

type boundedTenantStore struct {
	*fakeStore
	active  atomic.Int64
	peak    atomic.Int64
	calls   atomic.Int64
	entered chan struct{}
}

func (s *boundedTenantStore) Flush(ctx context.Context, _ []aggregate.Rollup, _ []store.Contribution) error {
	s.calls.Add(1)
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for old := s.peak.Load(); active > old; old = s.peak.Load() {
		if s.peak.CompareAndSwap(old, active) {
			break
		}
	}
	s.entered <- struct{}{}
	<-ctx.Done()
	return ctx.Err()
}

func TestTenantWorkersBoundConcurrencyAndQueuedWorkSharesDeadline(t *testing.T) {
	s := &boundedTenantStore{fakeStore: newFakeStore(), entered: make(chan struct{}, 8)}
	r := newRunner(t, s)
	r.flushConcurrency = 2
	r.storageTimeout = 100 * time.Millisecond
	for i := range 8 {
		if err := r.Handle(context.Background(), tenantDelivery(fmt.Sprint(i), "batch", "metric")); err != nil {
			t.Fatal(err)
		}
	}
	started := time.Now()
	err := r.FlushAll(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("checkpoint did not return its deadline", err)
	}
	if s.peak.Load() != 2 || s.calls.Load() != 2 || s.active.Load() != 0 {
		t.Fatal("worker limit or queued cancellation violated", s.peak.Load(), s.calls.Load(), s.active.Load())
	}
	if time.Since(started) > time.Second || r.Stats().MessagesNacked != 8 || r.Stats().PendingBytes != 0 {
		t.Fatal("queued tenants received separate deadlines or leaked receive credit", r.Stats())
	}
}

func TestTenantFlushConcurrencyBounds(t *testing.T) {
	for _, value := range []int{-1, 17} {
		if _, err := New(Options{Engine: aggregate.New(aggregate.Config{}), Store: newFakeStore(), FlushConcurrency: value}); err == nil {
			t.Fatal("unbounded tenant worker count accepted", value)
		}
	}
}
