package aggregator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
)

type stalledStore struct {
	*fakeStore
	lookup, write bool
}

func (s *stalledStore) SeenContributions(ctx context.Context, tenant, batch string, windows []time.Time) (map[string]bool, error) {
	if s.lookup {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return s.fakeStore.SeenContributions(ctx, tenant, batch, windows)
}

func (s *stalledStore) Flush(ctx context.Context, rows []aggregate.Rollup, claims []store.Contribution) error {
	if s.write {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.fakeStore.Flush(ctx, rows, claims)
}

func TestStorageTimeoutDeclinesLookupAndNacksUncommittedCheckpoint(t *testing.T) {
	s := &stalledStore{fakeStore: newFakeStore(), lookup: true}
	r := newRunner(t, s)
	r.storageTimeout = 5 * time.Millisecond
	d := deliveryFor("deadline", point("metric", 7, 0))
	d.EncodedBytes = 123
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Handle(ctx, d); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked ledger lookup did not time out", err)
	}
	if r.engine.Stats().PointsAccepted != 0 || r.Stats().PendingMessages != 0 {
		t.Fatal("failed ledger lookup admitted data")
	}
	s.lookup = false
	s.write = true
	if err := r.Handle(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err := r.FlushAll(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked checkpoint did not time out", err)
	}
	if got := r.Stats(); got.MessagesAcked != 0 || got.MessagesNacked != 1 || got.PendingBytes != 0 {
		t.Fatalf("timed-out checkpoint was not returned for retry: %+v", got)
	}
	s.write = false
	if err := r.Handle(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err := r.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != nil || s.total("metric", base) != 7 || r.Stats().MessagesAcked != 1 {
		t.Fatal("storage deadline prevented correct recovery")
	}
}
