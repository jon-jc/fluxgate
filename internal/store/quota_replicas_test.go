package store_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/ratelimit"
	"github.com/jon-jc/fluxgate/internal/store"
)

func lessorFor(db *store.DB) ratelimit.Lessor {
	return ratelimit.LessorFunc(func(ctx context.Context, key string, want int, l ratelimit.Limits) (ratelimit.Lease, error) {
		g, err := db.LeaseQuota(ctx, key, want, l.Rate, l.Burst)
		return ratelimit.Lease{Granted: g.Granted, Remaining: g.Remaining}, err
	})
}

// Several replicas, each with concurrent requests, metering one tenant through
// the real lease statement in real time. Admissions across all of them stay
// within one tenant's allowance plus one lease per replica -- where per-
// instance limiting would allow the allowance once per replica.
func TestLeasedReplicasEnforceOneTenantWideAllowance(t *testing.T) {
	db, tenant := openDB(t)
	const (
		replicas   = 4
		perReplica = 4
		cost       = 25
		horizon    = 50 * time.Millisecond
	)
	limits := ratelimit.Limits{Rate: 2000, Burst: 500}

	var admitted, fallbacks atomic.Int64
	meters := make([]*ratelimit.Leased, replicas)
	for i := range meters {
		meters[i] = ratelimit.NewLeased(lessorFor(db), ratelimit.New(limits), ratelimit.LeasedOptions{
			Defaults: limits,
			Horizon:  horizon,
			Timeout:  5 * time.Second,
			OnLease: func(outcome string, err error) {
				if outcome != "leased" {
					fallbacks.Add(1)
					t.Errorf("lease outcome %s: %v", outcome, err)
				}
			},
		})
	}

	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(1500 * time.Millisecond)
	for _, m := range meters {
		for range perReplica {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					if d, err := m.Meter(context.Background(), tenant, cost, ratelimit.Limits{}); err == nil && d.Allowed {
						admitted.Add(cost)
					}
				}
			}()
		}
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()

	allowance := float64(limits.Burst) + limits.Rate*elapsed
	ceiling := allowance + replicas*limits.Rate*horizon.Seconds()
	got := float64(admitted.Load())
	if got > ceiling {
		t.Fatalf("%d replicas admitted %.0f points in %.2fs; one tenant's ceiling is %.0f",
			replicas, got, elapsed, ceiling)
	}
	if got < 0.8*allowance {
		t.Fatalf("admitted only %.0f of a %.0f allowance: leasing is starving the tenant", got, allowance)
	}
	t.Logf("%d replicas admitted %.0f points against a %.0f allowance (ceiling %.0f); per-instance limits would allow %.0f",
		replicas, got, allowance, ceiling, replicas*allowance)
}
