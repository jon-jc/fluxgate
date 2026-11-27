package store_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The property everything above this table rests on: however many replicas
// lease concurrently, the bucket never grants more than it can emit -- and,
// so that the bound is not satisfied by a bucket that simply never refills,
// under saturation it grants close to that amount.
func TestLeaseQuotaNeverGrantsMoreThanTheBucketEmits(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	const (
		rate    = 400.0
		burst   = 50
		leasers = 32
	)

	var granted atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(1500 * time.Millisecond)
	for w := range leasers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				g, err := db.LeaseQuota(ctx, tenant, 1+w%7, rate, burst)
				if err != nil {
					t.Error(err)
					return
				}
				if g.Granted < 0 || g.Granted > 1+w%7 || g.Remaining < 0 {
					t.Errorf("impossible grant %+v for want %d", g, 1+w%7)
					return
				}
				granted.Add(int64(g.Granted))
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()

	ceiling := float64(burst) + rate*elapsed
	got := float64(granted.Load())
	if got > ceiling {
		t.Fatalf("granted %.0f tokens in %.3fs; the bucket can emit at most %.1f", got, elapsed, ceiling)
	}
	if got < 0.8*ceiling {
		t.Fatalf("granted only %.0f of a possible %.1f: the bucket is not refilling", got, ceiling)
	}
	t.Logf("granted %.0f of a possible %.1f (%.1f%%) across %d concurrent leasers",
		got, ceiling, 100*got/ceiling, leasers)
}

func TestLeaseQuotaStartsFullAndDoesNotBankPastBurst(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	first, err := db.LeaseQuota(ctx, tenant, 1000, 1000, 50)
	if err != nil {
		t.Fatal(err)
	}
	if first.Granted != 50 {
		t.Fatalf("a new tenant's first lease granted %d, want its full burst of 50", first.Granted)
	}

	// Idle long enough to earn 300 tokens at this rate; the bucket holds 50.
	time.Sleep(300 * time.Millisecond)
	again, err := db.LeaseQuota(ctx, tenant, 1000, 1000, 50)
	if err != nil {
		t.Fatal(err)
	}
	if again.Granted != 50 {
		t.Fatalf("after idling, granted %d; burst caps it at 50", again.Granted)
	}
}

func TestLeaseQuotaTenantsAreIndependent(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	other := tenant + "-other"

	if g, err := db.LeaseQuota(ctx, tenant, 10, 1, 10); err != nil || g.Granted != 10 {
		t.Fatalf("drain: %+v %v", g, err)
	}
	if g, err := db.LeaseQuota(ctx, other, 10, 1, 10); err != nil || g.Granted != 10 {
		t.Fatalf("a drained tenant starved another: %+v %v", g, err)
	}
	if g, err := db.LeaseQuota(ctx, tenant, 10, 1, 10); err != nil || g.Granted != 0 {
		t.Fatalf("a drained bucket granted %+v (%v)", g, err)
	}
}
