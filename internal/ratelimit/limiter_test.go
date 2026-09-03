package ratelimit

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced time source, so refill behaviour can be
// asserted exactly rather than approximated with sleeps.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestBurstIsAvailableImmediately(t *testing.T) {
	clock := newClock()
	l := New(Limits{Rate: 10, Burst: 5}, WithClock(clock.Now))

	// A tenant's first requests must not be throttled just because the process
	// started a moment ago.
	for i := range 5 {
		if d := l.Allow("acme", Limits{}); !d.Allowed {
			t.Fatalf("request %d denied while burst should still be available", i+1)
		}
	}
	if d := l.Allow("acme", Limits{}); d.Allowed {
		t.Error("the sixth request was allowed; the burst is 5")
	}
}

func TestTokensRefillAtTheConfiguredRate(t *testing.T) {
	clock := newClock()
	l := New(Limits{Rate: 10, Burst: 10}, WithClock(clock.Now))

	for range 10 {
		l.Allow("acme", Limits{})
	}
	if d := l.Allow("acme", Limits{}); d.Allowed {
		t.Fatal("bucket should be empty")
	}

	// At 10/s, half a second buys five tokens.
	clock.Advance(500 * time.Millisecond)
	for i := range 5 {
		if d := l.Allow("acme", Limits{}); !d.Allowed {
			t.Fatalf("request %d denied after a 500ms refill at 10/s", i+1)
		}
	}
	if d := l.Allow("acme", Limits{}); d.Allowed {
		t.Error("a sixth request was allowed; only five tokens had accrued")
	}
}

func TestTokensNeverExceedBurst(t *testing.T) {
	clock := newClock()
	l := New(Limits{Rate: 100, Burst: 10}, WithClock(clock.Now))

	l.Allow("acme", Limits{})

	// An hour of idleness must not bank an hour's worth of allowance; the
	// ceiling is the whole point of the burst parameter.
	clock.Advance(time.Hour)

	allowed := 0
	for range 100 {
		if l.Allow("acme", Limits{}).Allowed {
			allowed++
		}
	}
	if allowed != 10 {
		t.Errorf("allowed %d requests after an hour idle, want exactly the burst of 10", allowed)
	}
}

func TestTenantsAreIsolated(t *testing.T) {
	clock := newClock()
	l := New(Limits{Rate: 1, Burst: 2}, WithClock(clock.Now))

	for range 2 {
		l.Allow("noisy", Limits{})
	}
	if l.Allow("noisy", Limits{}).Allowed {
		t.Fatal("the noisy tenant should be throttled")
	}

	// One tenant exhausting its quota must not affect anyone else.
	if !l.Allow("quiet", Limits{}).Allowed {
		t.Error("an unrelated tenant was throttled")
	}
}

func TestPerKeyOverrideBeatsTheDefault(t *testing.T) {
	clock := newClock()
	l := New(Limits{Rate: 1, Burst: 1}, WithClock(clock.Now))

	premium := Limits{Rate: 100, Burst: 50}
	for i := range 50 {
		if d := l.Allow("premium", premium); !d.Allowed {
			t.Fatalf("request %d denied despite a burst of 50", i+1)
		}
	}
}

// TestLoweringBurstTakesEffectImmediately covers a quota reduction: it must not
// wait for an existing surplus to drain.
func TestLoweringBurstTakesEffectImmediately(t *testing.T) {
	clock := newClock()
	l := New(Limits{Rate: 1, Burst: 1}, WithClock(clock.Now))

	generous := Limits{Rate: 10, Burst: 100}
	l.Allow("acme", generous) // bucket now holds 99 tokens

	stingy := Limits{Rate: 10, Burst: 2}
	l.Allow("acme", stingy) // clamps to 2, then spends one

	if d := l.Allow("acme", stingy); !d.Allowed {
		t.Fatal("the second request under the reduced burst should still fit")
	}
	if d := l.Allow("acme", stingy); d.Allowed {
		t.Error("the reduced burst was not applied; the old surplus survived")
	}
}

func TestAllowNChargesPerPoint(t *testing.T) {
	clock := newClock()
	l := New(Limits{Rate: 100, Burst: 100}, WithClock(clock.Now))

	if d := l.AllowN("acme", 60, Limits{}); !d.Allowed {
		t.Fatal("a 60-point batch should fit in a burst of 100")
	}
	// 40 tokens remain, so a second 60-point batch must not fit. Charging per
	// request instead would let a caller evade its quota by batching.
	if d := l.AllowN("acme", 60, Limits{}); d.Allowed {
		t.Error("a second 60-point batch was allowed with only 40 tokens left")
	}
	if d := l.AllowN("acme", 40, Limits{}); !d.Allowed {
		t.Error("a 40-point batch should fit in the remaining 40 tokens")
	}
}

// TestOversizedBatchIsRejectedWithoutRetryAdvice covers the case where waiting
// cannot help: the batch is larger than the bucket will ever hold.
func TestOversizedBatchIsRejectedWithoutRetryAdvice(t *testing.T) {
	clock := newClock()
	l := New(Limits{Rate: 10, Burst: 100}, WithClock(clock.Now))

	d := l.AllowN("acme", 500, Limits{})
	if d.Allowed {
		t.Fatal("a 500-point batch was allowed against a burst of 100")
	}
	if d.RetryAfter != 0 {
		t.Errorf("RetryAfter = %v, want 0: retrying can never succeed, the client must split the batch",
			d.RetryAfter)
	}
}

func TestDeniedDecisionAdvisesWhenToRetry(t *testing.T) {
	clock := newClock()
	l := New(Limits{Rate: 10, Burst: 1}, WithClock(clock.Now))

	l.Allow("acme", Limits{})
	d := l.Allow("acme", Limits{})

	if d.Allowed {
		t.Fatal("expected the second request to be denied")
	}
	// At 10/s one token takes 100ms; the advice must be usable, not zero.
	if d.RetryAfter <= 0 || d.RetryAfter > time.Second {
		t.Errorf("RetryAfter = %v, want roughly 100ms", d.RetryAfter)
	}

	clock.Advance(d.RetryAfter)
	if !l.Allow("acme", Limits{}).Allowed {
		t.Error("a request at exactly RetryAfter was still denied; the advice must not be too optimistic")
	}
}

func TestRemainingTracksTheBucket(t *testing.T) {
	clock := newClock()
	l := New(Limits{Rate: 10, Burst: 10}, WithClock(clock.Now))

	for want := 9; want >= 0; want-- {
		d := l.Allow("acme", Limits{})
		if !d.Allowed {
			t.Fatalf("request denied with %d expected to remain", want)
		}
		if d.Remaining != want {
			t.Errorf("Remaining = %d, want %d", d.Remaining, want)
		}
	}
}

// TestIdleBucketsAreReclaimed keeps memory bounded over a long uptime.
func TestIdleBucketsAreReclaimed(t *testing.T) {
	clock := newClock()
	l := New(Limits{Rate: 10, Burst: 10},
		WithClock(clock.Now), WithIdleTTL(time.Minute))

	for i := range 100 {
		l.Allow("tenant-"+strconv.Itoa(i), Limits{})
	}
	if got := l.Len(); got != 100 {
		t.Fatalf("Len() = %d, want 100", got)
	}

	// Move past the idle TTL and touch one key, which triggers a sweep of its
	// shard. Every shard is swept once its own next request arrives.
	clock.Advance(2 * time.Minute)
	for i := range 100 {
		l.Allow("tenant-"+strconv.Itoa(i), Limits{})
	}
	// Each key was re-created by the second pass, so the count returns to 100
	// rather than growing without bound.
	if got := l.Len(); got > 100 {
		t.Errorf("Len() = %d after reclamation, want no more than 100", got)
	}

	clock.Advance(2 * time.Minute)
	l.Allow("survivor", Limits{})
	if got := l.Len(); got > 100 {
		t.Errorf("Len() = %d; idle buckets were not reclaimed", got)
	}
}

func TestZeroLimitsAllowEverything(t *testing.T) {
	// A limiter with no usable allowance must not silently deny every request;
	// configuration validation is the thing that complains about it.
	l := New(Limits{})

	if d := l.Allow("acme", Limits{}); !d.Allowed {
		t.Error("a limiter with zero limits denied a request")
	}
}

// TestNonMonotonicClockDoesNotGrantFreeTokens covers an NTP step or a suspended
// VM: time appearing to move backwards must not corrupt the bucket.
func TestNonMonotonicClockDoesNotGrantFreeTokens(t *testing.T) {
	clock := newClock()
	l := New(Limits{Rate: 10, Burst: 5}, WithClock(clock.Now))

	for range 5 {
		l.Allow("acme", Limits{})
	}

	clock.Advance(-time.Hour)

	if d := l.Allow("acme", Limits{}); d.Allowed {
		t.Error("a backwards clock step handed out a free token")
	}
}

func TestConcurrentAccessIsSafe(t *testing.T) {
	clock := newClock()
	l := New(Limits{Rate: 1_000_000, Burst: 1_000_000}, WithClock(clock.Now))

	const (
		goroutines = 16
		perG       = 200
	)

	var wg sync.WaitGroup
	allowed := make([]int, goroutines)

	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range perG {
				// Spread across keys to exercise every shard.
				if l.Allow("tenant-"+strconv.Itoa(i%32), Limits{}).Allowed {
					allowed[g]++
				}
			}
		}(g)
	}
	wg.Wait()

	total := 0
	for _, n := range allowed {
		total += n
	}
	// The allowance is far above the load, so nothing should have been denied.
	if total != goroutines*perG {
		t.Errorf("allowed %d of %d requests under a limit that should admit all",
			total, goroutines*perG)
	}
}

// TestBucketsSpreadAcrossShards confirms the sharding actually distributes
// keys; a hash that collapsed everything into one shard would reintroduce the
// single mutex the design exists to avoid.
func TestBucketsSpreadAcrossShards(t *testing.T) {
	l := New(Limits{Rate: 10, Burst: 10})

	for i := range 512 {
		l.Allow("tenant-"+strconv.Itoa(i), Limits{})
	}

	occupied := 0
	for _, sh := range l.shards {
		sh.mu.Lock()
		if len(sh.buckets) > 0 {
			occupied++
		}
		sh.mu.Unlock()
	}
	if occupied != shardCount {
		t.Errorf("%d of %d shards hold buckets; keys are not spreading",
			occupied, shardCount)
	}
}

func BenchmarkAllow(b *testing.B) {
	l := New(Limits{Rate: 1e9, Burst: 1e9})

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			l.Allow("tenant-"+strconv.Itoa(i%64), Limits{})
			i++
		}
	})
}
