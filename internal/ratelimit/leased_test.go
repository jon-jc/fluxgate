package ratelimit

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/resilience"
)

// sharedBucket is an exact model of the PostgreSQL bucket: one clock, one lock,
// created full, refilled to burst at rate, granting whole tokens.
type sharedBucket struct {
	mu      sync.Mutex
	clock   *fakeClock
	tokens  float64
	at      time.Time
	started bool
	leases  int
	fail    error
	// failEvery fails every nth lease: blips that never trip a breaker.
	failEvery int
}

func newSharedBucket() *sharedBucket {
	return &sharedBucket{clock: newClock()}
}

func (b *sharedBucket) Lease(ctx context.Context, _ string, want int, limits Limits) (Lease, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.leases++
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	if b.fail != nil {
		return Lease{}, b.fail
	}
	if b.failEvery > 0 && b.leases%b.failEvery == 0 {
		return Lease{}, errors.New("lease timed out")
	}
	now := b.clock.Now()
	if !b.started {
		b.tokens, b.at, b.started = float64(limits.Burst), now, true
	}
	if elapsed := now.Sub(b.at).Seconds(); elapsed > 0 {
		b.tokens = math.Min(float64(limits.Burst), b.tokens+limits.Rate*elapsed)
	}
	b.at = now
	granted := min(want, int(math.Floor(b.tokens)))
	b.tokens -= float64(granted)
	return Lease{Granted: granted, Remaining: b.tokens}, nil
}

func (b *sharedBucket) leaseCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.leases
}

var tenantLimits = Limits{Rate: 1000, Burst: 2000}

func newReplica(b *sharedBucket, opts LeasedOptions) *Leased {
	opts.Defaults = tenantLimits
	opts.Breaker.Now = b.clock.Now
	opts.Now = b.clock.Now
	return NewLeased(b, New(tenantLimits, WithClock(b.clock.Now)), opts)
}

// offer drives replicas with perStep requests each per 10ms step for duration,
// and returns how many points were admitted.
func offer(clock *fakeClock, replicas []Meter, duration time.Duration, cost, perStep int) int {
	admitted := 0
	for elapsed := time.Duration(0); elapsed < duration; elapsed += 10 * time.Millisecond {
		for _, r := range replicas {
			for range perStep {
				if d, err := r.Meter(context.Background(), "acme", cost, Limits{}); err == nil && d.Allowed {
					admitted += cost
				}
			}
		}
		clock.Advance(10 * time.Millisecond)
	}
	return admitted
}

// Four replicas, one tenant, ten seconds of saturating traffic. Per-instance
// limiting admits four tenants' worth; leasing admits one, within the stated
// bound.
func TestReplicasShareOneAllowance(t *testing.T) {
	const (
		replicas = 4
		duration = 10 * time.Second
		cost     = 100
	)
	bucket := newSharedBucket()
	opts := LeasedOptions{Horizon: 250 * time.Millisecond}
	leased := make([]Meter, replicas)
	perInstance := make([]Meter, replicas)
	for i := range replicas {
		leased[i] = newReplica(bucket, opts)
		perInstance[i] = New(tenantLimits, WithClock(bucket.clock.Now))
	}

	start := bucket.clock.Now()
	got := offer(bucket.clock, leased, duration, cost, 5)
	bucket.clock.now = start // replay the same ten seconds for the comparison
	old := offer(bucket.clock, perInstance, duration, cost, 5)

	chunk := tenantLimits.Rate * opts.Horizon.Seconds()
	ceiling := float64(tenantLimits.Burst) + tenantLimits.Rate*duration.Seconds() + replicas*chunk
	if float64(got) > ceiling {
		t.Fatalf("leasing admitted %d points; the bound is %.0f", got, ceiling)
	}
	if float64(got) < 0.95*(float64(tenantLimits.Burst)+tenantLimits.Rate*duration.Seconds()) {
		t.Fatalf("leasing admitted only %d points: the tenant is being starved", got)
	}
	if old < 3*got {
		t.Fatalf("per-instance limiting admitted %d, expected about %dx leasing's %d", old, replicas, got)
	}
	t.Logf("tenant limit %.0f/s over %s on %d replicas: leased admitted %d, per-instance admitted %d (%.1fx); %d leases",
		tenantLimits.Rate, duration, replicas, got, old, float64(old)/float64(got), bucket.leaseCount())
}

// A tenant over its limit is exactly when the limiter matters, and rejecting
// it must not cost a database round trip per request -- or exceeding a quota
// would become a way to load the database. Lease traffic should track the
// tenant's rate, not how hard the tenant is hammering.
func TestOverLimitTenantDoesNotLoadTheDatabase(t *testing.T) {
	leases := func(perStep int) (int, int) {
		bucket := newSharedBucket()
		replicas := make([]Meter, 4)
		for i := range replicas {
			replicas[i] = newReplica(bucket, LeasedOptions{})
		}
		offer(bucket.clock, replicas, 10*time.Second, 100, perStep)
		return bucket.leaseCount(), 4 * perStep * 1000
	}
	light, lightOffered := leases(5)
	heavy, heavyOffered := leases(50)
	if heavy > light*3/2 {
		t.Fatalf("10x the offered load caused %d leases against %d: rejections are reaching the database",
			heavy, light)
	}
	if heavy*20 > heavyOffered {
		t.Fatalf("%d leases for %d requests: rejected requests are leasing", heavy, heavyOffered)
	}
	t.Logf("%d requests -> %d leases; %d requests -> %d leases", lightOffered, light, heavyOffered, heavy)
}

// Splitting the rate by replica count would also fix the multiplication, but
// throttles a tenant whose traffic lands on one replica to a quarter of its
// allowance. Leasing follows the traffic.
func TestUnevenTrafficStillGetsTheFullAllowance(t *testing.T) {
	bucket := newSharedBucket()
	busy := newReplica(bucket, LeasedOptions{})
	_ = newReplica(bucket, LeasedOptions{}) // idle replicas hold nothing
	_ = newReplica(bucket, LeasedOptions{})

	got := offer(bucket.clock, []Meter{busy}, 10*time.Second, 100, 5)
	if want := 0.95 * (float64(tenantLimits.Burst) + tenantLimits.Rate*10); float64(got) < want {
		t.Fatalf("one busy replica admitted %d points, want at least %.0f", got, want)
	}
}

// A failed lease is a retry, not a change of policy: the request gets an
// error, which ingest answers with a retryable 503. Only once failures trip
// the breaker -- the shared bucket is persistently unavailable -- are requests
// metered per instance: the guarantee the service had before, never unlimited
// and never refusing everything. Leasing resumes once the bucket recovers.
func TestLeaseFailureRetriesThenDegradesToPerInstanceLimits(t *testing.T) {
	bucket := newSharedBucket()
	outcomes := map[string]int{}
	var mu sync.Mutex
	replica := newReplica(bucket, LeasedOptions{
		Breaker: resilience.Options{FailureThreshold: 2, Cooldown: time.Second},
		OnLease: func(outcome string, _ error) {
			mu.Lock()
			defer mu.Unlock()
			outcomes[outcome]++
		},
	})
	bucket.fail = errors.New("connection refused")

	for i := range 2 {
		if _, err := replica.Meter(context.Background(), "acme", 500, Limits{}); err == nil {
			t.Fatalf("request %d: a failed lease was neither retried nor degraded", i)
		}
	}
	// The breaker is open: per-instance limits, 2,000 burst, 500 a request.
	for i := range 4 {
		if d, err := replica.Meter(context.Background(), "acme", 500, Limits{}); err != nil || !d.Allowed {
			t.Fatalf("degraded request %d: %+v %v; per-instance burst should admit it", i, d, err)
		}
	}
	if d, err := replica.Meter(context.Background(), "acme", 500, Limits{}); err != nil || d.Allowed {
		t.Fatalf("degraded mode admitted past the per-instance burst: %+v %v", d, err)
	}
	if calls := bucket.leaseCount(); calls != 2 {
		t.Fatalf("lessor called %d times; the open breaker should stop calls after 2 failures", calls)
	}
	if outcomes["error"] != 2 || outcomes["fallback"] != 5 {
		t.Fatalf("outcomes = %v, want 2 errors then 5 fallbacks", outcomes)
	}

	bucket.fail = nil
	bucket.clock.Advance(2 * time.Second)
	if d, err := replica.Meter(context.Background(), "acme", 100, Limits{}); err != nil || !d.Allowed || outcomes["leased"] == 0 {
		t.Fatalf("leasing did not resume after recovery: %+v %v, outcomes=%v", d, err, outcomes)
	}
}

// The case the two-tier policy exists for. Leases that fail now and then --
// timeouts under load -- never trip a breaker that counts consecutive
// failures. Had each one fallen back to per-instance limiting, it would have
// metered against a bucket that had never been used: a full, unearned burst
// per blip, on every replica.
func TestTransientLeaseFailuresGrantNothingUnearned(t *testing.T) {
	bucket := newSharedBucket()
	bucket.failEvery = 3
	replicas := make([]Meter, 4)
	errorsSeen := 0
	for i := range replicas {
		replicas[i] = newReplica(bucket, LeasedOptions{OnLease: func(outcome string, _ error) {
			if outcome == "error" {
				errorsSeen++
			}
		}})
	}
	got := offer(bucket.clock, replicas, 10*time.Second, 100, 5)
	ceiling := float64(tenantLimits.Burst) + tenantLimits.Rate*10 + 4*tenantLimits.Rate*0.25
	if float64(got) > ceiling {
		t.Fatalf("with one lease in three failing, admitted %d points; the bound is %.0f", got, ceiling)
	}
	if errorsSeen == 0 {
		t.Fatal("no lease failed; the test is not exercising blips")
	}
	t.Logf("admitted %d of a %.0f ceiling with %d failed leases", got, ceiling, errorsSeen)
}

// A client that disconnects mid-request must not cancel the lease. If it did,
// disconnects would count as lease failures, and enough of them would trip
// the breaker and switch every tenant to per-instance limits.
func TestClientCancellationDoesNotCancelTheLease(t *testing.T) {
	bucket := newSharedBucket()
	var fallbacks atomic.Int64
	replica := newReplica(bucket, LeasedOptions{OnLease: func(outcome string, _ error) {
		if outcome != "leased" {
			fallbacks.Add(1)
		}
	}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if d, err := replica.Meter(ctx, "acme", 100, Limits{}); err != nil || !d.Allowed || fallbacks.Load() != 0 {
		t.Fatalf("a cancelled request context failed the lease: %+v %v, failures=%d", d, err, fallbacks.Load())
	}
}

func TestOversizedBatchesAndPartialGrants(t *testing.T) {
	bucket := newSharedBucket()
	replica := newReplica(bucket, LeasedOptions{Horizon: time.Millisecond})

	if d, _ := replica.Meter(context.Background(), "acme", tenantLimits.Burst+1, Limits{}); d.Allowed || d.RetryAfter != 0 {
		t.Fatalf("a batch larger than the burst: %+v; want denied with no retry delay", d)
	}
	if bucket.leaseCount() != 0 {
		t.Fatal("leased tokens for a batch that can never fit")
	}

	// Drain the bucket to 30 tokens, then ask for 50: denied, but the 30
	// granted stay in the pool rather than being lost.
	replica.Meter(context.Background(), "acme", 1970, Limits{})
	if d, _ := replica.Meter(context.Background(), "acme", 50, Limits{}); d.Allowed || d.RetryAfter <= 0 {
		t.Fatalf("a request beyond the remaining tokens: %+v", d)
	}
	leases := bucket.leaseCount()
	if d, _ := replica.Meter(context.Background(), "acme", 30, Limits{}); !d.Allowed || bucket.leaseCount() != leases {
		t.Fatalf("partially granted tokens were lost: %+v, leases %d -> %d", d, leases, bucket.leaseCount())
	}
}

// Many concurrent requests for one tenant on one replica: no tokens are created
// locally, and concurrent shortfalls share one lease instead of each going to
// the database. Run under the race detector in CI.
func TestConcurrentRequestsShareLeases(t *testing.T) {
	bucket := newSharedBucket()
	replica := newReplica(bucket, LeasedOptions{})
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				if d, err := replica.Meter(context.Background(), "acme", 10, Limits{}); err == nil && d.Allowed {
					admitted.Add(10)
				}
			}
		}()
	}
	wg.Wait()
	// The clock never moved, so only the initial burst exists.
	if got := admitted.Load(); got != int64(tenantLimits.Burst) {
		t.Fatalf("admitted %d points from a %d burst with a stopped clock", got, tenantLimits.Burst)
	}
	if leases := bucket.leaseCount(); leases > 64 {
		t.Fatalf("%d leases for 3,200 requests: concurrent shortfalls are not sharing a lease", leases)
	}
}
