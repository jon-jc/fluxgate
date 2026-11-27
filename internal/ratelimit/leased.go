package ratelimit

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jon-jc/fluxgate/internal/resilience"
)

// Meter decides whether a tenant may send cost points now. The in-memory
// Limiter meters per process; Leased meters against a bucket every replica
// shares. An error means the decision could not be made and the request
// should be retried; it is not a denial.
type Meter interface {
	Meter(ctx context.Context, key string, cost int, override Limits) (Decision, error)
}

// Meter implements Meter for a single process. It never fails.
func (l *Limiter) Meter(_ context.Context, key string, cost int, override Limits) (Decision, error) {
	return l.AllowN(key, cost, override), nil
}

// Lease is what a Lessor granted from a shared bucket.
type Lease struct {
	// Granted is the whole number of tokens moved to the caller.
	Granted int
	// Remaining is what the shared bucket holds after the grant.
	Remaining float64
}

// Lessor hands out tokens from a tenant's bucket shared by every replica.
type Lessor interface {
	Lease(ctx context.Context, key string, want int, limits Limits) (Lease, error)
}

// LessorFunc adapts a function to Lessor.
type LessorFunc func(ctx context.Context, key string, want int, limits Limits) (Lease, error)

// Lease implements Lessor.
func (f LessorFunc) Lease(ctx context.Context, key string, want int, limits Limits) (Lease, error) {
	return f(ctx, key, want, limits)
}

// LeasedOptions configures a Leased meter.
type LeasedOptions struct {
	// Defaults apply to keys without an override.
	Defaults Limits
	// Horizon sizes a lease: this long's worth of the rate, and never less
	// than the request in hand or more than the burst. It trades database
	// round trips for slack -- a replica holds at most one lease of tokens it
	// has not spent, which is the whole of the over-admission bound.
	Horizon time.Duration
	// Timeout bounds one lease. A lease that cannot finish in time meters that
	// request per instance instead.
	Timeout time.Duration
	// Breaker trips after consecutive lease failures, so a struggling database
	// costs one timeout rather than one per request.
	Breaker resilience.Options
	// OnLease observes each lease outcome: "leased"; "error", a failed lease
	// whose request is retried; or "fallback", a request metered per instance
	// because the breaker is open. Optional.
	OnLease func(outcome string, err error)
	// Now replaces the time source, for deterministic tests.
	Now func() time.Time
}

func (o *LeasedOptions) applyDefaults() {
	if o.Horizon <= 0 {
		o.Horizon = 250 * time.Millisecond
	}
	if o.Timeout <= 0 {
		o.Timeout = 250 * time.Millisecond
	}
	if o.Now == nil {
		o.Now = time.Now
	}
}

// Leased meters every replica against one bucket per tenant.
//
// Per-instance limiting multiplies a tenant's allowance by the replica count:
// four replicas admit four times the configured rate. Here a shared bucket is
// the only source of tokens. Each replica leases small batches from it and
// admits requests from its local pool, so the database is consulted only when
// a pool runs dry -- a few times a second per busy tenant, not per request.
//
// Because tokens are only ever created by the shared bucket, admissions
// across all replicas over any interval D are bounded by
//
//	burst + rate*D + R*lease
//
// where R is the number of replicas holding unspent tokens and lease is one
// Horizon's worth of rate. Per-instance limiting allows R*(burst + rate*D).
//
// A lease that fails is answered with an error, which the caller turns into a
// retryable response: one failed lease is a retry, not a reason to change
// policy. Only once failures trip the breaker -- the database is persistently
// unavailable -- are requests metered by the per-instance Limiter instead.
// That is the guarantee this service had before: degraded, but never
// unlimited and never refusing everything.
type Leased struct {
	lessor  Lessor
	local   *Limiter
	opts    LeasedOptions
	breaker *resilience.Breaker

	mu        sync.Mutex
	pools     map[string]*pool
	lastSweep time.Time
}

// pool is one tenant's unspent leased tokens on this replica. Its mutex also
// serialises leasing, so concurrent requests for a tenant trigger one lease
// and then share it, rather than each going to the database.
type pool struct {
	mu       sync.Mutex
	tokens   int
	shared   float64 // the shared bucket's balance at the last lease
	lastSeen atomic.Int64
	// emptyUntil is when the shared bucket, found too low to cover a request,
	// will have accrued that request's shortfall. Until then requests that
	// need a lease are denied here, without asking the database again.
	emptyUntil time.Time
}

const poolIdleTTL = 10 * time.Minute

// NewLeased returns a Meter backed by lessor, falling back to local.
func NewLeased(lessor Lessor, local *Limiter, opts LeasedOptions) *Leased {
	opts.applyDefaults()
	return &Leased{
		lessor:  lessor,
		local:   local,
		opts:    opts,
		breaker: resilience.New(opts.Breaker),
		pools:   make(map[string]*pool),
	}
}

// Breaker exposes the lease breaker's state for instrumentation.
func (l *Leased) Breaker() *resilience.Breaker { return l.breaker }

// Meter implements Meter.
func (l *Leased) Meter(ctx context.Context, key string, cost int, override Limits) (Decision, error) {
	if cost < 1 {
		cost = 1
	}
	limits := l.opts.Defaults
	if override.valid() {
		limits = override
	}
	if !limits.valid() {
		return Decision{Allowed: true, Limit: limits, Remaining: math.MaxInt32}, nil
	}
	if cost > limits.Burst {
		// As for the in-memory limiter: no amount of waiting fits this batch,
		// so there is no retry delay and nothing worth leasing.
		return Decision{Allowed: false, Limit: limits}, nil
	}

	p := l.pool(key)
	p.mu.Lock()
	defer p.mu.Unlock()
	now := l.opts.Now()

	if p.tokens < cost {
		// A tenant over its limit is exactly when the limiter matters, and
		// it must not cost a database round trip per rejected request -- or
		// exceeding a quota would become a way to load the database. While
		// the shared bucket is known to be short, deny here.
		if now.Before(p.emptyUntil) {
			return l.deny(limits, p, cost, p.emptyUntil.Sub(now)), nil
		}
		lease, err := l.lease(ctx, key, cost-p.tokens, limits)
		switch {
		case errors.Is(err, resilience.ErrOpen):
			// The shared bucket has been failing persistently. Degrade to
			// the per-instance guarantee rather than refuse every tenant.
			return l.local.AllowN(key, cost, override), nil
		case err != nil:
			// Falling back here instead would meter this request against a
			// per-instance bucket that has never been used -- a full,
			// unearned burst for every passing timeout, in a quantity no
			// one is watching, because the breaker never trips on blips.
			return Decision{}, err
		}
		p.tokens += lease.Granted
		p.shared = lease.Remaining
	}

	if p.tokens < cost {
		// Whatever was granted stays in the pool for the next request. The
		// shortfall accrues in the shared bucket at the tenant's rate; ask
		// again only once it could have.
		wait := secondsToDuration(float64(cost-p.tokens) / limits.Rate)
		p.emptyUntil = now.Add(wait)
		return l.deny(limits, p, cost, wait), nil
	}
	p.emptyUntil = time.Time{}

	p.tokens -= cost
	return Decision{
		Allowed:    true,
		Limit:      limits,
		Remaining:  p.tokens + int(p.shared),
		ResetAfter: secondsToDuration((float64(limits.Burst) - p.shared) / limits.Rate),
	}, nil
}

func (l *Leased) deny(limits Limits, p *pool, cost int, wait time.Duration) Decision {
	own := secondsToDuration(float64(cost-p.tokens) / limits.Rate)
	return Decision{
		Allowed:    false,
		Limit:      limits,
		Remaining:  p.tokens,
		RetryAfter: max(wait, own),
		ResetAfter: secondsToDuration((float64(limits.Burst) - p.shared) / limits.Rate),
	}
}

// lease asks the shared bucket for at least shortfall tokens.
func (l *Leased) lease(ctx context.Context, key string, shortfall int, limits Limits) (Lease, error) {
	want := max(shortfall, int(math.Ceil(limits.Rate*l.opts.Horizon.Seconds())))
	want = min(want, limits.Burst)

	token, err := l.breaker.Allow()
	if err != nil {
		l.observe("fallback", err)
		return Lease{}, err
	}
	// Detached from the request: a lease is shared state, and a client that
	// disconnects mid-request must not cancel it -- or enough disconnects
	// would trip the breaker and switch every tenant to per-instance limits.
	leaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), l.opts.Timeout)
	defer cancel()
	lease, err := l.lessor.Lease(leaseCtx, key, want, limits)
	if err != nil {
		token.Failure()
		l.observe("error", err)
		return Lease{}, err
	}
	token.Success()
	l.observe("leased", nil)
	return lease, nil
}

func (l *Leased) observe(outcome string, err error) {
	if l.opts.OnLease != nil {
		l.opts.OnLease(outcome, err)
	}
}

// pool returns key's pool, reclaiming pools idle long enough that their few
// unspent tokens are not worth the memory.
func (l *Leased) pool(key string) *pool {
	now := l.opts.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.lastSweep) >= time.Minute {
		l.lastSweep = now
		cutoff := now.Add(-poolIdleTTL).UnixNano()
		for k, p := range l.pools {
			if p.lastSeen.Load() < cutoff {
				delete(l.pools, k)
			}
		}
	}
	p, ok := l.pools[key]
	if !ok {
		p = &pool{}
		l.pools[key] = p
	}
	p.lastSeen.Store(now.UnixNano())
	return p
}

// Pools reports how many tenant pools are held, for tests and a gauge.
func (l *Leased) Pools() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.pools)
}
