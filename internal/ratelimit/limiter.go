// Package ratelimit implements per-tenant request throttling.
//
// The algorithm is a token bucket: a bucket refills at a steady rate up to a
// burst ceiling, and each request spends one token. It is chosen over a fixed
// window because a fixed window admits a double-rate spike across a boundary --
// a full window's allowance at 11:59:59 and another at 12:00:00 -- which is
// exactly the traffic shape that overwhelms a downstream service.
package ratelimit

import (
	"hash/maphash"
	"math"
	"sync"
	"time"
)

// Limits describes an allowance.
type Limits struct {
	// Rate is the sustained number of requests permitted per second.
	Rate float64
	// Burst is how many requests may arrive at once before throttling begins.
	Burst int
}

// valid reports whether the limits are usable.
func (l Limits) valid() bool { return l.Rate > 0 && l.Burst > 0 }

// Decision is the outcome of one admission check, carrying everything needed
// to populate the RateLimit response headers.
type Decision struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// Limit is the allowance that was applied.
	Limit Limits
	// Remaining is the whole number of requests still available right now.
	Remaining int
	// RetryAfter is how long until one token is available. Zero when allowed.
	RetryAfter time.Duration
	// ResetAfter is how long until the bucket is completely refilled.
	ResetAfter time.Duration
}

// shardCount is a power of two so the shard index is a mask rather than a
// modulo. Sixteen shards keep lock contention negligible at the tenant counts
// this service is built for, without wasting memory on mostly-empty maps.
const shardCount = 16

// Limiter tracks a token bucket per key.
//
// Buckets are spread across independently locked shards: a single mutex would
// serialise every admission check in the process, turning the rate limiter
// into the bottleneck it exists to prevent.
type Limiter struct {
	defaults Limits
	idleTTL  time.Duration
	sweepGap time.Duration
	now      func() time.Time

	seed   maphash.Seed
	shards [shardCount]*shard
}

type shard struct {
	mu        sync.Mutex
	buckets   map[string]*bucket
	lastSweep time.Time
}

type bucket struct {
	tokens   float64
	lastSeen time.Time
	limits   Limits
}

// Option customises a Limiter.
type Option func(*Limiter)

// WithClock replaces the time source, which makes refill behaviour testable
// without sleeping.
func WithClock(now func() time.Time) Option {
	return func(l *Limiter) { l.now = now }
}

// WithIdleTTL sets how long an untouched bucket is retained. A bucket that has
// been idle longer than its own refill time carries no useful state, so
// dropping it costs nothing and bounds memory.
func WithIdleTTL(d time.Duration) Option {
	return func(l *Limiter) { l.idleTTL = d }
}

// New returns a Limiter applying defaults to any key without an override.
func New(defaults Limits, opts ...Option) *Limiter {
	l := &Limiter{
		defaults: defaults,
		idleTTL:  10 * time.Minute,
		sweepGap: time.Minute,
		now:      time.Now,
		seed:     maphash.MakeSeed(),
	}
	for _, opt := range opts {
		opt(l)
	}
	for i := range l.shards {
		l.shards[i] = &shard{buckets: make(map[string]*bucket)}
	}
	return l
}

// Allow consumes one token for key and reports whether the request may
// proceed.
//
// override applies a per-key allowance; pass the zero value to use the
// service defaults. An override that changes between calls is adopted
// immediately, so raising a tenant's quota takes effect without a restart.
func (l *Limiter) Allow(key string, override Limits) Decision {
	return l.AllowN(key, 1, override)
}

// AllowN consumes cost tokens for key.
//
// Telemetry ingestion is metered in points rather than requests: a thousand
// points in one batch and a thousand single-point requests place the same load
// on everything downstream, so charging per request would let a caller evade
// its quota simply by batching.
//
// A cost larger than the burst ceiling can never be satisfied, so it is
// rejected outright rather than parked forever behind a bucket that will never
// hold enough tokens.
func (l *Limiter) AllowN(key string, cost int, override Limits) Decision {
	if cost < 1 {
		cost = 1
	}

	limits := l.defaults
	if override.valid() {
		limits = override
	}
	if !limits.valid() {
		// A limiter configured with no usable allowance must not silently
		// deny every request; treat it as unlimited and let configuration
		// validation be the thing that complains.
		return Decision{Allowed: true, Limit: limits, Remaining: math.MaxInt32}
	}

	now := l.now()
	sh := l.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	sh.sweepLocked(now, l.idleTTL, l.sweepGap)

	b, ok := sh.buckets[key]
	if !ok {
		// A first-time key starts full: a tenant's opening request should not
		// be throttled just because the process restarted a moment ago.
		b = &bucket{tokens: float64(limits.Burst), limits: limits}
		sh.buckets[key] = b
	}

	b.refill(now, limits)
	b.lastSeen = now

	if float64(cost) > b.tokens {
		deficit := float64(cost) - b.tokens

		retryAfter := secondsToDuration(deficit / limits.Rate)
		if cost > limits.Burst {
			// Waiting will never help: the bucket tops out below what this
			// request costs. Report no retry delay so the caller splits the
			// batch instead of sleeping and failing again.
			retryAfter = 0
		}

		return Decision{
			Allowed:    false,
			Limit:      limits,
			Remaining:  int(b.tokens),
			RetryAfter: retryAfter,
			ResetAfter: secondsToDuration((float64(limits.Burst) - b.tokens) / limits.Rate),
		}
	}

	b.tokens -= float64(cost)
	return Decision{
		Allowed:   true,
		Limit:     limits,
		Remaining: int(b.tokens),
		ResetAfter: secondsToDuration(
			(float64(limits.Burst) - b.tokens) / limits.Rate),
	}
}

// refill adds the tokens accrued since the last observation.
func (b *bucket) refill(now time.Time, limits Limits) {
	// Adopt a changed allowance, and clamp to the new ceiling so that lowering
	// a tenant's burst takes effect immediately rather than after their
	// existing surplus drains.
	if b.limits != limits {
		b.limits = limits
		b.tokens = math.Min(b.tokens, float64(limits.Burst))
	}

	if b.lastSeen.IsZero() {
		b.lastSeen = now
		return
	}

	elapsed := now.Sub(b.lastSeen).Seconds()
	// A non-monotonic clock (an NTP step, a suspended VM) must not hand out
	// free tokens or, worse, remove them.
	if elapsed <= 0 {
		return
	}

	b.tokens = math.Min(b.tokens+elapsed*limits.Rate, float64(limits.Burst))
}

// sweepLocked drops buckets nobody has touched recently.
//
// Reclamation happens inline rather than on a background goroutine: there is
// no ticker to stop, no goroutine to leak, and the work only happens on shards
// that are actually being used. The caller must hold sh.mu.
func (s *shard) sweepLocked(now time.Time, idleTTL, gap time.Duration) {
	if now.Sub(s.lastSweep) < gap {
		return
	}
	s.lastSweep = now

	cutoff := now.Add(-idleTTL)
	for key, b := range s.buckets {
		if b.lastSeen.Before(cutoff) {
			delete(s.buckets, key)
		}
	}
}

func (l *Limiter) shardFor(key string) *shard {
	h := maphash.String(l.seed, key)
	return l.shards[h&(shardCount-1)]
}

// Len reports how many buckets are currently tracked, across all shards. It
// exists for tests and for an operational gauge.
func (l *Limiter) Len() int {
	total := 0
	for _, sh := range l.shards {
		sh.mu.Lock()
		total += len(sh.buckets)
		sh.mu.Unlock()
	}
	return total
}

// secondsToDuration converts to a Duration, rounding up so a caller told to
// wait never retries a moment too early and gets denied again.
func secondsToDuration(seconds float64) time.Duration {
	if seconds <= 0 {
		return 0
	}
	d := time.Duration(seconds * float64(time.Second))
	if d < time.Millisecond {
		return time.Millisecond
	}
	return d
}
