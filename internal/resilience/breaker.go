// Package resilience holds the patterns that keep a failing dependency from
// becoming a failing service.
package resilience

import (
	"errors"
	"sync"
	"time"
)

// ErrOpen is returned when the breaker is rejecting calls.
var ErrOpen = errors.New("circuit breaker is open")

// State is a circuit breaker's current disposition.
type State int

// Breaker states.
const (
	// StateClosed passes calls through and watches for failures.
	StateClosed State = iota
	// StateOpen rejects calls immediately.
	StateOpen
	// StateHalfOpen admits a limited number of probes to test recovery.
	StateHalfOpen
)

// String implements fmt.Stringer.
func (s State) String() string {
	switch s {
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// Breaker trips after repeated failures and fails fast until a dependency
// recovers.
//
// The problem it solves is not the failure itself but the queue behind it.
// When Pub/Sub is unreachable, every publish waits out its full timeout while
// holding an HTTP connection, a goroutine and a request's worth of memory.
// A few seconds of that and the process is out of capacity for the healthy
// traffic it could still be serving. Failing fast turns a dependency outage
// into a fast, honest 503 instead of a slow, total collapse.
type Breaker struct {
	// failureThreshold is how many consecutive failures trip the breaker.
	failureThreshold int
	// cooldown is how long the breaker stays open before probing.
	cooldown time.Duration
	// halfOpenProbes is how many calls are admitted while probing.
	halfOpenProbes int
	// successThreshold is how many probes must succeed to close the breaker.
	successThreshold int

	now func() time.Time

	mu           sync.Mutex
	state        State
	failures     int
	successes    int
	probesIssued int
	openedAt     time.Time
	// generation invalidates results reported by calls that started in an
	// earlier state. Without it, a slow call begun before the breaker tripped
	// could report success afterwards and close it prematurely.
	generation uint64
}

// Options configures a Breaker. Zero fields take sensible defaults.
type Options struct {
	// FailureThreshold is how many consecutive failures trip the breaker.
	FailureThreshold int
	// Cooldown is how long to reject calls before probing for recovery.
	Cooldown time.Duration
	// HalfOpenProbes is how many calls are admitted while probing.
	HalfOpenProbes int
	// SuccessThreshold is how many probes must succeed to close the breaker.
	SuccessThreshold int
	// Now replaces the time source, for deterministic tests.
	Now func() time.Time
}

// New returns a closed Breaker.
func New(opts Options) *Breaker {
	if opts.FailureThreshold <= 0 {
		opts.FailureThreshold = 5
	}
	if opts.Cooldown <= 0 {
		opts.Cooldown = 10 * time.Second
	}
	if opts.HalfOpenProbes <= 0 {
		opts.HalfOpenProbes = 1
	}
	if opts.SuccessThreshold <= 0 {
		opts.SuccessThreshold = opts.HalfOpenProbes
	}
	if opts.SuccessThreshold > opts.HalfOpenProbes {
		// Requiring more successes than probes admitted would leave the
		// breaker open forever.
		opts.SuccessThreshold = opts.HalfOpenProbes
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	return &Breaker{
		failureThreshold: opts.FailureThreshold,
		cooldown:         opts.Cooldown,
		halfOpenProbes:   opts.HalfOpenProbes,
		successThreshold: opts.SuccessThreshold,
		now:              opts.Now,
		state:            StateClosed,
	}
}

// Token is permission to make one call. Report the outcome exactly once.
type Token struct {
	breaker    *Breaker
	generation uint64
	reported   bool
}

// Success records that the call succeeded.
func (t *Token) Success() {
	if t == nil || t.reported {
		return
	}
	t.reported = true
	t.breaker.report(t.generation, true)
}

// Failure records that the call failed.
func (t *Token) Failure() {
	if t == nil || t.reported {
		return
	}
	t.reported = true
	t.breaker.report(t.generation, false)
}

// Allow asks permission to make a call.
//
// It returns ErrOpen while the breaker is rejecting. On success the caller
// must report the outcome through the returned Token, or the breaker will
// never learn that the dependency recovered.
func (b *Breaker) Allow() (*Token, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == StateOpen {
		if b.now().Sub(b.openedAt) < b.cooldown {
			return nil, ErrOpen
		}
		// The cooldown has elapsed: admit a limited number of probes rather
		// than the full load, which would simply knock over a dependency that
		// is only just coming back.
		b.toHalfOpenLocked()
	}

	if b.state == StateHalfOpen {
		if b.probesIssued >= b.halfOpenProbes {
			return nil, ErrOpen
		}
		b.probesIssued++
	}

	return &Token{breaker: b, generation: b.generation}, nil
}

func (b *Breaker) report(generation uint64, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// A call that began in an earlier state says nothing about the current
	// one; a slow success from before the breaker tripped must not close it.
	if generation != b.generation {
		return
	}

	if ok {
		b.onSuccessLocked()
		return
	}
	b.onFailureLocked()
}

func (b *Breaker) onSuccessLocked() {
	switch b.state {
	case StateClosed:
		// Only consecutive failures trip the breaker, so any success resets
		// the count. A dependency failing one call in a thousand is working.
		b.failures = 0
	case StateHalfOpen:
		b.successes++
		if b.successes >= b.successThreshold {
			b.toClosedLocked()
		}
	case StateOpen:
		// Cannot happen: no token is issued while open.
	}
}

func (b *Breaker) onFailureLocked() {
	switch b.state {
	case StateClosed:
		b.failures++
		if b.failures >= b.failureThreshold {
			b.toOpenLocked()
		}
	case StateHalfOpen:
		// The dependency is not back. Re-open for another full cooldown
		// rather than continuing to probe.
		b.toOpenLocked()
	case StateOpen:
		// Cannot happen: no token is issued while open.
	}
}

func (b *Breaker) toOpenLocked() {
	b.state = StateOpen
	b.openedAt = b.now()
	b.failures = 0
	b.successes = 0
	b.probesIssued = 0
	b.generation++
}

func (b *Breaker) toHalfOpenLocked() {
	b.state = StateHalfOpen
	b.successes = 0
	b.probesIssued = 0
	b.generation++
}

func (b *Breaker) toClosedLocked() {
	b.state = StateClosed
	b.failures = 0
	b.successes = 0
	b.probesIssued = 0
	b.generation++
}

// State reports the breaker's current state, for logging and metrics.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Report the state a caller would actually encounter: once the cooldown
	// has elapsed the breaker is ready to probe, even though nothing has
	// called Allow yet to move it.
	if b.state == StateOpen && b.now().Sub(b.openedAt) >= b.cooldown {
		return StateHalfOpen
	}
	return b.state
}

// Do runs fn under the breaker, reporting the outcome automatically.
func (b *Breaker) Do(fn func() error) error {
	token, err := b.Allow()
	if err != nil {
		return err
	}

	err = fn()
	if err != nil {
		token.Failure()
		return err
	}
	token.Success()
	return nil
}
