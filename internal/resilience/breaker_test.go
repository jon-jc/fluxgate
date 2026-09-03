package resilience

import (
	"errors"
	"sync"
	"testing"
	"time"
)

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

// fail records n consecutive failures.
func fail(t *testing.T, b *Breaker, n int) {
	t.Helper()

	for i := range n {
		token, err := b.Allow()
		if err != nil {
			t.Fatalf("failure %d: Allow returned %v", i+1, err)
		}
		token.Failure()
	}
}

func TestClosedBreakerPassesCallsThrough(t *testing.T) {
	b := New(Options{FailureThreshold: 3, Now: newClock().Now})

	for range 100 {
		token, err := b.Allow()
		if err != nil {
			t.Fatalf("Allow on a healthy breaker: %v", err)
		}
		token.Success()
	}
	if got := b.State(); got != StateClosed {
		t.Errorf("state = %s, want closed", got)
	}
}

func TestTripsAfterConsecutiveFailures(t *testing.T) {
	clock := newClock()
	b := New(Options{FailureThreshold: 3, Cooldown: time.Minute, Now: clock.Now})

	fail(t, b, 3)

	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %s, want open after 3 failures", got)
	}
	// The whole point is failing fast: a caller must not wait out a timeout
	// against a dependency already known to be down.
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Errorf("Allow on an open breaker = %v, want ErrOpen", err)
	}
}

// TestSuccessResetsTheFailureRun keeps a healthy dependency from tripping on
// scattered failures: one bad call in a thousand is a working dependency.
func TestSuccessResetsTheFailureRun(t *testing.T) {
	clock := newClock()
	b := New(Options{FailureThreshold: 3, Cooldown: time.Minute, Now: clock.Now})

	for range 10 {
		fail(t, b, 2)

		token, err := b.Allow()
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		token.Success()
	}

	if got := b.State(); got != StateClosed {
		t.Errorf("state = %s, want closed; scattered failures must not trip it", got)
	}
}

func TestProbesAfterCooldown(t *testing.T) {
	clock := newClock()
	b := New(Options{FailureThreshold: 2, Cooldown: 30 * time.Second, Now: clock.Now})

	fail(t, b, 2)

	// Still inside the cooldown.
	clock.Advance(29 * time.Second)
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow before the cooldown elapsed = %v, want ErrOpen", err)
	}

	clock.Advance(2 * time.Second)
	if _, err := b.Allow(); err != nil {
		t.Fatalf("Allow after the cooldown = %v, want a probe to be admitted", err)
	}
}

// TestHalfOpenAdmitsOnlyLimitedProbes stops a recovering dependency from being
// knocked straight back over by the full backlog.
func TestHalfOpenAdmitsOnlyLimitedProbes(t *testing.T) {
	clock := newClock()
	b := New(Options{
		FailureThreshold: 2,
		Cooldown:         time.Second,
		HalfOpenProbes:   2,
		Now:              clock.Now,
	})

	fail(t, b, 2)
	clock.Advance(2 * time.Second)

	for i := range 2 {
		if _, err := b.Allow(); err != nil {
			t.Fatalf("probe %d rejected: %v", i+1, err)
		}
	}
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Errorf("a third probe was admitted; the limit is 2")
	}
}

func TestHalfOpenClosesOnSuccess(t *testing.T) {
	clock := newClock()
	b := New(Options{
		FailureThreshold: 2,
		Cooldown:         time.Second,
		HalfOpenProbes:   2,
		SuccessThreshold: 2,
		Now:              clock.Now,
	})

	fail(t, b, 2)
	clock.Advance(2 * time.Second)

	for range 2 {
		token, err := b.Allow()
		if err != nil {
			t.Fatalf("probe rejected: %v", err)
		}
		token.Success()
	}

	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %s, want closed after successful probes", got)
	}
	// Full traffic resumes.
	for range 10 {
		token, err := b.Allow()
		if err != nil {
			t.Fatalf("Allow after recovery: %v", err)
		}
		token.Success()
	}
}

func TestHalfOpenReopensOnFailure(t *testing.T) {
	clock := newClock()
	b := New(Options{FailureThreshold: 2, Cooldown: 10 * time.Second, Now: clock.Now})

	fail(t, b, 2)
	clock.Advance(11 * time.Second)

	token, err := b.Allow()
	if err != nil {
		t.Fatalf("probe rejected: %v", err)
	}
	token.Failure()

	// The dependency is not back, so the breaker must serve a fresh cooldown
	// rather than continuing to probe.
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Errorf("Allow = %v, want ErrOpen after a failed probe", err)
	}
	clock.Advance(5 * time.Second)
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Errorf("the cooldown did not restart after the failed probe")
	}
}

// TestStaleResultsAreIgnored covers a race that would otherwise close the
// breaker on false evidence: a call begun before the breaker tripped can only
// report on the state it started in.
func TestStaleResultsAreIgnored(t *testing.T) {
	clock := newClock()
	b := New(Options{
		FailureThreshold: 2,
		Cooldown:         time.Minute,
		HalfOpenProbes:   1,
		SuccessThreshold: 1,
		Now:              clock.Now,
	})

	// A slow call starts while the breaker is still closed.
	stale, err := b.Allow()
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}

	fail(t, b, 2)
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %s, want open", got)
	}

	// It finally succeeds, long after the breaker tripped. That success says
	// nothing about the dependency's state now.
	stale.Success()

	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Error("a stale success closed the breaker")
	}
}

func TestTokenReportsOnlyOnce(t *testing.T) {
	clock := newClock()
	b := New(Options{FailureThreshold: 2, Cooldown: time.Minute, Now: clock.Now})

	token, err := b.Allow()
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}

	// A defer-plus-explicit-call pattern can easily report twice; the second
	// report must not count as another failure.
	token.Failure()
	token.Failure()
	token.Success()

	if got := b.State(); got != StateClosed {
		t.Errorf("state = %s, want closed; one failure of two should not trip it", got)
	}
}

func TestDoReportsAutomatically(t *testing.T) {
	clock := newClock()
	b := New(Options{FailureThreshold: 2, Cooldown: time.Minute, Now: clock.Now})

	boom := errors.New("boom")
	for range 2 {
		if err := b.Do(func() error { return boom }); !errors.Is(err, boom) {
			t.Fatalf("Do = %v, want the caller's error unchanged", err)
		}
	}

	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %s, want open", got)
	}
	if err := b.Do(func() error {
		t.Error("Do ran the function while the breaker was open")
		return nil
	}); !errors.Is(err, ErrOpen) {
		t.Errorf("Do = %v, want ErrOpen", err)
	}
}

// TestStateReportsReadinessToProbe keeps an operational gauge honest: once the
// cooldown has elapsed the breaker is ready to probe, even before a caller has
// arrived to move it.
func TestStateReportsReadinessToProbe(t *testing.T) {
	clock := newClock()
	b := New(Options{FailureThreshold: 1, Cooldown: 10 * time.Second, Now: clock.Now})

	fail(t, b, 1)
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %s, want open", got)
	}

	clock.Advance(11 * time.Second)
	if got := b.State(); got != StateHalfOpen {
		t.Errorf("state = %s, want half-open once the cooldown has elapsed", got)
	}
}

func TestDefaultsAreUsable(t *testing.T) {
	b := New(Options{})

	// A zero-value Options must produce a breaker that trips eventually rather
	// than one that never trips or trips immediately.
	if _, err := b.Allow(); err != nil {
		t.Fatalf("a fresh breaker rejected the first call: %v", err)
	}
	for range 5 {
		token, _ := b.Allow()
		if token != nil {
			token.Failure()
		}
	}
	if got := b.State(); got != StateOpen {
		t.Errorf("state = %s, want open after the default threshold", got)
	}
}

func TestSuccessThresholdCannotExceedProbes(t *testing.T) {
	// Requiring more successes than probes admitted would leave the breaker
	// open forever.
	clock := newClock()
	b := New(Options{
		FailureThreshold: 1,
		Cooldown:         time.Second,
		HalfOpenProbes:   1,
		SuccessThreshold: 5,
		Now:              clock.Now,
	})

	fail(t, b, 1)
	clock.Advance(2 * time.Second)

	token, err := b.Allow()
	if err != nil {
		t.Fatalf("probe rejected: %v", err)
	}
	token.Success()

	if got := b.State(); got != StateClosed {
		t.Errorf("state = %s, want closed; the breaker must be able to recover", got)
	}
}

func TestConcurrentUseIsSafe(t *testing.T) {
	b := New(Options{FailureThreshold: 1000, Cooldown: time.Millisecond})

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				token, err := b.Allow()
				if err != nil {
					continue
				}
				if i%3 == 0 {
					token.Failure()
					continue
				}
				token.Success()
			}
		}()
	}
	wg.Wait()

	_ = b.State() // must not race
}

func TestStateString(t *testing.T) {
	for state, want := range map[State]string{
		StateClosed:   "closed",
		StateOpen:     "open",
		StateHalfOpen: "half-open",
	} {
		if got := state.String(); got != want {
			t.Errorf("State(%d).String() = %q, want %q", state, got, want)
		}
	}
}
