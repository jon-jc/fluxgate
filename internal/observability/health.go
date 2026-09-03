package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Checker reports whether one dependency is usable right now.
//
// Implementations must respect the context deadline and must be cheap: probes
// run on every scrape, and a check that talks to a shared database on each
// call turns a readiness probe into a load generator.
type Checker interface {
	// Name identifies the dependency in the probe response.
	Name() string
	// Check returns nil when the dependency is usable.
	Check(ctx context.Context) error
}

// CheckerFunc adapts a plain function to the Checker interface.
type CheckerFunc struct {
	CheckerName string
	Fn          func(ctx context.Context) error
}

// Name implements Checker.
func (c CheckerFunc) Name() string { return c.CheckerName }

// Check implements Checker.
func (c CheckerFunc) Check(ctx context.Context) error { return c.Fn(ctx) }

// Health tracks process liveness and dependency readiness.
//
// Liveness and readiness answer different questions and must not be conflated:
// liveness asks "is this process wedged and in need of a restart", readiness
// asks "should this instance receive traffic". A database blip should drain an
// instance, not kill it -- restarting the process does not fix the database,
// and a restart loop across the fleet turns a dependency blip into an outage.
type Health struct {
	// ready is flipped to false during shutdown so load balancers stop routing
	// to this instance before the listener actually closes.
	ready atomic.Bool

	// probeTimeout bounds the total time spent running dependency checks.
	probeTimeout time.Duration

	mu       sync.RWMutex
	checkers []Checker
}

// NewHealth returns a Health that starts out not-ready. Call SetReady once the
// process has finished booting.
func NewHealth(probeTimeout time.Duration) *Health {
	if probeTimeout <= 0 {
		probeTimeout = 2 * time.Second
	}
	return &Health{probeTimeout: probeTimeout}
}

// Register adds a dependency check. It is safe to call during startup as
// subsystems come up.
func (h *Health) Register(c Checker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checkers = append(h.checkers, c)
}

// SetReady marks the process as willing (or unwilling) to receive traffic.
func (h *Health) SetReady(ready bool) { h.ready.Store(ready) }

// Ready reports the current readiness flag, ignoring dependency checks.
func (h *Health) Ready() bool { return h.ready.Load() }

// status is the probe response body.
type status struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// LivenessHandler answers whether the process is running at all. It
// deliberately performs no dependency checks.
func (h *Health) LivenessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeStatus(w, http.StatusOK, status{Status: "ok"})
	})
}

// ReadinessHandler answers whether this instance should receive traffic. It
// reports 503 while draining, and 503 with per-dependency detail when a
// registered check fails.
func (h *Health) ReadinessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !h.ready.Load() {
			writeStatus(w, http.StatusServiceUnavailable, status{Status: "draining"})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), h.probeTimeout)
		defer cancel()

		results := h.runChecks(ctx)

		code := http.StatusOK
		body := status{Status: "ok", Checks: make(map[string]string, len(results))}
		for name, err := range results {
			if err != nil {
				code = http.StatusServiceUnavailable
				body.Status = "degraded"
				body.Checks[name] = err.Error()
				continue
			}
			body.Checks[name] = "ok"
		}
		writeStatus(w, code, body)
	})
}

// runChecks executes every registered check concurrently so that one slow
// dependency does not serialise behind the others and blow the probe budget.
func (h *Health) runChecks(ctx context.Context) map[string]error {
	h.mu.RLock()
	checkers := make([]Checker, len(h.checkers))
	copy(checkers, h.checkers)
	h.mu.RUnlock()

	results := make(map[string]error, len(checkers))
	if len(checkers) == 0 {
		return results
	}

	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, c := range checkers {
		wg.Add(1)
		go func(c Checker) {
			defer wg.Done()

			err := safeCheck(ctx, c)

			mu.Lock()
			results[c.Name()] = err
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	return results
}

// safeCheck runs one checker, converting a panic into a failed check.
//
// Checks run on their own goroutines, which puts them out of reach of the HTTP
// recovery middleware: an unrecovered panic in a dependency probe would take
// down the whole process. Reporting the instance as unhealthy is the strictly
// better outcome -- it drains this instance instead of crash-looping the fleet.
func safeCheck(ctx context.Context, c Checker) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("check panicked: %v", rec)
		}
	}()
	return c.Check(ctx)
}

// CheckNames returns the registered dependency names, sorted. Used by startup
// logging so an operator can see what readiness actually covers.
func (h *Health) CheckNames() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	names := make([]string, 0, len(h.checkers))
	for _, c := range h.checkers {
		names = append(names, c.Name())
	}
	sort.Strings(names)
	return names
}

func writeStatus(w http.ResponseWriter, code int, body status) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// Probe responses describe this instant and this instance; a cached one is
	// worse than useless.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
