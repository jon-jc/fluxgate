package observability

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func decodeStatus(t *testing.T, rec *httptest.ResponseRecorder) status {
	t.Helper()

	var s status
	if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil {
		t.Fatalf("decode probe body: %v (%s)", err, rec.Body.String())
	}
	return s
}

func probe(t *testing.T, h http.Handler) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/probe", nil))
	return rec
}

// TestLivenessIgnoresDependencies is the whole point of separating the probes:
// a database outage must drain instances, not restart them. Restarting does not
// fix the database, and a fleet-wide restart loop turns a blip into an outage.
func TestLivenessIgnoresDependencies(t *testing.T) {
	h := NewHealth(time.Second)
	h.SetReady(true)
	h.Register(CheckerFunc{
		CheckerName: "postgres",
		Fn:          func(context.Context) error { return errors.New("connection refused") },
	})

	rec := probe(t, h.LivenessHandler())
	if rec.Code != http.StatusOK {
		t.Fatalf("liveness status = %d, want 200 even with a failing dependency", rec.Code)
	}

	rec = probe(t, h.ReadinessHandler())
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readiness status = %d, want 503", rec.Code)
	}
}

func TestReadinessStartsUnready(t *testing.T) {
	// A process must not receive traffic before it has finished booting.
	h := NewHealth(time.Second)

	rec := probe(t, h.ReadinessHandler())
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 before SetReady", rec.Code)
	}
	if got := decodeStatus(t, rec).Status; got != "draining" {
		t.Errorf("status = %q, want draining", got)
	}
}

func TestReadinessReportsPerDependencyDetail(t *testing.T) {
	h := NewHealth(time.Second)
	h.SetReady(true)
	h.Register(CheckerFunc{
		CheckerName: "pubsub",
		Fn:          func(context.Context) error { return nil },
	})
	h.Register(CheckerFunc{
		CheckerName: "postgres",
		Fn:          func(context.Context) error { return errors.New("too many connections") },
	})

	rec := probe(t, h.ReadinessHandler())
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}

	body := decodeStatus(t, rec)
	if body.Status != "degraded" {
		t.Errorf("status = %q, want degraded", body.Status)
	}
	if body.Checks["pubsub"] != "ok" {
		t.Errorf("pubsub check = %q, want ok", body.Checks["pubsub"])
	}
	// Naming the failing dependency turns a page into a diagnosis.
	if body.Checks["postgres"] != "too many connections" {
		t.Errorf("postgres check = %q, want the failure reason", body.Checks["postgres"])
	}
}

func TestReadinessPassesWhenAllChecksPass(t *testing.T) {
	h := NewHealth(time.Second)
	h.SetReady(true)
	h.Register(CheckerFunc{
		CheckerName: "pubsub",
		Fn:          func(context.Context) error { return nil },
	})

	rec := probe(t, h.ReadinessHandler())
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body)
	}
	if got := decodeStatus(t, rec).Status; got != "ok" {
		t.Errorf("status = %q, want ok", got)
	}
}

// TestReadinessRunsChecksConcurrently keeps the probe budget meaningful: five
// checks that each sleep 100ms must not serialise into half a second.
func TestReadinessRunsChecksConcurrently(t *testing.T) {
	h := NewHealth(2 * time.Second)
	h.SetReady(true)

	const (
		checks = 5
		delay  = 100 * time.Millisecond
	)
	for i := range checks {
		h.Register(CheckerFunc{
			CheckerName: string(rune('a' + i)),
			Fn: func(context.Context) error {
				time.Sleep(delay)
				return nil
			},
		})
	}

	start := time.Now()
	rec := probe(t, h.ReadinessHandler())
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if elapsed > checks*delay/2 {
		t.Errorf("probe took %v; checks appear to run serially", elapsed)
	}
}

func TestReadinessBoundsSlowChecksWithProbeTimeout(t *testing.T) {
	h := NewHealth(50 * time.Millisecond)
	h.SetReady(true)
	h.Register(CheckerFunc{
		CheckerName: "sluggish",
		Fn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})

	start := time.Now()
	rec := probe(t, h.ReadinessHandler())

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("probe took %v; the timeout did not bound it", elapsed)
	}
}

func TestSetReadyTogglesTraffic(t *testing.T) {
	h := NewHealth(time.Second)
	h.SetReady(true)

	if rec := probe(t, h.ReadinessHandler()); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 once ready", rec.Code)
	}
	if !h.Ready() {
		t.Error("Ready() = false after SetReady(true)")
	}

	h.SetReady(false) // what OnDrain does at the start of shutdown

	if rec := probe(t, h.ReadinessHandler()); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 while draining", rec.Code)
	}
}

func TestProbesAreNotCacheable(t *testing.T) {
	h := NewHealth(time.Second)
	h.SetReady(true)

	for name, handler := range map[string]http.Handler{
		"liveness":  h.LivenessHandler(),
		"readiness": h.ReadinessHandler(),
	} {
		t.Run(name, func(t *testing.T) {
			// A cached probe response describes a moment that has passed and an
			// instance that may already be gone.
			if got := probe(t, handler).Header().Get("Cache-Control"); got != "no-store" {
				t.Errorf("Cache-Control = %q, want no-store", got)
			}
		})
	}
}

func TestRegisterIsSafeWhileProbing(t *testing.T) {
	// Subsystems register their checks as they come up, which can overlap with
	// the first probe an orchestrator sends.
	h := NewHealth(time.Second)
	h.SetReady(true)

	var registered atomic.Int64
	done := make(chan struct{})

	go func() {
		defer close(done)
		for i := range 50 {
			h.Register(CheckerFunc{
				CheckerName: string(rune('a'+i%26)) + string(rune('0'+i/26)),
				Fn:          func(context.Context) error { return nil },
			})
			registered.Add(1)
		}
	}()

	for range 50 {
		probe(t, h.ReadinessHandler())
	}
	<-done

	if got := int64(len(h.CheckNames())); got != registered.Load() {
		t.Errorf("registered %d checks, CheckNames reports %d", registered.Load(), got)
	}
}

func TestCheckNamesAreSorted(t *testing.T) {
	h := NewHealth(time.Second)
	for _, name := range []string{"pubsub", "postgres", "cache"} {
		h.Register(CheckerFunc{
			CheckerName: name,
			Fn:          func(context.Context) error { return nil },
		})
	}

	got := h.CheckNames()
	want := []string{"cache", "postgres", "pubsub"}
	if len(got) != len(want) {
		t.Fatalf("CheckNames() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("CheckNames() = %v, want %v", got, want)
		}
	}
}

// TestPanickingCheckerDoesNotKillTheProcess covers a failure mode the HTTP
// recovery middleware cannot reach: checks run on their own goroutines, so an
// unrecovered panic there would terminate the whole process.
func TestPanickingCheckerDoesNotKillTheProcess(t *testing.T) {
	h := NewHealth(time.Second)
	h.SetReady(true)
	h.Register(CheckerFunc{
		CheckerName: "exploding",
		Fn:          func(context.Context) error { panic("nil map write") },
	})
	h.Register(CheckerFunc{
		CheckerName: "healthy",
		Fn:          func(context.Context) error { return nil },
	})

	rec := probe(t, h.ReadinessHandler())

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	body := decodeStatus(t, rec)
	if got := body.Checks["exploding"]; got == "ok" || got == "" {
		t.Errorf("exploding check = %q, want the panic reported as a failure", got)
	}
	// One bad checker must not take the others down with it.
	if body.Checks["healthy"] != "ok" {
		t.Errorf("healthy check = %q, want ok", body.Checks["healthy"])
	}
}
