package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/config"
	"github.com/jon-jc/fluxgate/internal/httpx"
	"github.com/jon-jc/fluxgate/internal/observability"
)

// newTestRouter builds a router over a ready-to-serve health registry.
func newTestRouter(t *testing.T) (http.Handler, *observability.Health) {
	t.Helper()

	health := observability.NewHealth(time.Second)
	health.SetReady(true)

	cfg := config.Config{
		Service:     "fluxgate-test",
		Environment: config.EnvLocal,
		HTTP: config.HTTPConfig{
			HandlerTimeout:  5 * time.Second,
			MaxRequestBytes: 1 << 20,
		},
	}

	return NewRouter(Deps{
		Config: cfg,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Health: health,
	}), health
}

func do(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, http.NoBody))
	return rec
}

func TestProbeEndpoints(t *testing.T) {
	router, health := newTestRouter(t)

	if rec := do(t, router, http.MethodGet, PathLiveness); rec.Code != http.StatusOK {
		t.Errorf("%s = %d, want 200", PathLiveness, rec.Code)
	}
	if rec := do(t, router, http.MethodGet, PathReadiness); rec.Code != http.StatusOK {
		t.Errorf("%s = %d, want 200", PathReadiness, rec.Code)
	}

	health.SetReady(false)

	if rec := do(t, router, http.MethodGet, PathReadiness); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("%s while draining = %d, want 503", PathReadiness, rec.Code)
	}
	// Liveness must stay green while draining, or the orchestrator will kill
	// the process in the middle of its own graceful shutdown.
	if rec := do(t, router, http.MethodGet, PathLiveness); rec.Code != http.StatusOK {
		t.Errorf("%s while draining = %d, want 200", PathLiveness, rec.Code)
	}
}

func TestVersionEndpoint(t *testing.T) {
	router, _ := newTestRouter(t)

	rec := do(t, router, http.MethodGet, "/v1/version")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, key := range []string{"version", "commit", "go_version", "platform"} {
		if body[key] == "" {
			t.Errorf("%s is empty in %v", key, body)
		}
	}
}

// TestUnknownRouteReturnsProblemJSON keeps the error contract total: every
// failure, including a 404 from the mux itself, speaks problem+json.
func TestUnknownRouteReturnsProblemJSON(t *testing.T) {
	router, _ := newTestRouter(t)

	rec := do(t, router, http.MethodGet, "/v1/nope")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
		t.Fatalf("Content-Type = %q, want application/problem+json", ct)
	}

	var p httpx.Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	if p.Code != httpx.CodeNotFound {
		t.Errorf("code = %q, want %q", p.Code, httpx.CodeNotFound)
	}
	if p.RequestID == "" {
		t.Error("problem is missing a request ID")
	}
}

// TestWrongMethodReturns405 distinguishes "wrong verb" from "wrong path". A 404
// here would send a caller off to check a path that was never the problem.
func TestWrongMethodReturns405(t *testing.T) {
	router, _ := newTestRouter(t)

	rec := do(t, router, http.MethodPost, "/v1/version")

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405 (%s)", rec.Code, rec.Body)
	}
	// RFC 9110 requires Allow on a 405, and it is what lets a client correct
	// itself without reading the docs.
	allow := rec.Header().Get("Allow")
	if !strings.Contains(allow, http.MethodGet) {
		t.Errorf("Allow = %q, want it to list GET", allow)
	}
	if strings.Contains(allow, http.MethodPost) {
		t.Errorf("Allow = %q, must not list the rejected method", allow)
	}

	var p httpx.Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	if p.Code != httpx.CodeMethodNotAllowed {
		t.Errorf("code = %q, want %q", p.Code, httpx.CodeMethodNotAllowed)
	}
}

// TestUnknownPathIsStill404 keeps the 405 logic from swallowing genuine 404s.
func TestUnknownPathIsStill404(t *testing.T) {
	router, _ := newTestRouter(t)

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			rec := do(t, router, method, "/v1/does-not-exist")
			if rec.Code != http.StatusNotFound {
				t.Errorf("status = %d, want 404", rec.Code)
			}
			if allow := rec.Header().Get("Allow"); allow != "" {
				t.Errorf("Allow = %q, want it absent on a 404", allow)
			}
		})
	}
}

func TestProbePathsRejectWrites(t *testing.T) {
	router, _ := newTestRouter(t)

	rec := do(t, router, http.MethodDelete, PathReadiness)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("DELETE %s = %d, want 405", PathReadiness, rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
		t.Errorf("Content-Type = %q, want application/problem+json", ct)
	}
}

func TestEveryResponseCarriesRequestIDAndSecurityHeaders(t *testing.T) {
	router, _ := newTestRouter(t)

	for _, path := range []string{PathLiveness, PathReadiness, "/v1/version", "/v1/nope"} {
		t.Run(path, func(t *testing.T) {
			rec := do(t, router, http.MethodGet, path)

			if rec.Header().Get(httpx.HeaderRequestID) == "" {
				t.Error("response is missing X-Request-Id")
			}
			if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Error("response is missing X-Content-Type-Options")
			}
		})
	}
}

// TestRequestTimeoutSurfacesAsGatewayTimeout wires the configured handler
// timeout through to the error envelope, so a slow dependency produces a clean
// 504 rather than a dangling connection.
func TestRequestTimeoutSurfacesAsGatewayTimeout(t *testing.T) {
	// A probe timeout far longer than the handler timeout forces the request
	// deadline to fire first.
	slow := observability.NewHealth(time.Minute)
	slow.SetReady(true)
	slow.Register(observability.CheckerFunc{
		CheckerName: "sluggish",
		Fn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})

	router := NewRouter(Deps{
		Config: config.Config{
			Service:     "fluxgate-test",
			Environment: config.EnvLocal,
			HTTP: config.HTTPConfig{
				HandlerTimeout:  50 * time.Millisecond,
				MaxRequestBytes: 1 << 20,
			},
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Health: slow,
	})

	start := time.Now()
	rec := do(t, router, http.MethodGet, PathReadiness)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("request took %v; the handler timeout did not bound it", elapsed)
	}
}
