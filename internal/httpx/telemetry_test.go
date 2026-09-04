package httpx

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jon-jc/fluxgate/internal/observability"
)

// TestRoutePatternResolvesThroughTheMux guards a silent instrumentation bug:
// r.Pattern is only populated once the mux has routed, and the mux sets it on
// an internal clone -- so a wrapper outside the mux always sees it empty. Left
// unresolved, every span would be named "unmatched" and every metric would
// carry the same useless label.
func TestRoutePatternResolvesThroughTheMux(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/query", func(http.ResponseWriter, *http.Request) {})
	mux.HandleFunc("POST /v1/ingest", func(http.ResponseWriter, *http.Request) {})

	tests := map[string]struct {
		method string
		path   string
		want   string
	}{
		"registered get":  {http.MethodGet, "/v1/query", "GET /v1/query"},
		"registered post": {http.MethodPost, "/v1/ingest", "POST /v1/ingest"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, http.NoBody)

			// The precondition the whole helper exists for.
			if r.Pattern != "" {
				t.Fatalf("r.Pattern = %q before routing; the test assumes it is empty", r.Pattern)
			}
			if got := RoutePattern(mux, r); got != tc.want {
				t.Errorf("RoutePattern = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestUnmatchedRoutesShareOneLabel stops a scan for URLs that do not exist from
// minting a metric series per probe.
func TestUnmatchedRoutesShareOneLabel(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/query", func(http.ResponseWriter, *http.Request) {})

	for _, path := range []string{"/nope", "/wp-admin", "/.env", "/v1/absent"} {
		r := httptest.NewRequest(http.MethodGet, path, http.NoBody)
		if got := RoutePattern(mux, r); got != UnmatchedRoute {
			t.Errorf("RoutePattern(%q) = %q, want %q", path, got, UnmatchedRoute)
		}
	}
}

func TestRoutePatternWithoutAResolver(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/anything", http.NoBody)

	// A nil resolver must degrade to the bounded label, not panic.
	if got := RoutePattern(nil, r); got != UnmatchedRoute {
		t.Errorf("RoutePattern = %q, want %q", got, UnmatchedRoute)
	}
}

// TestRoutePatternPrefersAnAlreadyResolvedPattern keeps the helper correct when
// it runs somewhere the mux has already routed.
func TestRoutePatternPrefersAnAlreadyResolvedPattern(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/v1/query", http.NoBody)
	r.Pattern = "GET /v1/query"

	if got := RoutePattern(nil, r); got != "GET /v1/query" {
		t.Errorf("RoutePattern = %q, want the pattern already on the request", got)
	}
}

func TestMetricsMiddlewareIsANoOpWithoutInstruments(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /x", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	// A service running uninstrumented must still serve requests.
	h := Metrics(nil, mux, TelemetryOptions{})(mux)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", http.NoBody))

	if rec.Code != http.StatusTeapot {
		t.Errorf("status = %d, want 418", rec.Code)
	}
}

func TestTraceMiddlewareServesTheRequest(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /x", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	// With no provider installed the tracer is a no-op, and the request must
	// pass through untouched.
	h := Trace(mux, TelemetryOptions{})(mux)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", http.NoBody))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "ok") {
		t.Errorf("body = %q", rec.Body.String())
	}
}

// TestSpanNameDoesNotRepeatTheMethod: Go 1.22 patterns already begin with the
// verb, so naively prefixing produces "POST POST /v1/ingest".
func TestSpanNameDoesNotRepeatTheMethod(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/ingest", func(http.ResponseWriter, *http.Request) {})

	route := RoutePattern(mux, httptest.NewRequest(http.MethodPost, "/v1/ingest", http.NoBody))
	if strings.Count(route, http.MethodPost) != 1 {
		t.Errorf("route = %q, want the method to appear exactly once", route)
	}
}

// TestSkippedRoutesProduceNoTelemetry keeps machine traffic out of the signal.
//
// An orchestrator probes readiness every few seconds and Prometheus scrapes on
// its own interval. Tracing those buries the requests somebody actually cares
// about, and metering the scrape endpoint means reading the metrics changes
// them.
func TestSkippedRoutesProduceNoTelemetry(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(http.ResponseWriter, *http.Request) {})
	mux.HandleFunc("GET /metrics", func(http.ResponseWriter, *http.Request) {})
	mux.HandleFunc("GET /v1/query", func(http.ResponseWriter, *http.Request) {})

	m := observability.NewMetrics("fluxgate-test")
	opts := TelemetryOptions{SkipRoutes: []string{"GET /readyz", "GET /metrics"}}

	h := Chain(Trace(mux, opts), Metrics(m, mux, opts))(mux)

	for _, path := range []string{"/readyz", "/metrics", "/readyz", "/v1/query"} {
		h.ServeHTTP(httptest.NewRecorder(),
			httptest.NewRequest(http.MethodGet, path, http.NoBody))
	}

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	body := rec.Body.String()

	for _, quiet := range []string{`route="GET /readyz"`, `route="GET /metrics"`} {
		if strings.Contains(body, quiet) {
			t.Errorf("%s was metered despite being skipped", quiet)
		}
	}
	// The route that is not skipped still has to be measured, or the skip list
	// would be silently swallowing everything.
	if !strings.Contains(body, `route="GET /v1/query"`) {
		t.Error("a normal route was not metered")
	}
}
