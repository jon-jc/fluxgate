package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/config"
)

func testConfig() config.Config {
	return config.Config{Service: "fluxgate-test", Environment: config.EnvLocal}
}

// scrape renders the registry the way Prometheus would read it.
func scrape(t *testing.T, m *Metrics) string {
	t.Helper()

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))

	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

func TestMetricsExposeRuntimeCollectors(t *testing.T) {
	body := scrape(t, NewMetrics("fluxgate-test"))

	// These answer the questions that come first in an incident, before
	// anyone looks at an application metric.
	for _, want := range []string{"go_goroutines", "go_memstats_"} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape is missing %s", want)
		}
	}
}

func TestObserveRequest(t *testing.T) {
	m := NewMetrics("fluxgate-test")

	m.ObserveRequest("GET /v1/query", http.MethodGet, 200, 12*time.Millisecond)
	m.ObserveRequest("GET /v1/query", http.MethodGet, 500, time.Second)

	body := scrape(t, m)

	for _, want := range []string{
		`fluxgate_http_requests_total`,
		`route="GET /v1/query"`,
		`status="2xx"`,
		`status="5xx"`,
		`fluxgate_http_request_duration_seconds_bucket`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape is missing %q", want)
		}
	}
}

// TestStatusIsBucketedByClass keeps the series count bounded: the exact code
// is already in the access log, where it can be read in context.
func TestStatusIsBucketedByClass(t *testing.T) {
	for status, want := range map[int]string{
		200: "2xx", 201: "2xx", 301: "3xx",
		404: "4xx", 422: "4xx", 500: "5xx", 503: "5xx",
	} {
		if got := statusClass(status); got != want {
			t.Errorf("statusClass(%d) = %q, want %q", status, got, want)
		}
	}
	// An unexpected code is reported verbatim rather than silently bucketed
	// somewhere misleading.
	if got := statusClass(101); got != "101" {
		t.Errorf("statusClass(101) = %q, want 101", got)
	}
}

func TestTrackInFlight(t *testing.T) {
	m := NewMetrics("fluxgate-test")

	done := m.TrackInFlight()
	if !strings.Contains(scrape(t, m), `fluxgate_http_requests_in_flight{service="fluxgate-test"} 1`) {
		t.Error("the in-flight gauge did not rise")
	}

	done()
	if !strings.Contains(scrape(t, m), `fluxgate_http_requests_in_flight{service="fluxgate-test"} 0`) {
		t.Error("the in-flight gauge did not fall")
	}
}

func TestObserveIngest(t *testing.T) {
	m := NewMetrics("fluxgate-test")
	m.ObserveIngest("acme", 8, 2, 10)

	body := scrape(t, m)
	for _, want := range []string{
		`fluxgate_ingest_points_accepted_total{service="fluxgate-test",tenant="acme"} 8`,
		`reason="validation"`,
		`fluxgate_ingest_batch_points_count`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape is missing %q", want)
		}
	}
}

func TestObservePipeline(t *testing.T) {
	m := NewMetrics("fluxgate-test")

	m.ObservePublish("ok", 3*time.Millisecond)
	m.ObservePublish("error", time.Second)
	m.SetBreakerState("pubsub-publisher", 2)
	m.ObserveMessage("ok")
	m.ObserveFlush(2, 40, 8*time.Millisecond)
	m.SetAggregationState(3, 120, 45*time.Second)

	body := scrape(t, m)
	for _, want := range []string{
		`outcome="ok"`,
		`outcome="error"`,
		`fluxgate_resilience_breaker_state{breaker="pubsub-publisher",service="fluxgate-test"} 2`,
		`fluxgate_aggregate_windows_flushed_total{service="fluxgate-test"} 2`,
		`fluxgate_aggregate_rollups_written_total{service="fluxgate-test"} 40`,
		`fluxgate_aggregate_open_windows{service="fluxgate-test"} 3`,
		`fluxgate_aggregate_tracked_series{service="fluxgate-test"} 120`,
		`fluxgate_aggregate_watermark_lag_seconds{service="fluxgate-test"} 45`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape is missing %q", want)
		}
	}
}

// TestNilMetricsIsSafe lets a service run uninstrumented without every call
// site guarding for it.
func TestNilMetricsIsSafe(t *testing.T) {
	var m *Metrics

	m.ObserveRequest("route", "GET", 200, time.Second)
	m.ObserveIngest("acme", 1, 0, 1)
	m.ObservePublish("ok", time.Second)
	m.SetBreakerState("x", 0)
	m.ObserveMessage("ok")
	m.ObserveFlush(1, 1, time.Second)
	m.SetAggregationState(1, 1, time.Second)

	if done := m.TrackInFlight(); done == nil {
		t.Fatal("TrackInFlight returned a nil release function")
	} else {
		done()
	}
}

// TestRegistriesAreIndependent is what lets two services run in one test
// binary without their metrics colliding.
func TestRegistriesAreIndependent(t *testing.T) {
	a := NewMetrics("service-a")
	b := NewMetrics("service-b")

	a.ObserveRequest("GET /a", http.MethodGet, 200, time.Millisecond)

	if !strings.Contains(scrape(t, a), `service="service-a"`) {
		t.Error("service A's own metric is missing from its registry")
	}
	if strings.Contains(scrape(t, b), "GET /a") {
		t.Error("service B's registry contains service A's observation")
	}
}
