package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/auth"
	"github.com/jon-jc/fluxgate/internal/config"
	"github.com/jon-jc/fluxgate/internal/httpx"
	"github.com/jon-jc/fluxgate/internal/observability"
	"github.com/jon-jc/fluxgate/internal/query"
	"github.com/jon-jc/fluxgate/internal/store"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

var queryNow = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

// fakeReader stands in for Postgres so the handlers can be tested without one.
type fakeReader struct {
	mu sync.Mutex

	rollups []store.StoredRollup
	changed []store.StoredRollup
	metrics []store.MetricSummary
	keys    []string
	values  []string

	// lastFilter records what the handler asked for, so a test can assert the
	// tenant was taken from the credential rather than from a parameter.
	lastFilter store.QueryFilter
	err        error
}

func (f *fakeReader) Query(_ context.Context, filter store.QueryFilter) ([]store.StoredRollup, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.lastFilter = filter
	if f.err != nil {
		return nil, f.err
	}
	return f.rollups, nil
}

func (f *fakeReader) Changed(
	_ context.Context, _, _ string, cursor store.Cursor, _ int,
) ([]store.StoredRollup, store.Cursor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return nil, cursor, f.err
	}

	out := f.changed
	// Deliver once, so a polling test does not loop forever on the same row.
	f.changed = nil

	next := cursor
	for i := range out {
		next = next.After(out[i], queryNow.Add(time.Second))
	}
	return out, next, nil
}

func (f *fakeReader) NewestWriteTime(context.Context, string) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return queryNow, f.err
}

func (f *fakeReader) Metrics(context.Context, string, int) ([]store.MetricSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.metrics, f.err
}

func (f *fakeReader) LabelKeys(context.Context, string, string, int) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.keys, f.err
}

func (f *fakeReader) LabelValues(context.Context, string, string, string, int) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.values, f.err
}

func (f *fakeReader) filter() store.QueryFilter {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastFilter
}

// queryHarness is a fully wired read API over a fake store.
type queryHarness struct {
	router http.Handler
	reader *fakeReader
}

func newQueryHarness(t *testing.T, opts ...func(*api2Config)) *queryHarness {
	t.Helper()

	cfg := api2Config{
		limits: query.DefaultLimits(),
		stream: StreamOptions{
			PollInterval:      5 * time.Millisecond,
			HeartbeatInterval: 10 * time.Millisecond,
			MaxDuration:       2 * time.Second,
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	reader := &fakeReader{}

	keys := fmt.Sprintf(`[{"key_id":%q,"tenant_id":%q,"secret_sha256":%q}]`,
		testKeyID, testTenant, auth.HashSecret(testSecret))
	keyStore, err := auth.ParseKeys([]byte(keys))
	if err != nil {
		t.Fatalf("parse test keys: %v", err)
	}

	health := observability.NewHealth(time.Second)
	health.SetReady(true)

	router := NewQueryRouter(QueryRouterDeps{
		Config: config.Config{
			Service:     "fluxgate-query-test",
			Environment: config.EnvLocal,
			HTTP: config.HTTPConfig{
				HandlerTimeout:  5 * time.Second,
				MaxRequestBytes: 1 << 20,
			},
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Health: health,
		Auth:   auth.Options{Store: keyStore},
		Query: QueryDeps{
			Reader: reader,
			Limits: cfg.limits,
			Stream: cfg.stream,
			Now:    func() time.Time { return queryNow },
		},
	})

	return &queryHarness{router: router, reader: reader}
}

type api2Config struct {
	limits query.Limits
	stream StreamOptions
}

func withQueryLimits(l query.Limits) func(*api2Config) {
	return func(c *api2Config) { c.limits = l }
}

// get issues an authenticated GET.
func (h *queryHarness) get(t *testing.T, path string) *httptest.ResponseRecorder {
	t.Helper()

	r := httptest.NewRequest(http.MethodGet, path, http.NoBody)
	r.Header.Set("Authorization", "Bearer "+testToken)

	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, r)
	return rec
}

func storedRollup(metric string, offset time.Duration, labels map[string]string, values ...float64) store.StoredRollup {
	start := queryNow.Add(offset)

	var sum, minV, maxV, last float64
	for i, v := range values {
		sum += v
		if i == 0 || v < minV {
			minV = v
		}
		if i == 0 || v > maxV {
			maxV = v
		}
		last = v
	}

	return store.StoredRollup{
		Metric:      metric,
		Kind:        string(telemetry.KindGauge),
		Labels:      labels,
		WindowStart: start,
		WindowEnd:   start.Add(time.Minute),
		Count:       int64(len(values)),
		Sum:         sum,
		Min:         minV,
		Max:         maxV,
		Last:        last,
	}
}

func decodeResult(t *testing.T, rec *httptest.ResponseRecorder) query.Result {
	t.Helper()

	var result query.Result
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode result: %v (body=%s)", err, rec.Body.String())
	}
	return result
}

func TestQueryReturnsSeries(t *testing.T) {
	h := newQueryHarness(t)
	h.reader.rollups = []store.StoredRollup{
		storedRollup("cpu.util", 0, map[string]string{"host": "a"}, 10, 20, 30),
		storedRollup("cpu.util", -time.Minute, map[string]string{"host": "a"}, 5),
	}

	rec := h.get(t, "/v1/query?metric=cpu.util&agg=sum")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body)
	}

	result := decodeResult(t, rec)
	if len(result.Series) != 1 {
		t.Fatalf("got %d series, want 1", len(result.Series))
	}
	if len(result.Series[0].Points) != 2 {
		t.Fatalf("got %d points, want 2", len(result.Series[0].Points))
	}
	// Oldest first, because that is the order a chart draws.
	if result.Series[0].Points[0].Value != 5 {
		t.Errorf("first point = %v, want the older window's 5",
			result.Series[0].Points[0].Value)
	}
}

// TestQueryScopesToTheCredentialsTenant is the isolation property: a caller
// able to name someone else's tenant could read their data.
func TestQueryScopesToTheCredentialsTenant(t *testing.T) {
	h := newQueryHarness(t)

	// A tenant parameter is supplied and must be ignored entirely.
	rec := h.get(t, "/v1/query?metric=cpu.util&tenant=someone-else&tenant_id=someone-else")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body)
	}

	if got := h.reader.filter().TenantID; got != testTenant {
		t.Errorf("queried tenant %q, want %q from the credential", got, testTenant)
	}
}

func TestQueryPassesLabelFilters(t *testing.T) {
	h := newQueryHarness(t)

	rec := h.get(t, "/v1/query?metric=http.requests&label.status=500&label.service=checkout")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body)
	}

	labels := h.reader.filter().Labels
	if labels["status"] != "500" || labels["service"] != "checkout" {
		t.Errorf("label filter = %v, want status=500 service=checkout", labels)
	}
}

// TestLabelParametersAreNamespaced keeps a metric labelled "agg" queryable.
func TestLabelParametersAreNamespaced(t *testing.T) {
	h := newQueryHarness(t)

	rec := h.get(t, "/v1/query?metric=cpu.util&agg=max&label.agg=something")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body)
	}

	if got := decodeResult(t, rec).Aggregation; got != "max" {
		t.Errorf("aggregation = %q, want max; the label shadowed the parameter", got)
	}
	if got := h.reader.filter().Labels["agg"]; got != "something" {
		t.Errorf("label agg = %q, want something", got)
	}
}

func TestQueryRejectsInvalidParameters(t *testing.T) {
	h := newQueryHarness(t)

	tests := map[string]string{
		"no metric":           "/v1/query",
		"unknown aggregation": "/v1/query?metric=cpu.util&agg=median",
		"bad timestamp":       "/v1/query?metric=cpu.util&from=yesterday",
		"inverted range":      "/v1/query?metric=cpu.util&from=2026-09-03T12:00:00Z&to=2026-09-03T11:00:00Z",
		"excessive range":     "/v1/query?metric=cpu.util&from=-9000h",
	}

	for name, path := range tests {
		t.Run(name, func(t *testing.T) {
			rec := h.get(t, path)
			if rec.Code != http.StatusUnprocessableEntity {
				t.Errorf("status = %d, want 422 (%s)", rec.Code, rec.Body)
			}
			if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
				t.Errorf("Content-Type = %q, want application/problem+json", ct)
			}
		})
	}
}

func TestQueryRequiresAuthentication(t *testing.T) {
	h := newQueryHarness(t)

	for _, path := range []string{"/v1/query?metric=x", "/v1/metrics", "/v1/labels?metric=x", "/v1/stream"} {
		t.Run(path, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, path, http.NoBody)
			rec := httptest.NewRecorder()
			h.router.ServeHTTP(rec, r)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401 (%s)", rec.Code, rec.Body)
			}
		})
	}
}

func TestQueryReportsStoreFailureAsInternal(t *testing.T) {
	h := newQueryHarness(t)
	h.reader.err = errors.New("connection refused to 10.0.0.7:5432")

	rec := h.get(t, "/v1/query?metric=cpu.util")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (%s)", rec.Code, rec.Body)
	}
	// The internal address must not reach the caller.
	if strings.Contains(rec.Body.String(), "10.0.0.7") {
		t.Errorf("internal detail leaked: %s", rec.Body.String())
	}
}

func TestQueryWithNoDataReturnsAnEmptyArray(t *testing.T) {
	h := newQueryHarness(t)

	rec := h.get(t, "/v1/query?metric=cpu.util")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	// Not null: a client should iterate without a nil check.
	if !strings.Contains(rec.Body.String(), `"series":[]`) {
		t.Errorf("body = %s, want an empty series array", rec.Body.String())
	}
}

func TestQueryTruncationIsVisible(t *testing.T) {
	limits := query.DefaultLimits()
	limits.MaxSeries = 2

	h := newQueryHarness(t, withQueryLimits(limits))
	for i := range 10 {
		h.reader.rollups = append(h.reader.rollups,
			storedRollup("m", 0, map[string]string{"host": string(rune('a' + i))}, 1))
	}

	result := decodeResult(t, h.get(t, "/v1/query?metric=m&agg=sum"))
	if len(result.Series) != 2 {
		t.Errorf("got %d series, want the cap of 2", len(result.Series))
	}
	// A caller has to be able to tell an incomplete answer from an empty one.
	if !result.Truncated {
		t.Error("the response was cut short but not marked truncated")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	h := newQueryHarness(t)
	h.reader.metrics = []store.MetricSummary{
		{
			Metric: "cpu.util", Kind: "gauge", SeriesCount: 4,
			OldestPoint: queryNow.Add(-time.Hour), NewestPoint: queryNow,
		},
	}

	rec := h.get(t, "/v1/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body)
	}

	var body struct {
		Metrics []store.MetricSummary `json:"metrics"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Metrics) != 1 || body.Metrics[0].Metric != "cpu.util" {
		t.Fatalf("metrics = %+v", body.Metrics)
	}
	// The time range saves a caller from probing for it; a UI that guesses
	// renders an empty chart on its first attempt.
	if body.Metrics[0].NewestPoint.IsZero() {
		t.Error("the summary carries no time range")
	}
}

func TestMetricsWithNoDataReturnsAnEmptyArray(t *testing.T) {
	h := newQueryHarness(t)

	rec := h.get(t, "/v1/metrics")
	if !strings.Contains(rec.Body.String(), `"metrics":[]`) {
		t.Errorf("body = %s, want an empty array", rec.Body.String())
	}
}

func TestLabelsEndpoint(t *testing.T) {
	h := newQueryHarness(t)
	h.reader.keys = []string{"host", "service"}
	h.reader.values = []string{"checkout", "search"}

	t.Run("keys", func(t *testing.T) {
		rec := h.get(t, "/v1/labels?metric=http.requests")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body)
		}
		if !strings.Contains(rec.Body.String(), "service") {
			t.Errorf("body = %s, want the label keys", rec.Body.String())
		}
	})

	t.Run("values", func(t *testing.T) {
		rec := h.get(t, "/v1/labels?metric=http.requests&label=service")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body)
		}
		if !strings.Contains(rec.Body.String(), "checkout") {
			t.Errorf("body = %s, want the label values", rec.Body.String())
		}
	})

	t.Run("metric required", func(t *testing.T) {
		if rec := h.get(t, "/v1/labels"); rec.Code != http.StatusUnprocessableEntity {
			t.Errorf("status = %d, want 422", rec.Code)
		}
	})
}

// TestStreamEmitsRollups exercises the live tail end to end over SSE.
func TestStreamEmitsRollups(t *testing.T) {
	h := newQueryHarness(t)
	h.reader.changed = []store.StoredRollup{
		storedRollup("cpu.util", 0, map[string]string{"host": "a"}, 42),
	}

	srv := httptest.NewServer(h.router)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		srv.URL+"/v1/stream?metric=cpu.util", http.NoBody)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
	// A buffering proxy would defeat the endpoint entirely.
	if got := resp.Header.Get("X-Accel-Buffering"); got != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no", got)
	}

	// Read until the rollup event arrives or the context expires.
	buf := make([]byte, 4096)
	var seen strings.Builder
	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			seen.Write(buf[:n])
			if strings.Contains(seen.String(), "event: rollup") {
				break
			}
		}
		if err != nil {
			break
		}
	}

	body := seen.String()
	if !strings.Contains(body, "event: rollup") {
		t.Fatalf("no rollup event arrived; stream said:\n%s", body)
	}
	if !strings.Contains(body, `"metric":"cpu.util"`) {
		t.Errorf("event does not carry the metric:\n%s", body)
	}
	// The retry hint keeps a thousand tabs dropped by one deploy from all
	// returning in the same instant.
	if !strings.Contains(body, "retry:") {
		t.Errorf("stream did not advertise a reconnect delay:\n%s", body)
	}
}

// TestStreamIsExemptFromTheRequestTimeout: a blanket timeout would sever every
// stream at the deadline, which a client cannot tell from a server fault.
func TestStreamIsExemptFromTheRequestTimeout(t *testing.T) {
	h := newQueryHarness(t)

	srv := httptest.NewServer(h.router)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/stream", http.NoBody)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// An idle stream must stay open and send keep-alives rather than being
	// cut off; the harness heartbeat is 10ms.
	buf := make([]byte, 1024)
	deadline := time.Now().Add(2 * time.Second)
	var seen strings.Builder

	for time.Now().Before(deadline) {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			seen.Write(buf[:n])
			if strings.Contains(seen.String(), "keep-alive") {
				return // the stream is alive and heartbeating
			}
		}
		if err != nil {
			break
		}
	}
	t.Errorf("no keep-alive on an idle stream; got:\n%s", seen.String())
}

func TestStreamValidatesItsMetric(t *testing.T) {
	h := newQueryHarness(t)

	rec := h.get(t, "/v1/stream?metric=not%20a%20metric")
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422 (%s)", rec.Code, rec.Body)
	}
}

func TestQueryRouterServesProbesWithoutCredentials(t *testing.T) {
	h := newQueryHarness(t)

	for _, path := range []string{PathLiveness, PathReadiness, "/v1/version"} {
		t.Run(path, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, path, http.NoBody)
			rec := httptest.NewRecorder()
			h.router.ServeHTTP(rec, r)

			// An orchestrator holds no credentials.
			if rec.Code != http.StatusOK {
				t.Errorf("status = %d, want 200 (%s)", rec.Code, rec.Body)
			}
		})
	}
}

func TestQueryRouterUnknownRouteReturnsProblemJSON(t *testing.T) {
	h := newQueryHarness(t)

	rec := h.get(t, "/v1/nope")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}

	var p httpx.Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	if p.Code != httpx.CodeNotFound {
		t.Errorf("code = %q, want %q", p.Code, httpx.CodeNotFound)
	}
}

func TestQueryRouterRejectsWrongMethod(t *testing.T) {
	h := newQueryHarness(t)

	r := httptest.NewRequest(http.MethodPost, "/v1/query?metric=x", http.NoBody)
	r.Header.Set("Authorization", "Bearer "+testToken)

	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, r)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405 (%s)", rec.Code, rec.Body)
	}
	if allow := rec.Header().Get("Allow"); !strings.Contains(allow, http.MethodGet) {
		t.Errorf("Allow = %q, want it to list GET", allow)
	}
}
