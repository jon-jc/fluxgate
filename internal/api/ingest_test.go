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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/auth"
	"github.com/jon-jc/fluxgate/internal/config"
	"github.com/jon-jc/fluxgate/internal/httpx"
	"github.com/jon-jc/fluxgate/internal/idempotency"
	"github.com/jon-jc/fluxgate/internal/ingest"
	"github.com/jon-jc/fluxgate/internal/observability"
	"github.com/jon-jc/fluxgate/internal/ratelimit"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

const (
	testKeyID  = "k1"
	testSecret = "test-secret"
	testTenant = "acme"
	testToken  = "fxg_" + testKeyID + "_" + testSecret
)

// harness bundles a router with the collaborators a test needs to inspect.
type harness struct {
	router http.Handler
	sink   *ingest.MemorySink
	clock  func() time.Time
}

type harnessOption func(*harnessConfig)

type harnessConfig struct {
	limits      ratelimit.Limits
	authDisable bool
	idempotency bool
	sink        ingest.Sink
	now         time.Time
}

func withRateLimits(l ratelimit.Limits) harnessOption {
	return func(c *harnessConfig) { c.limits = l }
}

func withAuthDisabled() harnessOption {
	return func(c *harnessConfig) { c.authDisable = true }
}

func withoutIdempotency() harnessOption {
	return func(c *harnessConfig) { c.idempotency = false }
}

func withSink(s ingest.Sink) harnessOption {
	return func(c *harnessConfig) { c.sink = s }
}

// newHarness builds a fully wired router over an in-memory sink.
func newHarness(t *testing.T, opts ...harnessOption) *harness {
	t.Helper()

	cfg := harnessConfig{
		limits:      ratelimit.Limits{Rate: 1e6, Burst: 1e6},
		idempotency: true,
		now:         time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC),
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	memSink := ingest.NewMemorySink()
	sink := ingest.Sink(memSink)
	if cfg.sink != nil {
		sink = cfg.sink
	}

	keys := fmt.Sprintf(`[{"key_id":%q,"tenant_id":%q,"secret_sha256":%q}]`,
		testKeyID, testTenant, auth.HashSecret(testSecret))
	store, err := auth.ParseKeys([]byte(keys))
	if err != nil {
		t.Fatalf("parse test keys: %v", err)
	}

	health := observability.NewHealth(time.Second)
	health.SetReady(true)

	now := func() time.Time { return cfg.now }

	var idem *idempotency.Store
	if cfg.idempotency {
		idem = idempotency.New(time.Hour, idempotency.WithClock(now))
	}

	router := NewRouter(Deps{
		Config: config.Config{
			Service:     "fluxgate-test",
			Environment: config.EnvLocal,
			HTTP: config.HTTPConfig{
				HandlerTimeout:  5 * time.Second,
				MaxRequestBytes: 1 << 20,
			},
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Health: health,
		Auth:   auth.Options{Store: store, Disabled: cfg.authDisable},
		Ingest: IngestDeps{
			Sink: sink,
			Validator: telemetry.Validator{
				Limits: telemetry.DefaultLimits(),
				Clock:  now,
			},
			Limiter:         ratelimit.New(cfg.limits, ratelimit.WithClock(now)),
			Idempotency:     idem,
			MaxRequestBytes: 1 << 20,
		},
	})

	return &harness{router: router, sink: memSink, clock: now}
}

// post sends a body to the ingest endpoint with the given headers.
func (h *harness) post(t *testing.T, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	r := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+testToken)
	for k, v := range headers {
		if v == "" {
			r.Header.Del(k)
			continue
		}
		r.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, r)
	return rec
}

// validBody returns a batch of n well-formed points.
func (h *harness) validBody(n int) string {
	points := make([]string, n)
	ts := h.clock().Add(-time.Minute).Format(time.RFC3339)
	for i := range n {
		points[i] = fmt.Sprintf(
			`{"metric":"http.requests","kind":"counter","value":%d,"timestamp":%q,"labels":{"service":"checkout"}}`,
			i, ts)
	}
	return `{"points":[` + strings.Join(points, ",") + `]}`
}

func decodeIngest(t *testing.T, rec *httptest.ResponseRecorder) ingestResponse {
	t.Helper()

	var resp ingestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v (body=%s)", err, rec.Body.String())
	}
	return resp
}

func TestIngestAcceptsAValidBatch(t *testing.T) {
	h := newHarness(t)

	rec := h.post(t, h.validBody(3), nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", rec.Code, rec.Body)
	}

	resp := decodeIngest(t, rec)
	if resp.Accepted != 3 || resp.Rejected != 0 {
		t.Errorf("accepted/rejected = %d/%d, want 3/0", resp.Accepted, resp.Rejected)
	}
	if resp.BatchID == "" {
		t.Error("response is missing a batch_id")
	}

	if got := h.sink.PointCount(); got != 3 {
		t.Errorf("sink holds %d points, want 3", got)
	}
	batches := h.sink.Batches()
	if len(batches) != 1 {
		t.Fatalf("sink holds %d batches, want 1", len(batches))
	}
	if batches[0].TenantID != testTenant {
		t.Errorf("batch tenant = %q, want %q", batches[0].TenantID, testTenant)
	}
}

// TestIngestIsPartialSuccess is the endpoint's defining behaviour: one bad
// point must not blind a service's entire telemetry stream.
func TestIngestIsPartialSuccess(t *testing.T) {
	h := newHarness(t)

	ts := h.clock().Add(-time.Minute).Format(time.RFC3339)
	body := fmt.Sprintf(`{"points":[
	  {"metric":"good.one","kind":"gauge","value":1,"timestamp":%q},
	  {"metric":"9bad","kind":"gauge","value":1,"timestamp":%q},
	  {"metric":"good.two","kind":"gauge","value":2,"timestamp":%q}
	]}`, ts, ts, ts)

	rec := h.post(t, body, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", rec.Code, rec.Body)
	}

	resp := decodeIngest(t, rec)
	if resp.Accepted != 2 {
		t.Errorf("accepted = %d, want 2", resp.Accepted)
	}
	if resp.Rejected != 1 {
		t.Errorf("rejected = %d, want 1", resp.Rejected)
	}
	if len(resp.Errors) != 1 {
		t.Fatalf("errors = %+v, want exactly one", resp.Errors)
	}
	// The client has to know which element of its batch to fix.
	if !strings.HasPrefix(resp.Errors[0].Field, "points.1.") {
		t.Errorf("error field = %q, want it to identify points.1", resp.Errors[0].Field)
	}

	if got := h.sink.PointCount(); got != 2 {
		t.Errorf("sink holds %d points, want only the 2 valid ones", got)
	}
}

func TestIngestRejectsBatchWhereNothingIsValid(t *testing.T) {
	h := newHarness(t)

	body := `{"points":[
	  {"metric":"","kind":"gauge","value":1},
	  {"metric":"also bad","kind":"nope","value":1}
	]}`

	rec := h.post(t, body, nil)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (%s)", rec.Code, rec.Body)
	}
	// Nothing was publishable, so nothing may reach the pipeline.
	if got := h.sink.PointCount(); got != 0 {
		t.Errorf("sink holds %d points, want 0", got)
	}
}

func TestIngestRejectsEmptyBatch(t *testing.T) {
	h := newHarness(t)

	rec := h.post(t, `{"points":[]}`, nil)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422 (%s)", rec.Code, rec.Body)
	}
}

func TestIngestRejectsOversizedBatch(t *testing.T) {
	h := newHarness(t)

	limit := telemetry.DefaultLimits().MaxPointsPerBatch
	rec := h.post(t, h.validBody(limit+1), nil)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (%s)", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), strconv.Itoa(limit)) {
		t.Errorf("response does not state the limit: %s", rec.Body.String())
	}
}

// TestIngestTruncatesErrorEnumeration keeps a response from dwarfing the
// request that caused it, while leaving the counts exact.
func TestIngestTruncatesErrorEnumeration(t *testing.T) {
	h := newHarness(t)

	const bad = 300
	points := make([]string, bad+1)
	for i := range bad {
		points[i] = `{"metric":"1invalid","kind":"gauge","value":1}`
	}
	points[bad] = fmt.Sprintf(`{"metric":"fine.one","kind":"gauge","value":1,"timestamp":%q}`,
		h.clock().Format(time.RFC3339))

	rec := h.post(t, `{"points":[`+strings.Join(points, ",")+`]}`, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", rec.Code, rec.Body)
	}

	resp := decodeIngest(t, rec)
	if resp.Rejected != bad {
		t.Errorf("rejected = %d, want the exact count %d", resp.Rejected, bad)
	}
	if len(resp.Errors) > maxReportedErrors {
		t.Errorf("enumerated %d errors, cap is %d", len(resp.Errors), maxReportedErrors)
	}
	if !resp.ErrorsTruncated {
		t.Error("errors_truncated is false despite the list being cut short")
	}
}

func TestIngestRequiresAuthentication(t *testing.T) {
	h := newHarness(t)

	tests := map[string]string{
		"missing header": "",
		"wrong scheme":   "Basic " + testToken,
		"bad secret":     "Bearer fxg_" + testKeyID + "_wrong",
		"unknown key":    "Bearer fxg_nope_" + testSecret,
		"garbage":        "Bearer nonsense",
	}

	for name, header := range tests {
		t.Run(name, func(t *testing.T) {
			rec := h.post(t, h.validBody(1), map[string]string{"Authorization": header})

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401 (%s)", rec.Code, rec.Body)
			}
			if got := rec.Header().Get("WWW-Authenticate"); got == "" {
				t.Error("401 is missing the WWW-Authenticate challenge")
			}
			if h.sink.PointCount() != 0 {
				t.Error("an unauthenticated request reached the sink")
			}
		})
	}
}

// TestUnauthorizedResponsesAreIndistinguishable denies an attacker an oracle
// for enumerating valid key IDs.
func TestUnauthorizedResponsesAreIndistinguishable(t *testing.T) {
	h := newHarness(t)

	unknownKey := h.post(t, h.validBody(1),
		map[string]string{"Authorization": "Bearer fxg_doesnotexist_" + testSecret})
	badSecret := h.post(t, h.validBody(1),
		map[string]string{"Authorization": "Bearer fxg_" + testKeyID + "_wrong"})

	var a, b httpx.Problem
	if err := json.Unmarshal(unknownKey.Body.Bytes(), &a); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := json.Unmarshal(badSecret.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if a.Detail != b.Detail || a.Code != b.Code {
		t.Errorf("responses differ and leak which half of the credential was wrong:\n"+
			"unknown key: %s / %s\nbad secret:  %s / %s", a.Code, a.Detail, b.Code, b.Detail)
	}
}

func TestIngestAttributesToTheKeysTenant(t *testing.T) {
	h := newHarness(t)

	if rec := h.post(t, h.validBody(1), nil); rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", rec.Code, rec.Body)
	}

	batches := h.sink.Batches()
	if len(batches) != 1 {
		t.Fatalf("sink holds %d batches, want 1", len(batches))
	}
	// The tenant comes from the credential, never from the request body: a
	// caller must not be able to write into someone else's data.
	if batches[0].TenantID != testTenant {
		t.Errorf("tenant = %q, want %q", batches[0].TenantID, testTenant)
	}
}

func TestIngestRateLimits(t *testing.T) {
	h := newHarness(t, withRateLimits(ratelimit.Limits{Rate: 10, Burst: 10}))

	// Spend the burst.
	if rec := h.post(t, h.validBody(10), nil); rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", rec.Code, rec.Body)
	}

	rec := h.post(t, h.validBody(1), nil)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429 (%s)", rec.Code, rec.Body)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("429 is missing Retry-After")
	}
	if rec.Header().Get("X-RateLimit-Limit") == "" {
		t.Error("429 is missing X-RateLimit-Limit")
	}
}

// TestRateLimitChargesPerPoint stops a caller evading its quota by batching.
func TestRateLimitChargesPerPoint(t *testing.T) {
	h := newHarness(t, withRateLimits(ratelimit.Limits{Rate: 1, Burst: 100}))

	if rec := h.post(t, h.validBody(100), nil); rec.Code != http.StatusAccepted {
		t.Fatalf("first batch: status = %d, want 202 (%s)", rec.Code, rec.Body)
	}
	// A single further point must now be refused: the 100-point batch consumed
	// the whole allowance, not one request's worth of it.
	if rec := h.post(t, h.validBody(1), nil); rec.Code != http.StatusTooManyRequests {
		t.Errorf("second request: status = %d, want 429 (%s)", rec.Code, rec.Body)
	}
}

func TestRateLimitHeadersOnSuccess(t *testing.T) {
	h := newHarness(t, withRateLimits(ratelimit.Limits{Rate: 100, Burst: 100}))

	rec := h.post(t, h.validBody(10), nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}

	remaining := rec.Header().Get("X-RateLimit-Remaining")
	if remaining == "" {
		t.Fatal("X-RateLimit-Remaining is missing")
	}
	n, err := strconv.Atoi(remaining)
	if err != nil {
		t.Fatalf("X-RateLimit-Remaining = %q, not an integer", remaining)
	}
	if n != 90 {
		t.Errorf("X-RateLimit-Remaining = %d, want 90 after spending 10 of 100", n)
	}
}

// TestIdempotentRetryDoesNotDoubleCount is the property that lets a client
// safely retry a request it never saw the response to.
func TestIdempotentRetryDoesNotDoubleCount(t *testing.T) {
	h := newHarness(t)

	body := h.validBody(5)
	headers := map[string]string{HeaderIdempotencyKey: "batch-0001"}

	first := h.post(t, body, headers)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first: status = %d, want 202 (%s)", first.Code, first.Body)
	}

	second := h.post(t, body, headers)
	if second.Code != http.StatusAccepted {
		t.Fatalf("retry: status = %d, want 202 (%s)", second.Code, second.Body)
	}

	if second.Header().Get(HeaderIdempotencyReplayed) != "true" {
		t.Error("the retry was not marked as a replay")
	}
	if first.Body.String() != second.Body.String() {
		t.Errorf("replay differs from the original:\n%s\n%s", first.Body, second.Body)
	}
	// The decisive assertion: the data was recorded once, not twice.
	if got := h.sink.PointCount(); got != 5 {
		t.Errorf("sink holds %d points, want 5; the retry was double-counted", got)
	}
}

func TestIdempotencyKeyReuseWithDifferentPayloadConflicts(t *testing.T) {
	h := newHarness(t)

	headers := map[string]string{HeaderIdempotencyKey: "batch-0001"}

	if rec := h.post(t, h.validBody(1), headers); rec.Code != http.StatusAccepted {
		t.Fatalf("first: status = %d, want 202 (%s)", rec.Code, rec.Body)
	}

	// Replaying the first response here would silently discard the new data,
	// so the request has to be refused instead.
	rec := h.post(t, h.validBody(2), headers)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (%s)", rec.Code, rec.Body)
	}
	if got := h.sink.PointCount(); got != 1 {
		t.Errorf("sink holds %d points; the conflicting batch should not have been stored", got)
	}
}

func TestDistinctIdempotencyKeysAreIndependent(t *testing.T) {
	h := newHarness(t)

	for _, key := range []string{"a", "b", "c"} {
		rec := h.post(t, h.validBody(1), map[string]string{HeaderIdempotencyKey: key})
		if rec.Code != http.StatusAccepted {
			t.Fatalf("key %q: status = %d, want 202 (%s)", key, rec.Code, rec.Body)
		}
	}
	if got := h.sink.PointCount(); got != 3 {
		t.Errorf("sink holds %d points, want 3", got)
	}
}

func TestIngestWithoutIdempotencyKeyIsNotDeduplicated(t *testing.T) {
	h := newHarness(t)

	// Without a key there is nothing to correlate two requests by, so both
	// must be treated as new data.
	body := h.validBody(2)
	for range 2 {
		if rec := h.post(t, body, nil); rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 (%s)", rec.Code, rec.Body)
		}
	}
	if got := h.sink.PointCount(); got != 4 {
		t.Errorf("sink holds %d points, want 4", got)
	}
}

func TestIngestRejectsMalformedIdempotencyKey(t *testing.T) {
	h := newHarness(t)

	for name, key := range map[string]string{
		"too long":          strings.Repeat("k", maxIdempotencyKeyLen+1),
		"control character": "abc\x00def",
		"newline":           "abc\ndef",
	} {
		t.Run(name, func(t *testing.T) {
			rec := h.post(t, h.validBody(1), map[string]string{HeaderIdempotencyKey: key})
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400 (%s)", rec.Code, rec.Body)
			}
		})
	}
}

func TestIngestRejectsMalformedBodies(t *testing.T) {
	h := newHarness(t)

	tests := []struct {
		name string
		body string
		want int
	}{
		{name: "not json", body: `nonsense`, want: http.StatusBadRequest},
		{name: "empty", body: ``, want: http.StatusBadRequest},
		{name: "truncated", body: `{"points":[`, want: http.StatusBadRequest},
		{name: "unknown top-level field", body: `{"pointz":[]}`, want: http.StatusUnprocessableEntity},
		{
			name: "unknown point field",
			body: `{"points":[{"metric":"a","kind":"gauge","value":1,"tags":{}}]}`,
			want: http.StatusUnprocessableEntity,
		},
		{
			name: "wrong value type",
			body: `{"points":[{"metric":"a","kind":"gauge","value":"hot"}]}`,
			want: http.StatusUnprocessableEntity,
		},
		{
			name: "two documents",
			body: `{"points":[]}{"points":[]}`,
			want: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := h.post(t, tc.body, nil)
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d (%s)", rec.Code, tc.want, rec.Body)
			}
		})
	}
}

func TestIngestRejectsWrongContentType(t *testing.T) {
	h := newHarness(t)

	rec := h.post(t, h.validBody(1), map[string]string{"Content-Type": "text/plain"})
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Errorf("status = %d, want 415 (%s)", rec.Code, rec.Body)
	}
}

func TestIngestFillsMissingTimestampWithArrivalTime(t *testing.T) {
	h := newHarness(t)

	rec := h.post(t, `{"points":[{"metric":"a.b","kind":"gauge","value":1}]}`, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", rec.Code, rec.Body)
	}

	batches := h.sink.Batches()
	if len(batches) != 1 || len(batches[0].Points) != 1 {
		t.Fatalf("unexpected sink contents: %+v", batches)
	}
	if batches[0].Points[0].Timestamp.IsZero() {
		t.Error("the point was stored with a zero timestamp")
	}
}

// TestSinkFailureIsRetryable tells a client its data was not accepted, rather
// than reporting success for a batch that never left the process.
func TestSinkFailureIsRetryable(t *testing.T) {
	failing := ingest.SinkFunc(func(context.Context, telemetry.Batch) error {
		return errors.New("broker unreachable")
	})

	h := newHarness(t, withSink(failing))

	rec := h.post(t, h.validBody(1), nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (%s)", rec.Code, rec.Body)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("503 is missing Retry-After; the client cannot tell this is retryable")
	}
	// The internal failure must not be echoed to the caller.
	if strings.Contains(rec.Body.String(), "broker unreachable") {
		t.Errorf("internal error leaked: %s", rec.Body.String())
	}
}

func TestIngestWithAuthDisabledUsesAnonymousTenant(t *testing.T) {
	h := newHarness(t, withAuthDisabled())

	r := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(h.validBody(1)))
	r.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, r)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", rec.Code, rec.Body)
	}
	batches := h.sink.Batches()
	if len(batches) != 1 {
		t.Fatalf("sink holds %d batches, want 1", len(batches))
	}
	if batches[0].TenantID != auth.AnonymousTenant {
		t.Errorf("tenant = %q, want %q", batches[0].TenantID, auth.AnonymousTenant)
	}
}

func TestIngestWorksWithoutAnIdempotencyStore(t *testing.T) {
	h := newHarness(t, withoutIdempotency())

	// A key supplied to a service with no store must be ignored, not fail.
	rec := h.post(t, h.validBody(1), map[string]string{HeaderIdempotencyKey: "k"})
	if rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202 (%s)", rec.Code, rec.Body)
	}
}

func TestIngestRejectsOversizedBody(t *testing.T) {
	h := newHarness(t)

	// One enormous label value, well past the 1 MiB body cap.
	body := fmt.Sprintf(
		`{"points":[{"metric":"a.b","kind":"gauge","value":1,"labels":{"x":%q}}]}`,
		strings.Repeat("y", 2<<20))

	rec := h.post(t, body, nil)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413 (%s)", rec.Code, rec.Body)
	}
}
