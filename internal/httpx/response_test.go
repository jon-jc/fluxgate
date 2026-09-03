package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// decodeProblem reads a problem document off the recorder, failing the test if
// the response is not one.
func decodeProblem(t *testing.T, rec *httptest.ResponseRecorder) Problem {
	t.Helper()

	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
		t.Fatalf("Content-Type = %q, want application/problem+json", ct)
	}
	var p Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode problem: %v (body=%s)", err, rec.Body.String())
	}
	return p
}

func TestHandlerRendersReturnedError(t *testing.T) {
	h := Handler(func(http.ResponseWriter, *http.Request) error {
		return NotFound("No such stream.")
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/streams/x", http.NoBody))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	p := decodeProblem(t, rec)
	if p.Code != CodeNotFound {
		t.Errorf("code = %q, want %q", p.Code, CodeNotFound)
	}
	if p.Detail != "No such stream." {
		t.Errorf("detail = %q", p.Detail)
	}
	if p.Status != http.StatusNotFound {
		t.Errorf("status field = %d, want 404", p.Status)
	}
	if !strings.HasSuffix(p.Type, CodeNotFound) {
		t.Errorf("type = %q, want a URI ending in %q", p.Type, CodeNotFound)
	}
}

func TestHandlerSuccessWritesNoProblem(t *testing.T) {
	h := Handler(func(w http.ResponseWriter, r *http.Request) error {
		return WriteJSON(w, r, http.StatusCreated, map[string]string{"id": "abc"})
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/ingest", http.NoBody))

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q", got)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["id"] != "abc" {
		t.Errorf("body = %v", body)
	}
}

func TestWriteErrorHidesInternalCause(t *testing.T) {
	secret := errors.New("dial tcp 10.0.0.7:5432: connection refused")

	rec := httptest.NewRecorder()
	WriteError(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody), Internal(secret))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	// Leaking an internal address or a driver error to a caller is both an
	// information disclosure and useless to them.
	if strings.Contains(rec.Body.String(), "10.0.0.7") {
		t.Errorf("internal cause leaked to client: %s", rec.Body.String())
	}
	if p := decodeProblem(t, rec); p.Code != CodeInternal {
		t.Errorf("code = %q, want %q", p.Code, CodeInternal)
	}
}

func TestWriteErrorSetsRetryAfterForRetryableErrors(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody),
		RateLimited("Quota exhausted."))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("Retry-After header missing on a retryable error")
	}
}

func TestWriteErrorIncludesRequestID(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	r = r.WithContext(ContextWithRequestID(r.Context(), "abc123"))

	rec := httptest.NewRecorder()
	WriteError(rec, r, BadRequest("nope"))

	if p := decodeProblem(t, rec); p.RequestID != "abc123" {
		t.Errorf("request_id = %q, want abc123", p.RequestID)
	}
}

func TestWriteErrorMapsWellKnownErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{
			name: "body over limit",
			err:  &http.MaxBytesError{Limit: 1024},
			want: http.StatusRequestEntityTooLarge,
		},
		{
			name: "handler deadline",
			err:  context.DeadlineExceeded,
			want: http.StatusGatewayTimeout,
		},
		{
			name: "unclassified failure",
			err:  errors.New("boom"),
			want: http.StatusInternalServerError,
		},
		{
			name: "wrapped api error survives wrapping",
			err:  errors.Join(errors.New("context"), Conflict("duplicate key")),
			want: http.StatusConflict,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			WriteError(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody), tc.err)
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d (body=%s)", rec.Code, tc.want, rec.Body)
			}
		})
	}
}

func TestWriteErrorStaysSilentWhenClientDisconnected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody).WithContext(ctx)
	rec := httptest.NewRecorder()

	WriteError(rec, r, context.Canceled)

	// Nobody is left to read a response, and writing one would record a status
	// the client never saw.
	if rec.Body.Len() != 0 {
		t.Errorf("wrote a body to a disconnected client: %s", rec.Body.String())
	}
}

type ingestPayload struct {
	Stream string  `json:"stream"`
	Value  float64 `json:"value"`
}

func decodeInto(t *testing.T, body, contentType string, maxBytes int64) error {
	t.Helper()

	r := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	var dst ingestPayload
	return DecodeJSON(httptest.NewRecorder(), r, maxBytes, &dst)
}

func TestDecodeJSON(t *testing.T) {
	const limit = 1 << 20

	tests := []struct {
		name        string
		body        string
		contentType string
		wantStatus  int
		wantDetail  string
	}{
		{
			name:        "valid",
			body:        `{"stream":"cpu","value":0.5}`,
			contentType: "application/json",
		},
		{
			name:        "content type with charset is accepted",
			body:        `{"stream":"cpu","value":0.5}`,
			contentType: "application/json; charset=utf-8",
		},
		{
			name:        "vendor json subtype is accepted",
			body:        `{"stream":"cpu","value":0.5}`,
			contentType: "application/vnd.fluxgate.v1+json",
		},
		{
			name:        "wrong content type",
			body:        `{}`,
			contentType: "text/plain",
			wantStatus:  http.StatusUnsupportedMediaType,
		},
		{
			name:       "empty body",
			body:       ``,
			wantStatus: http.StatusBadRequest,
			wantDetail: "must not be empty",
		},
		{
			name:       "malformed json",
			body:       `{"stream":`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "wrong field type",
			body:       `{"stream":"cpu","value":"hot"}`,
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name:       "unknown field",
			body:       `{"stream":"cpu","streem_id":1}`,
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name:       "two documents",
			body:       `{"stream":"a"}{"stream":"b"}`,
			wantStatus: http.StatusBadRequest,
			wantDetail: "exactly one JSON document",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := decodeInto(t, tc.body, tc.contentType, limit)

			if tc.wantStatus == 0 {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}

			var apiErr *Error
			if !errors.As(err, &apiErr) {
				t.Fatalf("error = %v, want an *httpx.Error", err)
			}
			if apiErr.Status != tc.wantStatus {
				t.Errorf("status = %d, want %d (%v)", apiErr.Status, tc.wantStatus, apiErr)
			}
			if tc.wantDetail != "" && !strings.Contains(apiErr.Message, tc.wantDetail) {
				t.Errorf("message = %q, want it to contain %q", apiErr.Message, tc.wantDetail)
			}
		})
	}
}

func TestDecodeJSONNamesTheUnknownField(t *testing.T) {
	// A typo should tell the client exactly which key was wrong, not just that
	// something was.
	err := decodeInto(t, `{"stream":"cpu","valeu":1}`, "application/json", 1<<20)

	var apiErr *Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("error = %v, want an *httpx.Error", err)
	}
	if len(apiErr.Fields) != 1 {
		t.Fatalf("fields = %v, want exactly one", apiErr.Fields)
	}
	if apiErr.Fields[0].Field != "valeu" {
		t.Errorf("field = %q, want valeu", apiErr.Fields[0].Field)
	}
}

func TestDecodeJSONEnforcesSizeLimit(t *testing.T) {
	huge := `{"stream":"` + strings.Repeat("x", 4096) + `"}`

	err := decodeInto(t, huge, "application/json", 64)
	if err == nil {
		t.Fatal("expected an error for an oversized body")
	}

	rec := httptest.NewRecorder()
	WriteError(rec, httptest.NewRequest(http.MethodPost, "/", http.NoBody), err)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
}

func TestErrorUnwrapsToCause(t *testing.T) {
	cause := errors.New("underlying")
	err := BadRequest("bad").WithCause(cause)

	if !errors.Is(err, cause) {
		t.Error("errors.Is did not find the wrapped cause")
	}
	if !strings.Contains(err.Error(), "underlying") {
		t.Errorf("Error() = %q, want it to mention the cause", err.Error())
	}
}

func TestNoContent(t *testing.T) {
	rec := httptest.NewRecorder()
	if err := NoContent(rec); err != nil {
		t.Fatalf("NoContent: %v", err)
	}
	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body.String())
	}
}

// TestWriteJSONReportsMarshalFailure guards the buffer-first contract: an
// un-encodable value must produce a clean error rather than a 200 with a
// truncated body.
func TestWriteJSONReportsMarshalFailure(t *testing.T) {
	rec := httptest.NewRecorder()
	err := WriteJSON(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody),
		http.StatusOK, map[string]any{"bad": make(chan int)})

	if err == nil {
		t.Fatal("expected an error for an un-encodable value")
	}
	if rec.Body.Len() != 0 {
		t.Errorf("wrote a partial body: %q", rec.Body.String())
	}
	if rec.Code != http.StatusOK {
		// httptest defaults to 200 without an explicit WriteHeader; the point
		// is that WriteHeader was never called.
		t.Errorf("status = %d, want the header to be untouched", rec.Code)
	}
}

func TestUnavailableIsRetryable(t *testing.T) {
	if !Unavailable("shedding load").Retryable {
		t.Error("Unavailable errors must be marked retryable")
	}
	if !RateLimited("slow down").Retryable {
		t.Error("RateLimited errors must be marked retryable")
	}
}

// deadlineErr yields an error that unwraps to context.DeadlineExceeded exactly
// as a real expired request context would.
func deadlineErr(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	return ctx.Err()
}

func TestExpiredContextRendersGatewayTimeout(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody), deadlineErr(t))

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("a timeout is retryable and should carry Retry-After")
	}
}
