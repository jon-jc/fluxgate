package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/observability"
)

// captureLogs returns a logger writing JSON into buf, plus a helper that
// decodes the records emitted so far.
func captureLogs(t *testing.T) (logger *slog.Logger, records func() []map[string]any) {
	t.Helper()

	var (
		mu  sync.Mutex
		buf bytes.Buffer
	)
	logger = slog.New(slog.NewJSONHandler(&lockedWriter{mu: &mu, w: &buf},
		&slog.HandlerOptions{Level: slog.LevelDebug}))

	return logger, func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()

		var records []map[string]any
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if line == "" {
				continue
			}
			var rec map[string]any
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatalf("decode log line %q: %v", line, err)
			}
			records = append(records, rec)
		}
		return records
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func TestChainAppliesOutermostFirst(t *testing.T) {
	var order []string

	mark := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name)
				next.ServeHTTP(w, r)
			})
		}
	}

	h := Chain(mark("a"), mark("b"), mark("c"))(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			order = append(order, "handler")
		}))

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	want := []string{"a", "b", "c", "handler"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Errorf("order = %v, want %v", order, want)
	}
}

func TestRequestIDGeneratesAndEchoes(t *testing.T) {
	var seen string
	h := RequestID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = RequestIDFromContext(r.Context())
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	if seen == "" {
		t.Fatal("no request ID in context")
	}
	if len(seen) != 32 {
		t.Errorf("request ID = %q, want 32 hex characters", seen)
	}
	if got := rec.Header().Get(HeaderRequestID); got != seen {
		t.Errorf("response header = %q, want %q", got, seen)
	}
}

func TestRequestIDIsUniquePerRequest(t *testing.T) {
	seen := make(map[string]bool, 128)
	h := RequestID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		id := RequestIDFromContext(r.Context())
		if seen[id] {
			t.Errorf("duplicate request ID %q", id)
		}
		seen[id] = true
	}))

	for range 128 {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", http.NoBody))
	}
}

func TestRequestIDPropagatesInboundValue(t *testing.T) {
	var seen string
	h := RequestID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = RequestIDFromContext(r.Context())
	}))

	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	r.Header.Set(HeaderRequestID, "upstream-trace-1")
	h.ServeHTTP(httptest.NewRecorder(), r)

	if seen != "upstream-trace-1" {
		t.Errorf("request ID = %q, want the inbound value to be reused", seen)
	}
}

func TestRequestIDRejectsHostileInboundValues(t *testing.T) {
	tests := map[string]string{
		"header injection": "abc\r\nX-Admin: true",
		"log forging":      "abc\"} {\"severity\":\"ERROR",
		"oversized":        strings.Repeat("a", 65),
		"spaces":           "not a valid id",
	}

	for name, hostile := range tests {
		t.Run(name, func(t *testing.T) {
			var seen string
			h := RequestID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				seen = RequestIDFromContext(r.Context())
			}))

			r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
			r.Header.Set(HeaderRequestID, hostile)
			h.ServeHTTP(httptest.NewRecorder(), r)

			if seen == hostile {
				t.Fatalf("hostile request ID %q was accepted verbatim", hostile)
			}
			if len(seen) != 32 {
				t.Errorf("request ID = %q, want a freshly generated one", seen)
			}
		})
	}
}

func TestRealIPIgnoresForwardedHeaderWhenUntrusted(t *testing.T) {
	var seen string
	h := RealIP(false)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = ClientIPFromContext(r.Context())
	}))

	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	r.RemoteAddr = "203.0.113.9:44321"
	r.Header.Set("X-Forwarded-For", "1.2.3.4")
	h.ServeHTTP(httptest.NewRecorder(), r)

	// Trusting the header without a proxy in front lets any caller spoof its
	// address and slip past per-IP limits.
	if seen != "203.0.113.9" {
		t.Errorf("client IP = %q, want the socket address 203.0.113.9", seen)
	}
}

func TestRealIPUsesLeftmostForwardedEntryWhenTrusted(t *testing.T) {
	var seen string
	h := RealIP(true)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = ClientIPFromContext(r.Context())
	}))

	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	r.RemoteAddr = "10.0.0.1:44321"
	r.Header.Set("X-Forwarded-For", "1.2.3.4, 10.0.0.2, 10.0.0.1")
	h.ServeHTTP(httptest.NewRecorder(), r)

	if seen != "1.2.3.4" {
		t.Errorf("client IP = %q, want the original client 1.2.3.4", seen)
	}
}

func TestRealIPFallsBackWhenForwardedValueIsGarbage(t *testing.T) {
	var seen string
	h := RealIP(true)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = ClientIPFromContext(r.Context())
	}))

	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	r.RemoteAddr = "10.0.0.1:44321"
	r.Header.Set("X-Forwarded-For", "not-an-ip")
	h.ServeHTTP(httptest.NewRecorder(), r)

	if seen != "10.0.0.1" {
		t.Errorf("client IP = %q, want the socket address as a fallback", seen)
	}
}

func TestRecovererConvertsPanicToProblem(t *testing.T) {
	logger, records := captureLogs(t)

	h := Chain(RequestID, Recoverer)(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic("index out of range")
		}))

	r := httptest.NewRequest(http.MethodGet, "/v1/boom", http.NoBody)
	r = r.WithContext(observability.ContextWithLogger(r.Context(), logger))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	// The client must not learn anything about the internals.
	if strings.Contains(rec.Body.String(), "index out of range") {
		t.Errorf("panic value leaked to client: %s", rec.Body.String())
	}

	var sawPanic bool
	for _, rec := range records() {
		if rec["msg"] == "handler panic" {
			sawPanic = true
			if stack, _ := rec["stack"].(string); !strings.Contains(stack, "goroutine") {
				t.Error("panic record is missing a stack trace")
			}
			if rec[observability.KeyRequestID] == nil {
				t.Error("panic record is missing the request ID")
			}
		}
	}
	if !sawPanic {
		t.Error("panic was not logged")
	}
}

func TestRecovererRepanicsOnAbortHandler(t *testing.T) {
	// http.ErrAbortHandler is the documented way to abandon a response; it must
	// keep propagating so net/http closes the connection.
	defer func() {
		rec := recover()
		err, ok := rec.(error)
		if !ok || !errors.Is(err, http.ErrAbortHandler) {
			t.Errorf("recovered %v, want http.ErrAbortHandler to propagate", rec)
		}
	}()

	h := Recoverer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", http.NoBody))
}

func TestAccessLogSeverityTracksOutcome(t *testing.T) {
	tests := []struct {
		status int
		want   string
	}{
		{http.StatusOK, "INFO"},
		{http.StatusNotFound, "WARN"},
		{http.StatusInternalServerError, "ERROR"},
	}

	for _, tc := range tests {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			logger, records := captureLogs(t)

			h := AccessLog(AccessLogOptions{})(
				http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(tc.status)
				}))

			r := httptest.NewRequest(http.MethodGet, "/v1/thing", http.NoBody)
			r = r.WithContext(observability.ContextWithLogger(r.Context(), logger))
			h.ServeHTTP(httptest.NewRecorder(), r)

			recs := records()
			if len(recs) != 1 {
				t.Fatalf("got %d log records, want 1", len(recs))
			}
			if recs[0]["level"] != tc.want {
				t.Errorf("level = %v, want %s", recs[0]["level"], tc.want)
			}
			if got := recs[0]["status"]; got != float64(tc.status) {
				t.Errorf("status = %v, want %d", got, tc.status)
			}
		})
	}
}

func TestAccessLogSkipsHealthyProbesButLogsFailingOnes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		wantCount int
	}{
		{"healthy probe is silent", http.StatusOK, 0},
		{"failing probe is logged", http.StatusServiceUnavailable, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, records := captureLogs(t)

			h := AccessLog(AccessLogOptions{SkipPaths: []string{"/readyz"}})(
				http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(tc.status)
				}))

			r := httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody)
			r = r.WithContext(observability.ContextWithLogger(r.Context(), logger))
			h.ServeHTTP(httptest.NewRecorder(), r)

			if got := len(records()); got != tc.wantCount {
				t.Errorf("got %d log records, want %d", got, tc.wantCount)
			}
		})
	}
}

func TestAccessLogPromotesSlowRequests(t *testing.T) {
	logger, records := captureLogs(t)

	h := AccessLog(AccessLogOptions{SlowRequestThreshold: time.Nanosecond})(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))

	r := httptest.NewRequest(http.MethodGet, "/v1/slow", http.NoBody)
	r = r.WithContext(observability.ContextWithLogger(r.Context(), logger))
	h.ServeHTTP(httptest.NewRecorder(), r)

	recs := records()
	if len(recs) != 1 {
		t.Fatalf("got %d log records, want 1", len(recs))
	}
	if recs[0]["msg"] != "slow request" {
		t.Errorf("msg = %v, want \"slow request\"", recs[0]["msg"])
	}
}

func TestAccessLogRecordsBytesWritten(t *testing.T) {
	logger, records := captureLogs(t)

	body := "hello world"
	h := AccessLog(AccessLogOptions{})(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, body)
		}))

	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	r = r.WithContext(observability.ContextWithLogger(r.Context(), logger))
	h.ServeHTTP(httptest.NewRecorder(), r)

	recs := records()
	if len(recs) != 1 {
		t.Fatalf("got %d log records, want 1", len(recs))
	}
	if got := recs[0]["bytes"]; got != float64(len(body)) {
		t.Errorf("bytes = %v, want %d", got, len(body))
	}
	// An implicit 200 from a bare Write must still be recorded.
	if got := recs[0]["status"]; got != float64(http.StatusOK) {
		t.Errorf("status = %v, want 200", got)
	}
}

func TestRequestTimeoutSetsDeadline(t *testing.T) {
	var handlerErr error

	h := RequestTimeout(10 * time.Millisecond)(
		http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
			handlerErr = r.Context().Err()
		}))

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	if handlerErr != context.DeadlineExceeded {
		t.Errorf("context error = %v, want DeadlineExceeded", handlerErr)
	}
}

func TestRequestTimeoutIsANoOpWhenDisabled(t *testing.T) {
	var hasDeadline bool

	h := RequestTimeout(0)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, hasDeadline = r.Context().Deadline()
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	if hasDeadline {
		t.Error("a zero timeout must not impose a deadline")
	}
}

func TestMaxBytesRejectsOversizedBody(t *testing.T) {
	h := MaxBytes(16)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			WriteError(w, r, err)
		}
	}))

	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(strings.Repeat("x", 1024)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
}

func TestSecurityHeaders(t *testing.T) {
	h := SecurityHeaders(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	want := map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "no-referrer",
	}
	for k, v := range want {
		if got := rec.Header().Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	if rec.Header().Get("Content-Security-Policy") == "" {
		t.Error("Content-Security-Policy is missing")
	}
}

// TestResponseRecorderPreservesFlusher guards streaming endpoints: wrapping the
// writer for the access log must not cost the handler its ability to flush.
func TestResponseRecorderPreservesFlusher(t *testing.T) {
	var flushed bool

	h := AccessLog(AccessLogOptions{})(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			f, ok := w.(http.Flusher)
			if !ok {
				t.Error("wrapped writer no longer implements http.Flusher")
				return
			}
			_, _ = io.WriteString(w, "event: tick\n\n")
			f.Flush()
			flushed = true
		}))

	logger, _ := captureLogs(t)
	r := httptest.NewRequest(http.MethodGet, "/v1/stream", http.NoBody)
	r = r.WithContext(observability.ContextWithLogger(r.Context(), logger))
	h.ServeHTTP(httptest.NewRecorder(), r)

	if !flushed {
		t.Error("handler could not flush through the recorder")
	}
}

func TestResponseRecorderKeepsFirstStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	rr := &responseRecorder{ResponseWriter: rec, status: http.StatusOK}

	rr.WriteHeader(http.StatusCreated)
	rr.WriteHeader(http.StatusTeapot) // a stray second call must be ignored

	if rr.status != http.StatusCreated {
		t.Errorf("recorded status = %d, want 201", rr.status)
	}
	if rec.Code != http.StatusCreated {
		t.Errorf("client saw %d, want 201", rec.Code)
	}
}
