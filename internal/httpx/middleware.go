package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/jon-jc/fluxgate/internal/observability"
)

// Middleware decorates an http.Handler.
type Middleware func(http.Handler) http.Handler

// Chain composes middleware into a single Middleware.
//
// The first argument ends up outermost, so Chain(A, B, C) yields A(B(C(next)))
// and requests traverse A, B, C in the order written. That ordering matters:
// a panic recovered by A can only be logged with a request ID if the ID
// middleware ran before it.
func Chain(mw ...Middleware) Middleware {
	return func(next http.Handler) http.Handler {
		for i := len(mw) - 1; i >= 0; i-- {
			next = mw[i](next)
		}
		return next
	}
}

// HeaderRequestID is the header used to propagate a correlation ID across
// service hops.
const HeaderRequestID = "X-Request-Id"

type requestIDKey struct{}

type clientIPKey struct{}

// RequestIDFromContext returns the correlation ID for the request, or "" when
// the RequestID middleware did not run.
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// ContextWithRequestID attaches a correlation ID to ctx. Consumers of
// asynchronous messages use it to continue a trace started at the edge.
func ContextWithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// ClientIPFromContext returns the caller's IP as resolved by RealIP.
func ClientIPFromContext(ctx context.Context) string {
	ip, _ := ctx.Value(clientIPKey{}).(string)
	return ip
}

// NewRequestID returns a random 128-bit identifier as lowercase hex.
//
// The width and encoding deliberately match a W3C trace ID so the same value
// can seed a trace once distributed tracing is wired in.
func NewRequestID() string {
	var b [16]byte
	// crypto/rand.Read is documented never to fail as of Go 1.24; it panics
	// internally on an unusable entropy source, which is the correct outcome
	// for a process that can no longer generate unguessable identifiers.
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// RequestID ensures every request carries a correlation ID, reusing a valid
// inbound one so a trace survives across service boundaries, and echoes it on
// the response so a client can quote it in a bug report.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := sanitizeRequestID(r.Header.Get(HeaderRequestID))
		if id == "" {
			id = NewRequestID()
		}

		w.Header().Set(HeaderRequestID, id)
		ctx := ContextWithRequestID(r.Context(), id)

		// Bind the ID to the logger once so every downstream record carries
		// it without any handler having to remember to add it.
		log := observability.LoggerFromContext(ctx).
			With(slog.String(observability.KeyRequestID, id))
		ctx = observability.ContextWithLogger(ctx, log)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// maxRequestIDLen bounds an inbound correlation ID. Without a cap, a client
// could push an arbitrarily large string into every log line this request
// produces.
const maxRequestIDLen = 64

// sanitizeRequestID accepts an inbound ID only if it is short and composed of
// characters that cannot forge structure in a log line or response header.
func sanitizeRequestID(id string) string {
	if id == "" || len(id) > maxRequestIDLen {
		return ""
	}
	for _, c := range id {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9', c == '-', c == '_':
		default:
			return ""
		}
	}
	return id
}

// RealIP resolves the caller's IP address.
//
// X-Forwarded-For is trusted only when the deployment sits behind a proxy that
// rewrites it. Honouring it unconditionally would let any client spoof its
// address and walk straight through per-IP rate limits.
func RealIP(trustProxyHeader bool) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := remoteIP(r.RemoteAddr)

			if trustProxyHeader {
				if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
					// The left-most entry is the original client; everything
					// after it was appended by successive proxies.
					if first, _, _ := strings.Cut(fwd, ","); first != "" {
						if parsed := net.ParseIP(strings.TrimSpace(first)); parsed != nil {
							ip = parsed.String()
						}
					}
				}
			}

			ctx := context.WithValue(r.Context(), clientIPKey{}, ip)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func remoteIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

// Recoverer converts a panic into a 500 so that one poisoned request cannot
// take down a process serving thousands of healthy ones.
func Recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}

			// http.ErrAbortHandler is the documented way for a handler to
			// abandon a response deliberately; re-panicking preserves that
			// contract and lets net/http close the connection quietly.
			if rec == http.ErrAbortHandler {
				panic(rec)
			}

			observability.LoggerFromContext(r.Context()).Error("handler panic",
				slog.Any("panic", rec),
				slog.String("stack", string(debug.Stack())))

			WriteError(w, r, Internal(&panicError{value: rec}))
		}()

		next.ServeHTTP(w, r)
	})
}

// panicError adapts a recovered value to the error interface so it flows
// through the normal error path.
type panicError struct{ value any }

func (p *panicError) Error() string { return "panic: " + stringify(p.value) }

func stringify(v any) string {
	if err, ok := v.(error); ok {
		return err.Error()
	}
	if s, ok := v.(string); ok {
		return s
	}
	return "unprintable panic value"
}

// AccessLogOptions tunes request logging.
type AccessLogOptions struct {
	// SkipPaths are logged only when they fail. Probe endpoints are scraped
	// every few seconds and would otherwise drown out real traffic.
	SkipPaths []string
	// SlowRequestThreshold promotes a successful but sluggish request to a
	// warning. Zero disables the promotion.
	SlowRequestThreshold time.Duration
}

// AccessLog emits one structured record per request.
//
// Severity follows the outcome: 5xx logs at error, 4xx at warn, and everything
// else at info, so an alert on error-level records tracks server faults rather
// than client mistakes.
func AccessLog(opts AccessLogOptions) Middleware {
	skip := make(map[string]struct{}, len(opts.SkipPaths))
	for _, p := range opts.SkipPaths {
		skip[p] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r)

			elapsed := time.Since(start)
			if _, quiet := skip[r.URL.Path]; quiet && rec.status < http.StatusBadRequest {
				return
			}

			log := observability.LoggerFromContext(r.Context())
			attrs := []any{
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Int64("bytes", rec.bytes),
				slog.Duration("duration", elapsed),
				slog.String("client_ip", ClientIPFromContext(r.Context())),
				slog.String("user_agent", r.UserAgent()),
			}

			switch {
			case rec.status >= http.StatusInternalServerError:
				log.Error("request completed", attrs...)
			case rec.status >= http.StatusBadRequest:
				log.Warn("request completed", attrs...)
			case opts.SlowRequestThreshold > 0 && elapsed > opts.SlowRequestThreshold:
				log.Warn("slow request", attrs...)
			default:
				log.Info("request completed", attrs...)
			}
		})
	}
}

// RequestTimeout bounds how long a handler may run by giving the request
// context a deadline.
//
// This is deliberately not http.TimeoutHandler, which buffers the entire
// response in memory to be able to discard it -- that breaks streaming
// endpoints and adds an allocation proportional to every response body.
// Handlers here are expected to honour the context, and a deadline that fires
// is rendered as a 504 by the error mapper.
func RequestTimeout(d time.Duration) Middleware {
	return func(next http.Handler) http.Handler {
		if d <= 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// MaxBytes caps the request body size before a handler ever reads it, so an
// oversized upload is rejected without being buffered.
func MaxBytes(n int64) Middleware {
	return func(next http.Handler) http.Handler {
		if n <= 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, n)
			next.ServeHTTP(w, r)
		})
	}
}

// SecurityHeaders sets the response headers appropriate for a JSON API.
//
// The CSP and frame-ancestors directives matter even for an API: they stop a
// JSON response from being rendered or embedded if a browser is ever tricked
// into navigating to one directly.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		next.ServeHTTP(w, r)
	})
}

// responseRecorder captures the status code and body size for the access log.
type responseRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (rr *responseRecorder) WriteHeader(code int) {
	if rr.wroteHeader {
		// net/http already warns about a duplicate WriteHeader; swallowing it
		// here keeps the recorded status equal to what the client received.
		return
	}
	rr.status = code
	rr.wroteHeader = true
	rr.ResponseWriter.WriteHeader(code)
}

func (rr *responseRecorder) Write(b []byte) (int, error) {
	if !rr.wroteHeader {
		rr.WriteHeader(http.StatusOK)
	}
	n, err := rr.ResponseWriter.Write(b)
	rr.bytes += int64(n)
	return n, err
}

// Flush forwards to the underlying writer so streaming endpoints keep working
// through the recorder.
func (rr *responseRecorder) Flush() {
	if f, ok := rr.ResponseWriter.(http.Flusher); ok {
		if !rr.wroteHeader {
			rr.WriteHeader(http.StatusOK)
		}
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the original writer for
// capabilities the recorder does not implement itself, such as deadlines and
// connection hijacking.
func (rr *responseRecorder) Unwrap() http.ResponseWriter { return rr.ResponseWriter }
