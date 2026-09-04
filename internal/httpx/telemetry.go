package httpx

import (
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/jon-jc/fluxgate/internal/observability"
)

// tracerName identifies this instrumentation in the collected spans.
const tracerName = "github.com/jon-jc/fluxgate/internal/httpx"

// RouteResolver reports which registered pattern a request matches.
//
// *http.ServeMux satisfies it. Middleware needs this because r.Pattern is only
// populated once the mux has routed -- and it sets it on an internal clone, so
// a wrapper outside the mux never sees it. Without resolving the pattern up
// front, every span would be named "unmatched" and every metric would carry
// the same useless label.
type RouteResolver interface {
	Handler(r *http.Request) (h http.Handler, pattern string)
}

// Trace starts a server span for every request, continuing an upstream trace
// when the caller supplied one.
//
// It runs outside the metrics middleware so the span covers the whole
// measured request, and inside RequestID so that a log line, a metric and a
// span all agree on which request they describe.
func Trace(routes RouteResolver) Middleware {
	return func(next http.Handler) http.Handler {
		return traceHandler(routes, next)
	}
}

func traceHandler(routes RouteResolver, next http.Handler) http.Handler {
	tracer := observability.Tracer(tracerName)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract before starting: a caller that already has a trace should
		// have this span attached to it rather than beginning a second,
		// disconnected one.
		ctx := observability.ExtractTrace(
			r.Context(), observability.HeaderCarrier(r.Header))

		route := RoutePattern(routes, r)

		// Go 1.22 route patterns already begin with the method, so prefixing
		// again would name every span "POST POST /v1/ingest". Only the
		// unmatched label needs the verb attached.
		spanName := route
		if route == UnmatchedRoute {
			spanName = r.Method + " " + route
		}

		ctx, span := tracer.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPRequestMethodKey.String(r.Method),
				semconv.HTTPRoute(route),
				semconv.URLPath(r.URL.Path),
				semconv.UserAgentOriginal(r.UserAgent()),
				attribute.String("request.id", RequestIDFromContext(ctx)),
			))
		defer span.End()

		// Bind the trace onto the logger so every record for this request can
		// be joined to the span, and vice versa. Without that join an engineer
		// holding a trace has to guess which logs relate to it.
		ctx = observability.ContextWithLogger(ctx, observability.LoggerWithTrace(ctx))

		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(ctx))

		span.SetAttributes(semconv.HTTPResponseStatusCode(rec.status))

		// Only server faults mark the span as an error. A 404 is the server
		// working correctly, and colouring it red would make every trace view
		// useless during an incident.
		if rec.status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(rec.status))
		}
	})
}

// Metrics records request counts, latency and concurrency.
func Metrics(m *observability.Metrics, routes RouteResolver) Middleware {
	return func(next http.Handler) http.Handler {
		if m == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			done := m.TrackInFlight()
			defer done()

			start := time.Now()
			rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r)

			m.ObserveRequest(
				RoutePattern(routes, r), r.Method, rec.status, time.Since(start))
		})
	}
}

// UnmatchedRoute labels a request no registered pattern claimed.
//
// It is a single constant rather than the path, so a scan for URLs that do not
// exist cannot mint a metric series per probe.
const UnmatchedRoute = "unmatched"

// RoutePattern returns the registered pattern a request matches.
//
// Labelling a metric or naming a span by the raw path would mint a new series
// per distinct URL. On an API that accepts arbitrary metric names in query
// strings, that is an unbounded label -- the exact cardinality explosion this
// system exists to help people find.
//
// Resolving through the mux costs a second route lookup per request. That is
// the price of a bounded label: the alternative is reading r.Pattern, which is
// empty at this point in the chain and would silently label every request
// "unmatched".
func RoutePattern(routes RouteResolver, r *http.Request) string {
	if pattern := r.Pattern; pattern != "" {
		return pattern
	}
	if routes != nil {
		if _, pattern := routes.Handler(r); pattern != "" {
			return pattern
		}
	}
	return UnmatchedRoute
}
