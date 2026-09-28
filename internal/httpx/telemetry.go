package httpx

import (
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
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

// TelemetryOptions tunes what the tracing and metrics middleware observe.
type TelemetryOptions struct {
	// TrustTraceParent is only for a gateway that authenticates trace context.
	TrustTraceParent bool
	// SkipRoutes are route patterns that produce no span and no metric.
	//
	// Probes and the scrape endpoint belong here. An orchestrator polls
	// readiness every few seconds and Prometheus scrapes on its own interval,
	// so tracing them buries real requests under machine traffic -- and
	// metering the scrape endpoint means the act of reading metrics changes
	// them.
	SkipRoutes []string
}

func (o TelemetryOptions) skipSet() map[string]struct{} {
	skip := make(map[string]struct{}, len(o.SkipRoutes))
	for _, route := range o.SkipRoutes {
		skip[route] = struct{}{}
	}
	return skip
}

// Trace starts a server span for every request. Public parents are linked;
// only explicitly trusted gateways may select the parent and sampling decision.
//
// It runs outside the metrics middleware so the span covers the whole
// measured request, and inside RequestID so that a log line, a metric and a
// span all agree on which request they describe.
func Trace(routes RouteResolver, opts TelemetryOptions) Middleware {
	skip := opts.skipSet()

	return func(next http.Handler) http.Handler {
		return traceHandler(routes, skip, opts.TrustTraceParent, next)
	}
}

func traceHandler(routes RouteResolver, skip map[string]struct{}, trustParent bool, next http.Handler) http.Handler {
	tracer := observability.Tracer(tracerName)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, quiet := skip[RoutePattern(routes, r)]; quiet {
			next.ServeHTTP(w, r)
			return
		}

		// Public callers must not force sampling or propagate arbitrary baggage
		// into broker messages. Link their trace for correlation, but make a
		// fresh local sampling decision unless an authenticated gateway is trusted.
		ctx := baggage.ContextWithoutBaggage(r.Context())
		externalCtx := propagation.TraceContext{}.Extract(ctx, propagation.HeaderCarrier(r.Header))
		parent := trace.SpanContextFromContext(externalCtx)
		startOptions := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindServer)}
		if trustParent {
			ctx = externalCtx
		} else {
			startOptions = append(startOptions, trace.WithNewRoot())
			if parent.IsValid() {
				startOptions = append(startOptions, trace.WithLinks(trace.Link{SpanContext: parent.WithTraceState(trace.TraceState{})}))
			}
		}

		route := RoutePattern(routes, r)
		method := observability.HTTPMethod(r.Method)

		// Go 1.22 route patterns already begin with the method, so prefixing
		// again would name every span "POST POST /v1/ingest". Only the
		// unmatched label needs the verb attached.
		spanName := route
		if route == UnmatchedRoute {
			spanName = method + " " + route
		}

		startOptions = append(startOptions, trace.WithAttributes(
			semconv.HTTPRequestMethodKey.String(method),
			semconv.HTTPRoute(route),
			semconv.URLPath(r.URL.Path),
			semconv.UserAgentOriginal(r.UserAgent()),
			attribute.String("request.id", RequestIDFromContext(ctx)),
		))
		ctx, span := tracer.Start(ctx, spanName, startOptions...)
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
func Metrics(m *observability.Metrics, routes RouteResolver, opts TelemetryOptions) Middleware {
	skip := opts.skipSet()

	return func(next http.Handler) http.Handler {
		if m == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			route := RoutePattern(routes, r)
			if _, quiet := skip[route]; quiet {
				next.ServeHTTP(w, r)
				return
			}

			done := m.TrackInFlight()
			defer done()

			start := time.Now()
			rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r)

			m.ObserveRequest(route, r.Method, rec.status, time.Since(start))
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
