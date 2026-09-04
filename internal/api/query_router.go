package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/jon-jc/fluxgate/internal/auth"
	"github.com/jon-jc/fluxgate/internal/config"
	"github.com/jon-jc/fluxgate/internal/httpx"
	"github.com/jon-jc/fluxgate/internal/observability"
)

// QueryRouterDeps are the collaborators the read API needs.
type QueryRouterDeps struct {
	Config config.Config
	Logger *slog.Logger
	Health *observability.Health
	Auth   auth.Options
	Query  QueryDeps
	// Metrics instruments the HTTP surface. Optional.
	Metrics *observability.Metrics
}

// NewQueryRouter builds the HTTP handler for the read API.
//
// It is a separate router from the ingest one, in a separate binary, because
// reads and writes scale on different axes and fail in different ways: a
// dashboard issuing an expensive query should not be able to slow down
// telemetry ingestion, and an ingest spike should not make dashboards
// unreadable.
func NewQueryRouter(deps QueryRouterDeps) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET "+PathLiveness, deps.Health.LivenessHandler())
	mux.Handle("GET "+PathReadiness, deps.Health.ReadinessHandler())
	mux.Handle("GET /v1/version", httpx.Handler(handleVersion))

	mountMetrics(mux, deps.Config, deps.Metrics)

	authenticated := auth.Middleware(deps.Auth)

	mux.Handle("GET /v1/query", authenticated(handleQuery(deps.Query)))
	mux.Handle("GET /v1/metrics", authenticated(handleMetrics(deps.Query)))
	mux.Handle("GET /v1/labels", authenticated(handleLabels(deps.Query)))
	mux.Handle("GET /v1/stream", authenticated(handleStream(deps.Query)))

	mux.Handle("/", handleUnmatched(mux))

	// The streaming endpoint is the reason this stack differs from the ingest
	// one: a request timeout would cut a live tail off mid-connection, and the
	// access log's slow-request warning would fire on every healthy stream.
	// The stream bounds itself instead, through StreamOptions.MaxDuration.
	stack := httpx.Chain(
		httpx.RequestID,
		httpx.RealIP(deps.Config.HTTP.TrustedProxyHeader),
		httpx.Trace(mux),
		httpx.Metrics(deps.Metrics, mux),
		httpx.Recoverer,
		httpx.AccessLog(httpx.AccessLogOptions{
			SkipPaths: []string{PathLiveness, PathReadiness},
			// Well above any ordinary read, so the warning still means
			// something on the query endpoints.
			SlowRequestThreshold: 5 * time.Second,
		}),
		httpx.SecurityHeaders,
		auth.WWWAuthenticate,
		httpx.MaxBytes(deps.Config.HTTP.MaxRequestBytes),
	)

	return withBaseLogger(deps.Logger, timeoutExceptStream(
		deps.Config.HTTP.HandlerTimeout, stack(mux)))
}

// PathStream is the live-tail endpoint, exempted from the request timeout.
const PathStream = "/v1/stream"

// timeoutExceptStream applies the handler timeout to everything but the live
// tail.
//
// A blanket timeout would sever every stream at the deadline, which a client
// cannot distinguish from a server fault: it would reconnect, be cut off again,
// and settle into a reconnect loop that looks exactly like an outage. The
// stream instead bounds itself with its own, much longer, budget.
func timeoutExceptStream(d time.Duration, next http.Handler) http.Handler {
	timed := httpx.RequestTimeout(d)(next)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == PathStream {
			next.ServeHTTP(w, r)
			return
		}
		timed.ServeHTTP(w, r)
	})
}
