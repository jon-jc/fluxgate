// Package api assembles the public HTTP surface of the ingest service.
package api

import (
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jon-jc/fluxgate/internal/auth"
	"github.com/jon-jc/fluxgate/internal/config"
	"github.com/jon-jc/fluxgate/internal/httpx"
	"github.com/jon-jc/fluxgate/internal/observability"
	"github.com/jon-jc/fluxgate/internal/version"
)

// Deps are the collaborators the router needs. Passing them explicitly keeps
// the package free of package-level state, which is what makes the whole
// surface testable without a running process.
type Deps struct {
	Config config.Config
	Logger *slog.Logger
	Health *observability.Health
	// Auth configures credential checking for the authenticated route group.
	Auth auth.Options
	// Ingest supplies the collaborators for POST /v1/ingest.
	Ingest IngestDeps
}

// Probe paths are mounted outside the versioned namespace because
// orchestrators, not API clients, consume them; they must never change shape
// across API versions.
const (
	PathLiveness  = "/healthz"
	PathReadiness = "/readyz"
)

// NewRouter builds the fully decorated HTTP handler for the ingest service.
func NewRouter(deps Deps) http.Handler {
	mux := http.NewServeMux()

	// Probes bypass the versioned prefix and are exempt from access logging
	// while healthy -- see the AccessLog options below.
	mux.Handle("GET "+PathLiveness, deps.Health.LivenessHandler())
	mux.Handle("GET "+PathReadiness, deps.Health.ReadinessHandler())

	mux.Handle("GET /v1/version", httpx.Handler(handleVersion))

	// Everything that touches tenant data goes through authentication. Applying
	// it per-route rather than to the whole mux keeps the probes reachable by an
	// orchestrator that holds no credentials, and makes the authenticated set
	// something a reviewer can see at a glance rather than infer from a path
	// prefix convention.
	authenticated := auth.Middleware(deps.Auth)
	mux.Handle("POST /v1/ingest", authenticated(handleIngest(deps.Ingest)))

	// ServeMux's implicit 404 is a plain-text body, which would make this the
	// only endpoint in the API that does not speak problem+json. Registering a
	// catch-all also shadows the mux's own 405 handling, so the fallback has to
	// reconstruct it -- see handleUnmatched.
	mux.Handle("/", handleUnmatched(mux))

	stack := httpx.Chain(
		// Outermost first: a panic in any later layer still gets a request ID
		// attached to its log record.
		httpx.RequestID,
		httpx.RealIP(deps.Config.HTTP.TrustedProxyHeader),
		httpx.Recoverer,
		httpx.AccessLog(httpx.AccessLogOptions{
			SkipPaths:            []string{PathLiveness, PathReadiness},
			SlowRequestThreshold: 1 * time.Second,
		}),
		httpx.SecurityHeaders,
		// Applied above the route table so the challenge accompanies every 401
		// the API can produce, not only those raised inside the auth middleware.
		auth.WWWAuthenticate,
		httpx.RequestTimeout(deps.Config.HTTP.HandlerTimeout),
		httpx.MaxBytes(deps.Config.HTTP.MaxRequestBytes),
	)

	return withBaseLogger(deps.Logger, stack(mux))
}

// withBaseLogger seeds the request context with the process logger so that
// middleware and handlers never fall back to slog's global default.
func withBaseLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := observability.ContextWithLogger(r.Context(), logger)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func handleVersion(w http.ResponseWriter, r *http.Request) error {
	return httpx.WriteJSON(w, r, http.StatusOK, version.Get())
}

// probeMethods are the verbs handleUnmatched tests a path against when
// deciding between 404 and 405.
var probeMethods = []string{
	http.MethodGet,
	http.MethodHead,
	http.MethodPost,
	http.MethodPut,
	http.MethodPatch,
	http.MethodDelete,
	http.MethodOptions,
}

// handleUnmatched serves every request the route table did not claim.
//
// A registered catch-all takes precedence over the mux's built-in 405, so
// without this a POST to a GET-only route would be reported as "no such route"
// -- sending the caller off to check the path when the path was fine. The
// handler re-probes the mux with each verb to work out which ones the path does
// accept, and answers 405 with the Allow header RFC 9110 requires.
func handleUnmatched(mux *http.ServeMux) httpx.Handler {
	return func(w http.ResponseWriter, r *http.Request) error {
		if allowed := allowedMethods(mux, r); len(allowed) > 0 {
			w.Header().Set("Allow", strings.Join(allowed, ", "))
			return httpx.MethodNotAllowed(
				r.Method + " is not supported on " + r.URL.Path + ".")
		}
		return httpx.NotFound("No route matches " + r.Method + " " + r.URL.Path + ".")
	}
}

// allowedMethods returns the verbs the mux would route for this path, ignoring
// the catch-all itself.
func allowedMethods(mux *http.ServeMux, r *http.Request) []string {
	var allowed []string
	for _, method := range probeMethods {
		probe := &http.Request{
			Method: method,
			URL:    &url.URL{Path: r.URL.Path},
			Host:   r.Host,
		}
		// An empty pattern means the mux would redirect rather than route, and
		// the catch-all matches everything by definition; neither counts.
		if _, pattern := mux.Handler(probe); pattern != "" && pattern != "/" {
			allowed = append(allowed, method)
		}
	}
	return allowed
}
