package auth

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/jon-jc/fluxgate/internal/httpx"
	"github.com/jon-jc/fluxgate/internal/observability"
)

type principalKey struct{}

// ContextWithPrincipal attaches an authenticated caller to ctx.
func ContextWithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFromContext returns the authenticated caller. The boolean reports
// whether the request passed through authentication at all, which lets a
// handler fail loudly rather than silently treating an unauthenticated request
// as belonging to the zero-value tenant.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// AnonymousTenant is the tenant assigned when authentication is disabled for
// local development.
const AnonymousTenant = "anonymous"

// Options configures the authentication middleware.
type Options struct {
	// Store resolves key IDs. Required unless Disabled is set.
	Store Store
	// Disabled turns authentication off and assigns every request to
	// AnonymousTenant. Configuration validation refuses to allow this outside
	// local and dev tiers, so it cannot reach a deployed environment.
	Disabled bool
}

// Middleware authenticates every request it wraps.
//
// All credential failures render an identical 401: a client that learns
// "unknown key" versus "bad secret" has been handed an oracle for enumerating
// valid key IDs. The specific reason is logged server-side, where it is useful
// and not attacker-visible.
func Middleware(opts Options) httpx.Middleware {
	return func(next http.Handler) http.Handler {
		return httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
			if opts.Disabled {
				ctx := ContextWithPrincipal(r.Context(), Principal{
					TenantID: AnonymousTenant,
					KeyID:    "anonymous",
				})
				next.ServeHTTP(w, r.WithContext(ctx))
				return nil
			}

			token, err := bearerToken(r)
			if err != nil {
				return unauthorized(w, r, err, "")
			}

			principal, err := Verify(opts.Store, token)
			if err != nil {
				return unauthorized(w, r, err, keyIDFor(token))
			}

			ctx := ContextWithPrincipal(r.Context(), principal)

			// Bind the tenant to the logger so every record for this request
			// is attributable without a handler having to add it.
			log := observability.LoggerFromContext(ctx).With(
				slog.String(observability.KeyTenantID, principal.TenantID),
				slog.String("key_id", principal.KeyID))
			ctx = observability.ContextWithLogger(ctx, log)

			next.ServeHTTP(w, r.WithContext(ctx))
			return nil
		})
	}
}

// keyIDFor extracts just the key ID for logging. The secret half is never
// logged, even at debug level -- log aggregators are a common place for
// credentials to end up somewhere they should not be.
func keyIDFor(token string) string {
	keyID, _, err := splitToken(token)
	if err != nil {
		return ""
	}
	return keyID
}

func unauthorized(_ http.ResponseWriter, r *http.Request, cause error, keyID string) error {
	observability.LoggerFromContext(r.Context()).Warn("authentication failed",
		slog.String("reason", cause.Error()),
		slog.String("key_id", keyID))

	return httpx.Unauthorized(
		"A valid API key is required. Send it as: Authorization: Bearer fxg_<key id>_<secret>.").
		WithCause(cause)
}

// bearerToken extracts the credential from the Authorization header.
func bearerToken(r *http.Request) (string, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", ErrMalformedCredential
	}

	scheme, token, ok := strings.Cut(header, " ")
	// The scheme is case-insensitive per RFC 9110; some clients send "bearer".
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", ErrMalformedCredential
	}

	token = strings.TrimSpace(token)
	if token == "" {
		return "", ErrMalformedCredential
	}
	return token, nil
}

// WWWAuthenticate sets the challenge header RFC 9110 requires on a 401. It is
// applied by the router rather than the middleware so that the header is
// present on every 401 the API produces, including those raised by handlers.
func WWWAuthenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &challengeWriter{ResponseWriter: w}
		next.ServeHTTP(rec, r)
	})
}

// challengeWriter adds the Bearer challenge to any 401 passing through it.
type challengeWriter struct {
	http.ResponseWriter
	wroteHeader bool
}

// WriteHeader attaches the Bearer challenge to a 401 on its way out, and
// ignores a stray second call so the challenge cannot be added twice.
func (c *challengeWriter) WriteHeader(code int) {
	if c.wroteHeader {
		return
	}
	c.wroteHeader = true
	if code == http.StatusUnauthorized {
		c.Header().Set("WWW-Authenticate", `Bearer realm="fluxgate"`)
	}
	c.ResponseWriter.WriteHeader(code)
}

// Write defaults the status to 200 for a handler that writes a body without
// calling WriteHeader first.
func (c *challengeWriter) Write(b []byte) (int, error) {
	if !c.wroteHeader {
		c.WriteHeader(http.StatusOK)
	}
	return c.ResponseWriter.Write(b)
}

// Flush forwards to the underlying writer so streaming endpoints keep working.
func (c *challengeWriter) Flush() {
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the original writer to http.ResponseController.
func (c *challengeWriter) Unwrap() http.ResponseWriter { return c.ResponseWriter }
