package auth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func middlewareStore(t *testing.T) *StaticStore {
	t.Helper()
	return testStore(t, func(k *Key) {
		k.RateLimitPerSecond = 42
		k.Burst = 84
	})
}

// capture runs the middleware and reports the principal the handler saw.
func capture(t *testing.T, opts Options, authHeader string) (*httptest.ResponseRecorder, Principal, bool) {
	t.Helper()

	var (
		seen  Principal
		found bool
	)
	h := Middleware(opts)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen, found = PrincipalFromContext(r.Context())
	}))

	r := httptest.NewRequest(http.MethodPost, "/v1/ingest", http.NoBody)
	if authHeader != "" {
		r.Header.Set("Authorization", authHeader)
	}

	rec := httptest.NewRecorder()
	WWWAuthenticate(h).ServeHTTP(rec, r)
	return rec, seen, found
}

func TestMiddlewarePassesThePrincipalToTheHandler(t *testing.T) {
	rec, principal, found := capture(t,
		Options{Store: middlewareStore(t)}, "Bearer fxg_k1_"+testSecret)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body)
	}
	if !found {
		t.Fatal("no principal in the handler's context")
	}
	if principal.TenantID != "acme" || principal.KeyID != "k1" {
		t.Errorf("principal = %+v, want tenant acme / key k1", principal)
	}
	// The per-key quota has to reach the handler, or the override is inert.
	if principal.RateLimitPerSecond != 42 || principal.Burst != 84 {
		t.Errorf("quota = %g/%d, want 42/84", principal.RateLimitPerSecond, principal.Burst)
	}
}

func TestMiddlewareAcceptsALowercaseScheme(t *testing.T) {
	// RFC 9110 makes the scheme case-insensitive, and real clients do send
	// "bearer".
	rec, _, found := capture(t,
		Options{Store: middlewareStore(t)}, "bearer fxg_k1_"+testSecret)

	if rec.Code != http.StatusOK || !found {
		t.Errorf("status = %d, found = %v; a lowercase scheme must be accepted", rec.Code, found)
	}
}

func TestMiddlewareRejectsBadCredentials(t *testing.T) {
	store := middlewareStore(t)

	tests := map[string]string{
		"no header":       "",
		"empty bearer":    "Bearer ",
		"wrong scheme":    "Basic fxg_k1_" + testSecret,
		"no scheme":       "fxg_k1_" + testSecret,
		"bad secret":      "Bearer fxg_k1_nope",
		"unknown key":     "Bearer fxg_absent_" + testSecret,
		"malformed token": "Bearer garbage",
	}

	for name, header := range tests {
		t.Run(name, func(t *testing.T) {
			rec, _, found := capture(t, Options{Store: store}, header)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401 (%s)", rec.Code, rec.Body)
			}
			if found {
				t.Error("the handler ran despite a failed authentication")
			}
			if got := rec.Header().Get("WWW-Authenticate"); !strings.Contains(got, "Bearer") {
				t.Errorf("WWW-Authenticate = %q, want a Bearer challenge", got)
			}
		})
	}
}

// TestFailureResponsesAreUniform denies a probing client an oracle for
// distinguishing a valid key ID from an invalid one.
func TestFailureResponsesAreUniform(t *testing.T) {
	store := middlewareStore(t)

	bodies := make([]string, 0, 3)
	for _, header := range []string{
		"Bearer fxg_k1_wrong",             // real key, wrong secret
		"Bearer fxg_absent_" + testSecret, // no such key
		"Bearer nonsense",                 // not even well-formed
	} {
		rec, _, _ := capture(t, Options{Store: store}, header)

		var problem struct {
			Detail string `json:"detail"`
			Code   string `json:"code"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
			t.Fatalf("decode problem: %v (%s)", err, rec.Body)
		}
		bodies = append(bodies, problem.Code+"|"+problem.Detail)
	}

	for i := 1; i < len(bodies); i++ {
		if bodies[i] != bodies[0] {
			t.Errorf("failure responses differ and leak which part was wrong:\n%s\n%s",
				bodies[0], bodies[i])
		}
	}
}

// TestSecretIsNotEchoedInTheResponse guards against a credential reaching a
// log aggregator by way of an error message.
func TestSecretIsNotEchoedInTheResponse(t *testing.T) {
	rec, _, _ := capture(t, Options{Store: middlewareStore(t)}, "Bearer fxg_k1_"+testSecret)

	if strings.Contains(rec.Body.String(), testSecret) {
		t.Errorf("the secret appears in the response body: %s", rec.Body.String())
	}
}

func TestDisabledAuthAssignsTheAnonymousTenant(t *testing.T) {
	rec, principal, found := capture(t, Options{Disabled: true}, "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !found {
		t.Fatal("no principal was attached")
	}
	// A handler must still find a principal, so it can rely on one being there
	// rather than branching on whether authentication happened to be on.
	if principal.TenantID != AnonymousTenant {
		t.Errorf("tenant = %q, want %q", principal.TenantID, AnonymousTenant)
	}
}

func TestPrincipalFromContextReportsAbsence(t *testing.T) {
	// A handler mounted outside the authenticated group must be able to tell,
	// rather than silently treating the request as the zero-value tenant.
	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)

	if _, found := PrincipalFromContext(r.Context()); found {
		t.Error("PrincipalFromContext found a principal on an unauthenticated request")
	}
}

func TestChallengeWriterOnlyAddsTheHeaderTo401(t *testing.T) {
	h := WWWAuthenticate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	if got := rec.Header().Get("WWW-Authenticate"); got != "" {
		t.Errorf("WWW-Authenticate = %q on a 200, want it absent", got)
	}
}
