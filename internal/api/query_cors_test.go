package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestQueryCORSAllowsOnlyExplicitReadOrigins(t *testing.T) {
	for _, tc := range []struct {
		name, origin, path, method, requestedMethod, headers string
		wantStatus                                           int
		wantOrigin                                           bool
	}{
		{"allowed read still requires auth", "https://docs.example.com", "/v1/query", "GET", "", "", 401, true},
		{"preflight", "https://docs.example.com", "/v1/stream", "OPTIONS", "GET", "authorization, accept", 204, true},
		{"stream resume preflight", "https://docs.example.com", "/v1/stream", "OPTIONS", "GET", "authorization, accept, last-event-id", 204, true},
		{"resume header only on the stream", "https://docs.example.com", "/v1/query", "OPTIONS", "GET", "authorization, last-event-id", 403, false},
		{"denied origin", "https://evil.example.com", "/v1/query", "GET", "", "", 401, false},
		{"no wildcard subdomain", "https://sub.docs.example.com", "/v1/query", "GET", "", "", 401, false},
		{"no null origin", "null", "/v1/query", "GET", "", "", 401, false},
		{"no origin", "", "/v1/query", "GET", "", "", 401, false},
		{"no writes", "https://docs.example.com", "/v1/query", "OPTIONS", "POST", "authorization", 403, false},
		{"no arbitrary headers", "https://docs.example.com", "/v1/query", "OPTIONS", "GET", "x-tenant", 403, false},
		{"no ingest", "https://docs.example.com", "/v1/ingest", "OPTIONS", "GET", "authorization", 401, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := queryCORS([]string{"https://docs.example.com"})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(401) }))
			r := httptest.NewRequest(tc.method, tc.path, http.NoBody)
			r.Header.Set("Origin", tc.origin)
			r.Header.Set("Access-Control-Request-Method", tc.requestedMethod)
			r.Header.Set("Access-Control-Request-Headers", tc.headers)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.wantStatus {
				t.Fatalf("status=%d, want %d", w.Code, tc.wantStatus)
			}
			if got := w.Header().Get("Access-Control-Allow-Origin"); (got != "") != tc.wantOrigin || (got != "" && got != tc.origin) {
				t.Fatalf("allow-origin=%q", got)
			}
			if w.Header().Get("Access-Control-Allow-Credentials") != "" {
				t.Fatal("cookies must not be enabled")
			}
			if tc.path != "/v1/ingest" && !strings.Contains(strings.Join(w.Header().Values("Vary"), ","), "Origin") {
				t.Fatal("missing Vary")
			}
		})
	}
}

// A browser checks the preflight response, not only its status: the resume
// header must be named in Access-Control-Allow-Headers on the stream, and only
// there.
func TestStreamPreflightNamesTheResumeHeader(t *testing.T) {
	const origin = "https://docs.example.com"
	h := queryCORS([]string{origin})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	for path, want := range map[string]bool{"/v1/stream": true, "/v1/query": false} {
		r := httptest.NewRequest(http.MethodOptions, path, http.NoBody)
		r.Header.Set("Origin", origin)
		r.Header.Set("Access-Control-Request-Method", http.MethodGet)
		r.Header.Set("Access-Control-Request-Headers", "authorization")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if got := strings.Contains(w.Header().Get("Access-Control-Allow-Headers"), "Last-Event-ID"); got != want {
			t.Errorf("%s: allow-headers %q, want Last-Event-ID named: %v",
				path, w.Header().Get("Access-Control-Allow-Headers"), want)
		}
	}
}
