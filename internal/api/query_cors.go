package api

import (
	"net/http"
	"strings"
)

// queryCORS grants browser reads only to explicitly configured origins. It does
// not grant credentials or bypass tenant authentication, and never enables
// ingestion. Vary applies even to disallowed origins to prevent cache leakage.
func queryCORS(origins []string) func(http.Handler) http.Handler {
	allowed := make(map[string]bool, len(origins))
	for _, origin := range origins {
		allowed[origin] = true
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v1/query", "/v1/metrics", "/v1/labels", PathStream:
			default:
				next.ServeHTTP(w, r)
				return
			}
			w.Header().Add("Vary", "Origin")
			if !allowed[r.Header.Get("Origin")] {
				next.ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodOptions {
				w.Header().Add("Vary", "Access-Control-Request-Method")
				w.Header().Add("Vary", "Access-Control-Request-Headers")
				if r.Header.Get("Access-Control-Request-Method") != http.MethodGet {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				for _, header := range strings.Split(r.Header.Get("Access-Control-Request-Headers"), ",") {
					switch strings.ToLower(strings.TrimSpace(header)) {
					case "", "authorization", "accept":
					default:
						w.WriteHeader(http.StatusForbidden)
						return
					}
				}
				w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))
				w.Header().Set("Access-Control-Allow-Methods", "GET")
				w.Header().Set("Access-Control-Allow-Headers", "Authorization, Accept")
				w.Header().Set("Access-Control-Max-Age", "600")
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if r.Method == http.MethodGet {
				w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))
				w.Header().Set("Access-Control-Expose-Headers", "Retry-After, X-Request-Id")
			}
			next.ServeHTTP(w, r)
		})
	}
}
