package dashboard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmbeddedDashboardAssetsAndPolicy(t *testing.T) {
	for _, path := range []string{"/dashboard/", "/dashboard/app.js", "/dashboard/core.js", "/dashboard/demo.js", "/dashboard/dashboard.css", "/dashboard/favicon.svg"} {
		w := httptest.NewRecorder()
		Handler().ServeHTTP(w, httptest.NewRequest("GET", path, http.NoBody))
		if w.Code != http.StatusOK || w.Body.Len() == 0 {
			t.Fatalf("%s: %d", path, w.Code)
		}
		if !strings.Contains(w.Header().Get("Content-Security-Policy"), "script-src 'self'") {
			t.Fatal("missing script policy")
		}
		if w.Header().Get("Cache-Control") != "no-cache" {
			t.Fatal("unversioned assets must revalidate")
		}
	}
	w := httptest.NewRecorder()
	Handler().ServeHTTP(w, httptest.NewRequest("GET", "/dashboard/secret.env", http.NoBody))
	if w.Code != 404 {
		t.Fatalf("unexpected file exposed: %d", w.Code)
	}
}
