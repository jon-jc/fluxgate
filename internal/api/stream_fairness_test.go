package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/auth"
	"github.com/jon-jc/fluxgate/internal/query"
)

func TestStreamTenantQuotaLeavesCapacityForOtherTenants(t *testing.T) {
	handler := handleStream(QueryDeps{Reader: &fakeReader{}, Limits: query.DefaultLimits(),
		Stream: StreamOptions{MaxConcurrent: 2, MaxPerTenant: 1, MaxDuration: time.Second}})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Test harness supplies authenticated identity; the real router uses the
		// verified credential, never a tenant request header.
		ctx := auth.ContextWithPrincipal(r.Context(), auth.Principal{TenantID: r.Header.Get("Test-Tenant")})
		handler.ServeHTTP(w, r.WithContext(ctx))
	}))
	defer srv.Close()
	request := func(tenant string) (int, string) {
		r, err := http.NewRequest("GET", srv.URL, http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Test-Tenant", tenant)
		resp, err := srv.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp.StatusCode, resp.Header.Get("Retry-After")
	}
	if code, _ := request("a"); code != 200 {
		t.Fatal(code)
	}
	if code, retry := request("a"); code != 429 || retry == "" {
		t.Fatal(code)
	}
	if code, _ := request("b"); code != 200 {
		t.Fatal(code)
	}
	if code, _ := request("c"); code != 503 {
		t.Fatal(code)
	}
}
