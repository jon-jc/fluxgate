package httpx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAdmissionSharedAcrossRoutesAndHeldUntilHandlerExits(t *testing.T) {
	admit := AdmitConcurrent(1)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	first := admit(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))
	second := admit(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		defer close(done)
		first.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", http.NoBody).WithContext(ctx))
	}()
	<-entered
	cancel() // Cancellation alone must not release a still-running handler.
	r := httptest.NewRecorder()
	second.ServeHTTP(r, httptest.NewRequest("GET", "/", http.NoBody))
	close(release)
	<-done
	if r.Code != 503 || r.Header().Get("Retry-After") == "" {
		t.Fatalf("overload response: %d %s", r.Code, r.Body.String())
	}
	r = httptest.NewRecorder()
	second.ServeHTTP(r, httptest.NewRequest("GET", "/", http.NoBody))
	if r.Code != 204 {
		t.Fatalf("slot not released: %d", r.Code)
	}
}
