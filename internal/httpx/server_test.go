package httpx

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/config"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testServerOptions(handler http.Handler) ServerOptions {
	return ServerOptions{
		HTTP: config.HTTPConfig{
			Addr:              "127.0.0.1:0", // let the kernel pick a free port
			ReadHeaderTimeout: time.Second,
			ReadTimeout:       5 * time.Second,
			WriteTimeout:      5 * time.Second,
			IdleTimeout:       time.Second,
		},
		Shutdown: config.ShutdownConfig{
			GracePeriod:  0, // exercised separately; zero keeps tests quick
			DrainTimeout: 5 * time.Second,
		},
		Handler: handler,
		Logger:  discardLogger(),
	}
}

// startServer runs srv until the returned cancel func is called, and reports
// the run error on the returned channel.
func startServer(t *testing.T, srv *Server) (cancel context.CancelFunc, done <-chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run(ctx) }()

	// Wait for the listener to bind so callers have a real address to dial.
	deadline := time.Now().Add(5 * time.Second)
	for srv.Addr() == "" {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("server did not bind within 5s")
		}
		time.Sleep(time.Millisecond)
	}
	return cancel, errCh
}

func TestServerServesAndStopsCleanly(t *testing.T) {
	srv := NewServer(testServerOptions(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "pong")
		})))

	cancel, done := startServer(t, srv)

	resp, err := http.Get("http://" + srv.Addr() + "/ping")
	if err != nil {
		cancel()
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if string(body) != "pong" {
		t.Errorf("body = %q, want pong", body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil after a clean shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server did not shut down within 10s")
	}
}

// TestServerDrainsInFlightRequest is the property that makes a rolling deploy
// safe: a request already being served when SIGTERM arrives must finish, not
// be cut off mid-response.
func TestServerDrainsInFlightRequest(t *testing.T) {
	released := make(chan struct{})
	started := make(chan struct{})

	srv := NewServer(testServerOptions(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(started)
			<-released
			_, _ = io.WriteString(w, "finished")
		})))

	cancel, done := startServer(t, srv)

	type result struct {
		body string
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		resp, err := http.Get("http://" + srv.Addr() + "/slow")
		if err != nil {
			resCh <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		resCh <- result{body: string(b), err: err}
	}()

	<-started
	cancel() // SIGTERM arrives mid-request

	// Give shutdown a moment to begin, then let the handler complete.
	time.Sleep(50 * time.Millisecond)
	close(released)

	select {
	case res := <-resCh:
		if res.err != nil {
			t.Fatalf("in-flight request failed during drain: %v", res.err)
		}
		if res.body != "finished" {
			t.Errorf("body = %q, want finished", res.body)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight request never completed")
	}

	if err := <-done; err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
}

func TestServerRunsOnDrainBeforeClosing(t *testing.T) {
	drained := make(chan struct{})

	opts := testServerOptions(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	opts.OnDrain = func() { close(drained) }

	srv := NewServer(opts)
	cancel, done := startServer(t, srv)
	cancel()

	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("OnDrain was never called")
	}
	<-done
}

func TestServerReportsListenFailure(t *testing.T) {
	opts := testServerOptions(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	opts.HTTP.Addr = "127.0.0.1:-1"

	err := NewServer(opts).Run(context.Background())
	if err == nil {
		t.Fatal("expected a listen error for an invalid address")
	}
}

// TestServerForcesCloseWhenDrainBudgetExpires covers the escape hatch: a
// handler that ignores its context must not stop the process from exiting
// before the orchestrator escalates to SIGKILL.
func TestServerForcesCloseWhenDrainBudgetExpires(t *testing.T) {
	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })

	started := make(chan struct{})
	opts := testServerOptions(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(started)
		<-stuck
	}))
	opts.Shutdown.DrainTimeout = 100 * time.Millisecond

	srv := NewServer(opts)
	cancel, done := startServer(t, srv)

	go func() {
		resp, err := http.Get("http://" + srv.Addr() + "/stuck")
		if err == nil {
			_ = resp.Body.Close()
		}
	}()

	<-started
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run returned nil, want a drain-timeout error")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error = %v, want it to wrap context.DeadlineExceeded", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server never gave up on the stuck handler")
	}
}

func TestServerHonoursGracePeriodBeforeClosing(t *testing.T) {
	opts := testServerOptions(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	opts.Shutdown.GracePeriod = 250 * time.Millisecond

	srv := NewServer(opts)
	cancel, done := startServer(t, srv)

	start := time.Now()
	cancel()

	// Requests arriving during the grace period are still served: the load
	// balancer has not necessarily noticed the failing readiness probe yet.
	resp, err := http.Get("http://" + srv.Addr() + "/late")
	if err != nil {
		t.Fatalf("request during grace period failed: %v", err)
	}
	_ = resp.Body.Close()

	<-done
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Errorf("shutdown took %v, want at least the 250ms grace period", elapsed)
	}
}
