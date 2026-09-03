package httpx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/jon-jc/fluxgate/internal/config"
)

// ServerOptions configures a Server.
type ServerOptions struct {
	// HTTP holds listener and timeout settings.
	HTTP config.HTTPConfig
	// Shutdown holds the drain schedule.
	Shutdown config.ShutdownConfig
	// Handler serves requests.
	Handler http.Handler
	// Logger receives lifecycle events and net/http's internal errors.
	Logger *slog.Logger
	// OnDrain runs at the very start of shutdown, before the grace period. It
	// is where readiness is flipped to false so load balancers stop sending
	// new work to this instance.
	OnDrain func()
}

// Server runs an HTTP listener with a shutdown sequence that does not drop
// requests during a rolling deploy.
type Server struct {
	opts ServerOptions
	srv  *http.Server

	mu   sync.RWMutex
	addr string
}

// NewServer builds a Server. It does not bind a socket; call Run for that.
func NewServer(opts ServerOptions) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	s := &Server{opts: opts}
	s.srv = &http.Server{
		Handler:           opts.Handler,
		ReadHeaderTimeout: opts.HTTP.ReadHeaderTimeout,
		ReadTimeout:       opts.HTTP.ReadTimeout,
		WriteTimeout:      opts.HTTP.WriteTimeout,
		IdleTimeout:       opts.HTTP.IdleTimeout,
		// Route net/http's own errors (TLS handshake failures, malformed
		// requests) into structured logging instead of letting them escape to
		// stderr in a format nothing can parse.
		ErrorLog: slog.NewLogLogger(opts.Logger.Handler(), slog.LevelWarn),
	}
	return s
}

// Addr reports the address the listener actually bound to. It is only
// meaningful after Run has begun and is primarily useful for tests that bind
// to port zero.
func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.addr
}

// Run serves until ctx is cancelled, then drains.
//
// The shutdown sequence is deliberately three-phase:
//
//  1. Fail readiness immediately, so the load balancer stops routing new
//     requests to this instance.
//  2. Keep serving for the grace period. The instance is still in rotation
//     until the balancer notices, and requests that arrive in that window are
//     served normally rather than refused.
//  3. Close the listener and wait for in-flight requests, bounded by the
//     drain timeout.
//
// Skipping step 2 is the usual cause of a handful of 502s on every deploy: the
// process is gone before the balancer has caught up.
func (s *Server) Run(ctx context.Context) error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", s.opts.HTTP.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.opts.HTTP.Addr, err)
	}

	s.mu.Lock()
	s.addr = ln.Addr().String()
	s.mu.Unlock()

	log := s.opts.Logger
	log.Info("http server listening", slog.String("addr", s.addr))

	serveErr := make(chan error, 1)
	go func() {
		// ErrServerClosed is the expected outcome of a deliberate shutdown,
		// not a failure to report.
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	case <-ctx.Done():
		return s.drain(log, serveErr)
	}
}

func (s *Server) drain(log *slog.Logger, serveErr <-chan error) error {
	if s.opts.OnDrain != nil {
		s.opts.OnDrain()
	}

	if grace := s.opts.Shutdown.GracePeriod; grace > 0 {
		log.Info("draining: failing readiness before close",
			slog.Duration("grace_period", grace))
		// Deliberately not cancellable: the whole point is to stay in service
		// long enough for the load balancer to notice. A shorter wait here is
		// exactly the bug this phase exists to prevent.
		time.Sleep(grace)
	}

	log.Info("shutting down http server",
		slog.Duration("drain_timeout", s.opts.Shutdown.DrainTimeout))

	// Shutdown must not inherit the already-cancelled parent context, or it
	// would return instantly and defeat the drain.
	shutdownCtx, cancel := context.WithTimeout(
		context.WithoutCancel(context.Background()), s.opts.Shutdown.DrainTimeout)
	defer cancel()

	if err := s.srv.Shutdown(shutdownCtx); err != nil {
		// A request outlived the drain budget. Force the connections closed so
		// the process can exit before the orchestrator escalates to SIGKILL.
		log.Error("graceful shutdown timed out; forcing close",
			slog.Any("error", err))
		if closeErr := s.srv.Close(); closeErr != nil {
			return fmt.Errorf("force close after failed drain: %w", closeErr)
		}
		<-serveErr
		return fmt.Errorf("drain timed out: %w", err)
	}

	if err := <-serveErr; err != nil {
		return fmt.Errorf("serve: %w", err)
	}

	log.Info("http server stopped cleanly")
	return nil
}
