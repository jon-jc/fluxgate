// Command ingest-api serves the Fluxgate telemetry ingestion API.
//
// It accepts batches of metric points over HTTP and hands them to the
// asynchronous publish pipeline. The process is designed to be run by an
// orchestrator: configuration comes from the environment, logs go to stdout as
// JSON, and SIGTERM starts a drain rather than an immediate exit.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jon-jc/fluxgate/internal/api"
	"github.com/jon-jc/fluxgate/internal/auth"
	"github.com/jon-jc/fluxgate/internal/config"
	"github.com/jon-jc/fluxgate/internal/httpx"
	"github.com/jon-jc/fluxgate/internal/idempotency"
	"github.com/jon-jc/fluxgate/internal/ingest"
	"github.com/jon-jc/fluxgate/internal/observability"
	"github.com/jon-jc/fluxgate/internal/ratelimit"
	"github.com/jon-jc/fluxgate/internal/telemetry"
	"github.com/jon-jc/fluxgate/internal/version"
)

func main() {
	if err := run(); err != nil {
		// Configuration can fail before a logger exists, so this last-resort
		// path writes plainly to stderr rather than assuming structured
		// logging is available.
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger := observability.NewLogger(os.Stdout, cfg)
	slog.SetDefault(logger)

	logger.Info("starting fluxgate",
		slog.String("build", version.Short()),
		slog.String("go_version", version.Get().GoVersion),
		slog.String("addr", cfg.HTTP.Addr))

	// NotifyContext cancels on the first signal and restores default handling
	// afterwards, so a second Ctrl-C during a slow drain kills the process
	// immediately instead of appearing to hang.
	ctx, stop := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	health := observability.NewHealth(cfg.HTTP.HandlerTimeout)

	authOpts, err := buildAuth(cfg, logger)
	if err != nil {
		return err
	}

	// The sink is in-memory for now; the Pub/Sub publisher replaces it in the
	// next milestone without the handler changing.
	sink := ingest.NewMemorySink()

	handler := api.NewRouter(api.Deps{
		Config: cfg,
		Logger: logger,
		Health: health,
		Auth:   authOpts,
		Ingest: api.IngestDeps{
			Sink: sink,
			Validator: telemetry.Validator{
				Limits: telemetry.Limits{
					MaxPointsPerBatch: cfg.Ingest.MaxPointsPerBatch,
					MaxMetricNameLen:  telemetry.DefaultLimits().MaxMetricNameLen,
					MaxLabels:         telemetry.DefaultLimits().MaxLabels,
					MaxLabelKeyLen:    telemetry.DefaultLimits().MaxLabelKeyLen,
					MaxLabelValueLen:  telemetry.DefaultLimits().MaxLabelValueLen,
					MaxClockSkew:      cfg.Ingest.MaxClockSkew,
					MaxBackfill:       cfg.Ingest.MaxBackfill,
				},
				Clock: time.Now,
			},
			Limiter: ratelimit.New(ratelimit.Limits{
				Rate:  cfg.Ingest.RateLimitPointsPerSecond,
				Burst: cfg.Ingest.RateLimitBurst,
			}),
			Idempotency:     idempotency.New(cfg.Ingest.IdempotencyTTL),
			MaxRequestBytes: cfg.HTTP.MaxRequestBytes,
		},
	})

	server := httpx.NewServer(httpx.ServerOptions{
		HTTP:     cfg.HTTP,
		Shutdown: cfg.Shutdown,
		Handler:  handler,
		Logger:   logger,
		OnDrain: func() {
			// Readiness must fail before the listener closes; see
			// (*httpx.Server).Run for why the ordering matters.
			health.SetReady(false)
			logger.Info("readiness disabled, draining")
		},
	})

	// Everything the service needs is up, so start accepting traffic.
	health.SetReady(true)

	if err := server.Run(ctx); err != nil {
		return fmt.Errorf("http server: %w", err)
	}

	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	logger.Info("shutdown complete")
	return nil
}

// buildAuth resolves the API key store, or confirms that running without one
// is permitted on this tier.
//
// Configuration validation has already refused AUTH_DISABLED outside local and
// dev, so reaching the disabled branch here is safe by construction. The loud
// warning exists because an unauthenticated ingest endpoint is worth noticing
// in a log even when it is intentional.
func buildAuth(cfg config.Config, logger *slog.Logger) (auth.Options, error) {
	if cfg.Auth.Disabled {
		logger.Warn("authentication is DISABLED; every request is attributed to the anonymous tenant",
			slog.String("env", string(cfg.Environment)))
		return auth.Options{Disabled: true}, nil
	}

	store, err := auth.LoadStore(cfg.Auth.Keys, cfg.Auth.KeysFile)
	if err != nil {
		return auth.Options{}, fmt.Errorf("load API keys: %w", err)
	}

	logger.Info("api keys loaded",
		slog.Int("keys", store.Len()),
		slog.Any("tenants", store.TenantIDs()))

	return auth.Options{Store: store}, nil
}
