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
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
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
	"github.com/jon-jc/fluxgate/internal/pubsubx"
	"github.com/jon-jc/fluxgate/internal/ratelimit"
	"github.com/jon-jc/fluxgate/internal/resilience"
	"github.com/jon-jc/fluxgate/internal/telemetry"
	"github.com/jon-jc/fluxgate/internal/version"
)

// healthcheck runs the process as a probe instead of a server.
//
// The runtime image is distroless: no shell, no curl, nothing an orchestrator
// could exec to test the service. Having the binary probe itself is the
// standard answer, and it keeps the image free of tools an attacker would
// otherwise inherit on breaking in.
var healthcheck = flag.Bool("healthcheck", false,
	"probe the local readiness endpoint and exit 0 if healthy")

func main() {
	flag.Parse()

	if *healthcheck {
		if err := probe(); err != nil {
			fmt.Fprintln(os.Stderr, "unhealthy:", err)
			os.Exit(1)
		}
		return
	}

	if err := run(); err != nil {
		// Configuration can fail before a logger exists, so this last-resort
		// path writes plainly to stderr rather than assuming structured
		// logging is available.
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

// serviceName labels this binary's logs, metrics and traces.
const serviceName = "fluxgate-ingest-api"

func run() error {
	cfg, err := config.Load(serviceName, config.Requirements{Auth: true, PubSub: true})
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

	sink, closeSink, err := buildSink(ctx, cfg, logger, health)
	if err != nil {
		return err
	}
	// Flushing happens after the server has drained, so a batch accepted a
	// moment before SIGTERM is still delivered rather than dying with the
	// process.
	defer closeSink()

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

// buildSink resolves where accepted batches go, returning a cleanup function.
//
// The in-memory sink acknowledges a batch and then loses it on the next
// restart, which is the right trade on a laptop and unacceptable anywhere
// else. Configuration validation already refuses it on staging and prod; the
// warning here is for the tiers where it is merely surprising.
func buildSink(
	ctx context.Context, cfg config.Config, logger *slog.Logger, health *observability.Health,
) (ingest.Sink, func(), error) {
	if !cfg.PubSub.Enabled {
		logger.Warn("using the in-memory sink; accepted batches are NOT durable",
			slog.String("env", string(cfg.Environment)))
		return ingest.NewMemorySink(), func() {}, nil
	}

	client, err := pubsubx.NewClient(ctx, pubsubx.Config{
		ProjectID:    cfg.PubSub.ProjectID,
		EmulatorHost: cfg.PubSub.EmulatorHost,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("connect to pub/sub: %w", err)
	}

	if cfg.PubSub.Bootstrap {
		topo := pubsubx.DefaultTopology(
			cfg.PubSub.ProjectID,
			cfg.PubSub.RawTopic,
			cfg.PubSub.DeadLetterTopic,
			cfg.PubSub.AggregatorSubscription)

		if bootstrapErr := pubsubx.Ensure(ctx, client, topo, logger); bootstrapErr != nil {
			_ = client.Close()
			return nil, nil, fmt.Errorf("bootstrap pub/sub topology: %w", bootstrapErr)
		}
	}

	publisher, err := pubsubx.NewPublisher(client, pubsubx.PublisherOptions{
		Topic:                  cfg.PubSub.RawTopic,
		PublishTimeout:         cfg.PubSub.PublishTimeout,
		BatchDelay:             cfg.PubSub.BatchDelay,
		BatchCount:             cfg.PubSub.BatchCount,
		MaxOutstandingMessages: cfg.PubSub.MaxOutstandingMessages,
		MaxOutstandingBytes:    cfg.PubSub.MaxOutstandingBytes,
		Breaker: resilience.Options{
			FailureThreshold: cfg.PubSub.BreakerFailureThreshold,
			Cooldown:         cfg.PubSub.BreakerCooldown,
		},
		Logger: logger,
	})
	if err != nil {
		_ = client.Close()
		return nil, nil, fmt.Errorf("build publisher: %w", err)
	}

	// Readiness now reflects the transport: an instance that cannot publish
	// should be taken out of rotation rather than accepting batches it will
	// only reject.
	health.Register(publisher)

	logger.Info("publishing to pub/sub",
		slog.String("project", cfg.PubSub.ProjectID),
		slog.String("topic", cfg.PubSub.RawTopic),
		slog.String("emulator", cfg.PubSub.EmulatorHost))

	cleanup := func() {
		publisher.Close()
		if err := client.Close(); err != nil {
			logger.Warn("closing pub/sub client", slog.Any("error", err))
		}
	}
	return publisher, cleanup, nil
}

// probe checks the local readiness endpoint.
//
// It reads the same configuration the server does, so a service moved to a
// different port stays probeable without the healthcheck being updated
// separately and silently drifting out of sync.
func probe() error {
	cfg, err := config.Load(serviceName, config.Requirements{})
	if err != nil {
		return err
	}

	host, port, err := net.SplitHostPort(cfg.HTTP.Addr)
	if err != nil {
		return fmt.Errorf("parse listen address %q: %w", cfg.HTTP.Addr, err)
	}
	// A listener bound to every interface is still reached over the loopback.
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}

	url := "http://" + net.JoinHostPort(host, port) + api.PathReadiness

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned %s", api.PathReadiness, resp.Status)
	}
	return nil
}
