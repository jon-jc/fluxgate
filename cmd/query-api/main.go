// Command query-api serves stored telemetry rollups.
//
// It is deliberately a separate process from the ingest API. Reads and writes
// scale on different axes and fail in different ways: an expensive dashboard
// query should not be able to slow telemetry ingestion, and an ingest spike
// should not make dashboards unreadable. Separating them also means the read
// path holds only a read-shaped database pool and no publisher at all.
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
	"github.com/jon-jc/fluxgate/internal/observability"
	"github.com/jon-jc/fluxgate/internal/query"
	"github.com/jon-jc/fluxgate/internal/store"
	"github.com/jon-jc/fluxgate/internal/version"
)

// serviceName labels this binary's logs, metrics and traces.
const serviceName = "fluxgate-query-api"

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
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(serviceName, config.Requirements{
		Auth:     true,
		Database: true,
	})
	if err != nil {
		return err
	}

	logger := observability.NewLogger(os.Stdout, cfg)
	slog.SetDefault(logger)

	logger.Info("starting fluxgate query api",
		slog.String("build", version.Short()),
		slog.String("addr", cfg.HTTP.Addr))

	ctx, stop := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	health := observability.NewHealth(cfg.HTTP.HandlerTimeout)

	db, err := store.Open(ctx, store.Config{
		DSN:             cfg.Database.DSN,
		MaxConns:        cfg.Database.MaxConns,
		MinConns:        cfg.Database.MinConns,
		MaxConnLifetime: cfg.Database.MaxConnLifetime,
		ConnectTimeout:  cfg.Database.ConnectTimeout,
	}, logger)
	if err != nil {
		return err
	}
	defer db.Close()

	// The read path does not own the schema. Migrations belong to the writer,
	// so a read replica rolling out first cannot apply a change the writer is
	// not yet running.
	health.Register(db)

	authOpts, err := buildAuth(cfg, logger)
	if err != nil {
		return err
	}

	handler := api.NewQueryRouter(api.QueryRouterDeps{
		Config: cfg,
		Logger: logger,
		Health: health,
		Auth:   authOpts,
		Query: api.QueryDeps{
			Reader: db,
			Limits: query.Limits{
				MaxRange:     cfg.Query.MaxRange,
				MaxSeries:    cfg.Query.MaxSeries,
				MaxPoints:    cfg.Query.MaxPoints,
				DefaultRange: cfg.Query.DefaultRange,
			},
			Stream: api.StreamOptions{
				PollInterval:      cfg.Query.StreamPollInterval,
				HeartbeatInterval: cfg.Query.StreamHeartbeat,
				MaxDuration:       cfg.Query.StreamMaxDuration,
			},
			Now: time.Now,
		},
	})

	server := httpx.NewServer(httpx.ServerOptions{
		HTTP:     cfg.HTTP,
		Shutdown: cfg.Shutdown,
		Handler:  handler,
		Logger:   logger,
		OnDrain: func() {
			health.SetReady(false)
			logger.Info("readiness disabled, draining")
		},
	})

	health.SetReady(true)

	if err := server.Run(ctx); err != nil {
		return fmt.Errorf("http server: %w", err)
	}

	logger.Info("shutdown complete")
	return nil
}

// buildAuth resolves the API key store shared with the ingest API.
//
// The same credential reads and writes: a tenant's key identifies the tenant,
// and splitting into read and write keys would double the rotation burden
// without changing who can see what.
func buildAuth(cfg config.Config, logger *slog.Logger) (auth.Options, error) {
	if cfg.Auth.Disabled {
		logger.Warn("authentication is DISABLED; every request reads the anonymous tenant",
			slog.String("env", string(cfg.Environment)))
		return auth.Options{Disabled: true}, nil
	}

	keys, err := auth.LoadStore(cfg.Auth.Keys, cfg.Auth.KeysFile)
	if err != nil {
		return auth.Options{}, fmt.Errorf("load API keys: %w", err)
	}

	logger.Info("api keys loaded",
		slog.Int("keys", keys.Len()),
		slog.Any("tenants", keys.TenantIDs()))

	return auth.Options{Store: keys}, nil
}

// probe checks the local readiness endpoint, for a container healthcheck in an
// image with no shell.
func probe() error {
	cfg, err := config.Load(serviceName, config.Requirements{})
	if err != nil {
		return err
	}

	host, port, err := net.SplitHostPort(cfg.HTTP.Addr)
	if err != nil {
		return fmt.Errorf("parse listen address %q: %w", cfg.HTTP.Addr, err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://"+net.JoinHostPort(host, port)+api.PathReadiness, http.NoBody)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return errors.New("readiness returned " + resp.Status)
	}
	return nil
}
