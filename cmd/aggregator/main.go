// Command aggregator consumes telemetry batches and writes windowed rollups.
//
// It subscribes to the raw telemetry topic, folds points into tumbling windows
// keyed by event time, and commits each closed window to Postgres together with
// a ledger of the batches that produced it. A message is acknowledged only once
// its data is durable, which is what turns at-least-once delivery into
// exactly-once accumulation.
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

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/aggregator"
	"github.com/jon-jc/fluxgate/internal/config"
	"github.com/jon-jc/fluxgate/internal/httpx"
	"github.com/jon-jc/fluxgate/internal/observability"
	"github.com/jon-jc/fluxgate/internal/pubsubx"
	"github.com/jon-jc/fluxgate/internal/store"
	"github.com/jon-jc/fluxgate/internal/version"
)

// serviceName labels this binary's logs, metrics and traces.
const serviceName = "fluxgate-aggregator"

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
	cfg, err := config.Load(serviceName, config.Requirements{Database: true})
	if err != nil {
		return err
	}

	logger := observability.NewLogger(os.Stdout, cfg)
	slog.SetDefault(logger)

	logger.Info("starting fluxgate aggregator",
		slog.String("build", version.Short()),
		slog.Duration("window", cfg.Aggregator.WindowSize),
		slog.Duration("allowed_lateness", cfg.Aggregator.AllowedLateness))

	ctx, stop := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	health := observability.NewHealth(5 * time.Second)

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

	if cfg.Database.Migrate {
		if migrateErr := db.Migrate(ctx); migrateErr != nil {
			return fmt.Errorf("apply migrations: %w", migrateErr)
		}
	}
	health.Register(db)

	client, err := pubsubx.NewClient(ctx, pubsubx.Config{
		ProjectID:    cfg.PubSub.ProjectID,
		EmulatorHost: cfg.PubSub.EmulatorHost,
	})
	if err != nil {
		return fmt.Errorf("connect to pub/sub: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			logger.Warn("closing pub/sub client", slog.Any("error", closeErr))
		}
	}()

	if cfg.PubSub.Bootstrap {
		topo := pubsubx.DefaultTopology(
			cfg.PubSub.ProjectID,
			cfg.PubSub.RawTopic,
			cfg.PubSub.DeadLetterTopic,
			cfg.PubSub.AggregatorSubscription)

		if bootstrapErr := pubsubx.Ensure(ctx, client, topo, logger); bootstrapErr != nil {
			return fmt.Errorf("bootstrap pub/sub topology: %w", bootstrapErr)
		}
	}

	engine := aggregate.New(aggregate.Config{
		WindowSize:      cfg.Aggregator.WindowSize,
		AllowedLateness: cfg.Aggregator.AllowedLateness,
		MaxSeries:       cfg.Aggregator.MaxSeries,
		IdleTimeout:     cfg.Aggregator.IdleTimeout,
	})

	runner, err := aggregator.New(aggregator.Options{
		Engine:        engine,
		Store:         db,
		FlushInterval: cfg.Aggregator.FlushInterval,
		Logger:        logger,
	})
	if err != nil {
		return err
	}

	subscriber, err := pubsubx.NewSubscriber(client, runner.Handle, pubsubx.SubscriberOptions{
		Subscription:           cfg.PubSub.AggregatorSubscription,
		MaxOutstandingMessages: cfg.Aggregator.MaxOutstandingMessages,
		NumGoroutines:          cfg.Aggregator.Concurrency,
		// A message's lease has to survive until its window closes and flushes,
		// which is bounded by the window size plus the lateness allowance. The
		// extension budget is set generously above that so a lease never
		// expires on data that is merely waiting its turn.
		MaxExtension: maxExtensionFor(cfg),
		// The runner settles messages itself, once their windows are durable.
		ManualAck: true,
		Logger:    logger,
	})
	if err != nil {
		return err
	}

	// Probes are served on their own listener: an orchestrator needs to reach
	// this process even though it exposes no API of its own.
	probes := httpx.NewServer(httpx.ServerOptions{
		HTTP:     cfg.HTTP,
		Shutdown: cfg.Shutdown,
		Handler:  probeRouter(logger, health),
		Logger:   logger,
		OnDrain:  func() { health.SetReady(false) },
	})

	health.SetReady(true)

	return runAll(ctx, logger,
		named{"subscriber", subscriber.Run},
		named{"flusher", runner.Run},
		named{"probes", probes.Run},
		named{"retention", func(ctx context.Context) error { return prune(ctx, db, cfg, logger) }},
		named{"reporter", func(ctx context.Context) error { return report(ctx, engine, runner, logger) }},
	)
}

// named pairs a goroutine with a label, so a failure names the component that
// produced it rather than a line number in a fan-in.
type named struct {
	name string
	run  func(context.Context) error
}

// runAll runs every component until one fails or the context is cancelled,
// then shuts the rest down.
//
// The ordering on the way down matters: cancelling the shared context stops the
// subscriber from accepting new messages first, and the flusher's own shutdown
// path then drains the windows those messages fed. Stopping them in the other
// order would strand data in memory that nothing will ever write.
func runAll(ctx context.Context, logger *slog.Logger, components ...named) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, len(components))
	for _, c := range components {
		go func(c named) {
			if err := c.run(ctx); err != nil {
				errs <- fmt.Errorf("%s: %w", c.name, err)
				return
			}
			errs <- nil
		}(c)
	}

	// The first component to stop takes the others with it: a subscriber that
	// died leaves a flusher with nothing to flush, and a flusher that died
	// leaves a subscriber accumulating data nobody will write.
	var firstErr error
	for range components {
		if err := <-errs; err != nil && firstErr == nil {
			firstErr = err
			logger.Error("component failed; shutting down", slog.Any("error", err))
			cancel()
		}
	}

	if firstErr != nil {
		return firstErr
	}
	logger.Info("shutdown complete")
	return nil
}

// maxExtensionFor sizes the acknowledgement lease.
//
// A message is held until every window it fed has been written, so the lease
// has to cover a window's width, the lateness allowance, and a flush interval,
// with headroom for a slow write. Sizing it too tightly would cause redelivery
// of data that is simply waiting for its window to close -- correct, thanks to
// the ledger, but pure waste.
func maxExtensionFor(cfg config.Config) time.Duration {
	budget := cfg.Aggregator.WindowSize +
		cfg.Aggregator.AllowedLateness +
		cfg.Aggregator.FlushInterval

	extension := 3 * budget
	if extension < 10*time.Minute {
		extension = 10 * time.Minute
	}
	return extension
}

// probeRouter serves liveness and readiness for an orchestrator.
func probeRouter(logger *slog.Logger, health *observability.Health) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", health.LivenessHandler())
	mux.Handle("GET /readyz", health.ReadinessHandler())

	stack := httpx.Chain(
		httpx.RequestID,
		httpx.Recoverer,
		httpx.AccessLog(httpx.AccessLogOptions{
			SkipPaths: []string{"/healthz", "/readyz"},
		}),
		httpx.SecurityHeaders,
	)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := observability.ContextWithLogger(r.Context(), logger)
		stack(mux).ServeHTTP(w, r.WithContext(ctx))
	})
}

// prune enforces retention on rollups and the delivery ledger.
func prune(ctx context.Context, db *store.DB, cfg config.Config, logger *slog.Logger) error {
	ticker := time.NewTicker(cfg.Aggregator.PruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			ledger, err := db.PruneProcessedBatches(ctx, cfg.Aggregator.LedgerRetention)
			if err != nil {
				// Retention falling behind is a capacity problem, not a
				// correctness one; it must not take the aggregator down.
				logger.Error("pruning the delivery ledger failed", slog.Any("error", err))
				continue
			}

			rollups, err := db.PruneRollups(ctx, cfg.Aggregator.RollupRetention)
			if err != nil {
				logger.Error("pruning rollups failed", slog.Any("error", err))
				continue
			}

			if ledger > 0 || rollups > 0 {
				logger.Info("retention applied",
					slog.Int64("ledger_rows", ledger),
					slog.Int64("rollup_rows", rollups))
			}
		}
	}
}

// report logs a periodic summary, so an operator can see watermark progress
// and memory pressure without attaching a debugger.
func report(
	ctx context.Context, engine *aggregate.Engine, runner *aggregator.Runner, logger *slog.Logger,
) error {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			engineStats := engine.Stats()
			runnerStats := runner.Stats()

			logger.Info("aggregator status",
				slog.Int("open_windows", engineStats.OpenWindows),
				slog.Int("tracked_series", engineStats.TrackedSeries),
				slog.Int("inflight_messages", runner.InflightMessages()),
				slog.Time("watermark", time.Unix(engineStats.WatermarkUnixSec, 0).UTC()),
				slog.Int64("points_accepted", engineStats.PointsAccepted),
				slog.Int64("points_late", engineStats.PointsLate),
				slog.Int64("points_shed", engineStats.PointsShed),
				slog.Int64("batches_duplicate", runnerStats.BatchesDuplicate),
				slog.Int64("flushes_failed", runnerStats.FlushesFailed))
		}
	}
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
		"http://"+net.JoinHostPort(host, port)+"/readyz", http.NoBody)
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
