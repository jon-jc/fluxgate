// Command migrate applies schema changes using a separate, privileged identity.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jon-jc/fluxgate/internal/config"
	"github.com/jon-jc/fluxgate/internal/store"
)

func main() {
	grant := flag.Bool("grant-runtime-roles", false, "restrict and grant the three Terraform runtime database users")
	flag.Parse()
	if err := run(*grant); err != nil {
		slog.Error("migration failed", "error", err)
		os.Exit(1)
	}
}

func run(grant bool) error {
	cfg, err := config.Load("migrate", config.Requirements{Database: true})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	db, err := store.Open(ctx, store.Config{DSN: cfg.Database.DSN, Instance: cfg.Database.Instance, MaxConns: 1}, slog.Default())
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		return err
	}
	if grant {
		return db.ProvisionRuntimeRoles(ctx)
	}
	return nil
}
