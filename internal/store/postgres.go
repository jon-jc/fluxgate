// Package store persists windowed rollups and the delivery ledger that makes
// writing them exactly-once.
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Config describes how to reach Postgres.
type Config struct {
	// DSN is the connection string.
	DSN string
	// MaxConns caps the pool. It should be sized against the database's own
	// connection limit divided by the number of replicas, not against how much
	// concurrency this process would like.
	MaxConns int32
	// MinConns keeps a floor of warm connections so a traffic spike does not
	// pay TLS and authentication costs on every request.
	MinConns int32
	// MaxConnLifetime recycles connections so that a database failover or a
	// rolling upgrade is picked up without a restart.
	MaxConnLifetime time.Duration
	// MaxConnIdleTime closes connections nobody is using.
	MaxConnIdleTime time.Duration
	// ConnectTimeout bounds the initial connection.
	ConnectTimeout time.Duration
}

func (c *Config) applyDefaults() {
	if c.MaxConns <= 0 {
		c.MaxConns = 10
	}
	if c.MinConns < 0 {
		c.MinConns = 0
	}
	if c.MaxConnLifetime <= 0 {
		c.MaxConnLifetime = time.Hour
	}
	if c.MaxConnIdleTime <= 0 {
		c.MaxConnIdleTime = 30 * time.Minute
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 10 * time.Second
	}
}

// DB is a Postgres-backed store.
type DB struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// Open connects to Postgres and verifies the connection.
//
// The ping is not ceremony: pgxpool connects lazily, so without it a
// misconfigured DSN would surface as a failure on the first real query, long
// after the process reported itself started.
func Open(ctx context.Context, cfg Config, log *slog.Logger) (*DB, error) {
	if cfg.DSN == "" {
		return nil, errors.New("store: DSN is required")
	}
	cfg.applyDefaults()
	if log == nil {
		log = slog.Default()
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("store: parse DSN: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("store: create pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}

	log.Info("connected to postgres",
		slog.String("host", poolCfg.ConnConfig.Host),
		slog.String("database", poolCfg.ConnConfig.Database),
		slog.Int("max_conns", int(cfg.MaxConns)))

	return &DB{pool: pool, log: log}, nil
}

// Close releases the pool.
func (db *DB) Close() { db.pool.Close() }

// Pool exposes the underlying pool for packages that need direct access.
func (db *DB) Pool() *pgxpool.Pool { return db.pool }

// Migrate applies any migrations that have not run yet.
//
// The runner is deliberately small rather than a dependency: the schema is
// applied by one process, forward only, and every statement is written to be
// re-runnable. A migration framework would add a dependency and a second
// mental model in exchange for features this project does not use.
//
// An advisory lock serialises concurrent starters, so several replicas rolling
// out at once cannot race to create the same table.
func (db *DB) Migrate(ctx context.Context) error {
	// The lock ID is an arbitrary constant unique to this application.
	const advisoryLockID = 0x464C5558 // "FLUX"

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("migrate: acquire connection: %w", err)
	}
	defer conn.Release()

	if _, lockErr := conn.Exec(
		ctx, "SELECT pg_advisory_lock($1)", advisoryLockID,
	); lockErr != nil {
		return fmt.Errorf("migrate: acquire advisory lock: %w", lockErr)
	}
	defer func() {
		// Released explicitly rather than relying on the session ending, so a
		// pooled connection does not carry the lock into its next user.
		if _, unlockErr := conn.Exec(
			context.WithoutCancel(ctx),
			"SELECT pg_advisory_unlock($1)", advisoryLockID,
		); unlockErr != nil {
			db.log.Warn("releasing migration lock", slog.Any("error", unlockErr))
		}
	}()

	if _, schemaErr := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); schemaErr != nil {
		return fmt.Errorf("migrate: create schema_migrations: %w", schemaErr)
	}

	applied, err := db.appliedVersions(ctx, conn.Conn())
	if err != nil {
		return err
	}

	files, err := migrationFiles()
	if err != nil {
		return err
	}

	for _, name := range files {
		if applied[name] {
			continue
		}

		body, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("migrate: read %s: %w", name, err)
		}

		// Each migration runs in its own transaction, so a failure half way
		// through a file leaves nothing behind to reconcile by hand.
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("migrate: begin %s: %w", name, err)
		}

		if _, err := tx.Exec(ctx, string(body)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migrate: apply %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			"INSERT INTO schema_migrations (version) VALUES ($1)", name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migrate: record %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("migrate: commit %s: %w", name, err)
		}

		db.log.Info("applied migration", slog.String("version", name))
	}
	return nil
}

func (db *DB) appliedVersions(ctx context.Context, conn *pgx.Conn) (map[string]bool, error) {
	rows, err := conn.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("migrate: read applied versions: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("migrate: scan version: %w", err)
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("migrate: read applied versions: %w", err)
	}
	return applied, nil
}

// migrationFiles lists the embedded migrations in lexical order, which is why
// they are named with a zero-padded numeric prefix.
func migrationFiles() ([]string, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("migrate: list migrations: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// Check implements observability.Checker.
//
// Ping is deliberately cheap. A readiness probe runs on every scrape, and a
// check that runs a real query turns the probe into a load generator against a
// database that may already be struggling.
func (db *DB) Check(ctx context.Context) error {
	if err := db.pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres unreachable: %w", err)
	}
	return nil
}

// Name implements observability.Checker.
func (db *DB) Name() string { return "postgres" }
