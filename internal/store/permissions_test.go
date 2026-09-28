package store_test

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/jon-jc/fluxgate/internal/store"
)

func TestRuntimeDatabasePermissions(t *testing.T) {
	db, _ := openDB(t)
	ctx := context.Background()
	// Tests require a disposable database controlled by TEST_DATABASE_URL.
	// The names intentionally match Terraform; each SET ROLE is transactional.
	for _, role := range []string{"fluxgate_ingest", "fluxgate_aggregator", "fluxgate_query"} {
		var exists bool
		if err := db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			if _, err := db.Pool().Exec(ctx, "CREATE ROLE "+role+" LOGIN CREATEDB CREATEROLE"); err != nil {
				t.Fatal(err)
			}
		}
	}
	for range 2 {
		if err := db.ProvisionRuntimeRoles(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.RequireRuntimeRole(ctx, "fluxgate-query-api"); err == nil {
		t.Fatal("owner credential accepted for query runtime")
	}
	dsn, err := url.Parse(os.Getenv(dsnEnvVar))
	if err != nil {
		t.Fatal(err)
	}
	params := dsn.Query()
	params.Set("options", "-c role=fluxgate_query")
	dsn.RawQuery = params.Encode()
	reader, err := store.Open(ctx, store.Config{DSN: dsn.String(), MaxConns: 1}, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := reader.RequireRuntimeRole(ctx, "fluxgate-query-api"); err != nil {
		t.Fatal(err)
	}
	if err := reader.RequireRuntimeRole(ctx, "fluxgate-ingest-api"); err == nil {
		t.Fatal("wrong service identity accepted")
	}
	for _, tc := range []struct {
		role, sql string
		allowed   bool
	}{
		{"fluxgate_query", "SELECT * FROM rollups LIMIT 1", true},
		{"fluxgate_query", "DELETE FROM rollups WHERE false", false},
		{"fluxgate_query", "SELECT * FROM ingest_requests LIMIT 1", false},
		{"fluxgate_query", "SELECT * FROM processed_batches LIMIT 1", false},
		{"fluxgate_query", "CREATE TABLE public.query_must_not_create(x int)", false},
		{"fluxgate_ingest", "UPDATE ingest_requests SET published=true WHERE false", true},
		{"fluxgate_ingest", "SELECT * FROM rollups LIMIT 1", false},
		{"fluxgate_ingest", "DELETE FROM ingest_requests WHERE false", false},
		{"fluxgate_aggregator", "DELETE FROM processed_batches WHERE false", true},
		{"fluxgate_aggregator", "DELETE FROM ingest_requests WHERE false", true},
		{"fluxgate_aggregator", "UPDATE rollups SET sum=0 WHERE false", true},
		{"fluxgate_aggregator", "SELECT * FROM schema_migrations LIMIT 1", false},
	} {
		t.Run(tc.role+"/"+tc.sql, func(t *testing.T) {
			tx, err := db.Pool().Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			if _, err = tx.Exec(ctx, "SET LOCAL ROLE "+tc.role); err != nil {
				t.Fatal(err)
			}
			_, err = tx.Exec(ctx, tc.sql)
			if (err == nil) != tc.allowed {
				t.Fatalf("allowed=%v, error=%v", tc.allowed, err)
			}
		})
	}
}
