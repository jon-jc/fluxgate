package store_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
)

func TestChunkedFlushRollsBackAllRowsAndClaims(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	var rows []aggregate.Rollup
	var claims []store.Contribution
	for i := range 300 {
		rows = append(rows, labelledRollup(tenant, "chunked", 0, map[string]string{"i": fmt.Sprint(i)}, 1))
		claims = append(claims, contribution(tenant, fmt.Sprintf("batch-%03d", i), 0))
	}
	// This sorts last, after a full chunk has reached the server. PostgreSQL
	// rejects NUL in JSONB, so both earlier rows and every claim must roll back.
	rows = append(rows, labelledRollup(tenant, "zz.invalid", 0, map[string]string{"bad": "\x00"}, 1))
	if err := db.Flush(ctx, rows, claims); err == nil {
		t.Fatal("invalid final chunk committed")
	}
	got, err := db.Query(ctx, store.QueryFilter{TenantID: tenant, Metric: "chunked", From: base, To: base.Add(time.Hour)})
	if err != nil || len(got) != 0 {
		t.Fatalf("earlier chunk survived rollback: %d, %v", len(got), err)
	}
	seen, err := db.SeenContributions(ctx, tenant, "batch-000", []time.Time{base})
	if err != nil || len(seen) != 0 {
		t.Fatalf("claim survived rollback: %v, %v", seen, err)
	}
	if err := db.Flush(ctx, rows[:300], claims); err != nil {
		t.Fatal("clean retry failed", err)
	}
}

func TestRetentionIndexUsesWindowEnd(t *testing.T) {
	db, _ := openDB(t)
	var definition string
	err := db.Pool().QueryRow(context.Background(), `SELECT indexdef FROM pg_indexes
		WHERE schemaname='public' AND indexname='rollups_window_end_idx'`).Scan(&definition)
	if err != nil || !strings.Contains(definition, "(window_end)") {
		t.Fatalf("retention predicate is not indexed: %q, %v", definition, err)
	}
}
