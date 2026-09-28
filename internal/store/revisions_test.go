package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

func TestTailSeesTransactionThatStartedBeforeItsCursor(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	if err := db.Flush(ctx, []aggregate.Rollup{rollup(tenant, "slow", 0, telemetry.KindGauge, 1)}, nil); err != nil {
		t.Fatal(err)
	}
	slow, beginErr := db.Pool().Begin(ctx)
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	defer func() { _ = slow.Rollback(ctx) }()
	var started time.Time
	if err := slow.QueryRow(ctx, "SELECT now()").Scan(&started); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(ctx, []aggregate.Rollup{rollup(tenant, "fast", 0, telemetry.KindGauge, 2)}, nil); err != nil {
		t.Fatal(err)
	}
	_, cursor, readErr := db.Changed(ctx, tenant, "", store.Cursor{}, 100)
	if readErr != nil {
		t.Fatal(readErr)
	}
	// Emulate the delayed writer using the same revision/row transaction. Its
	// updated_at is intentionally older than the write already observed.
	var revision int64
	if err := slow.QueryRow(ctx, "UPDATE tenant_revisions SET revision=revision+1 WHERE tenant_id=$1 RETURNING revision", tenant).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	if _, err := slow.Exec(ctx, "UPDATE rollups SET sum=11, revision=$2, updated_at=now() WHERE tenant_id=$1 AND metric='slow'", tenant, revision); err != nil {
		t.Fatal(err)
	}
	if err := slow.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	rows, next, err := db.Changed(ctx, tenant, "", cursor, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Metric != "slow" || rows[0].Sum != 11 || next.Revision <= cursor.Revision {
		t.Fatalf("delayed commit lost: rows=%+v cursor=%+v next=%+v", rows, cursor, next)
	}
}

func TestFlushOrdersRevisionsPerTenantAndRollsBackOnTimeout(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	if err := db.Flush(ctx, []aggregate.Rollup{rollup(tenant, "metric", 0, telemetry.KindGauge, 1)}, nil); err != nil {
		t.Fatal(err)
	}
	lock, beginErr := db.Pool().Begin(ctx)
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	defer func() { _ = lock.Rollback(ctx) }()
	if _, err := lock.Exec(ctx, "SELECT revision FROM tenant_revisions WHERE tenant_id=$1 FOR UPDATE", tenant); err != nil {
		t.Fatal(err)
	}
	bounded, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := db.Flush(bounded, []aggregate.Rollup{rollup(tenant, "metric", 0, telemetry.KindGauge, 10)}, nil); err == nil {
		t.Fatal("flush bypassed tenant commit-order lock")
	}
	if err := db.Flush(ctx, []aggregate.Rollup{rollup(tenant+"-other", "metric", 0, telemetry.KindGauge, 3)}, nil); err != nil {
		t.Fatalf("unrelated tenant blocked: %v", err)
	}
	if err := lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	revision, err := db.NewestRevision(ctx, tenant)
	if err != nil || revision != 1 {
		t.Fatalf("failed flush advanced revision: %d, %v", revision, err)
	}
}
