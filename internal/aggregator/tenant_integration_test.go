package aggregator

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/pubsubx"
	"github.com/jon-jc/fluxgate/internal/store"
)

// A real PostgreSQL row lock must not prevent a different tenant from becoming
// queryable and acknowledged. This also exercises the same per-tenant revision
// order used by live-query cursors, not just the fake store's scheduling.
func TestPostgresBlockedTenantDoesNotHoldHealthyCheckpoint(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, openErr := store.Open(ctx, store.Config{DSN: dsn, MaxConns: 8}, nil)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("tenant-isolation-%d-", time.Now().UnixNano())
	slowTenant, fastTenant := prefix+"a", prefix+"b"
	if _, err := db.Pool().Exec(ctx, "INSERT INTO tenant_revisions (tenant_id,revision) VALUES ($1,1)", slowTenant); err != nil {
		t.Fatal(err)
	}
	blocker, beginErr := db.Pool().Begin(ctx)
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if _, err := blocker.Exec(ctx, "SELECT revision FROM tenant_revisions WHERE tenant_id=$1 FOR UPDATE", slowTenant); err != nil {
		t.Fatal(err)
	}
	r := newRunner(t, db)
	for _, d := range []pubsubx.Delivery{tenantDelivery(slowTenant, "batch", "metric"), tenantDelivery(fastTenant, "batch", "metric")} {
		if err := r.Handle(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- r.FlushAll(ctx) }()
	joined := false
	// Always release the lock and join the worker before closing the pool,
	// including on a failed assertion.
	defer func() {
		_ = blocker.Rollback(context.Background())
		if !joined {
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for r.Stats().MessagesAcked != 1 {
		select {
		case <-timer.C:
			t.Fatal("healthy checkpoint waited for another tenant's PostgreSQL lock")
		case <-time.After(5 * time.Millisecond):
		}
	}
	rows, cursor, err := db.Changed(ctx, fastTenant, "", store.Cursor{}, 10)
	if err != nil || len(rows) != 2 || cursor.Revision != 1 {
		t.Fatal("healthy checkpoint not visible to live queries", rows, cursor, err)
	}
	for _, row := range rows {
		if row.Count != 1 || (row.Sum != 7 && row.Sum != 11) {
			t.Fatal("incorrect healthy tenant total", row)
		}
	}
	rows, _, err = db.Changed(ctx, slowTenant, "", store.Cursor{}, 10)
	if err != nil || len(rows) != 0 || r.Stats().PendingMessages != 1 {
		t.Fatal("blocked tenant became visible or settled before commit", rows, err)
	}
	if err = blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	err = <-done
	joined = true
	if err != nil || r.Stats().MessagesAcked != 2 || r.Stats().PendingMessages != 0 {
		t.Fatal("blocked tenant did not settle after lock release", err, r.Stats())
	}
	rows, cursor, err = db.Changed(ctx, slowTenant, "", store.Cursor{}, 10)
	if err != nil || len(rows) != 2 || cursor.Revision != 2 {
		t.Fatal("unblocked checkpoint lost rows or commit revision", rows, cursor, err)
	}
}
