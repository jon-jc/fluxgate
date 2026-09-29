package store_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

func TestBulkFlushPreservesIntegerPrecisionKindsAndBuckets(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	const count = int64(1<<53 + 1)
	gauge := rollup(tenant, "bulk.precision", 0, telemetry.KindGauge, 1e100)
	gauge.Acc.Count = count
	hist := rollup(tenant, "bulk.precision", 0, telemetry.KindHistogram, -3, 0, .002, 9, 1e100)
	hist.Acc.Observe(7, base.Add(4*time.Second+123456*time.Microsecond).UnixNano())
	if err := db.Flush(ctx, []aggregate.Rollup{gauge, hist}, nil); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryRollups(ctx, tenant, "bulk.precision", base, base.Add(time.Hour), 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%+v, err=%v", rows, err)
	}
	for _, row := range rows {
		if row.Kind == string(telemetry.KindGauge) {
			if row.Count != count || row.Sum != 1e100 || row.Buckets != nil {
				t.Fatalf("numeric precision or null buckets changed: %+v", row)
			}
		} else {
			buckets, _ := hist.Acc.Buckets()
			if !reflect.DeepEqual(row.Buckets, buckets) || !row.LastEventAt.Equal(time.Unix(0, hist.Acc.LastTimestampUnixNano)) {
				t.Fatal("histogram buckets or microsecond timestamp changed")
			}
		}
	}
}

func TestBulkFlushMergesDuplicateRowsWithoutLosingCounts(t *testing.T) {
	db, tenant := openDB(t)
	r := rollup(tenant, "bulk.same", 0, telemetry.KindGauge, 7)
	if err := db.Flush(context.Background(), []aggregate.Rollup{r, r, r}, nil); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryRollups(context.Background(), tenant, "bulk.same", base, base.Add(time.Hour), 10)
	if err != nil || len(rows) != 1 || rows[0].Sum != 21 || rows[0].Count != 3 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
}

func TestBulkClaimConflictInLaterChunkRollsBackEarlierClaims(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	r := rollup(tenant, "bulk.conflict", 0, telemetry.KindGauge, 7)
	conflict := contribution(tenant, "z-existing", 0)
	if err := db.Flush(ctx, []aggregate.Rollup{r}, []store.Contribution{conflict}); err != nil {
		t.Fatal(err)
	}
	claims := make([]store.Contribution, 300)
	for i := range claims {
		claims[i] = contribution(tenant, fmt.Sprintf("a-%03d", i), 0)
	}
	claims = append(claims, conflict)
	if err := db.Flush(ctx, []aggregate.Rollup{r}, claims); !errors.Is(err, store.ErrContributionConflict) {
		t.Fatal("overlapping later claim should abort the transaction", err)
	}
	seen, err := db.SeenContributions(ctx, tenant, "a-000", []time.Time{base})
	if err != nil || len(seen) != 0 {
		t.Fatalf("earlier chunk committed: %v, %v", seen, err)
	}
	rows, err := db.QueryRollups(ctx, tenant, "bulk.conflict", base, base.Add(time.Hour), 10)
	if err != nil || len(rows) != 1 || rows[0].Sum != 7 {
		t.Fatalf("overlap changed totals: %+v, %v", rows, err)
	}
}

func TestBlockedBulkWriteDeadlineRollsBackEarlierChunks(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	locked := rollup(tenant, "zz.locked", 0, telemetry.KindGauge, 7)
	if err := db.Flush(ctx, []aggregate.Rollup{locked}, nil); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, "SELECT 1 FROM rollups WHERE tenant_id=$1 FOR UPDATE", tenant); err != nil {
		t.Fatal(err)
	}
	var rows []aggregate.Rollup
	var claims []store.Contribution
	for i := range 300 {
		rows = append(rows, labelledRollup(tenant, "deadline", 0, map[string]string{"i": fmt.Sprint(i)}, 1))
		claims = append(claims, contribution(tenant, fmt.Sprintf("deadline-%03d", i), 0))
	}
	rows = append(rows, locked)
	writeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err = db.Flush(writeCtx, rows, claims); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expected blocked write deadline", err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = db.Pool().QueryRow(ctx, "SELECT count(*) FROM rollups WHERE tenant_id=$1 AND metric='deadline'", tenant).Scan(&count); err != nil || count != 0 {
		t.Fatalf("earlier rows survived deadline: %d, %v", count, err)
	}
	seen, err := db.SeenContributions(ctx, tenant, "deadline-000", []time.Time{base})
	if err != nil || len(seen) != 0 {
		t.Fatalf("earlier claim survived deadline: %v, %v", seen, err)
	}
	if err = db.Flush(ctx, rows, claims); err != nil {
		t.Fatal("clean retry failed after cancellation", err)
	}
}
