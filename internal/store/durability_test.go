package store_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

func TestConcurrentDuplicateFlushIsAtomic(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	var wins atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := db.Flush(ctx, []aggregate.Rollup{rollup(tenant, "duplicate", 0, telemetry.KindGauge, 7)},
				[]store.Contribution{contribution(tenant, "same", 0)})
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, store.ErrContributionConflict) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("successful writers=%d", wins.Load())
	}
	rows, err := db.QueryRollups(ctx, tenant, "duplicate", base, base.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Count != 1 || rows[0].Sum != 7 {
		t.Fatalf("rollups=%+v", rows)
	}
}

func TestOverlappingFlushRollsBackNewContributions(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	c := contribution(tenant, "original", 0)
	r := rollup(tenant, "overlap", 0, telemetry.KindGauge, 3)
	if err := db.Flush(ctx, []aggregate.Rollup{r}, []store.Contribution{c}); err != nil {
		t.Fatal(err)
	}
	newClaim := contribution(tenant, "another", 0)
	err := db.Flush(ctx, []aggregate.Rollup{rollup(tenant, "overlap", 0, telemetry.KindGauge, 8)}, []store.Contribution{c, newClaim})
	if !errors.Is(err, store.ErrContributionConflict) {
		t.Fatalf("err=%v", err)
	}
	seen, err := db.SeenContributions(ctx, tenant, "another", []time.Time{base})
	if err != nil || len(seen) != 0 {
		t.Fatalf("rolled-back claim survived: %v, %v", seen, err)
	}
	if err = db.Flush(ctx, []aggregate.Rollup{rollup(tenant, "overlap", 0, telemetry.KindGauge, 5)}, []store.Contribution{newClaim}); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryRollups(ctx, tenant, "overlap", base, base.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Sum != 8 {
		t.Fatalf("rollups=%+v", rows)
	}
}

func TestLedgerNamespacesTenants(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	for _, name := range []string{tenant, tenant + "-other"} {
		if err := db.Flush(ctx, []aggregate.Rollup{rollup(name, "metric", 0, telemetry.KindGauge, 1)},
			[]store.Contribution{contribution(name, "shared-id", 0)}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDifferentKindsNeverMerge(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	for _, kind := range []telemetry.Kind{telemetry.KindGauge, telemetry.KindHistogram} {
		if err := db.Flush(ctx, []aggregate.Rollup{rollup(tenant, "mixed", 0, kind, 2)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := db.QueryRollups(ctx, tenant, "mixed", base, base.Add(time.Hour), 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	for _, r := range rows {
		if r.Count != 1 {
			t.Fatal("different metric kinds merged")
		}
	}
}
