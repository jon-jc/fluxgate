package store_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
)

func TestReadBudgetsRejectLargeQueriesAndPageChangesWithoutLoss(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	const count = 500
	batch := make([]aggregate.Rollup, 0, count)
	for i := range count {
		labels := map[string]string{"idx": fmt.Sprint(i)}
		for j := range 19 {
			labels[fmt.Sprintf("label%d", j)] = strings.Repeat("x", 256)
		}
		batch = append(batch, labelledRollup(tenant, "wide.labels", 0, labels, 1))
	}
	if err := db.Flush(ctx, batch, nil); err != nil {
		t.Fatal(err)
	}
	f := store.QueryFilter{TenantID: tenant, Metric: "wide.labels", From: base, To: base.Add(time.Hour)}
	if rows, err := db.Query(ctx, f); !errors.Is(err, store.ErrReadBudget) || len(rows) != 0 {
		t.Fatalf("oversized query: %d rows, %v", len(rows), err)
	}
	f.Labels = map[string]string{"idx": "0"}
	if rows, err := db.Query(ctx, f); err != nil || len(rows) != 1 {
		t.Fatalf("narrow query: %d rows, %v", len(rows), err)
	}
	seen := map[string]bool{}
	cursor := store.Cursor{}
	for page := 0; page <= count; page++ {
		rows, next, err := db.Changed(ctx, tenant, "wide.labels", cursor, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		if len(rows) >= count {
			t.Fatal("byte budget did not shorten the page")
		}
		for _, row := range rows {
			if seen[row.LabelHash] {
				t.Fatal("cursor repeated a row")
			}
			seen[row.LabelHash] = true
		}
		cursor = next
	}
	if len(seen) != count {
		t.Fatalf("lost changes: got %d of %d", len(seen), count)
	}
}
