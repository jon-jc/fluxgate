package store_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
)

func flushNumbered(t *testing.T, db *store.DB, tenant, metric string, flush, rows int) {
	t.Helper()
	batch := make([]aggregate.Rollup, 0, rows)
	for i := range rows {
		batch = append(batch, labelledRollup(tenant, metric, 0,
			map[string]string{"idx": fmt.Sprintf("%d-%02d", flush, i)}, float64(i)))
	}
	if err := db.Flush(context.Background(), batch, nil); err != nil {
		t.Fatalf("flush %d: %v", flush, err)
	}
}

// More must be exact in both directions. A page that stops early must say so,
// or a catching-up stream waits a whole poll per page; and a page that
// happens to drain exactly to its limit must not, or the stream issues an
// empty read on every poll.
func TestChangedPageReportsMoreExactly(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	flushNumbered(t, db, tenant, "more.metric", 0, 7)
	flushNumbered(t, db, tenant, "more.metric", 1, 7)

	first, err := db.ChangedPage(ctx, tenant, "more.metric", store.Cursor{}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Rows) != 7 || !first.More {
		t.Fatalf("first page: %d rows, more=%v; want 7, true", len(first.Rows), first.More)
	}
	second, err := db.ChangedPage(ctx, tenant, "more.metric", first.Next, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Rows) != 7 || second.More {
		t.Fatalf("page draining exactly to its limit: %d rows, more=%v; want 7, false",
			len(second.Rows), second.More)
	}
	for i, r := range append(first.Rows, second.Rows...) {
		if r.Revision <= 0 || (i > 0 && r.Revision < first.Rows[0].Revision) {
			t.Fatalf("row %d carries revision %d", i, r.Revision)
		}
	}
}

// Every row's resume token, decoded and passed back, yields exactly the rows
// after it -- under PostgreSQL's own comparison and timestamp precision, not a
// model of them. This is what makes a stream's per-event id a position a
// client can rely on.
func TestChangedPageResumesExactlyFromEveryRowToken(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	flushNumbered(t, db, tenant, "resume.metric", 0, 6)
	flushNumbered(t, db, tenant, "resume.metric", 1, 5)

	var all []store.StoredRollup
	for cursor := (store.Cursor{}); ; {
		page, err := db.ChangedPage(ctx, tenant, "resume.metric", cursor, 3)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page.Rows...)
		cursor = page.Next
		if !page.More {
			break
		}
	}
	if len(all) != 11 {
		t.Fatalf("paged read returned %d rows, want 11", len(all))
	}

	identity := func(rows []store.StoredRollup) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = fmt.Sprintf("%d/%s", r.Revision, r.LabelHash)
		}
		return out
	}
	for i, r := range all {
		token := store.Cursor{}.After(r, r.Revision).Token(tenant)
		cursor, err := store.ParseCursorToken(tenant, token)
		if err != nil {
			t.Fatalf("row %d: token does not parse: %v", i, err)
		}
		page, err := db.ChangedPage(ctx, tenant, "resume.metric", cursor, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := identity(page.Rows), identity(all[i+1:]); !slices.Equal(got, want) || page.More {
			t.Fatalf("resuming after row %d:\n got %v\nwant %v (more=%v)", i, got, want, page.More)
		}
	}
}
