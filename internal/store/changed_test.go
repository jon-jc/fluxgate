package store_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

// TestChangedDoesNotDropRowsSharingATimestamp is a regression test for silent,
// permanent data loss on the live tail.
//
// A flush writes every rollup in one transaction, and Postgres `now()` is
// transaction-scoped, so all of those rows carry a byte-identical `updated_at`.
// A cursor of the form "WHERE updated_at > $cursor ORDER BY updated_at LIMIT n"
// then loses data whenever one flush writes more than n rows: the page returns
// n of them, the cursor advances to the shared timestamp, and the next poll's
// strict inequality excludes the remainder -- including the rows that were
// never returned. They never appear on the tail again, and nothing reports it.
//
// The fix is a keyset cursor over the full ordering key rather than over time
// alone.
func TestChangedDoesNotDropRowsSharingATimestamp(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	before := time.Now().UTC().Add(-time.Second)

	// One flush, one transaction, one `now()`: 20 rows that are
	// indistinguishable by timestamp.
	const rows = 20
	batch := make([]aggregate.Rollup, 0, rows)
	for i := range rows {
		batch = append(batch, labelledRollup(tenant, "shared.timestamp", 0,
			map[string]string{"idx": fmt.Sprintf("%02d", i)}, float64(i)))
	}
	if err := db.Flush(ctx, batch, nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Page through with a limit well below the flush size, exactly as the live
	// tail does.
	const pageSize = 5

	seen := make(map[string]bool, rows)
	cursor := store.Cursor{Since: before}

	for page := range 10 {
		changed, next, err := db.Changed(ctx, tenant, "shared.timestamp", cursor, pageSize)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		if len(changed) == 0 {
			break
		}
		for _, r := range changed {
			if seen[r.LabelHash] {
				t.Errorf("page %d re-delivered %s; the cursor went backwards", page, r.LabelHash)
			}
			seen[r.LabelHash] = true
		}
		cursor = next
	}

	if len(seen) != rows {
		t.Errorf("the tail delivered %d of %d rows written in one flush; "+
			"%d were silently and permanently dropped",
			len(seen), rows, rows-len(seen))
	}
}

// TestChangedCursorIsStableAcrossAnEmptyPoll keeps a quiet moment from
// rewinding the tail and replaying everything a client has already seen.
func TestChangedCursorIsStableAcrossAnEmptyPoll(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	before := time.Now().UTC().Add(-time.Second)

	if err := db.Flush(ctx,
		[]aggregate.Rollup{rollup(tenant, "quiet.metric", 0, telemetry.KindGauge, 1)},
		nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	changed, cursor, err := db.Changed(ctx, tenant, "quiet.metric", store.Cursor{Since: before}, 100)
	if err != nil {
		t.Fatalf("Changed: %v", err)
	}
	if len(changed) != 1 {
		t.Fatalf("first poll returned %d rows, want 1", len(changed))
	}

	// Several polls with nothing new must return nothing and leave the cursor
	// where it is.
	for poll := range 3 {
		empty, next, err := db.Changed(ctx, tenant, "quiet.metric", cursor, 100)
		if err != nil {
			t.Fatalf("poll %d: %v", poll, err)
		}
		if len(empty) != 0 {
			t.Fatalf("poll %d re-delivered %d rows", poll, len(empty))
		}
		cursor = next
	}
}

// TestChangedSurfacesLaterWritesAtTheSameInstant covers the boundary the keyset
// exists for: a second flush landing in the same clock tick as the first must
// still be delivered.
func TestChangedSurfacesLaterWritesAtTheSameInstant(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	before := time.Now().UTC().Add(-time.Second)

	// Two flushes in quick succession. Whether they share a timestamp depends
	// on clock resolution, which is exactly why the tail must not depend on
	// them differing.
	for i := range 2 {
		if err := db.Flush(ctx, []aggregate.Rollup{
			labelledRollup(tenant, "rapid.metric", 0,
				map[string]string{"flush": fmt.Sprintf("%d", i)}, 1),
		}, nil); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}

	seen := 0
	cursor := store.Cursor{Since: before}

	for range 5 {
		changed, next, err := db.Changed(ctx, tenant, "rapid.metric", cursor, 1)
		if err != nil {
			t.Fatalf("Changed: %v", err)
		}
		if len(changed) == 0 {
			break
		}
		seen += len(changed)
		cursor = next
	}

	if seen != 2 {
		t.Errorf("the tail delivered %d of 2 rows, paging one at a time", seen)
	}
}

// TestNewestWriteTimeSeedsTheTailFromTheDatabaseClock checks that a new
// subscriber's starting position comes from the database rather than locally.
//
// The tail starts "from now". Taking that from the reading process's clock is
// wrong: rows are stamped by the database, and any skew between the two either
// hides events (app clock ahead) or replays history (app clock behind). Both
// are silent.
func TestNewestWriteTimeSeedsTheTailFromTheDatabaseClock(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	seed, err := db.NewestWriteTime(ctx, tenant)
	if err != nil {
		t.Fatalf("NewestWriteTime: %v", err)
	}

	if err = db.Flush(ctx,
		[]aggregate.Rollup{rollup(tenant, "seeded.metric", 0, telemetry.KindGauge, 1)},
		nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	changed, _, err := db.Changed(ctx, tenant, "seeded.metric", store.Cursor{Since: seed}, 100)
	if err != nil {
		t.Fatalf("Changed: %v", err)
	}
	// A write made after the seed must be visible, regardless of how the
	// reader's own clock compares to the database's.
	if len(changed) != 1 {
		t.Errorf("got %d rows after seeding from the database clock, want 1", len(changed))
	}
}
