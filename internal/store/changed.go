package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Cursor is a position in the stream of rollup writes.
//
// It is a keyset rather than a bare timestamp, and that is not a
// micro-optimisation. A flush writes every rollup in one transaction, and
// Postgres `now()` is transaction-scoped, so all of those rows carry a
// byte-identical `updated_at`. Paging on time alone loses data the moment one
// flush writes more rows than the page size: the page returns some of them, the
// cursor advances to the shared timestamp, and the next poll's strict
// inequality excludes the rest -- including rows that were never returned. They
// never appear again, and nothing reports it.
//
// Carrying the rest of the ordering key makes the position exact, so a page
// boundary can fall anywhere, including inside a group of rows written at the
// same instant.
type Cursor struct {
	// Since is the write time of the last row delivered.
	Since time.Time
	// Metric, LabelHash and WindowStart disambiguate rows sharing Since. They
	// are empty on a fresh cursor, which reads as "everything at or after
	// Since".
	Metric      string
	LabelHash   string
	WindowStart time.Time

	// primed reports whether the tie-breaking fields are meaningful. A fresh
	// cursor is not primed, and uses a strict inequality on Since alone.
	primed bool
}

// After returns a cursor positioned immediately after r.
func (c Cursor) After(r StoredRollup, updatedAt time.Time) Cursor {
	return Cursor{
		Since:       updatedAt,
		Metric:      r.Metric,
		LabelHash:   r.LabelHash,
		WindowStart: r.WindowStart,
		primed:      true,
	}
}

// Changed reads rollups written since a cursor, oldest write first.
//
// The live tail polls this rather than subscribing to a topic. A per-instance
// Pub/Sub subscription would deliver changes with lower latency, but every
// query-api replica would need its own subscription created and torn down with
// the instance -- runtime topology management, for a feature whose usable
// latency floor is a human looking at a screen.
//
// The cursor advances on write time, not event time: a late arrival updates a
// window that closed minutes ago, and a tail ordered by event time would never
// show it.
func (db *DB) Changed(
	ctx context.Context, tenantID, metric string, cursor Cursor, limit int,
) ([]StoredRollup, Cursor, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	args := []any{tenantID, cursor.Since}

	// A fresh cursor wants everything strictly after its timestamp. A primed
	// one wants everything after a specific row, which is the row-wise
	// comparison below: identical semantics to a compound "greater than",
	// expressed so the index can serve it.
	var position string
	if cursor.primed {
		args = append(args, cursor.Metric, cursor.LabelHash, cursor.WindowStart)
		position = fmt.Sprintf(
			` AND (updated_at, metric, label_hash, window_start) > ($2, $%d, $%d, $%d)`,
			len(args)-2, len(args)-1, len(args))
	} else {
		position = " AND updated_at > $2"
	}

	var metricClause string
	if metric != "" {
		args = append(args, metric)
		metricClause = fmt.Sprintf(" AND metric = $%d", len(args))
	}

	args = append(args, limit)

	sql := `
		SELECT tenant_id, metric, kind, label_hash, labels,
		       window_start, window_end,
		       count, sum, min, max, last, last_event_at, buckets, updated_at
		  FROM rollups
		 WHERE tenant_id = $1` + position + metricClause + `
		 ORDER BY updated_at, metric, label_hash, window_start
		 LIMIT $` + fmt.Sprint(len(args))

	rows, err := db.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, cursor, fmt.Errorf("query changes: %w", err)
	}
	defer rows.Close()

	var out []StoredRollup
	next := cursor

	for rows.Next() {
		var (
			r         StoredRollup
			rawLabels []byte
			updatedAt time.Time
		)
		if err := rows.Scan(
			&r.TenantID, &r.Metric, &r.Kind, &r.LabelHash, &rawLabels,
			&r.WindowStart, &r.WindowEnd,
			&r.Count, &r.Sum, &r.Min, &r.Max, &r.Last, &r.LastEventAt, &r.Buckets,
			&updatedAt,
		); err != nil {
			return nil, cursor, fmt.Errorf("query changes: scan: %w", err)
		}
		if len(rawLabels) > 0 {
			if err := json.Unmarshal(rawLabels, &r.Labels); err != nil {
				return nil, cursor, fmt.Errorf("query changes: decode labels: %w", err)
			}
		}

		out = append(out, r)
		// Advanced per row rather than to the page's maximum timestamp, so the
		// next page resumes exactly where this one stopped.
		next = next.After(r, updatedAt)
	}
	if err := rows.Err(); err != nil {
		return nil, cursor, fmt.Errorf("query changes: %w", err)
	}

	return out, next, nil
}

// NewestWriteTime returns the most recent rollup write time for a tenant,
// measured by the database's own clock.
//
// The live tail starts "from now". Taking that from the reading process's clock
// would be wrong: rows are stamped by the database, and any skew between the
// two either hides events (reader ahead) or replays history (reader behind) --
// both silently. Asking the database removes the question.
//
// A tenant with no rollups yet gets the database's current time, which is the
// same answer for a stream that is about to see its first row.
func (db *DB) NewestWriteTime(ctx context.Context, tenantID string) (time.Time, error) {
	var newest time.Time

	err := db.pool.QueryRow(ctx, `
		SELECT COALESCE(max(updated_at), now())
		  FROM rollups
		 WHERE tenant_id = $1`, tenantID).Scan(&newest)
	if err != nil {
		return time.Time{}, fmt.Errorf("newest write time: %w", err)
	}
	return newest.UTC(), nil
}
