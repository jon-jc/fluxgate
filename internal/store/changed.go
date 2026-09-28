package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Cursor identifies a committed tenant revision and the last row within it.
// Per-tenant revision locks make commit order independent of transaction start
// time, wall-clock adjustments and ties in timestamp precision.
type Cursor struct {
	// Revision is assigned under a tenant row lock held through commit.
	Revision int64
	// Metric, LabelHash and WindowStart disambiguate rows sharing Revision. They
	// are empty on a fresh cursor, which reads as "everything strictly after
	// Revision".
	Kind        string
	Metric      string
	LabelHash   string
	WindowStart time.Time

	// primed reports whether the tie-breaking fields are meaningful. A fresh
	// cursor is not primed, and uses a strict inequality on Revision alone.
	primed bool
}

// After returns a cursor positioned immediately after r.
func (c Cursor) After(r StoredRollup, revision int64) Cursor {
	return Cursor{
		Revision:    revision,
		Kind:        r.Kind,
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
// The cursor advances on commit revision, not event time: a late arrival updates a
// window that closed minutes ago, and a tail ordered by event time would never
// show it.
func (db *DB) Changed(
	ctx context.Context, tenantID, metric string, cursor Cursor, limit int,
) ([]StoredRollup, Cursor, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	args := []any{tenantID, cursor.Revision}

	// A fresh cursor wants everything strictly after its revision. A primed
	// one wants everything after a specific row, which is the row-wise
	// comparison below: identical semantics to a compound "greater than",
	// expressed so the index can serve it.
	var position string
	if cursor.primed {
		args = append(args, cursor.Metric, cursor.Kind, cursor.LabelHash, cursor.WindowStart)
		position = fmt.Sprintf(
			` AND (revision, metric, kind, label_hash, window_start) > ($2, $%d, $%d, $%d, $%d)`,
			len(args)-3, len(args)-2, len(args)-1, len(args))
	} else {
		position = " AND revision > $2"
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
		       count, sum, min, max, last, last_event_at, buckets, revision
		  FROM rollups
		 WHERE tenant_id = $1` + position + metricClause + `
		 ORDER BY revision, metric, kind, label_hash, window_start
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
			revision  int64
		)
		if err := rows.Scan(
			&r.TenantID, &r.Metric, &r.Kind, &r.LabelHash, &rawLabels,
			&r.WindowStart, &r.WindowEnd,
			&r.Count, &r.Sum, &r.Min, &r.Max, &r.Last, &r.LastEventAt, &r.Buckets,
			&revision,
		); err != nil {
			return nil, cursor, fmt.Errorf("query changes: scan: %w", err)
		}
		if len(rawLabels) > 0 {
			if err := json.Unmarshal(rawLabels, &r.Labels); err != nil {
				return nil, cursor, fmt.Errorf("query changes: decode labels: %w", err)
			}
		}

		out = append(out, r)
		// Advanced per row rather than to the page's maximum revision, so the
		// next page resumes exactly where this one stopped.
		next = next.After(r, revision)
	}
	if err := rows.Err(); err != nil {
		return nil, cursor, fmt.Errorf("query changes: %w", err)
	}

	return out, next, nil
}

// NewestRevision seeds a new live tail at the latest committed tenant revision.
// An in-flight flush is excluded; its higher revision appears on a later poll.
func (db *DB) NewestRevision(ctx context.Context, tenantID string) (int64, error) {
	var newest int64

	err := db.pool.QueryRow(ctx, `
		SELECT COALESCE(max(revision), 0)
		  FROM tenant_revisions
		 WHERE tenant_id = $1`, tenantID).Scan(&newest)
	if err != nil {
		return 0, fmt.Errorf("newest revision: %w", err)
	}
	return newest, nil
}
