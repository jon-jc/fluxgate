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

// ChangePage is one page of the live tail.
type ChangePage struct {
	// Rows are in commit order. Each carries the revision that wrote it, so a
	// caller can issue a resume position for every row, not only the last.
	Rows []StoredRollup
	// Next is positioned immediately after the last row in Rows.
	Next Cursor
	// More reports that the page stopped at its row or byte bound while further
	// committed changes remained. A caller trying to keep up should read again
	// now rather than on its next schedule: waiting a full poll per page caps a
	// tail at one page per interval, however fast the tenant is writing.
	More bool
}

// Changed reads rollups written since a cursor, oldest write first.
//
// It is ChangedPage without the per-page detail, for callers that only need
// the rows and the next position.
func (db *DB) Changed(
	ctx context.Context, tenantID, metric string, cursor Cursor, limit int,
) ([]StoredRollup, Cursor, error) {
	page, err := db.ChangedPage(ctx, tenantID, metric, cursor, limit)
	if err != nil {
		return nil, cursor, err
	}
	return page.Rows, page.Next, nil
}

// ChangedPage reads one page of rollups written since a cursor.
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
func (db *DB) ChangedPage(
	ctx context.Context, tenantID, metric string, cursor Cursor, limit int,
) (ChangePage, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	none := ChangePage{Next: cursor}

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

	// One row past the limit answers "is there more?" without a second query.
	args = append(args, limit+1)

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
		return none, fmt.Errorf("query changes: %w", err)
	}
	defer rows.Close()

	var out []StoredRollup
	var usedBytes int
	next := cursor
	more := false

	for rows.Next() {
		if len(out) == limit {
			more = true
			break
		}
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
			return none, fmt.Errorf("query changes: scan: %w", err)
		}
		rowBytes := readRowBytes(r, rawLabels)
		if rowBytes > maxChangeBytes {
			return none, ErrReadBudget
		}
		if usedBytes+rowBytes > maxChangeBytes {
			// Keep the cursor on the last returned row. The next poll includes
			// this row; a byte-limited page must never skip a change.
			more = true
			break
		}
		usedBytes += rowBytes
		if len(rawLabels) > 0 {
			if err := json.Unmarshal(rawLabels, &r.Labels); err != nil {
				return none, fmt.Errorf("query changes: decode labels: %w", err)
			}
		}

		r.Revision = revision
		out = append(out, r)
		// Advanced per row rather than to the page's maximum revision, so the
		// next page resumes exactly where this one stopped.
		next = next.After(r, revision)
	}
	if err := rows.Err(); err != nil {
		return none, fmt.Errorf("query changes: %w", err)
	}

	return ChangePage{Rows: out, Next: next, More: more}, nil
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
