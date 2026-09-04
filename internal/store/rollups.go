package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jon-jc/fluxgate/internal/aggregate"
)

// upsertRollup merges one window's aggregate into the stored row.
//
// The merge is additive rather than replacing, so a window flushed in two
// pieces -- by two replicas, or by one replica across a restart -- ends up with
// the same totals as if it had been flushed once. That is only sound because
// every statistic is associative, which is also why `last` is resolved by event
// time rather than by which write arrived second.
const upsertRollup = `
INSERT INTO rollups (
	tenant_id, metric, kind, label_hash,
	window_start, window_end, labels,
	count, sum, min, max, last, last_event_at, buckets
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
ON CONFLICT (tenant_id, metric, label_hash, window_start) DO UPDATE SET
	count         = rollups.count + EXCLUDED.count,
	sum           = rollups.sum + EXCLUDED.sum,
	min           = LEAST(rollups.min, EXCLUDED.min),
	max           = GREATEST(rollups.max, EXCLUDED.max),
	last          = CASE
	                    WHEN EXCLUDED.last_event_at >= rollups.last_event_at
	                    THEN EXCLUDED.last ELSE rollups.last
	                END,
	last_event_at = GREATEST(rollups.last_event_at, EXCLUDED.last_event_at),
	buckets       = fluxgate_array_add(rollups.buckets, EXCLUDED.buckets),
	updated_at    = now()
`

// Contribution records that one batch supplied data to one window.
//
// The window is part of the identity, not decoration: a batch that straddles a
// boundary contributes to two windows that flush independently, and each has to
// be recorded exactly when it commits.
type Contribution struct {
	BatchID     string
	TenantID    string
	WindowStart time.Time
}

// Key renders a contribution for use as a map key.
func (c Contribution) Key() string {
	return c.BatchID + "@" + c.WindowStart.UTC().Format(time.RFC3339)
}

// Flush writes a set of rollups and records the contributions that produced
// them, in one transaction.
//
// The single transaction is the entire correctness argument. Rollups and the
// delivery ledger commit together, so there is no interval in which the data
// is stored but the contribution is not recorded, or the reverse. Combined
// with acknowledging the message only after this returns, that turns Pub/Sub
// at-least-once delivery into exactly-once accumulation:
//
//   - Crash before commit: no ledger entry, no acknowledgement. Redelivery
//     re-accumulates, and the result is correct.
//   - Crash after commit, before acknowledgement: the ledger entry exists.
//     Redelivery finds it, skips that window, and the result is correct.
//
// contributions may be empty, for a flush triggered by a timer rather than by
// newly arrived data.
func (db *DB) Flush(ctx context.Context, rollups []aggregate.Rollup, contributions []Contribution) error {
	if len(rollups) == 0 && len(contributions) == 0 {
		return nil
	}

	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("flush: begin: %w", err)
	}
	// Rollback after a successful commit is a no-op, so this is safe to defer
	// unconditionally and removes every early-return leak.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	batch := &pgx.Batch{}

	// Indexed rather than ranged by value: a Rollup is large enough that
	// copying one per iteration is measurable on a wide flush.
	for i := range rollups {
		r := &rollups[i]

		labels, err := json.Marshal(orEmpty(r.Labels))
		if err != nil {
			return fmt.Errorf("flush: encode labels for %s: %w", r.Key.Metric, err)
		}

		buckets, _ := r.Acc.Buckets() // nil for kinds without a histogram

		batch.Queue(upsertRollup,
			r.Key.TenantID,
			r.Key.Metric,
			string(r.Key.Kind),
			r.Key.LabelHash,
			r.Window.Start,
			r.Window.End,
			labels,
			r.Acc.Count,
			r.Acc.Sum,
			r.Acc.MinValue(),
			r.Acc.MaxValue(),
			r.Acc.Last,
			time.Unix(0, r.Acc.LastTimestampUnixNano).UTC(),
			buckets,
		)
	}

	for _, c := range contributions {
		// ON CONFLICT DO NOTHING rather than an error: a duplicate here means
		// the same contribution was accumulated twice within one flush, which
		// the in-memory guard should prevent but which must not fail the write
		// of everything else in the transaction.
		batch.Queue(`
			INSERT INTO processed_batches (batch_id, window_start, tenant_id)
			VALUES ($1, $2, $3)
			ON CONFLICT (batch_id, window_start) DO NOTHING`,
			c.BatchID, c.WindowStart, c.TenantID)
	}

	results := tx.SendBatch(ctx, batch)
	// Every queued statement must be consumed before the batch can be closed,
	// or pgx reports the connection as busy on its next use.
	for i := range batch.Len() {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("flush: statement %d of %d: %w", i+1, batch.Len(), err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("flush: close batch: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("flush: commit: %w", err)
	}
	return nil
}

// SeenContributions returns which of the given (batch, window) pairs have
// already been committed.
//
// It is the durable half of duplicate suppression: the in-memory guard covers a
// redelivery within one process lifetime, and this covers one that arrives
// after a restart. Both are needed -- a redelivery before the flush is not in
// the database yet, and one after a restart is not in memory.
func (db *DB) SeenContributions(ctx context.Context, batchID string, windows []time.Time) (map[string]bool, error) {
	if batchID == "" || len(windows) == 0 {
		return map[string]bool{}, nil
	}

	rows, err := db.pool.Query(ctx, `
		SELECT window_start FROM processed_batches
		 WHERE batch_id = $1 AND window_start = ANY($2)`, batchID, windows)
	if err != nil {
		return nil, fmt.Errorf("seen contributions: %w", err)
	}
	defer rows.Close()

	seen := make(map[string]bool, len(windows))
	for rows.Next() {
		var start time.Time
		if err := rows.Scan(&start); err != nil {
			return nil, fmt.Errorf("seen contributions: scan: %w", err)
		}
		seen[Contribution{BatchID: batchID, WindowStart: start}.Key()] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("seen contributions: %w", err)
	}
	return seen, nil
}

// PruneProcessedBatches reclaims ledger entries older than the retention
// period.
//
// The ledger only has to outlive the longest redelivery Pub/Sub could produce.
// Keeping it beyond that grows a table forever to guard against a duplicate
// that can no longer arrive.
func (db *DB) PruneProcessedBatches(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := db.pool.Exec(ctx,
		"DELETE FROM processed_batches WHERE processed_at < now() - $1::interval",
		olderThan.String())
	if err != nil {
		return 0, fmt.Errorf("prune processed batches: %w", err)
	}
	return tag.RowsAffected(), nil
}

// PruneRollups deletes rollups whose window ended before the retention
// horizon.
func (db *DB) PruneRollups(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := db.pool.Exec(ctx,
		"DELETE FROM rollups WHERE window_start < now() - $1::interval",
		olderThan.String())
	if err != nil {
		return 0, fmt.Errorf("prune rollups: %w", err)
	}
	return tag.RowsAffected(), nil
}

// StoredRollup is a rollup read back from the database.
type StoredRollup struct {
	TenantID    string
	Metric      string
	Kind        string
	LabelHash   string
	Labels      map[string]string
	WindowStart time.Time
	WindowEnd   time.Time
	Count       int64
	Sum         float64
	Min         float64
	Max         float64
	Last        float64
	LastEventAt time.Time
	Buckets     []int64
}

// Mean returns the arithmetic mean over the window.
func (s StoredRollup) Mean() float64 {
	if s.Count == 0 {
		return 0
	}
	return s.Sum / float64(s.Count)
}

// QueryRollups reads rollups for one metric over a half-open time range.
//
// The range is half-open so that consecutive queries tile without either
// double-counting the shared boundary or leaving a gap at it.
func (db *DB) QueryRollups(
	ctx context.Context, tenantID, metric string, from, to time.Time, limit int,
) ([]StoredRollup, error) {
	if limit <= 0 {
		limit = 1000
	}

	rows, err := db.pool.Query(ctx, `
		SELECT tenant_id, metric, kind, label_hash, labels,
		       window_start, window_end,
		       count, sum, min, max, last, last_event_at, buckets
		  FROM rollups
		 WHERE tenant_id = $1
		   AND metric = $2
		   AND window_start >= $3
		   AND window_start < $4
		 ORDER BY window_start DESC, label_hash
		 LIMIT $5`,
		tenantID, metric, from, to, limit)
	if err != nil {
		return nil, fmt.Errorf("query rollups: %w", err)
	}
	defer rows.Close()

	var out []StoredRollup
	for rows.Next() {
		var (
			r         StoredRollup
			rawLabels []byte
		)
		if err := rows.Scan(
			&r.TenantID, &r.Metric, &r.Kind, &r.LabelHash, &rawLabels,
			&r.WindowStart, &r.WindowEnd,
			&r.Count, &r.Sum, &r.Min, &r.Max, &r.Last, &r.LastEventAt, &r.Buckets,
		); err != nil {
			return nil, fmt.Errorf("query rollups: scan: %w", err)
		}
		if len(rawLabels) > 0 {
			if err := json.Unmarshal(rawLabels, &r.Labels); err != nil {
				return nil, fmt.Errorf("query rollups: decode labels: %w", err)
			}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query rollups: %w", err)
	}
	return out, nil
}

// ListMetrics returns the distinct metric names a tenant has data for.
func (db *DB) ListMetrics(ctx context.Context, tenantID string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 500
	}

	rows, err := db.pool.Query(ctx, `
		SELECT DISTINCT metric FROM rollups
		 WHERE tenant_id = $1
		 ORDER BY metric
		 LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("list metrics: %w", err)
	}
	defer rows.Close()

	var metrics []string
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			return nil, fmt.Errorf("list metrics: scan: %w", err)
		}
		metrics = append(metrics, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list metrics: %w", err)
	}
	return metrics, nil
}

func orEmpty(labels map[string]string) map[string]string {
	if labels == nil {
		return map[string]string{}
	}
	return labels
}
