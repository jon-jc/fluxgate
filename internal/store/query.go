package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// QueryFilter selects which rollups to read.
type QueryFilter struct {
	// TenantID scopes the query. It always comes from the caller's credential,
	// never from a request parameter.
	TenantID string
	// Metric is the metric name to read.
	Metric string
	// From and To bound the window starts read, half-open: [From, To).
	From time.Time
	To   time.Time
	// Labels restricts results to series whose labels contain all of these.
	Labels map[string]string
	// Limit caps the rows returned.
	Limit int
}

// maxQueryRows bounds any single read. A query spanning a year of one-minute
// windows across a thousand series would otherwise try to materialise half a
// billion rows into one HTTP response.
const maxQueryRows = 50_000

// Query reads rollups matching a filter, newest window first.
//
// Label filtering uses JSONB containment rather than a join against a labels
// table. The rollup already carries its labels, so containment answers the
// question in one index lookup, and adding a normalised label table would mean
// a join on every read to reconstruct data the row already holds.
func (db *DB) Query(ctx context.Context, f QueryFilter) ([]StoredRollup, error) {
	if f.Limit <= 0 || f.Limit > maxQueryRows {
		f.Limit = maxQueryRows
	}

	// Parameters are bound, never interpolated: a tenant ID or a label value
	// reaching a query string as text is how a filter becomes an injection.
	args := []any{f.TenantID, f.Metric, f.From, f.To}

	var labelClause string
	if len(f.Labels) > 0 {
		encoded, err := json.Marshal(f.Labels)
		if err != nil {
			return nil, fmt.Errorf("query: encode label filter: %w", err)
		}
		args = append(args, encoded)
		labelClause = fmt.Sprintf(" AND labels @> $%d::jsonb", len(args))
	}

	args = append(args, f.Limit)

	sql := `
		SELECT tenant_id, metric, kind, label_hash, labels,
		       window_start, window_end,
		       count, sum, min, max, last, last_event_at, buckets
		  FROM rollups
		 WHERE tenant_id = $1
		   AND metric = $2
		   AND window_start >= $3
		   AND window_start < $4` + labelClause + `
		 ORDER BY window_start DESC, label_hash
		 LIMIT $` + fmt.Sprint(len(args))

	rows, err := db.pool.Query(ctx, sql, args...)
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

// LabelValues returns the distinct values a label takes for a metric, so a UI
// can offer a filter without the caller guessing.
func (db *DB) LabelValues(
	ctx context.Context, tenantID, metric, label string, limit int,
) ([]string, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	rows, err := db.pool.Query(ctx, `
		SELECT DISTINCT labels ->> $3 AS value
		  FROM rollups
		 WHERE tenant_id = $1
		   AND metric = $2
		   AND labels ? $3
		 ORDER BY value
		 LIMIT $4`, tenantID, metric, label, limit)
	if err != nil {
		return nil, fmt.Errorf("label values: %w", err)
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var v *string
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("label values: scan: %w", err)
		}
		if v != nil {
			values = append(values, *v)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("label values: %w", err)
	}
	return values, nil
}

// LabelKeys returns the distinct label keys present on a metric.
func (db *DB) LabelKeys(ctx context.Context, tenantID, metric string, limit int) ([]string, error) {
	if limit <= 0 || limit > 500 {
		limit = 500
	}

	rows, err := db.pool.Query(ctx, `
		SELECT DISTINCT jsonb_object_keys(labels) AS key
		  FROM rollups
		 WHERE tenant_id = $1
		   AND metric = $2
		 ORDER BY key
		 LIMIT $3`, tenantID, metric, limit)
	if err != nil {
		return nil, fmt.Errorf("label keys: %w", err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, fmt.Errorf("label keys: scan: %w", err)
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("label keys: %w", err)
	}
	return keys, nil
}

// MetricSummary describes a metric a tenant has data for.
type MetricSummary struct {
	Metric      string    `json:"metric"`
	Kind        string    `json:"kind"`
	SeriesCount int64     `json:"series_count"`
	OldestPoint time.Time `json:"oldest_window"`
	NewestPoint time.Time `json:"newest_window"`
}

// Metrics lists a tenant's metrics with enough context to query them.
//
// Returning the time range alongside each name saves a caller from probing for
// it: a UI that has to guess a range renders an empty chart on its first
// attempt, which reads as "the system is broken" rather than "you asked about
// the wrong hour".
func (db *DB) Metrics(ctx context.Context, tenantID string, limit int) ([]MetricSummary, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	rows, err := db.pool.Query(ctx, `
		SELECT metric,
		       min(kind)              AS kind,
		       count(DISTINCT label_hash) AS series_count,
		       min(window_start)      AS oldest,
		       max(window_start)      AS newest
		  FROM rollups
		 WHERE tenant_id = $1
		 GROUP BY metric
		 ORDER BY metric
		 LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("list metrics: %w", err)
	}
	defer rows.Close()

	var out []MetricSummary
	for rows.Next() {
		var m MetricSummary
		if err := rows.Scan(
			&m.Metric, &m.Kind, &m.SeriesCount, &m.OldestPoint, &m.NewestPoint,
		); err != nil {
			return nil, fmt.Errorf("list metrics: scan: %w", err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list metrics: %w", err)
	}
	return out, nil
}

// SeriesFingerprint renders a label set as a stable string, used as a map key
// when grouping rollups into series.
func SeriesFingerprint(labels map[string]string) string {
	if len(labels) == 0 {
		return "{}"
	}

	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	// Sorted, because Go randomises map iteration and an unstable key would
	// split one series across several groups.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}

	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	b.WriteByte('}')
	return b.String()
}
