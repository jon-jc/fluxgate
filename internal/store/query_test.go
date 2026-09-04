package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

// labelledRollup builds a rollup with a chosen label set.
func labelledRollup(
	tenant, metric string, offset time.Duration, labels map[string]string, values ...float64,
) aggregate.Rollup {
	start := base.Add(offset)
	acc := aggregate.NewAccumulator(telemetry.KindGauge)
	for i, v := range values {
		acc.Observe(v, start.Add(time.Duration(i)*time.Second).UnixNano())
	}

	return aggregate.Rollup{
		Window: aggregate.Window{Start: start, End: start.Add(time.Minute)},
		Key: aggregate.SeriesKey{
			TenantID:  tenant,
			Metric:    metric,
			Kind:      telemetry.KindGauge,
			LabelHash: aggregate.HashLabels(labels),
		},
		Labels: labels,
		Acc:    acc,
	}
}

// TestQueryFiltersByLabelContainment exercises the GIN index and the JSONB
// containment operator the read path depends on.
func TestQueryFiltersByLabelContainment(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	rollups := []aggregate.Rollup{
		labelledRollup(tenant, "http.requests", 0,
			map[string]string{"status": "200", "service": "checkout"}, 1),
		labelledRollup(tenant, "http.requests", 0,
			map[string]string{"status": "500", "service": "checkout"}, 2),
		labelledRollup(tenant, "http.requests", 0,
			map[string]string{"status": "200", "service": "search"}, 3),
	}
	if err := db.Flush(ctx, rollups, nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	tests := []struct {
		name   string
		labels map[string]string
		want   int
	}{
		{name: "no filter", want: 3},
		{name: "one label", labels: map[string]string{"status": "200"}, want: 2},
		{
			name:   "both labels",
			labels: map[string]string{"status": "200", "service": "checkout"},
			want:   1,
		},
		{name: "no match", labels: map[string]string{"status": "418"}, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := db.Query(ctx, store.QueryFilter{
				TenantID: tenant,
				Metric:   "http.requests",
				From:     base,
				To:       base.Add(time.Hour),
				Labels:   tc.labels,
			})
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if len(got) != tc.want {
				t.Errorf("got %d rows, want %d", len(got), tc.want)
			}
		})
	}
}

// TestQueryIsScopedByTenant: a cross-tenant read would be a data breach rather
// than a bug, so it is asserted at the SQL boundary as well as the HTTP one.
func TestQueryIsScopedByTenant(t *testing.T) {
	db, tenantA := openDB(t)
	_, tenantB := openDB(t)
	ctx := context.Background()

	if err := db.Flush(ctx,
		[]aggregate.Rollup{rollup(tenantA, "secret.metric", 0, telemetry.KindGauge, 42)},
		nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := db.Query(ctx, store.QueryFilter{
		TenantID: tenantB,
		Metric:   "secret.metric",
		From:     base,
		To:       base.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("tenant %s read %d rows belonging to %s", tenantB, len(got), tenantA)
	}
}

// TestQueryBindsLabelValuesRatherThanInterpolating: a label value reaching the
// query string as text is how a filter becomes an injection.
func TestQueryBindsLabelValuesRatherThanInterpolating(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	if err := db.Flush(ctx,
		[]aggregate.Rollup{rollup(tenant, "cpu.util", 0, telemetry.KindGauge, 1)},
		nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Deliberately hostile: a statement terminator, a destructive command and
	// a comment to swallow the rest.
	hostile := map[string]string{
		"service": string([]byte{'\'', ';', ' '}) + "DROP TABLE rollups; --",
	}

	got, err := db.Query(ctx, store.QueryFilter{
		TenantID: tenant,
		Metric:   "cpu.util",
		From:     base,
		To:       base.Add(time.Hour),
		Labels:   hostile,
	})
	if err != nil {
		t.Fatalf("Query with a hostile label value: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("the hostile filter matched %d rows", len(got))
	}

	// The table is still there, which it would not be had the value been
	// interpolated.
	if _, err := db.Query(ctx, store.QueryFilter{
		TenantID: tenant, Metric: "cpu.util", From: base, To: base.Add(time.Hour),
	}); err != nil {
		t.Fatalf("the rollups table did not survive: %v", err)
	}
}

func TestQueryRangeIsHalfOpenAtBothEnds(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	for _, offset := range []time.Duration{0, time.Minute, 2 * time.Minute} {
		if err := db.Flush(ctx,
			[]aggregate.Rollup{rollup(tenant, "cpu.util", offset, telemetry.KindGauge, 1)},
			nil); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}

	got, err := db.Query(ctx, store.QueryFilter{
		TenantID: tenant,
		Metric:   "cpu.util",
		From:     base,
		To:       base.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// The window starting exactly at `to` is excluded, so adjacent queries
	// tile without overlapping at the boundary.
	if len(got) != 2 {
		t.Errorf("got %d rows, want 2 for a half-open range", len(got))
	}
}

// TestChangedTailsByWriteTime is what makes the live tail show late arrivals: a
// straggler updates a window that closed minutes ago, and a tail ordered by
// event time would never surface it.
func TestChangedTailsByWriteTime(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	before := time.Now().UTC().Add(-time.Second)

	// A day-old window, written right now.
	old := rollup(tenant, "late.metric", 0, telemetry.KindGauge, 7)
	old.Window = aggregate.Window{
		Start: base.Add(-24 * time.Hour),
		End:   base.Add(-24*time.Hour + time.Minute),
	}
	if err := db.Flush(ctx, []aggregate.Rollup{old}, nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	changed, cursor, err := db.Changed(ctx, tenant, "late.metric", store.Cursor{Since: before}, 100)
	if err != nil {
		t.Fatalf("Changed: %v", err)
	}
	if len(changed) != 1 {
		t.Fatalf("got %d changes, want 1 despite the window being a day old", len(changed))
	}
	if !cursor.Since.After(before) {
		t.Errorf("cursor = %v, want it to advance past %v", cursor.Since, before)
	}

	// The cursor must not re-deliver what it has already reported.
	again, _, err := db.Changed(ctx, tenant, "late.metric", cursor, 100)
	if err != nil {
		t.Fatalf("Changed: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("got %d changes on the second poll, want 0", len(again))
	}
}

func TestChangedFiltersByMetric(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	before := time.Now().UTC().Add(-time.Second)

	for _, metric := range []string{"wanted.metric", "other.metric"} {
		if err := db.Flush(ctx,
			[]aggregate.Rollup{rollup(tenant, metric, 0, telemetry.KindGauge, 1)},
			nil); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}

	changed, _, err := db.Changed(ctx, tenant, "wanted.metric", store.Cursor{Since: before}, 100)
	if err != nil {
		t.Fatalf("Changed: %v", err)
	}
	if len(changed) != 1 {
		t.Fatalf("got %d changes, want only the requested metric", len(changed))
	}
	if changed[0].Metric != "wanted.metric" {
		t.Errorf("metric = %q, want wanted.metric", changed[0].Metric)
	}

	// With no metric named, everything the tenant wrote is tailed.
	all, _, err := db.Changed(ctx, tenant, "", store.Cursor{Since: before}, 100)
	if err != nil {
		t.Fatalf("Changed: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("got %d changes across all metrics, want 2", len(all))
	}
}

func TestMetricsSummary(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	if err := db.Flush(ctx, []aggregate.Rollup{
		labelledRollup(tenant, "http.requests", 0, map[string]string{"host": "a"}, 1),
		labelledRollup(tenant, "http.requests", time.Minute, map[string]string{"host": "b"}, 2),
	}, nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	metrics, err := db.Metrics(ctx, tenant, 100)
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("got %d metrics, want 1", len(metrics))
	}

	m := metrics[0]
	switch {
	case m.Metric != "http.requests":
		t.Errorf("metric = %q", m.Metric)
	case m.SeriesCount != 2:
		t.Errorf("series count = %d, want 2", m.SeriesCount)
	case !m.OldestPoint.Equal(base):
		t.Errorf("oldest = %v, want %v", m.OldestPoint, base)
	case !m.NewestPoint.Equal(base.Add(time.Minute)):
		t.Errorf("newest = %v, want %v", m.NewestPoint, base.Add(time.Minute))
	}
}

func TestLabelKeysAndValues(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	if err := db.Flush(ctx, []aggregate.Rollup{
		labelledRollup(tenant, "http.requests", 0,
			map[string]string{"status": "200", "service": "checkout"}, 1),
		labelledRollup(tenant, "http.requests", 0,
			map[string]string{"status": "500", "service": "search"}, 2),
	}, nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	keys, err := db.LabelKeys(ctx, tenant, "http.requests", 100)
	if err != nil {
		t.Fatalf("LabelKeys: %v", err)
	}
	if len(keys) != 2 || keys[0] != "service" || keys[1] != "status" {
		t.Errorf("keys = %v, want [service status] sorted", keys)
	}

	values, err := db.LabelValues(ctx, tenant, "http.requests", "status", 100)
	if err != nil {
		t.Fatalf("LabelValues: %v", err)
	}
	if len(values) != 2 || values[0] != "200" || values[1] != "500" {
		t.Errorf("values = %v, want [200 500] sorted", values)
	}

	// A label nothing carries yields nothing, rather than an error.
	absent, err := db.LabelValues(ctx, tenant, "http.requests", "region", 100)
	if err != nil {
		t.Fatalf("LabelValues: %v", err)
	}
	if len(absent) != 0 {
		t.Errorf("values for an absent label = %v, want none", absent)
	}
}

func TestQueryLimitIsBounded(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	if err := db.Flush(ctx,
		[]aggregate.Rollup{rollup(tenant, "cpu.util", 0, telemetry.KindGauge, 1)},
		nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// A caller asking for an absurd limit gets the cap, not an error and not
	// an unbounded scan.
	got, err := db.Query(ctx, store.QueryFilter{
		TenantID: tenant,
		Metric:   "cpu.util",
		From:     base,
		To:       base.Add(time.Hour),
		Limit:    10_000_000,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("got %d rows, want 1", len(got))
	}
}
