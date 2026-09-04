package query

import (
	"strings"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

var now = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

func parse(p Params) (Request, []Violation) {
	p.Now = now
	return Parse("acme", p, DefaultLimits())
}

func fieldsOf(violations []Violation) string {
	fields := make([]string, len(violations))
	for i, v := range violations {
		fields[i] = v.Field
	}
	return strings.Join(fields, ",")
}

func hasField(violations []Violation, field string) bool {
	for _, v := range violations {
		if v.Field == field {
			return true
		}
	}
	return false
}

func TestParseDefaults(t *testing.T) {
	req, violations := parse(Params{Metric: "http.requests"})
	if len(violations) != 0 {
		t.Fatalf("violations = %v", violations)
	}

	if req.Aggregation != AggAvg {
		t.Errorf("aggregation = %q, want avg", req.Aggregation)
	}
	if !req.To.Equal(now) {
		t.Errorf("to = %v, want now", req.To)
	}
	if got := req.To.Sub(req.From); got != DefaultLimits().DefaultRange {
		t.Errorf("range = %v, want the default %v", got, DefaultLimits().DefaultRange)
	}
	// The tenant comes from the credential, never from a parameter.
	if req.TenantID != "acme" {
		t.Errorf("tenant = %q, want acme", req.TenantID)
	}
}

func TestParseRequiresAMetric(t *testing.T) {
	_, violations := parse(Params{})
	if !hasField(violations, "metric") {
		t.Errorf("violations = %s, want one for metric", fieldsOf(violations))
	}
}

// TestRelativeRanges exist because "the last fifteen minutes" is what a human
// actually wants, and forcing them to compute two timestamps is how a dashboard
// ends up with a hard-coded range that silently goes stale.
func TestRelativeRanges(t *testing.T) {
	req, violations := parse(Params{Metric: "cpu.util", From: "-15m"})
	if len(violations) != 0 {
		t.Fatalf("violations = %v", violations)
	}

	if !req.To.Equal(now) {
		t.Errorf("to = %v, want now", req.To)
	}
	if !req.From.Equal(now.Add(-15 * time.Minute)) {
		t.Errorf("from = %v, want 15 minutes ago", req.From)
	}
}

// TestDefaultRangeAnchorsToTo keeps a caller who supplies only `to` from
// getting a window that ignores it.
func TestDefaultRangeAnchorsToTo(t *testing.T) {
	to := now.Add(-24 * time.Hour)

	req, violations := parse(Params{
		Metric: "cpu.util",
		To:     to.Format(time.RFC3339),
	})
	if len(violations) != 0 {
		t.Fatalf("violations = %v", violations)
	}

	if !req.To.Equal(to) {
		t.Errorf("to = %v, want %v", req.To, to)
	}
	if !req.From.Equal(to.Add(-DefaultLimits().DefaultRange)) {
		t.Errorf("from = %v, want the default range before `to`", req.From)
	}
}

func TestParseAbsoluteRange(t *testing.T) {
	from := now.Add(-2 * time.Hour)

	req, violations := parse(Params{
		Metric: "cpu.util",
		From:   from.Format(time.RFC3339),
		To:     now.Format(time.RFC3339),
	})
	if len(violations) != 0 {
		t.Fatalf("violations = %v", violations)
	}
	if !req.From.Equal(from) || !req.To.Equal(now) {
		t.Errorf("range = %v..%v, want %v..%v", req.From, req.To, from, now)
	}
}

func TestParseRejectsBadInput(t *testing.T) {
	tests := []struct {
		name   string
		params Params
		field  string
	}{
		{
			name:   "unknown aggregation",
			params: Params{Metric: "cpu.util", Aggregation: "median"},
			field:  "agg",
		},
		{
			name:   "unparseable from",
			params: Params{Metric: "cpu.util", From: "yesterday"},
			field:  "from",
		},
		{
			name:   "unparseable to",
			params: Params{Metric: "cpu.util", To: "soon"},
			field:  "to",
		},
		{
			name: "inverted range",
			params: Params{
				Metric: "cpu.util",
				From:   now.Format(time.RFC3339),
				To:     now.Add(-time.Hour).Format(time.RFC3339),
			},
			field: "from",
		},
		{
			name:   "zero-width range",
			params: Params{Metric: "cpu.util", From: "-0s"},
			field:  "from",
		},
		{
			name:   "range beyond the maximum",
			params: Params{Metric: "cpu.util", From: "-9000h"},
			field:  "from",
		},
		{
			name:   "invalid metric name",
			params: Params{Metric: "not a metric"},
			field:  "metric",
		},
		{
			name:   "invalid label key",
			params: Params{Metric: "cpu.util", Labels: map[string]string{"bad-key": "x"}},
			field:  "label.bad-key",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, violations := parse(tc.params)
			if !hasField(violations, tc.field) {
				t.Errorf("violations = %s, want one for %s", fieldsOf(violations), tc.field)
			}
		})
	}
}

// TestParseReportsEveryProblemAtOnce keeps a caller iterating on a query from
// discovering the second mistake only after fixing the first.
func TestParseReportsEveryProblemAtOnce(t *testing.T) {
	_, violations := parse(Params{
		Metric:      "bad metric",
		Aggregation: "median",
		From:        "yesterday",
	})

	for _, want := range []string{"metric", "agg", "from"} {
		if !hasField(violations, want) {
			t.Errorf("violations = %s, missing %s", fieldsOf(violations), want)
		}
	}
}

func TestAggregationHelpers(t *testing.T) {
	for _, a := range Aggregations {
		if !a.Valid() {
			t.Errorf("%q is listed but reports invalid", a)
		}
	}
	if Aggregation("median").Valid() {
		t.Error("an unlisted aggregation reported valid")
	}

	if !AggP95.IsQuantile() {
		t.Error("p95 is not recognised as a quantile")
	}
	if AggSum.IsQuantile() {
		t.Error("sum was recognised as a quantile")
	}
}

func TestAggregationIsCaseInsensitive(t *testing.T) {
	req, violations := parse(Params{Metric: "cpu.util", Aggregation: "P95"})
	if len(violations) != 0 {
		t.Fatalf("violations = %v", violations)
	}
	if req.Aggregation != AggP95 {
		t.Errorf("aggregation = %q, want p95", req.Aggregation)
	}
}

// rollup builds a stored rollup for the build tests.
func rollup(metric string, offset time.Duration, labels map[string]string, values ...float64) store.StoredRollup {
	start := now.Add(offset)
	acc := aggregate.NewAccumulator(telemetry.KindGauge)
	for i, v := range values {
		acc.Observe(v, start.Add(time.Duration(i)*time.Second).UnixNano())
	}

	return store.StoredRollup{
		Metric:      metric,
		Kind:        string(telemetry.KindGauge),
		Labels:      labels,
		WindowStart: start,
		WindowEnd:   start.Add(time.Minute),
		Count:       acc.Count,
		Sum:         acc.Sum,
		Min:         acc.MinValue(),
		Max:         acc.MaxValue(),
		Last:        acc.Last,
	}
}

func histogramRollup(metric string, offset time.Duration, values ...float64) store.StoredRollup {
	start := now.Add(offset)
	acc := aggregate.NewAccumulator(telemetry.KindHistogram)
	for i, v := range values {
		acc.Observe(v, start.Add(time.Duration(i)*time.Second).UnixNano())
	}
	buckets, _ := acc.Buckets()

	return store.StoredRollup{
		Metric:      metric,
		Kind:        string(telemetry.KindHistogram),
		WindowStart: start,
		WindowEnd:   start.Add(time.Minute),
		Count:       acc.Count,
		Sum:         acc.Sum,
		Min:         acc.MinValue(),
		Max:         acc.MaxValue(),
		Last:        acc.Last,
		Buckets:     buckets,
	}
}

func buildFor(t *testing.T, agg Aggregation, rollups []store.StoredRollup) Result {
	t.Helper()

	req, violations := parse(Params{Metric: "cpu.util", Aggregation: string(agg)})
	if len(violations) != 0 {
		t.Fatalf("parse: %v", violations)
	}
	return Build(req, rollups, DefaultLimits())
}

func TestBuildExtractsEachAggregation(t *testing.T) {
	rollups := []store.StoredRollup{rollup("cpu.util", 0, nil, 10, 20, 30)}

	for agg, want := range map[Aggregation]float64{
		AggSum:   60,
		AggCount: 3,
		AggAvg:   20,
		AggMin:   10,
		AggMax:   30,
		AggLast:  30,
	} {
		t.Run(string(agg), func(t *testing.T) {
			result := buildFor(t, agg, rollups)
			if len(result.Series) != 1 || len(result.Series[0].Points) != 1 {
				t.Fatalf("result = %+v, want one point", result)
			}
			if got := result.Series[0].Points[0].Value; got != want {
				t.Errorf("%s = %v, want %v", agg, got, want)
			}
		})
	}
}

func TestBuildGroupsByLabels(t *testing.T) {
	rollups := []store.StoredRollup{
		rollup("http.requests", 0, map[string]string{"status": "200"}, 1),
		rollup("http.requests", -time.Minute, map[string]string{"status": "200"}, 2),
		rollup("http.requests", 0, map[string]string{"status": "500"}, 3),
	}

	result := buildFor(t, AggSum, rollups)
	if len(result.Series) != 2 {
		t.Fatalf("got %d series, want 2", len(result.Series))
	}

	for _, s := range result.Series {
		switch s.Labels["status"] {
		case "200":
			if len(s.Points) != 2 {
				t.Errorf("status=200 has %d points, want 2", len(s.Points))
			}
		case "500":
			if len(s.Points) != 1 {
				t.Errorf("status=500 has %d points, want 1", len(s.Points))
			}
		default:
			t.Errorf("unexpected labels %v", s.Labels)
		}
	}
}

// TestBuildEmitsPointsOldestFirst: the store returns newest-first because that
// is what the index serves cheaply, but a chart draws left to right.
func TestBuildEmitsPointsOldestFirst(t *testing.T) {
	rollups := []store.StoredRollup{
		rollup("cpu.util", 0, nil, 3),
		rollup("cpu.util", -time.Minute, nil, 2),
		rollup("cpu.util", -2*time.Minute, nil, 1),
	}

	result := buildFor(t, AggSum, rollups)
	if len(result.Series) != 1 {
		t.Fatalf("got %d series, want 1", len(result.Series))
	}

	points := result.Series[0].Points
	for i := 1; i < len(points); i++ {
		if !points[i].Timestamp.After(points[i-1].Timestamp) {
			t.Fatalf("points are not in ascending time order: %+v", points)
		}
	}
	if points[0].Value != 1 || points[2].Value != 3 {
		t.Errorf("values = %v, want 1..3 in order", points)
	}
}

func TestBuildOrdersSeriesStably(t *testing.T) {
	rollups := []store.StoredRollup{
		rollup("m", 0, map[string]string{"host": "c"}, 1),
		rollup("m", 0, map[string]string{"host": "a"}, 1),
		rollup("m", 0, map[string]string{"host": "b"}, 1),
	}

	// A client diffing two responses should not see series shuffle between
	// identical calls; Go randomises map iteration, so this needs sorting.
	first := buildFor(t, AggSum, rollups)
	for range 20 {
		next := buildFor(t, AggSum, rollups)
		for i := range next.Series {
			if next.Series[i].Labels["host"] != first.Series[i].Labels["host"] {
				t.Fatalf("series order is unstable: %v then %v",
					first.Series[i].Labels, next.Series[i].Labels)
			}
		}
	}
}

func TestBuildComputesQuantilesFromStoredBuckets(t *testing.T) {
	values := make([]float64, 0, 1000)
	for i := range 1000 {
		values = append(values, float64(i+1))
	}

	result := buildFor(t, AggP95, []store.StoredRollup{
		histogramRollup("latency.ms", 0, values...),
	})

	if len(result.Series) != 1 || len(result.Series[0].Points) != 1 {
		t.Fatalf("result = %+v, want one point", result)
	}

	got := result.Series[0].Points[0].Value
	// Bucket boundaries bound the error; the estimate must land near 950.
	if got < 900 || got > 1100 {
		t.Errorf("p95 = %v, want roughly 950", got)
	}
}

// TestQuantileOfANonHistogramWarnsRatherThanLying: returning zero would let a
// dashboard render a percentile that was never computed.
func TestQuantileOfANonHistogramWarnsRatherThanLying(t *testing.T) {
	result := buildFor(t, AggP99, []store.StoredRollup{
		rollup("cpu.util", 0, nil, 10, 20),
	})

	for _, s := range result.Series {
		if len(s.Points) > 0 {
			t.Errorf("a percentile was reported for a metric with no histogram: %+v", s.Points)
		}
	}
	if len(result.Warnings) == 0 {
		t.Error("no warning explained why the series is empty")
	}
	if !strings.Contains(strings.Join(result.Warnings, " "), "histogram") {
		t.Errorf("warnings = %v, want them to mention histograms", result.Warnings)
	}
}

func TestBuildTruncatesAtTheSeriesLimit(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxSeries = 3

	var rollups []store.StoredRollup
	for i := range 20 {
		rollups = append(rollups, rollup("m", 0,
			map[string]string{"host": string(rune('a' + i))}, 1))
	}

	req, _ := parse(Params{Metric: "m", Aggregation: "sum"})
	req.MaxSeries = limits.MaxSeries
	result := Build(req, rollups, limits)

	if len(result.Series) != 3 {
		t.Errorf("got %d series, want the cap of 3", len(result.Series))
	}
	// A caller has to be able to tell an incomplete answer from an empty one.
	if !result.Truncated {
		t.Error("the response was cut short but not marked truncated")
	}
}

func TestBuildTruncatesAtThePointLimit(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxPoints = 5

	var rollups []store.StoredRollup
	for i := range 50 {
		rollups = append(rollups, rollup("m", -time.Duration(i)*time.Minute, nil, 1))
	}

	req, _ := parse(Params{Metric: "m", Aggregation: "sum"})
	result := Build(req, rollups, limits)

	total := 0
	for _, s := range result.Series {
		total += len(s.Points)
	}
	if total > limits.MaxPoints {
		t.Errorf("returned %d points, cap is %d", total, limits.MaxPoints)
	}
	if !result.Truncated {
		t.Error("the response was cut short but not marked truncated")
	}
}

func TestBuildWithNoDataReturnsAnEmptyArray(t *testing.T) {
	result := buildFor(t, AggSum, nil)

	// Not null: a client should be able to iterate the response without a nil
	// check.
	if result.Series == nil {
		t.Error("Series is nil, want an empty array")
	}
	if len(result.Series) != 0 {
		t.Errorf("Series = %v, want empty", result.Series)
	}
}

func TestBuildEchoesTheQuery(t *testing.T) {
	result := buildFor(t, AggMax, []store.StoredRollup{rollup("cpu.util", 0, nil, 1)})

	if result.Metric != "cpu.util" {
		t.Errorf("metric = %q", result.Metric)
	}
	if result.Aggregation != "max" {
		t.Errorf("aggregation = %q, want max", result.Aggregation)
	}
	// Echoing the resolved range is what lets a caller see what a relative
	// range actually meant.
	if result.From == "" || result.To == "" {
		t.Errorf("range = %q..%q, want both populated", result.From, result.To)
	}
}

func TestSeriesFingerprintIsOrderIndependent(t *testing.T) {
	a := map[string]string{"a": "1", "b": "2", "c": "3"}
	b := map[string]string{"c": "3", "a": "1", "b": "2"}

	for range 50 {
		if store.SeriesFingerprint(a) != store.SeriesFingerprint(b) {
			t.Fatal("the same label set fingerprinted differently between calls")
		}
	}
	if store.SeriesFingerprint(nil) != store.SeriesFingerprint(map[string]string{}) {
		t.Error("nil and empty label maps fingerprinted differently")
	}
}
