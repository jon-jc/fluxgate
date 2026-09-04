package aggregate

import (
	"math"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/telemetry"
)

var base = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

func testEngine(lateness time.Duration) *Engine {
	return New(Config{
		WindowSize:      time.Minute,
		AllowedLateness: lateness,
		MaxSeries:       1000,
	})
}

// point builds an observation at base+offset.
func point(metric string, value float64, offset time.Duration, labels map[string]string) telemetry.Point {
	return telemetry.Point{
		Metric:    metric,
		Kind:      telemetry.KindGauge,
		Value:     value,
		Timestamp: base.Add(offset),
		Labels:    labels,
	}
}

func batchOf(points ...telemetry.Point) telemetry.Batch {
	return telemetry.Batch{ID: "b", TenantID: "acme", Points: points}
}

// rollupFor finds the rollup for a metric, failing if it is absent.
func rollupFor(t *testing.T, rollups []Rollup, metric string) Rollup {
	t.Helper()

	for i := range rollups {
		if rollups[i].Key.Metric == metric {
			return rollups[i]
		}
	}
	t.Fatalf("no rollup for %q in %d rollups", metric, len(rollups))
	return Rollup{}
}

func TestWindowForAnchorsToTheEpoch(t *testing.T) {
	// Two instances must derive identical windows from identical timestamps,
	// or their rollups can never merge. Anchoring to process start would make
	// that impossible.
	ts := time.Date(2026, 9, 3, 12, 0, 37, 500, time.UTC)

	w := WindowFor(ts, time.Minute)
	if !w.Start.Equal(time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("start = %v, want 12:00:00", w.Start)
	}
	if !w.End.Equal(time.Date(2026, 9, 3, 12, 1, 0, 0, time.UTC)) {
		t.Errorf("end = %v, want 12:01:00", w.End)
	}
}

func TestWindowIsHalfOpen(t *testing.T) {
	// A timestamp exactly on a boundary belongs to the window it starts, not
	// the one it ends; otherwise a point could land in two windows.
	boundary := base.Add(time.Minute)

	w := WindowFor(boundary, time.Minute)
	if !w.Start.Equal(boundary) {
		t.Errorf("a boundary timestamp landed in the previous window: %v", w)
	}
}

func TestAggregatesPointsIntoOneWindow(t *testing.T) {
	e := testEngine(0)

	result := e.Ingest(batchOf(
		point("cpu.util", 10, 0, nil),
		point("cpu.util", 30, 10*time.Second, nil),
		point("cpu.util", 20, 20*time.Second, nil),
	))
	if result.Accepted != 3 {
		t.Fatalf("accepted = %d, want 3", result.Accepted)
	}
	if len(result.Windows) != 1 {
		t.Fatalf("windows = %v, want exactly one", result.Windows)
	}

	// Advance past the window so it closes.
	e.Ingest(batchOf(point("other.metric", 1, 2*time.Minute, nil)))

	rollups, _ := e.Collect()
	r := rollupFor(t, rollups, "cpu.util")

	if r.Acc.Count != 3 {
		t.Errorf("count = %d, want 3", r.Acc.Count)
	}
	if r.Acc.Sum != 60 {
		t.Errorf("sum = %v, want 60", r.Acc.Sum)
	}
	if r.Acc.Mean() != 20 {
		t.Errorf("mean = %v, want 20", r.Acc.Mean())
	}
	if r.Acc.MinValue() != 10 || r.Acc.MaxValue() != 30 {
		t.Errorf("min/max = %v/%v, want 10/30", r.Acc.MinValue(), r.Acc.MaxValue())
	}
	if r.Acc.Last != 20 {
		t.Errorf("last = %v, want 20 (the latest by event time)", r.Acc.Last)
	}
}

func TestPointsSplitAcrossWindows(t *testing.T) {
	e := testEngine(0)

	result := e.Ingest(batchOf(
		point("cpu.util", 1, 30*time.Second, nil),  // window 12:00
		point("cpu.util", 2, 90*time.Second, nil),  // window 12:01
		point("cpu.util", 3, 150*time.Second, nil), // window 12:02
	))
	if len(result.Windows) != 3 {
		t.Fatalf("windows = %v, want 3", result.Windows)
	}

	// The watermark is at 12:02:30, so the first two windows are closed and
	// the third is still open.
	rollups, windows := e.Collect()
	if len(windows) != 2 {
		t.Fatalf("collected %d windows, want the 2 closed ones", len(windows))
	}
	if len(rollups) != 2 {
		t.Fatalf("collected %d rollups, want 2", len(rollups))
	}
	for _, r := range rollups {
		if r.Acc.Count != 1 {
			t.Errorf("window %s has count %d, want 1", r.Window, r.Acc.Count)
		}
	}
}

// TestLabelsSeparateSeries is what makes a breakdown query possible at all.
func TestLabelsSeparateSeries(t *testing.T) {
	e := testEngine(0)

	e.Ingest(batchOf(
		point("http.requests", 1, 0, map[string]string{"status": "200"}),
		point("http.requests", 1, time.Second, map[string]string{"status": "200"}),
		point("http.requests", 1, 2*time.Second, map[string]string{"status": "500"}),
	))
	e.Ingest(batchOf(point("advance", 1, 2*time.Minute, nil)))

	rollups, _ := e.Collect()

	var found int
	for _, r := range rollups {
		if r.Key.Metric != "http.requests" {
			continue
		}
		found++
		switch r.Labels["status"] {
		case "200":
			if r.Acc.Count != 2 {
				t.Errorf("status=200 count = %d, want 2", r.Acc.Count)
			}
		case "500":
			if r.Acc.Count != 1 {
				t.Errorf("status=500 count = %d, want 1", r.Acc.Count)
			}
		default:
			t.Errorf("unexpected labels %v", r.Labels)
		}
	}
	if found != 2 {
		t.Errorf("found %d series for http.requests, want 2", found)
	}
}

// TestLabelHashIsOrderIndependent guards the property that Go's randomised map
// iteration would otherwise break: the same label set must always hash the
// same, or every point would land in a series of its own.
func TestLabelHashIsOrderIndependent(t *testing.T) {
	a := map[string]string{"a": "1", "b": "2", "c": "3"}
	b := map[string]string{"c": "3", "b": "2", "a": "1"}

	for range 50 {
		if HashLabels(a) != HashLabels(b) {
			t.Fatal("the same label set hashed differently between calls")
		}
	}
}

// TestLabelHashResistsConcatenationCollisions is why the hash is
// length-prefixed rather than merely delimited.
func TestLabelHashResistsConcatenationCollisions(t *testing.T) {
	pairs := [][2]map[string]string{
		{{"ab": "c"}, {"a": "bc"}},
		{{"a": "b:c"}, {"a": "b", "c": ""}},
		{{"x": "1", "y": "2"}, {"x": "1y", "": "2"}},
	}

	for _, pair := range pairs {
		if HashLabels(pair[0]) == HashLabels(pair[1]) {
			t.Errorf("distinct label sets collided: %v and %v", pair[0], pair[1])
		}
	}
}

func TestEmptyLabelsHashConsistently(t *testing.T) {
	if HashLabels(nil) != HashLabels(map[string]string{}) {
		t.Error("nil and empty label maps hashed differently")
	}
}

// TestWatermarkTrailsByAllowedLateness is the whole out-of-order story: a
// window must not close until stragglers have had their chance.
func TestWatermarkTrailsByAllowedLateness(t *testing.T) {
	e := testEngine(30 * time.Second)

	// An observation at 12:01:30 puts the watermark at 12:01:00, which is
	// exactly the end of the first window -- so it closes, and no later one
	// does.
	e.Ingest(batchOf(point("cpu.util", 1, 90*time.Second, nil)))

	if got := e.Watermark(); !got.Equal(base.Add(time.Minute)) {
		t.Errorf("watermark = %v, want 12:01:00", got)
	}

	_, windows := e.Collect()
	if len(windows) != 0 {
		t.Errorf("collected %v; the only window is still open", windows)
	}
}

func TestLateArrivalStillJoinsAnOpenWindow(t *testing.T) {
	e := testEngine(60 * time.Second)

	// A point far ahead, then one for the earlier window. With a minute of
	// allowed lateness the earlier window is still open, so the straggler
	// counts rather than being discarded.
	e.Ingest(batchOf(point("cpu.util", 100, 90*time.Second, nil)))
	result := e.Ingest(batchOf(point("cpu.util", 1, 10*time.Second, nil)))

	if result.Late != 0 {
		t.Fatalf("late = %d, want 0; the window was still open", result.Late)
	}
	if result.Accepted != 1 {
		t.Fatalf("accepted = %d, want 1", result.Accepted)
	}
}

// TestLateArrivalAfterFlushIsRejected covers the other half: once a rollup has
// been emitted, accepting more data for it would silently change a number
// somebody may already have read.
func TestLateArrivalAfterFlushIsRejected(t *testing.T) {
	e := testEngine(0)

	e.Ingest(batchOf(point("cpu.util", 1, 10*time.Second, nil)))
	e.Ingest(batchOf(point("cpu.util", 2, 90*time.Second, nil)))

	_, windows := e.Collect()
	if len(windows) != 1 {
		t.Fatalf("collected %v, want the first window", windows)
	}
	// Only a confirmed write closes a window to further data.
	e.MarkFlushed(windows)

	result := e.Ingest(batchOf(point("cpu.util", 999, 20*time.Second, nil)))
	if result.Late != 1 {
		t.Errorf("late = %d, want 1", result.Late)
	}
	if result.Accepted != 0 {
		t.Errorf("accepted = %d, want 0", result.Accepted)
	}
	if got := e.Stats().PointsLate; got != 1 {
		t.Errorf("PointsLate = %d, want 1", got)
	}
}

// TestReplayIsDeterministic is the payoff of driving everything from event
// time: the same data replayed later produces the same rollups.
func TestReplayIsDeterministic(t *testing.T) {
	points := []telemetry.Point{
		point("cpu.util", 5, 0, map[string]string{"host": "a"}),
		point("cpu.util", 15, 20*time.Second, map[string]string{"host": "a"}),
		point("cpu.util", 25, 40*time.Second, map[string]string{"host": "b"}),
		point("cpu.util", 35, 70*time.Second, map[string]string{"host": "a"}),
		point("cpu.util", 45, 200*time.Second, nil),
	}

	collect := func(order []int) map[string]float64 {
		e := testEngine(0)
		for _, i := range order {
			e.Ingest(batchOf(points[i]))
		}
		rollups, _ := e.CollectAll()

		out := make(map[string]float64, len(rollups))
		for _, r := range rollups {
			key := r.Window.String() + "|" + r.Key.Metric + "|" + LabelsFingerprint(r.Labels)
			out[key] = r.Acc.Sum
		}
		return out
	}

	inOrder := collect([]int{0, 1, 2, 3, 4})
	// Deliberately out of arrival order. Event time decides the outcome, so
	// the result must be identical.
	shuffled := collect([]int{2, 0, 3, 1, 4})

	if len(inOrder) != len(shuffled) {
		t.Fatalf("series count differs: %d vs %d", len(inOrder), len(shuffled))
	}
	for key, want := range inOrder {
		if got, ok := shuffled[key]; !ok || got != want {
			t.Errorf("series %s: got %v, want %v", key, got, want)
		}
	}
}

// TestLastIsOrderedByEventTime keeps a redelivered message from changing an
// answer that should be stable.
func TestLastIsOrderedByEventTime(t *testing.T) {
	e := testEngine(0)

	// The later observation arrives first.
	e.Ingest(batchOf(point("gauge.x", 99, 50*time.Second, nil)))
	e.Ingest(batchOf(point("gauge.x", 11, 10*time.Second, nil)))
	e.Ingest(batchOf(point("advance", 1, 3*time.Minute, nil)))

	rollups, _ := e.Collect()
	r := rollupFor(t, rollups, "gauge.x")

	if r.Acc.Last != 99 {
		t.Errorf("last = %v, want 99, the latest by event time", r.Acc.Last)
	}
}

// TestSeriesCapShedsRatherThanGrows: an OOM kill loses every window the process
// was holding; shedding a point loses one point.
func TestSeriesCapShedsRatherThanGrows(t *testing.T) {
	e := New(Config{WindowSize: time.Minute, MaxSeries: 10})

	points := make([]telemetry.Point, 0, 50)
	for i := range 50 {
		points = append(points, point("cardinality.bomb", 1, time.Second,
			map[string]string{"id": string(rune('a'+i%26)) + string(rune('a'+i/26))}))
	}

	result := e.Ingest(batchOf(points...))

	if result.Shed == 0 {
		t.Fatal("nothing was shed despite 50 series against a cap of 10")
	}
	if result.Accepted > 10 {
		t.Errorf("accepted %d series, cap is 10", result.Accepted)
	}
	if got := e.Stats().TrackedSeries; got > 10 {
		t.Errorf("tracking %d series, cap is 10", got)
	}
}

// TestCollectEmptiesTheEngine is the contract that makes the caller's
// responsibility unambiguous: once collected, the data exists only in the
// caller's hands.
func TestCollectEmptiesTheEngine(t *testing.T) {
	e := testEngine(0)

	e.Ingest(batchOf(point("cpu.util", 1, 0, nil)))
	e.Ingest(batchOf(point("cpu.util", 2, 2*time.Minute, nil)))

	first, _ := e.Collect()
	if len(first) == 0 {
		t.Fatal("the first collect returned nothing")
	}

	second, windows := e.Collect()
	if len(second) != 0 || len(windows) != 0 {
		t.Errorf("a second collect returned %d rollups; the window should be gone", len(second))
	}
}

func TestCollectAllDrainsOpenWindows(t *testing.T) {
	e := testEngine(0)

	e.Ingest(batchOf(point("cpu.util", 1, 0, nil)))

	// Nothing is closed yet.
	if rollups, _ := e.Collect(); len(rollups) != 0 {
		t.Fatalf("Collect returned %d rollups, want none", len(rollups))
	}
	// At shutdown, the alternative to emitting a partial window is discarding
	// data nobody will ever complete.
	rollups, windows := e.CollectAll()
	if len(rollups) != 1 || len(windows) != 1 {
		t.Errorf("CollectAll returned %d rollups / %d windows, want 1 each",
			len(rollups), len(windows))
	}
}

func TestCollectEmitsOldestWindowFirst(t *testing.T) {
	e := testEngine(0)

	for i := range 5 {
		e.Ingest(batchOf(point("cpu.util", 1, time.Duration(i)*time.Minute, nil)))
	}
	e.Ingest(batchOf(point("advance", 1, 10*time.Minute, nil)))

	_, windows := e.Collect()
	if len(windows) < 2 {
		t.Fatalf("collected %d windows, want several", len(windows))
	}
	// A partial failure should leave a contiguous prefix persisted, not holes
	// scattered through the timeline.
	for i := 1; i < len(windows); i++ {
		if !windows[i].Start.After(windows[i-1].Start) {
			t.Errorf("windows are not in order: %v then %v", windows[i-1], windows[i])
		}
	}
}

func TestTenantsAreSeparateSeries(t *testing.T) {
	e := testEngine(0)

	e.Ingest(telemetry.Batch{ID: "b1", TenantID: "acme", Points: []telemetry.Point{
		point("shared.metric", 10, 0, nil),
	}})
	e.Ingest(telemetry.Batch{ID: "b2", TenantID: "globex", Points: []telemetry.Point{
		point("shared.metric", 20, time.Second, nil),
	}})
	e.Ingest(telemetry.Batch{ID: "b3", TenantID: "acme", Points: []telemetry.Point{
		point("advance", 1, 3*time.Minute, nil),
	}})

	rollups, _ := e.Collect()

	byTenant := make(map[string]float64)
	for _, r := range rollups {
		if r.Key.Metric == "shared.metric" {
			byTenant[r.Key.TenantID] = r.Acc.Sum
		}
	}
	// One tenant's data appearing in another's rollup would be a data breach,
	// not merely a bug.
	if byTenant["acme"] != 10 || byTenant["globex"] != 20 {
		t.Errorf("sums by tenant = %v, want acme=10 globex=20", byTenant)
	}
}

func TestEngineIsConcurrencySafe(t *testing.T) {
	e := New(Config{WindowSize: time.Minute, MaxSeries: 100_000})

	done := make(chan struct{})
	for g := range 8 {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			for i := range 200 {
				e.Ingest(batchOf(point("concurrent.metric", float64(i),
					time.Duration(i)*time.Second,
					map[string]string{"g": string(rune('a' + g))})))
			}
		}(g)
	}
	go func() {
		defer func() { done <- struct{}{} }()
		for range 50 {
			e.Collect()
			e.Stats()
			e.PendingWindows()
		}
	}()

	for range 9 {
		<-done
	}
}

func TestAccumulatorMergeIsAssociative(t *testing.T) {
	// Merging is what lets partial results combine across a restart or across
	// shards, and it is only sound because every statistic is associative.
	build := func(values ...float64) *Accumulator {
		a := NewAccumulator(telemetry.KindGauge)
		for i, v := range values {
			a.Observe(v, int64(i))
		}
		return a
	}

	whole := build(1, 2, 3, 4, 5, 6)

	left, right := build(1, 2, 3), build(4, 5, 6)
	left.Merge(right)

	if left.Count != whole.Count {
		t.Errorf("count = %d, want %d", left.Count, whole.Count)
	}
	if left.Sum != whole.Sum {
		t.Errorf("sum = %v, want %v", left.Sum, whole.Sum)
	}
	if left.MinValue() != whole.MinValue() || left.MaxValue() != whole.MaxValue() {
		t.Errorf("min/max = %v/%v, want %v/%v",
			left.MinValue(), left.MaxValue(), whole.MinValue(), whole.MaxValue())
	}
}

func TestMergeWithEmptyAccumulator(t *testing.T) {
	a := NewAccumulator(telemetry.KindGauge)
	a.Observe(5, 1)

	a.Merge(NewAccumulator(telemetry.KindGauge))
	a.Merge(nil)

	// An empty accumulator carries +Inf/-Inf sentinels; merging one must not
	// let those escape into a real result.
	if a.MinValue() != 5 || a.MaxValue() != 5 {
		t.Errorf("min/max = %v/%v, want 5/5", a.MinValue(), a.MaxValue())
	}
	if a.Count != 1 {
		t.Errorf("count = %d, want 1", a.Count)
	}
}

func TestEmptyAccumulatorReportsZeroNotInfinity(t *testing.T) {
	a := NewAccumulator(telemetry.KindGauge)

	if math.IsInf(a.MinValue(), 0) || math.IsInf(a.MaxValue(), 0) {
		t.Error("an empty accumulator leaked its infinity sentinels")
	}
	if a.Mean() != 0 {
		t.Errorf("mean = %v, want 0", a.Mean())
	}
}

func TestStatsTrackTheEngine(t *testing.T) {
	e := testEngine(0)

	e.Ingest(batchOf(point("cpu.util", 1, 0, nil)))
	if s := e.Stats(); s.OpenWindows != 1 || s.TrackedSeries != 1 {
		t.Errorf("stats = %+v, want 1 window and 1 series", s)
	}

	e.Ingest(batchOf(point("cpu.util", 2, 2*time.Minute, nil)))
	_, windows := e.Collect()
	e.MarkFlushed(windows)

	s := e.Stats()
	if s.WindowsFlushed == 0 {
		t.Error("WindowsFlushed was not incremented")
	}
	if s.PointsAccepted != 2 {
		t.Errorf("PointsAccepted = %d, want 2", s.PointsAccepted)
	}
	if s.String() == "" {
		t.Error("Stats.String() is empty")
	}
}

func TestLabelsAreCopiedNotAliased(t *testing.T) {
	e := testEngine(0)

	labels := map[string]string{"host": "a"}
	e.Ingest(batchOf(point("cpu.util", 1, 0, labels)))

	// The source map belongs to a decoded message the caller may reuse.
	labels["host"] = "tampered"

	e.Ingest(batchOf(point("advance", 1, 3*time.Minute, nil)))
	rollups, _ := e.Collect()

	if got := rollupFor(t, rollups, "cpu.util").Labels["host"]; got != "a" {
		t.Errorf("label = %q, want a; the engine aliased the caller's map", got)
	}
}

func TestPendingWindowsAreOrdered(t *testing.T) {
	e := testEngine(time.Hour) // nothing closes

	for _, offset := range []time.Duration{5 * time.Minute, time.Minute, 3 * time.Minute} {
		e.Ingest(batchOf(point("cpu.util", 1, offset, nil)))
	}

	windows := e.PendingWindows()
	if len(windows) != 3 {
		t.Fatalf("pending = %v, want 3", windows)
	}
	for i := 1; i < len(windows); i++ {
		if !windows[i].Start.After(windows[i-1].Start) {
			t.Errorf("pending windows out of order: %v", windows)
		}
	}
}

func TestDefaultsAreUsable(t *testing.T) {
	e := New(Config{})

	if e.WindowSize() <= 0 {
		t.Error("a zero-value config produced a non-positive window size")
	}
	if result := e.Ingest(batchOf(point("cpu.util", 1, 0, nil))); result.Accepted != 1 {
		t.Errorf("accepted = %d, want 1", result.Accepted)
	}
}

func BenchmarkIngest(b *testing.B) {
	e := New(Config{WindowSize: time.Minute, MaxSeries: 1_000_000})

	points := make([]telemetry.Point, 100)
	for i := range points {
		points[i] = point("bench.metric", float64(i), time.Duration(i)*time.Millisecond,
			map[string]string{"host": string(rune('a' + i%26))})
	}
	batch := batchOf(points...)

	b.ReportAllocs()
	for b.Loop() {
		e.Ingest(batch)
	}
}

func BenchmarkHashLabels(b *testing.B) {
	labels := map[string]string{
		"service": "checkout", "region": "us-central1",
		"pod": "checkout-7d9f8b", "version": "v2.14.1",
	}

	b.ReportAllocs()
	for b.Loop() {
		HashLabels(labels)
	}
}

// TestCollectedButUnconfirmedWindowStillAcceptsData is the property that makes
// a failed write recoverable. The engine hands the rollups over on Collect, but
// until the caller confirms the write, a redelivery for that window is not late
// -- it is the raw material the retry needs.
func TestCollectedButUnconfirmedWindowStillAcceptsData(t *testing.T) {
	e := testEngine(0)

	e.Ingest(batchOf(point("cpu.util", 1, 10*time.Second, nil)))
	e.Ingest(batchOf(point("cpu.util", 2, 90*time.Second, nil)))

	rollups, windows := e.Collect()
	if len(rollups) == 0 {
		t.Fatal("Collect returned nothing")
	}
	// The caller's write fails, so MarkFlushed is deliberately not called.

	result := e.Ingest(batchOf(point("cpu.util", 1, 10*time.Second, nil)))
	if result.Late != 0 {
		t.Errorf("late = %d; an unconfirmed window must still accept its retry", result.Late)
	}
	if result.Accepted != 1 {
		t.Errorf("accepted = %d, want 1", result.Accepted)
	}

	// Once the write is confirmed, the same point is genuinely late.
	e.MarkFlushed(windows)
	if result := e.Ingest(batchOf(point("cpu.util", 1, 10*time.Second, nil))); result.Late != 1 {
		t.Errorf("late = %d after MarkFlushed, want 1", result.Late)
	}
}

func TestMarkFlushedIsIdempotent(t *testing.T) {
	e := testEngine(0)

	e.Ingest(batchOf(point("cpu.util", 1, 0, nil)))
	e.Ingest(batchOf(point("cpu.util", 2, 2*time.Minute, nil)))

	_, windows := e.Collect()
	e.MarkFlushed(windows)
	before := e.Stats().WindowsFlushed

	// A retry that reports the same windows again must not rewind the
	// high-water mark.
	e.MarkFlushed(windows)

	if result := e.Ingest(batchOf(point("cpu.util", 1, 10*time.Second, nil))); result.Late != 1 {
		t.Errorf("late = %d, want 1; the second MarkFlushed moved the mark backwards", result.Late)
	}
	if after := e.Stats().WindowsFlushed; after <= before {
		t.Errorf("WindowsFlushed went from %d to %d", before, after)
	}
}

// clock is a manually advanced processing-time source.
type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

// TestIdleStreamDoesNotStrandItsLastWindow covers the failure mode event-time
// watermarks have by construction: the watermark only advances when data
// arrives, so a producer that stops leaves its final window one observation
// short of closing -- forever.
func TestIdleStreamDoesNotStrandItsLastWindow(t *testing.T) {
	c := &clock{now: base}
	e := New(Config{
		WindowSize:  time.Minute,
		IdleTimeout: 30 * time.Second,
		Clock:       c.Now,
	})

	e.Ingest(batchOf(point("cpu.util", 5, 10*time.Second, nil)))

	// The producer goes quiet. Nothing closes on its own.
	if rollups, _ := e.Collect(); len(rollups) != 0 {
		t.Fatalf("collected %d rollups while the window was still open", len(rollups))
	}
	if e.AdvanceOnIdle() {
		t.Fatal("the watermark advanced before the idle timeout elapsed")
	}

	c.now = base.Add(2 * time.Minute)

	if !e.AdvanceOnIdle() {
		t.Fatal("the watermark did not advance after the idle timeout")
	}
	rollups, windows := e.Collect()
	if len(rollups) != 1 || len(windows) != 1 {
		t.Fatalf("collected %d rollups / %d windows, want 1 each", len(rollups), len(windows))
	}
	if rollups[0].Acc.Sum != 5 {
		t.Errorf("sum = %v, want 5", rollups[0].Acc.Sum)
	}
	if e.Stats().IdleAdvances != 1 {
		t.Errorf("IdleAdvances = %d, want 1", e.Stats().IdleAdvances)
	}
}

// TestIdleAdvanceOnlyClosesTheOldestWindow keeps a resuming producer's newer
// windows open: jumping the watermark to the wall clock would slam every one of
// them shut at once.
func TestIdleAdvanceOnlyClosesTheOldestWindow(t *testing.T) {
	c := &clock{now: base}
	e := New(Config{
		WindowSize:  time.Minute,
		IdleTimeout: 30 * time.Second,
		Clock:       c.Now,
	})

	e.Ingest(batchOf(
		point("cpu.util", 1, 10*time.Second, nil),
		point("cpu.util", 2, 70*time.Second, nil),
		point("cpu.util", 3, 130*time.Second, nil),
	))
	// The last point already closed the first two windows by event time, so
	// drain them and confirm the third stays open.
	_, windows := e.Collect()
	e.MarkFlushed(windows)

	c.now = base.Add(time.Hour)
	if !e.AdvanceOnIdle() {
		t.Fatal("the watermark did not advance on idleness")
	}

	_, windows = e.Collect()
	if len(windows) != 1 {
		t.Errorf("collected %d windows, want only the one remaining", len(windows))
	}
}

func TestIdleAdvanceIsANoOpWithoutOpenWindows(t *testing.T) {
	c := &clock{now: base}
	e := New(Config{WindowSize: time.Minute, IdleTimeout: time.Second, Clock: c.Now})

	c.now = base.Add(time.Hour)

	// Nothing is waiting, so there is nothing to unblock.
	if e.AdvanceOnIdle() {
		t.Error("the watermark advanced with no open windows")
	}
}

func TestIdleAdvanceIsDisabledByDefault(t *testing.T) {
	// A zero timeout keeps the engine purely event-time driven, which is what
	// makes a replay bit-for-bit reproducible.
	e := New(Config{WindowSize: time.Minute})

	e.Ingest(batchOf(point("cpu.util", 1, 0, nil)))

	if e.AdvanceOnIdle() {
		t.Error("the watermark advanced despite the idle fallback being off")
	}
}

// TestSubSecondWindowsDoNotCollide covers silent data corruption.
//
// Windows were keyed by Unix *seconds*. Any window shorter than a second maps
// several distinct windows onto one key, so their points merge into a single
// rollup and the reconstructed boundaries describe none of them. Nothing
// reports it: the totals are simply wrong.
func TestSubSecondWindowsDoNotCollide(t *testing.T) {
	e := New(Config{WindowSize: 250 * time.Millisecond, MaxSeries: 100})

	// Four points, one per quarter-second: four distinct windows inside one
	// second.
	for i := range 4 {
		e.Ingest(batchOf(point("cpu.util", float64(i+1),
			time.Duration(i)*250*time.Millisecond, nil)))
	}
	// Advance well past them all.
	e.Ingest(batchOf(point("cpu.util", 99, 5*time.Second, nil)))

	rollups, windows := e.Collect()

	cpu := 0
	for i := range rollups {
		if rollups[i].Key.Metric == "cpu.util" {
			cpu++
			if rollups[i].Acc.Count != 1 {
				t.Errorf("window %s holds %d points, want 1; windows collided",
					rollups[i].Window, rollups[i].Acc.Count)
			}
		}
	}
	if cpu != 4 {
		t.Errorf("got %d cpu.util windows, want 4 distinct quarter-second windows", cpu)
	}

	// The reconstructed boundaries must be the real ones, not truncated to a
	// second.
	for _, w := range windows {
		if got := w.End.Sub(w.Start); got != 250*time.Millisecond {
			t.Errorf("window %s spans %v, want 250ms", w, got)
		}
	}
}

// TestWindowForIsAnchoredToTheEpoch: time.Truncate anchors to the zero time,
// January 1 of year 1, which coincides with the epoch only for durations that
// divide evenly into a day. A weekly window truncated that way lands on a
// boundary inherited from the calendar rather than the one an operator
// configuring "168h" would expect.
func TestWindowForIsAnchoredToTheEpoch(t *testing.T) {
	for _, size := range []time.Duration{
		time.Second, 10 * time.Second, time.Minute, 5 * time.Minute,
		time.Hour, 90 * time.Minute, 24 * time.Hour, 7 * 24 * time.Hour,
	} {
		t.Run(size.String(), func(t *testing.T) {
			ts := time.Date(2026, 9, 3, 12, 40, 17, 500_000_000, time.UTC)
			w := WindowFor(ts, size)

			// The defining property: the start is a whole number of windows
			// from the epoch.
			if rem := w.Start.UnixNano() % size.Nanoseconds(); rem != 0 {
				t.Errorf("window start %s is %d ns off an epoch boundary", w.Start, rem)
			}
			if !w.Start.After(ts) && !w.End.After(ts) {
				t.Errorf("window %s does not contain %s", w, ts)
			}
			if w.Start.After(ts) {
				t.Errorf("window %s starts after %s", w, ts)
			}
		})
	}
}

// TestWindowForHandlesPreEpochTimestamps: truncation toward zero would put a
// negative timestamp in the window *after* the one containing it.
func TestWindowForHandlesPreEpochTimestamps(t *testing.T) {
	ts := time.Date(1969, 12, 31, 23, 59, 30, 0, time.UTC) // 30s before the epoch

	w := WindowFor(ts, time.Minute)

	if w.Start.After(ts) || !w.End.After(ts) {
		t.Errorf("window %s does not contain %s", w, ts)
	}
	if want := time.Date(1969, 12, 31, 23, 59, 0, 0, time.UTC); !w.Start.Equal(want) {
		t.Errorf("start = %s, want %s", w.Start, want)
	}
}
