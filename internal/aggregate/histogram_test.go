package aggregate

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/jon-jc/fluxgate/internal/telemetry"
)

// exactQuantile computes the true quantile from the raw observations, to
// measure the histogram's error against.
func exactQuantile(values []float64, q float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)

	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func TestHistogramOnlyExistsForHistogramKind(t *testing.T) {
	// A counter's 95th percentile is not a meaningful number, and returning
	// zero would let a dashboard render it as though it were.
	for _, kind := range []telemetry.Kind{telemetry.KindCounter, telemetry.KindGauge} {
		a := NewAccumulator(kind)
		a.Observe(1, 0)

		if _, ok := a.Quantile(0.95); ok {
			t.Errorf("%s reported a quantile", kind)
		}
		if _, ok := a.Buckets(); ok {
			t.Errorf("%s allocated histogram buckets", kind)
		}
	}

	a := NewAccumulator(telemetry.KindHistogram)
	a.Observe(1, 0)
	if _, ok := a.Quantile(0.95); !ok {
		t.Error("a histogram series reported no quantile")
	}
}

func TestQuantileOfEmptyHistogram(t *testing.T) {
	a := NewAccumulator(telemetry.KindHistogram)

	v, ok := a.Quantile(0.5)
	if ok {
		t.Errorf("an empty accumulator reported a quantile of %v", v)
	}
}

// TestQuantileAccuracy pins the guarantee the bucket layout is chosen for:
// relative error stays within a few percent across the whole range.
func TestQuantileAccuracy(t *testing.T) {
	rng := rand.New(rand.NewSource(1))

	// A latency-shaped distribution: mostly fast, with a heavy tail.
	values := make([]float64, 0, 20_000)
	for range 18_000 {
		values = append(values, 1+rng.Float64()*20) // 1-21ms
	}
	for range 1_900 {
		values = append(values, 50+rng.Float64()*200) // 50-250ms
	}
	for range 100 {
		values = append(values, 1_000+rng.Float64()*4_000) // 1-5s
	}

	a := NewAccumulator(telemetry.KindHistogram)
	for i, v := range values {
		a.Observe(v, int64(i))
	}

	for _, q := range []float64{0.5, 0.9, 0.95, 0.99, 0.999} {
		got, ok := a.Quantile(q)
		if !ok {
			t.Fatalf("no quantile for q=%v", q)
		}

		want := exactQuantile(values, q)
		relErr := math.Abs(got-want) / want

		// The layout bounds relative error at roughly the bucket growth ratio.
		if relErr > 0.16 {
			t.Errorf("q=%v: got %.3f, want %.3f (relative error %.1f%%)",
				q, got, want, relErr*100)
		}
	}
}

// TestQuantileNeverUnderReports matters for an SLO: an estimate that rounds
// down would let a breach quietly pass.
func TestQuantileNeverUnderReports(t *testing.T) {
	a := NewAccumulator(telemetry.KindHistogram)

	values := []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000}
	for i, v := range values {
		a.Observe(v, int64(i))
	}

	got, _ := a.Quantile(1.0)
	if got < 1000 {
		t.Errorf("p100 = %v, want at least the largest observation 1000", got)
	}
}

func TestQuantileClampsOutOfRangeArguments(t *testing.T) {
	a := NewAccumulator(telemetry.KindHistogram)
	for i := range 100 {
		a.Observe(float64(i+1), int64(i))
	}

	low, _ := a.Quantile(-1)
	high, _ := a.Quantile(2)

	if low <= 0 {
		t.Errorf("q=-1 gave %v, want it clamped to the minimum", low)
	}
	if high < 100 {
		t.Errorf("q=2 gave %v, want it clamped to the maximum", high)
	}
}

// TestHistogramHandlesNonPositiveValues keeps Count consistent with the
// histogram's own total: a gauge can legitimately be negative or zero, and
// silently dropping those would make the two disagree.
func TestHistogramHandlesNonPositiveValues(t *testing.T) {
	a := NewAccumulator(telemetry.KindHistogram)

	for i, v := range []float64{-5, -1, 0, 0, 1, 10} {
		a.Observe(v, int64(i))
	}

	counts, ok := a.Buckets()
	if !ok {
		t.Fatal("no buckets")
	}

	var total int64
	for _, c := range counts {
		total += c
	}
	if total != a.Count {
		t.Errorf("bucket total = %d, but Count = %d", total, a.Count)
	}
	// Negatives and zeros are tracked separately, at the ends of the layout.
	if counts[0] != 2 {
		t.Errorf("negative count = %d, want 2", counts[0])
	}
	if counts[1] != 2 {
		t.Errorf("zero count = %d, want 2", counts[1])
	}
}

func TestHistogramIgnoresNaN(t *testing.T) {
	// Validation rejects these at the edge; the fallback must not corrupt
	// every quantile in the window.
	a := NewAccumulator(telemetry.KindHistogram)
	a.Observe(10, 0)
	a.Observe(math.NaN(), 1)

	counts, _ := a.Buckets()
	var total int64
	for _, c := range counts {
		total += c
	}
	if total != 1 {
		t.Errorf("bucket total = %d, want 1; the NaN should not have been bucketed", total)
	}
}

func TestHistogramOverflow(t *testing.T) {
	a := NewAccumulator(telemetry.KindHistogram)

	// Far beyond the representable range.
	a.Observe(1e12, 0)
	a.Observe(1e15, 1)

	counts, _ := a.Buckets()
	if counts[len(counts)-1] != 2 {
		t.Errorf("overflow count = %d, want 2", counts[len(counts)-1])
	}

	got, ok := a.Quantile(0.99)
	if !ok {
		t.Fatal("no quantile")
	}
	if got <= 0 {
		t.Errorf("quantile of overflow observations = %v, want the top of the range", got)
	}
}

// TestHistogramMergeMatchesDirectObservation is what makes partial results
// combinable across a restart or across shards.
func TestHistogramMergeMatchesDirectObservation(t *testing.T) {
	rng := rand.New(rand.NewSource(7))

	values := make([]float64, 5_000)
	for i := range values {
		values[i] = rng.Float64() * 500
	}

	whole := NewAccumulator(telemetry.KindHistogram)
	for i, v := range values {
		whole.Observe(v, int64(i))
	}

	left := NewAccumulator(telemetry.KindHistogram)
	right := NewAccumulator(telemetry.KindHistogram)
	for i, v := range values {
		if i%2 == 0 {
			left.Observe(v, int64(i))
			continue
		}
		right.Observe(v, int64(i))
	}
	left.Merge(right)

	for _, q := range []float64{0.5, 0.9, 0.99} {
		merged, _ := left.Quantile(q)
		direct, _ := whole.Quantile(q)
		if merged != direct {
			t.Errorf("q=%v: merged %v, direct %v; the fixed layout should make these identical",
				q, merged, direct)
		}
	}
}

func TestHistogramRoundTripsThroughStoredCounts(t *testing.T) {
	a := NewAccumulator(telemetry.KindHistogram)
	for i := range 500 {
		a.Observe(float64(i%97)+0.5, int64(i))
	}

	counts, ok := a.Buckets()
	if !ok {
		t.Fatal("no buckets")
	}

	restored := histogramFromCounts(counts)
	if restored.total() != a.Count {
		t.Errorf("restored total = %d, want %d", restored.total(), a.Count)
	}
	for _, q := range []float64{0.5, 0.95, 0.99} {
		want, _ := a.Quantile(q)
		if got := restored.quantile(q); got != want {
			t.Errorf("q=%v: restored %v, want %v", q, got, want)
		}
	}
}

func TestHistogramFromMalformedCountsIsEmpty(t *testing.T) {
	// A row written by a future schema with a different layout must not be
	// misread as data in this one.
	for _, counts := range [][]int64{nil, {1, 2, 3}, make([]int64, histogramBuckets)} {
		if got := histogramFromCounts(counts).total(); got != 0 {
			t.Errorf("malformed counts produced a total of %d, want 0", got)
		}
	}
}

func TestBucketBoundariesAreMonotonic(t *testing.T) {
	previous := 0.0
	for i := range histogramBuckets {
		b := boundary(i)
		if b <= previous {
			t.Fatalf("boundary %d = %v, not greater than the previous %v", i, b, previous)
		}
		previous = b
	}
}

func TestBucketForIsConsistentWithBoundaries(t *testing.T) {
	for _, v := range []float64{0.0001, 0.5, 1, 12.5, 100, 5_000, 100_000} {
		idx := bucketFor(v)
		if idx >= histogramBuckets {
			continue
		}
		if b := boundary(idx); b < v {
			t.Errorf("value %v landed in bucket %d whose upper bound is %v", v, idx, b)
		}
	}
}

func BenchmarkHistogramObserve(b *testing.B) {
	a := NewAccumulator(telemetry.KindHistogram)

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		a.Observe(float64(i%1000)+0.5, int64(i))
		i++
	}
}
