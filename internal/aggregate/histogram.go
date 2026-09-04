package aggregate

import "math"

// Histogram layout.
//
// Buckets are exponential with a fixed base, which is what makes them useful
// for latency: absolute error grows with the value, so a 1ms measurement is
// resolved far more finely than a 10s one -- exactly the trade you want when
// the interesting question is "is p99 2ms or 20ms", not "is it 10.001s or
// 10.002s".
//
// The layout is fixed rather than adaptive so that two accumulators can always
// be merged by adding their bucket counts. An adaptive layout would give
// slightly better accuracy per series and make merging -- across a restart, or
// across shards -- impossible without re-deriving boundaries from data that has
// already been discarded.
const (
	// histogramGrowth is the ratio between consecutive bucket boundaries.
	// 1.15 bounds the relative error of any estimate at about 7%, which is far
	// finer than the decisions a latency percentile is used to make.
	histogramGrowth = 1.15
	// histogramMin is the lowest positive boundary, in the same unit as the
	// observations. Values below it land in the first bucket.
	histogramMin = 1e-3
	// histogramBuckets covers histogramMin up to roughly 1e6 -- microseconds to
	// hours if the unit is milliseconds -- in 150 buckets, which is 1.2KB per
	// series and cheap enough to keep one per active series.
	histogramBuckets = 150
)

// histogram is a fixed-layout exponential histogram.
type histogram struct {
	// negative and zero counts sit outside the exponential layout, which is
	// only defined for positive values. A gauge can legitimately be negative,
	// and silently dropping those observations would make Count disagree with
	// the histogram's own total.
	negative int64
	zero     int64
	buckets  [histogramBuckets]int64
	overflow int64
}

func newHistogram() *histogram { return &histogram{} }

// bucketFor returns the index a positive value belongs in.
func bucketFor(v float64) int {
	if v <= histogramMin {
		return 0
	}
	idx := int(math.Log(v/histogramMin) / math.Log(histogramGrowth))
	if idx < 0 {
		return 0
	}
	return idx
}

// boundary returns the upper bound of bucket i.
func boundary(i int) float64 {
	return histogramMin * math.Pow(histogramGrowth, float64(i+1))
}

func (h *histogram) observe(v float64) {
	switch {
	case math.IsNaN(v):
		// Validation rejects these at the edge; ignoring one here rather than
		// letting it corrupt every quantile is the safe fallback.
		return
	case v < 0:
		h.negative++
		return
	case v == 0:
		h.zero++
		return
	}

	idx := bucketFor(v)
	if idx >= histogramBuckets {
		h.overflow++
		return
	}
	h.buckets[idx]++
}

func (h *histogram) merge(other *histogram) {
	h.negative += other.negative
	h.zero += other.zero
	h.overflow += other.overflow
	for i := range h.buckets {
		h.buckets[i] += other.buckets[i]
	}
}

func (h *histogram) total() int64 {
	total := h.negative + h.zero + h.overflow
	// Ranging over the array itself would copy all 1200 bytes of it on every
	// call, and total() is on the path of every quantile.
	for _, c := range &h.buckets {
		total += c
	}
	return total
}

// quantile estimates the value below which q of the observations fall.
//
// The estimate is the upper boundary of the bucket the target rank falls in,
// which is the conventional and conservative choice: it never under-reports a
// latency percentile, so an SLO evaluated against it does not quietly pass on
// rounding.
func (h *histogram) quantile(q float64) float64 {
	total := h.total()
	if total == 0 {
		return 0
	}

	q = math.Min(math.Max(q, 0), 1)
	// Rank is 1-based: the 100th percentile is the last observation, not one
	// past it.
	target := int64(math.Ceil(q * float64(total)))
	if target < 1 {
		target = 1
	}

	cumulative := h.negative
	if target <= cumulative {
		// The exact value is not recoverable from a single lumped count; zero
		// is the tightest honest upper bound for the negative region.
		return 0
	}

	cumulative += h.zero
	if target <= cumulative {
		return 0
	}

	for i, count := range &h.buckets {
		cumulative += count
		if target <= cumulative {
			return boundary(i)
		}
	}

	// Everything remaining is in the overflow bucket, which has no upper
	// boundary; report the top of the representable range.
	return boundary(histogramBuckets - 1)
}

// counts returns the bucket counts for storage, with the out-of-layout
// observations at the ends: [negative, zero, ...buckets, overflow].
func (h *histogram) counts() []int64 {
	out := make([]int64, 0, histogramBuckets+3)
	out = append(out, h.negative, h.zero)
	out = append(out, h.buckets[:]...)
	out = append(out, h.overflow)
	return out
}

// histogramFromCounts rebuilds a histogram from stored counts, so a rollup read
// back from the database can be merged with a live one.
func histogramFromCounts(counts []int64) *histogram {
	h := newHistogram()
	if len(counts) != histogramBuckets+3 {
		return h
	}
	h.negative = counts[0]
	h.zero = counts[1]
	copy(h.buckets[:], counts[2:2+histogramBuckets])
	h.overflow = counts[len(counts)-1]
	return h
}
