// Package aggregate turns a stream of individual observations into windowed
// rollups.
//
// The package is deliberately free of I/O. Windowing, watermarks and
// accumulation are pure computation over values and an injectable clock, which
// is what makes the interesting cases -- late data, window boundaries, quantile
// accuracy -- testable without a broker or a database in the loop.
package aggregate

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"sort"
	"strings"

	"github.com/jon-jc/fluxgate/internal/telemetry"
)

// SeriesKey identifies one time series: a metric, broken down by one exact
// combination of label values, belonging to one tenant.
type SeriesKey struct {
	TenantID string
	Metric   string
	Kind     telemetry.Kind
	// LabelHash identifies the label set. The labels themselves are carried
	// alongside on the rollup so a reader never has to reverse the hash.
	LabelHash string
}

// SeriesKeyFor derives the key for a point.
func SeriesKeyFor(tenantID string, p telemetry.Point) SeriesKey {
	return SeriesKey{
		TenantID:  tenantID,
		Metric:    p.Metric,
		Kind:      p.Kind,
		LabelHash: HashLabels(p.Labels),
	}
}

// HashLabels returns a stable digest of a label set.
//
// Go map iteration order is deliberately randomised, so the keys are sorted
// before hashing: without that, the same label set would hash differently on
// each call and every point would land in a series of its own.
//
// Keys and values are length-prefixed rather than merely delimited, so that
// {"ab": "c"} and {"a": "bc"} cannot collide by concatenating to the same
// string.
func HashLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return emptyLabelHash
	}

	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	var buf [20]byte
	for _, k := range keys {
		writeLengthPrefixed(h, buf[:0], k)
		writeLengthPrefixed(h, buf[:0], labels[k])
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// emptyLabelHash is the digest for a point with no labels, precomputed so the
// common case allocates nothing.
var emptyLabelHash = func() string {
	sum := sha256.Sum256(nil)
	return hex.EncodeToString(sum[:])[:32]
}()

type byteWriter interface{ Write([]byte) (int, error) }

func writeLengthPrefixed(h byteWriter, buf []byte, s string) {
	buf = appendUint(buf, uint64(len(s)))
	buf = append(buf, ':')
	_, _ = h.Write(buf)
	_, _ = h.Write([]byte(s))
}

func appendUint(buf []byte, n uint64) []byte {
	if n == 0 {
		return append(buf, '0')
	}
	var digits [20]byte
	i := len(digits)
	for n > 0 {
		i--
		digits[i] = byte('0' + n%10)
		n /= 10
	}
	return append(buf, digits[i:]...)
}

// Accumulator holds the running aggregate for one series in one window.
//
// Every statistic here is computed incrementally in constant space. Retaining
// the individual observations to compute them at flush time would make memory
// proportional to throughput, which is precisely the property a streaming
// aggregator exists to avoid.
type Accumulator struct {
	// Count is how many observations landed in this window.
	Count int64
	// Sum is their total.
	Sum float64
	// Min and Max are the extremes.
	Min float64
	Max float64
	// Last is the most recent value by event time, not arrival order: messages
	// can be redelivered and reordered, so "the latest one we happened to see"
	// is not a stable answer.
	Last float64
	// LastTimestampUnixNano orders Last.
	LastTimestampUnixNano int64

	// histogram bounds the distribution for quantile estimation. It is only
	// populated for histogram-kind series, since a counter's quantiles are
	// meaningless and the buckets would be pure overhead.
	histogram *histogram
}

// NewAccumulator returns an accumulator ready for the given kind.
func NewAccumulator(kind telemetry.Kind) *Accumulator {
	a := &Accumulator{
		Min: math.Inf(1),
		Max: math.Inf(-1),
	}
	if kind == telemetry.KindHistogram {
		a.histogram = newHistogram()
	}
	return a
}

// Observe folds one point into the aggregate.
func (a *Accumulator) Observe(value float64, timestampUnixNano int64) {
	a.Count++
	a.Sum += value

	if value < a.Min {
		a.Min = value
	}
	if value > a.Max {
		a.Max = value
	}

	// Ordering by event time rather than arrival keeps Last stable across
	// redelivery: the same window replayed in a different order must produce
	// the same answer.
	if a.Count == 1 || timestampUnixNano >= a.LastTimestampUnixNano {
		a.Last = value
		a.LastTimestampUnixNano = timestampUnixNano
	}

	if a.histogram != nil {
		a.histogram.observe(value)
	}
}

// Mean returns the arithmetic mean, or zero for an empty accumulator.
func (a *Accumulator) Mean() float64 {
	if a.Count == 0 {
		return 0
	}
	return a.Sum / float64(a.Count)
}

// Quantile estimates the value below which q of the observations fall.
//
// It returns false for a series with no histogram: a counter's 95th percentile
// is not a meaningful number, and returning zero would let a dashboard render
// it as though it were.
func (a *Accumulator) Quantile(q float64) (float64, bool) {
	if a.histogram == nil || a.Count == 0 {
		return 0, false
	}
	return a.histogram.quantile(q), true
}

// Buckets returns the histogram's bucket counts, for storage. The boolean
// reports whether this series has a histogram at all.
func (a *Accumulator) Buckets() ([]int64, bool) {
	if a.histogram == nil {
		return nil, false
	}
	return a.histogram.counts(), true
}

// Merge folds another accumulator into this one.
//
// Merging is what lets partial results be combined -- across a restart, or
// across shards -- and it is only sound because every statistic here is
// associative: sum, count, min and max all combine without reference to the
// order the observations arrived in.
func (a *Accumulator) Merge(other *Accumulator) {
	if other == nil || other.Count == 0 {
		return
	}
	if a.Count == 0 {
		a.Min = other.Min
		a.Max = other.Max
	} else {
		a.Min = math.Min(a.Min, other.Min)
		a.Max = math.Max(a.Max, other.Max)
	}

	a.Count += other.Count
	a.Sum += other.Sum

	if other.LastTimestampUnixNano >= a.LastTimestampUnixNano {
		a.Last = other.Last
		a.LastTimestampUnixNano = other.LastTimestampUnixNano
	}

	if a.histogram != nil && other.histogram != nil {
		a.histogram.merge(other.histogram)
	}
}

// MinValue returns Min, or zero when nothing has been observed, so callers do
// not have to handle an infinity they never asked for.
func (a *Accumulator) MinValue() float64 {
	if a.Count == 0 {
		return 0
	}
	return a.Min
}

// MaxValue returns Max, or zero when nothing has been observed.
func (a *Accumulator) MaxValue() float64 {
	if a.Count == 0 {
		return 0
	}
	return a.Max
}

// LabelsFingerprint renders a label set as a stable, human-readable string,
// used in logs where a hash would tell an operator nothing.
func LabelsFingerprint(labels map[string]string) string {
	if len(labels) == 0 {
		return "{}"
	}

	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

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
