package telemetry

import (
	"math"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// FuzzValidatePoint asserts that a point the validator passes is genuinely safe
// for every stage downstream of it.
//
// ValidatePoint is the contract the rest of the pipeline is written against.
// The aggregator does not re-check anything: it hashes the labels, adds the
// value into an accumulator, and writes the result to Postgres. So a point that
// slips through with a NaN corrupts a rollup permanently (NaN poisons the sum
// for that series forever, and it is not recoverable by reaggregating), an
// unbounded label value becomes an unbounded row, and an out-of-range timestamp
// lands in a window that has already been flushed.
//
// Returning no violations is therefore a promise about all of it, and this
// asserts the promise directly rather than trusting the branch structure.
func FuzzValidatePoint(f *testing.F) {
	f.Add("http.requests", "counter", 1.0, int64(0), "service", "checkout")
	f.Add("", "", 0.0, int64(0), "", "")
	f.Add("m", "gauge", math.Inf(1), int64(0), "k", "v")
	f.Add("m", "gauge", math.NaN(), int64(0), "k", "v")
	f.Add("m", "counter", -1.0, int64(0), "k", "v")
	f.Add(strings.Repeat("m", 4096), "histogram", 1.0, int64(0), "k", "v")
	f.Add("m", "gauge", 1.0, int64(0), "__reserved", "v")
	f.Add("m", "gauge", 1.0, int64(1<<62), "k", strings.Repeat("v", 65536))
	f.Add("m.n_o-p", "GAUGE", 1.0, int64(-1), "k.1", "v")

	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	v := Validator{Limits: DefaultLimits(), Clock: func() time.Time { return now }}
	limits := v.Limits

	f.Fuzz(func(t *testing.T, metric, kind string, value float64, offset int64,
		labelKey, labelValue string,
	) {
		// Keep the offset inside what time.Time represents; a client cannot
		// express anything wider on the wire.
		if offset > 1<<61 || offset < -(1<<61) {
			t.Skip()
		}

		p := Point{
			Metric:    metric,
			Kind:      Kind(kind),
			Value:     value,
			Timestamp: now.Add(time.Duration(offset)),
			Labels:    map[string]string{labelKey: labelValue},
		}
		if labelKey == "" && labelValue == "" {
			p.Labels = nil
		}

		violations := v.ValidatePoint(0, p)
		if len(violations) > 0 {
			// Every rejection must name a field, or the 422 body tells the
			// caller something is wrong without saying what.
			for _, viol := range violations {
				if !strings.HasPrefix(viol.Field, "points.0") {
					t.Fatalf("violation field %q is not addressed to the point "+
						"that caused it", viol.Field)
				}
				if strings.TrimSpace(viol.Message) == "" {
					t.Fatalf("violation on %q carries no message", viol.Field)
				}
			}
			return
		}

		// Accepted. Everything below is what the aggregator assumes without
		// checking.
		if math.IsNaN(p.Value) || math.IsInf(p.Value, 0) {
			t.Fatalf("accepted a non-finite value (%v), which would poison "+
				"every future rollup for this series", p.Value)
		}
		if !p.Kind.Valid() {
			t.Fatalf("accepted kind %q, which no accumulator implements", p.Kind)
		}
		if p.Kind == KindCounter && p.Value < 0 {
			t.Fatalf("accepted a negative counter delta (%v)", p.Value)
		}
		if p.Metric == "" || len(p.Metric) > limits.MaxMetricNameLen {
			t.Fatalf("accepted metric name of length %d (limit %d)",
				len(p.Metric), limits.MaxMetricNameLen)
		}
		if len(p.Labels) > limits.MaxLabels {
			t.Fatalf("accepted %d labels (limit %d)", len(p.Labels), limits.MaxLabels)
		}
		for k, val := range p.Labels {
			if k == "" {
				t.Fatal("accepted an empty label key, which is not addressable")
			}
			if len(k) > limits.MaxLabelKeyLen || len(val) > limits.MaxLabelValueLen {
				t.Fatalf("accepted an oversized label: %d/%d bytes (limits %d/%d)",
					len(k), len(val), limits.MaxLabelKeyLen, limits.MaxLabelValueLen)
			}
			if !utf8.ValidString(k) || !utf8.ValidString(val) {
				t.Fatalf("accepted a non-UTF-8 label %q=%q, which JSON cannot "+
					"carry to the aggregator unchanged", k, val)
			}
		}

		// The timestamp must sit inside the window the engine will still
		// accept, in both directions.
		if ahead := p.Timestamp.Sub(now); ahead > limits.MaxClockSkew {
			t.Fatalf("accepted a timestamp %s in the future (skew limit %s)",
				ahead, limits.MaxClockSkew)
		}
		if behind := now.Sub(p.Timestamp); behind > limits.MaxBackfill {
			t.Fatalf("accepted a timestamp %s in the past (backfill limit %s)",
				behind, limits.MaxBackfill)
		}

		// Normalization must not undo any of the above.
		n := p.Normalize(now)
		if n.Timestamp.IsZero() || n.Timestamp.Location() != time.UTC {
			t.Fatalf("normalize produced %s, which is not a UTC instant",
				n.Timestamp)
		}
		if len(v.ValidatePoint(0, n)) != 0 {
			t.Fatalf("a valid point stopped being valid after normalization: %+v", n)
		}
	})
}
