package aggregate

import (
	"math"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/telemetry"
)

func TestExtremeHistogramValueUsesOverflowBucket(t *testing.T) {
	h := newHistogram()
	h.observe(math.MaxFloat64)
	if h.overflow != 1 || h.buckets[0] != 0 {
		t.Fatalf("large finite value went to the wrong bucket: %+v", h)
	}
}

func TestLastValueUsesDatabasePrecision(t *testing.T) {
	timestamp := time.Now().Truncate(time.Microsecond).UnixNano()
	a := NewAccumulator(telemetry.KindGauge)
	a.Observe(20, timestamp+1)
	a.Observe(10, timestamp+900)
	b := NewAccumulator(telemetry.KindGauge)
	b.Observe(10, timestamp+900)
	b.Observe(20, timestamp+1)
	if a.Last != 20 || b.Last != 20 || a.LastTimestampUnixNano != timestamp {
		t.Fatalf("unstable microsecond tie: %+v / %+v", a, b)
	}
}
