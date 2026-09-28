package observability

import (
	"fmt"
	"testing"
	"time"
)

func TestArbitraryMethodsCannotGrowMetricSeries(t *testing.T) {
	m := NewMetrics("bounded-test")
	for i := range 2000 {
		m.ObserveRequest("unmatched", fmt.Sprintf("METHOD%d", i), 404, time.Millisecond)
	}
	m.ObserveRequest("unmatched", "GET", 404, time.Millisecond)
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "fluxgate_http_requests_total" {
			if len(family.Metric) != 2 {
				t.Fatalf("unbounded method series: %d", len(family.Metric))
			}
			return
		}
	}
	t.Fatal("request counter missing")
}
