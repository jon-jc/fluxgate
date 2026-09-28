package main

import (
	"testing"
	"time"
)

func TestReportFailsOnClientAndTransportErrors(t *testing.T) {
	for _, status := range []int{401, 409, 429, 503} {
		s := summary{byStatus: map[int]int{status: 1}, latencies: []time.Duration{time.Millisecond}, started: time.Now().Add(-time.Second), ended: time.Now()}
		if err := report(s, config{batchSize: 1}); err == nil {
			t.Fatalf("status %d reported as a successful load run", status)
		}
	}
	s := summary{byStatus: map[int]int{}, errors: 1, started: time.Now().Add(-time.Second), ended: time.Now()}
	if err := report(s, config{batchSize: 1}); err == nil {
		t.Fatal("transport failure reported as success")
	}
}

func TestInvalidLoadConfigurationReturnsError(t *testing.T) {
	if err := run(config{workers: 1, batchSize: 1, metrics: 0, hosts: 1, duration: time.Second}); err == nil {
		t.Fatal("zero metrics accepted")
	}
}
