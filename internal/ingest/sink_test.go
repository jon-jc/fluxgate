package ingest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/telemetry"
)

func batch(id string, points int) telemetry.Batch {
	pts := make([]telemetry.Point, points)
	for i := range pts {
		pts[i] = telemetry.Point{
			Metric:    "a.b",
			Kind:      telemetry.KindGauge,
			Value:     float64(i),
			Timestamp: time.Now(),
		}
	}
	return telemetry.Batch{ID: id, TenantID: "acme", Points: pts}
}

func TestMemorySinkRetainsBatches(t *testing.T) {
	s := NewMemorySink()

	if err := s.Publish(context.Background(), batch("b1", 3)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := s.Publish(context.Background(), batch("b2", 2)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if got := len(s.Batches()); got != 2 {
		t.Errorf("Batches() has %d entries, want 2", got)
	}
	if got := s.PointCount(); got != 5 {
		t.Errorf("PointCount() = %d, want 5", got)
	}
}

// TestBatchesReturnsACopy stops a caller mutating the sink's own slice.
func TestBatchesReturnsACopy(t *testing.T) {
	s := NewMemorySink()
	if err := s.Publish(context.Background(), batch("b1", 1)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got := s.Batches()
	got[0] = telemetry.Batch{ID: "tampered"}

	if s.Batches()[0].ID != "b1" {
		t.Error("mutating the returned slice changed the sink's contents")
	}
}

func TestMemorySinkReset(t *testing.T) {
	s := NewMemorySink()
	if err := s.Publish(context.Background(), batch("b1", 4)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	s.Reset()

	if got := len(s.Batches()); got != 0 {
		t.Errorf("Batches() has %d entries after Reset, want 0", got)
	}
	if got := s.PointCount(); got != 0 {
		t.Errorf("PointCount() = %d after Reset, want 0", got)
	}
}

func TestSinkFuncAdaptsAFunction(t *testing.T) {
	want := errors.New("nope")

	var got telemetry.Batch
	s := SinkFunc(func(_ context.Context, b telemetry.Batch) error {
		got = b
		return want
	})

	if err := s.Publish(context.Background(), batch("b1", 1)); !errors.Is(err, want) {
		t.Errorf("Publish = %v, want %v", err, want)
	}
	if got.ID != "b1" {
		t.Errorf("the adapter did not pass the batch through: %+v", got)
	}
}

func TestMemorySinkIsConcurrencySafe(t *testing.T) {
	s := NewMemorySink()

	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for range 50 {
				_ = s.Publish(context.Background(), batch("b", 2))
			}
		}(g)
	}
	wg.Wait()

	if got := s.PointCount(); got != 16*50*2 {
		t.Errorf("PointCount() = %d, want %d", got, 16*50*2)
	}
}
