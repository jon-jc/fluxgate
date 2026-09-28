package api

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/idempotency"
	"github.com/jon-jc/fluxgate/internal/ingest"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

func TestAmbiguousPublishReusesIdentityAndTimestamps(t *testing.T) {
	var batches []telemetry.Batch
	sink := ingest.SinkFunc(func(_ context.Context, b telemetry.Batch) error {
		batches = append(batches, b)
		if len(batches) == 1 {
			return errors.New("broker accepted, response lost")
		}
		return nil
	})
	h := newHarness(t, withSink(sink))
	body := `{"points":[{"metric":"requests","kind":"counter","value":1}]}`
	headers := map[string]string{HeaderIdempotencyKey: "retry"}
	if rec := h.post(t, body, headers); rec.Code != 503 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
	}
	if rec := h.post(t, body, headers); rec.Code != 202 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
	}
	if len(batches) != 2 || batches[0].ID != batches[1].ID || !batches[0].Points[0].Timestamp.Equal(batches[1].Points[0].Timestamp) {
		t.Fatalf("identity changed: %+v", batches)
	}
	if rec := h.post(t, body, headers); rec.Code != 202 {
		t.Fatal(rec.Body)
	}
	if len(batches) != 2 {
		t.Fatal("confirmed response was republished")
	}
}

func TestConcurrentRequestsShareReservedBatch(t *testing.T) {
	var mu sync.Mutex
	ids := make(map[string]bool)
	sink := ingest.SinkFunc(func(_ context.Context, b telemetry.Batch) error {
		mu.Lock()
		defer mu.Unlock()
		ids[b.ID] = true
		return nil
	})
	h := newHarness(t, withSink(sink))
	body := h.validBody(1)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempt := 0; attempt < 3; attempt++ {
				rec := h.post(t, body, map[string]string{HeaderIdempotencyKey: "shared"})
				if rec.Code == 503 && rec.Header().Get("Retry-After") == "1" {
					// This burst deliberately exceeds the default admission cap.
					// Retry the same payload/key as a real caller must.
					time.Sleep(time.Second)
					continue
				}
				if rec.Code != 202 {
					t.Error(rec.Body)
				}
				return
			}
			t.Error("concurrent retry did not recover from admission pressure")
		}()
	}
	wg.Wait()
	if len(ids) != 1 {
		t.Fatalf("published %d distinct batch IDs", len(ids))
	}
}

func TestReservationCapacityFailsBeforePublish(t *testing.T) {
	s := idempotency.New(time.Hour, idempotency.WithMaxSize(1))
	ctx := context.Background()
	if _, err := s.Reserve(ctx, "tenant", "first", idempotency.Record{Fingerprint: "first"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Reserve(ctx, "tenant", "second", idempotency.Record{Fingerprint: "second"}); !errors.Is(err, idempotency.ErrFull) {
		t.Fatalf("err=%v", err)
	}
	if _, err := s.Reserve(ctx, "tenant", "first", idempotency.Record{Fingerprint: "changed"}); !errors.Is(err, idempotency.ErrPayloadMismatch) {
		t.Fatalf("err=%v", err)
	}
}
