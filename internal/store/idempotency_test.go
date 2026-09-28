package store_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/idempotency"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

func retryRecord(id string) idempotency.Record {
	return idempotency.Record{Status: 202, Body: []byte(`{"accepted":1}`), Fingerprint: "original-body",
		Batch: telemetry.Batch{ID: id, TenantID: "tenant", ReceivedAt: base, Points: []telemetry.Point{{Metric: "cpu", Kind: telemetry.KindGauge, Value: 5, Timestamp: base}}}}
}

func TestRetryReservationSurvivesNewRepository(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	first := idempotency.NewPostgres(db.Pool(), time.Hour)
	reserved, err := first.Reserve(ctx, tenant, "request", retryRecord("original"))
	if err != nil {
		t.Fatal(err)
	}
	if reserved.Published {
		t.Fatal("unpublished reservation reported success")
	}
	second := idempotency.NewPostgres(db.Pool(), time.Hour)
	again, err := second.Reserve(ctx, tenant, "request", retryRecord("replacement"))
	if err != nil || again.Batch.ID != "original" || !again.Batch.ReceivedAt.Equal(base) {
		t.Fatalf("reservation=%+v err=%v", again, err)
	}
	if err = second.Complete(ctx, tenant, "request", "original"); err != nil {
		t.Fatal(err)
	}
	replayed, found, err := first.Get(ctx, tenant, "request", "original-body")
	if err != nil || !found || !replayed.Published || !bytes.Equal(replayed.Body, reserved.Body) {
		t.Fatalf("replay=%+v found=%v err=%v", replayed, found, err)
	}
	if replayed.Batch.ID != reserved.Batch.ID || len(replayed.Batch.Points) != 0 {
		t.Fatal("completed outcome must preserve its identity and reclaim the telemetry payload")
	}
	if _, _, err = second.Get(ctx, tenant, "request", "changed-body"); !errors.Is(err, idempotency.ErrPayloadMismatch) {
		t.Fatalf("mismatch: %v", err)
	}
	if _, found, err = second.Get(ctx, tenant+"-other", "request", "original-body"); err != nil || found {
		t.Fatal("tenant boundary broken")
	}
}

func TestConcurrentRetryReservationsHaveOneIdentity(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	ids := make(chan string, 16)
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := idempotency.NewPostgres(db.Pool(), time.Hour)
			r, err := p.Reserve(ctx, tenant, "same-key", retryRecord(fmt.Sprint(i)))
			if err != nil {
				t.Error(err)
				return
			}
			ids <- r.Batch.ID
		}()
	}
	wg.Wait()
	close(ids)
	winner := ""
	for id := range ids {
		if winner == "" {
			winner = id
		}
		if id != winner {
			t.Fatalf("two identities: %q and %q", winner, id)
		}
	}
	if winner == "" {
		t.Fatal("no reservation succeeded")
	}
}

func TestExpiredRetryCannotCompleteReplacement(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()
	p := idempotency.NewPostgres(db.Pool(), time.Hour)
	if _, err := p.Reserve(ctx, tenant, "key", retryRecord("old")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool().Exec(ctx, "UPDATE ingest_requests SET expires_at=now()-interval '1 second' WHERE tenant_id=$1", tenant); err != nil {
		t.Fatal(err)
	}
	r, err := p.Reserve(ctx, tenant, "key", retryRecord("new"))
	if err != nil || r.Batch.ID != "new" {
		t.Fatalf("reservation=%+v err=%v", r, err)
	}
	if err = p.Complete(ctx, tenant, "key", "old"); !errors.Is(err, idempotency.ErrExpired) {
		t.Fatalf("old completion: %v", err)
	}
}
