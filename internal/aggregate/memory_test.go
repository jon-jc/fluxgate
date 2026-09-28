package aggregate

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/telemetry"
)

func TestByteBudgetIsAtomicAndReleasedOnCollect(t *testing.T) {
	p := point("wide", 1, 0, map[string]string{"value": strings.Repeat("x", 256)})
	p.Kind = telemetry.KindHistogram
	one := seriesBytes(SeriesKeyFor("acme", p), p.Labels)
	e := New(Config{MaxBytes: one, MaxSeries: 100})
	if _, err := e.IngestDurable(batchOf(p, p)); err != nil {
		t.Fatal(err)
	}
	if got := e.Stats().BufferedBytes; got != one {
		t.Fatalf("same series counted twice: %d", got)
	}
	other := p
	other.Timestamp = p.Timestamp.Add(time.Minute)
	if _, err := e.IngestDurable(batchOf(p, other)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("new window bypassed byte budget: %v", err)
	}
	rows, _ := e.CollectAll()
	if len(rows) != 1 || rows[0].Acc.Count != 2 {
		t.Fatal("rejected batch was partially admitted")
	}
	if s := e.Stats(); s.BufferedBytes != 0 || s.TrackedSeries != 0 {
		t.Fatalf("unreleased budget: %+v", s)
	}
	if _, err := e.IngestDurable(batchOf(other)); err != nil {
		t.Fatal("freed budget cannot be reused", err)
	}
}

func TestRejectedPointDoesNotCreateEmptyWindows(t *testing.T) {
	e := New(Config{MaxBytes: 1, MaxSeries: 100})
	if got := e.Ingest(batchOf(point("too.big", 1, 0, nil))); got.Shed != 1 {
		t.Fatal(got)
	}
	if e.Stats().OpenWindows != 0 {
		t.Fatal("rejection retained an empty window")
	}
}
