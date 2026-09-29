package aggregator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
)

func runCheckpointer(t *testing.T, r *Runner) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(2 * time.Second):
			t.Error("checkpointer did not stop")
		}
	})
}

func awaitCheckpoint(t *testing.T, r *Runner, acks int64) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if s := r.Stats(); s.MessagesAcked == acks && s.PendingMessages == 0 && s.PendingBytes == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("checkpoint not settled: %+v", r.Stats())
		case <-ticker.C:
		}
	}
}

func TestTimerCheckpointsOpenWindowWithoutIdleOrNewEvents(t *testing.T) {
	s := newFakeStore()
	r := newRunner(t, s)
	r.flushInterval = 10 * time.Millisecond
	d := deliveryFor("open", point("metric", 7, 0))
	d.EncodedBytes = 123
	if err := r.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	// The watermark cannot close this window. Receive credits below a pressure
	// threshold still have to be released, even if the next message cannot fit.
	runCheckpointer(t, r)
	awaitCheckpoint(t, r, 1)
	if s.total("metric", base) != 7 {
		t.Fatal("checkpoint did not persist the open window")
	}
}

func TestPressureCheckpointWaitsForDurability(t *testing.T) {
	for _, byBytes := range []bool{false, true} {
		t.Run(map[bool]string{false: "messages", true: "bytes"}[byBytes], func(t *testing.T) {
			s := &blockedFlush{newFakeStore(), make(chan struct{}), make(chan struct{})}
			r := newRunner(t, s)
			r.flushInterval = time.Hour
			if byBytes {
				r.checkpointBytes = 200
			} else {
				r.checkpointMessages = 2
			}
			d := deliveryFor("duplicate", point("metric", 7, 0))
			d.EncodedBytes = 100
			for range 2 {
				if err := r.Handle(context.Background(), d); err != nil {
					t.Fatal(err)
				}
			}
			runCheckpointer(t, r)
			// Registered after the runner cleanup, so a failed assertion releases
			// storage before waiting for shutdown.
			t.Cleanup(func() { close(s.release) })
			select {
			case <-s.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("pressure did not trigger an early checkpoint")
			}
			if got := r.Stats(); got.MessagesAcked != 0 || got.PendingMessages != 2 || got.PendingBytes != 200 {
				t.Fatalf("receive credit released before commit: %+v", got)
			}
		})
	}
}

func TestRepeatedOpenWindowCheckpointsAndRedelivery(t *testing.T) {
	s := newFakeStore()
	r := newRunner(t, s)
	r.checkpointMessages = 1
	r.flushInterval = time.Hour
	runCheckpointer(t, r)
	ctx := context.Background()
	for i, name := range []string{"first", "second", "third"} {
		d := deliveryFor(name, point("metric", 7, 0), point("metric", 11, time.Minute))
		d.EncodedBytes = 200
		if err := r.Handle(ctx, d); err != nil {
			t.Fatal(err)
		}
		awaitCheckpoint(t, r, int64(i+1))
		// A replay must not accumulate the earlier checkpoint again.
		if err := r.Handle(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	if s.total("metric", base) != 21 || s.total("metric", base.Add(time.Minute)) != 33 {
		t.Fatal("partial checkpoint lost or duplicated data across windows")
	}
}

func TestCapacityRejectionRequestsCheckpointAndCanRetry(t *testing.T) {
	s := newFakeStore()
	r := newRunner(t, s)
	r.engine = aggregate.New(aggregate.Config{WindowSize: time.Minute, MaxSeries: 1})
	r.flushInterval = time.Hour
	ctx := context.Background()
	first := deliveryFor("first", point("one", 10, 0))
	first.EncodedBytes = 100
	second := deliveryFor("second", point("two", 20, 0))
	if err := r.Handle(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := r.Handle(ctx, second); !errors.Is(err, aggregate.ErrCapacity) {
		t.Fatalf("expected capacity rejection, got %v", err)
	}
	runCheckpointer(t, r)
	awaitCheckpoint(t, r, 1)
	if err := r.Handle(ctx, second); err != nil {
		t.Fatal("retry could not make progress", err)
	}
	if err := r.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	awaitCheckpoint(t, r, 2)
	if s.total("one", base) != 10 || s.total("two", base) != 20 {
		t.Fatal("capacity checkpoint changed totals")
	}
}

func TestFailedCheckpointReleasesCreditOnceAcrossWindows(t *testing.T) {
	s := newFakeStore()
	r := newRunner(t, s)
	d := deliveryFor("straddle", point("metric", 7, 0), point("metric", 11, time.Minute))
	d.EncodedBytes = 123
	ctx := context.Background()
	if err := r.Handle(ctx, d); err != nil {
		t.Fatal(err)
	}
	s.failNext = 1
	if err := r.FlushAll(ctx); err == nil {
		t.Fatal("expected database failure")
	}
	if got := r.Stats(); got.PendingBytes != 0 || got.PendingMessages != 0 || got.MessagesNacked != 1 {
		t.Fatalf("failed multi-window checkpoint credit: %+v", got)
	}
	if err := r.Handle(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err := r.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	awaitCheckpoint(t, r, 1)
	if s.total("metric", base) != 7 || s.total("metric", base.Add(time.Minute)) != 11 {
		t.Fatal("checkpoint failure/retry changed totals")
	}
}
