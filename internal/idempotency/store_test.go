package idempotency

import (
	"bytes"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func record(body string) Record {
	return Record{Status: 202, Body: []byte(body), Fingerprint: Fingerprint([]byte(body))}
}

func TestSaveAndReplay(t *testing.T) {
	clock := newClock()
	s := New(time.Hour, WithClock(clock.Now))

	rec := record(`{"accepted":5}`)
	s.Save("acme", "k1", rec)

	got, found, err := s.Lookup("acme", "k1", rec.Fingerprint)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !found {
		t.Fatal("record not found")
	}
	if !bytes.Equal(got.Body, rec.Body) || got.Status != rec.Status {
		t.Errorf("replayed %d/%s, want %d/%s", got.Status, got.Body, rec.Status, rec.Body)
	}
}

func TestLookupMissIsNotAnError(t *testing.T) {
	s := New(time.Hour)

	_, found, err := s.Lookup("acme", "never-seen", "fp")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if found {
		t.Error("found a record that was never stored")
	}
}

// TestKeyReuseWithDifferentPayloadIsRejected covers a serious client bug:
// replaying the first response would silently discard the second batch.
func TestKeyReuseWithDifferentPayloadIsRejected(t *testing.T) {
	s := New(time.Hour)

	s.Save("acme", "k1", record(`{"accepted":5}`))

	_, _, err := s.Lookup("acme", "k1", Fingerprint([]byte(`{"accepted":9}`)))
	if !errors.Is(err, ErrPayloadMismatch) {
		t.Errorf("Lookup = %v, want ErrPayloadMismatch", err)
	}
}

// TestTenantsAreIsolated stops one tenant reading another's response by
// guessing an idempotency key as obvious as "1".
func TestTenantsAreIsolated(t *testing.T) {
	s := New(time.Hour)

	rec := record(`{"secret":"acme data"}`)
	s.Save("acme", "1", rec)

	_, found, err := s.Lookup("globex", "1", rec.Fingerprint)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if found {
		t.Fatal("one tenant read another tenant's stored response")
	}
}

func TestRecordsExpire(t *testing.T) {
	clock := newClock()
	s := New(time.Minute, WithClock(clock.Now))

	rec := record(`{"accepted":1}`)
	s.Save("acme", "k1", rec)

	clock.Advance(30 * time.Second)
	if _, found, _ := s.Lookup("acme", "k1", rec.Fingerprint); !found {
		t.Fatal("record vanished before its TTL elapsed")
	}

	clock.Advance(2 * time.Minute)
	if _, found, _ := s.Lookup("acme", "k1", rec.Fingerprint); found {
		t.Error("an expired record was still replayed")
	}
}

// TestExpiredRecordsAreReclaimed keeps memory bounded: expiry must actually
// free the entry, not merely hide it from lookups.
func TestExpiredRecordsAreReclaimed(t *testing.T) {
	clock := newClock()
	s := New(time.Minute, WithClock(clock.Now))

	for i := range 100 {
		s.Save("acme", strconv.Itoa(i), record(`{}`))
	}
	if got := s.Len(); got != 100 {
		t.Fatalf("Len() = %d, want 100", got)
	}

	// Past the TTL and past the sweep interval, the next operation reclaims.
	clock.Advance(5 * time.Minute)
	s.Save("acme", "fresh", record(`{}`))

	if got := s.Len(); got != 1 {
		t.Errorf("Len() = %d after reclamation, want 1", got)
	}
}

// TestSizeCapBoundsMemory stops a caller turning the store into an unbounded
// leak by sending a fresh key on every request.
func TestSizeCapBoundsMemory(t *testing.T) {
	clock := newClock()
	s := New(time.Hour, WithClock(clock.Now), WithMaxSize(10))

	for i := range 100 {
		s.Save("acme", strconv.Itoa(i), record(`{}`))
	}

	if got := s.Len(); got > 10 {
		t.Errorf("Len() = %d, want no more than the cap of 10", got)
	}
}

func TestSaveOverwritesAnExistingKeyEvenAtCapacity(t *testing.T) {
	s := New(time.Hour, WithMaxSize(1))

	s.Save("acme", "k1", record(`{"v":1}`))

	// Replacing an existing record does not grow the store, so the cap must
	// not block it.
	updated := record(`{"v":2}`)
	s.Save("acme", "k1", updated)

	got, found, err := s.Lookup("acme", "k1", updated.Fingerprint)
	if err != nil || !found {
		t.Fatalf("Lookup: found=%v err=%v", found, err)
	}
	if string(got.Body) != `{"v":2}` {
		t.Errorf("body = %s, want the updated record", got.Body)
	}
}

func TestEmptyKeyIsIgnored(t *testing.T) {
	s := New(time.Hour)

	// A request without an idempotency key has nothing to correlate on; the
	// store must no-op rather than collapsing every such request onto one
	// shared entry.
	s.Save("acme", "", record(`{}`))
	if got := s.Len(); got != 0 {
		t.Errorf("Len() = %d, want 0", got)
	}

	if _, found, err := s.Lookup("acme", "", "fp"); found || err != nil {
		t.Errorf("Lookup with an empty key: found=%v err=%v", found, err)
	}
}

func TestFingerprintIsStableAndDistinguishing(t *testing.T) {
	a := Fingerprint([]byte(`{"points":[1]}`))
	b := Fingerprint([]byte(`{"points":[1]}`))
	c := Fingerprint([]byte(`{"points":[2]}`))

	if a != b {
		t.Error("the same body produced two different fingerprints")
	}
	if a == c {
		t.Error("different bodies produced the same fingerprint")
	}
}

func TestConcurrentAccessIsSafe(t *testing.T) {
	clock := newClock()
	s := New(time.Hour, WithClock(clock.Now))

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 100 {
				key := strconv.Itoa(g) + "-" + strconv.Itoa(i)
				rec := record(`{"g":` + strconv.Itoa(g) + `}`)
				s.Save("acme", key, rec)
				s.Lookup("acme", key, rec.Fingerprint)
			}
		}(g)
	}
	wg.Wait()

	if got := s.Len(); got == 0 {
		t.Error("nothing survived concurrent writes")
	}
}
