// Package idempotency makes retried writes safe to repeat.
//
// A client that times out mid-request cannot tell whether the server processed
// its batch. Without a way to identify a retry, the safe choice for the client
// (retry) and the safe choice for the data (do not double-count) are in direct
// conflict. An idempotency key resolves it: the server recognises the repeat
// and replays the original outcome instead of doing the work twice.
package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// ErrPayloadMismatch means the key was reused with a different request body.
//
// This is always a client bug, and a serious one: it means two distinct
// operations are competing for one identity. Replaying the first response
// would silently discard the second batch, so the request is rejected instead.
var ErrPayloadMismatch = errors.New("idempotency key reused with a different payload")

// Record is a completed outcome, replayed verbatim on a repeat.
type Record struct {
	// Status is the HTTP status code originally returned.
	Status int
	// Body is the response body originally returned.
	Body []byte
	// Fingerprint identifies the request that produced this outcome.
	Fingerprint string
	// StoredAt is when the record was written.
	StoredAt time.Time
}

// Fingerprint returns a stable digest of a request body, used to detect a key
// reused for different content.
func Fingerprint(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// Store retains recent outcomes keyed by tenant and idempotency key.
//
// This implementation is per-process and in-memory, which is honest about its
// limits: with more than one replica, a retry that lands on a different
// instance is not recognised. That is an acceptable trade for the current
// stage -- duplicate suppression also happens downstream, where it is
// authoritative -- and the interface is shaped so a shared backing store can
// replace it without touching a caller.
type Store struct {
	ttl      time.Duration
	maxSize  int
	now      func() time.Time
	sweepGap time.Duration

	mu        sync.Mutex
	records   map[string]Record
	lastSweep time.Time
}

// Option customises a Store.
type Option func(*Store)

// WithClock replaces the time source so expiry is testable without sleeping.
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// WithMaxSize caps how many records are retained. The cap is what stops a
// caller from turning the store into an unbounded memory leak by sending a
// fresh key on every request.
func WithMaxSize(n int) Option {
	return func(s *Store) { s.maxSize = n }
}

// New returns a Store retaining records for ttl.
func New(ttl time.Duration, opts ...Option) *Store {
	s := &Store{
		ttl:      ttl,
		maxSize:  10_000,
		now:      time.Now,
		sweepGap: time.Minute,
		records:  make(map[string]Record),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Lookup returns the stored outcome for a key.
//
// The boolean reports whether a record was found. ErrPayloadMismatch is
// returned when the key exists but was stored for different content.
func (s *Store) Lookup(tenantID, key, fingerprint string) (Record, bool, error) {
	if key == "" {
		return Record{}, false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	s.sweepLocked(now)

	rec, ok := s.records[s.compositeKey(tenantID, key)]
	if !ok {
		return Record{}, false, nil
	}
	// A record past its TTL is indistinguishable from one that never existed.
	if now.Sub(rec.StoredAt) > s.ttl {
		return Record{}, false, nil
	}
	if rec.Fingerprint != fingerprint {
		return Record{}, false, ErrPayloadMismatch
	}
	return rec, true, nil
}

// Save records an outcome.
func (s *Store) Save(tenantID, key string, rec Record) {
	if key == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	rec.StoredAt = now
	s.sweepLocked(now)

	// If the sweep could not bring the store under its cap, every record is
	// still live and the only options are to grow without bound or to refuse.
	// Dropping the new record is the safer failure: the client loses duplicate
	// protection it can retry for, rather than the process losing its memory.
	if len(s.records) >= s.maxSize {
		if _, replacing := s.records[s.compositeKey(tenantID, key)]; !replacing {
			return
		}
	}

	s.records[s.compositeKey(tenantID, key)] = rec
}

// compositeKey namespaces keys by tenant so that two tenants choosing the same
// idempotency key -- "1", say -- cannot read each other's responses.
func (s *Store) compositeKey(tenantID, key string) string {
	return tenantID + "\x00" + key
}

// sweepLocked drops expired records. The caller must hold s.mu.
func (s *Store) sweepLocked(now time.Time) {
	if now.Sub(s.lastSweep) < s.sweepGap {
		return
	}
	s.lastSweep = now

	for k, rec := range s.records {
		if now.Sub(rec.StoredAt) > s.ttl {
			delete(s.records, k)
		}
	}
}

// Len reports how many records are retained, for tests and an operational
// gauge.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}
