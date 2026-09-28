package idempotency

import (
	"context"
	"encoding/json"
	"errors"
)

// Repository reserves a stable batch before attempting a broker publish.
// Concurrent callers may publish that same batch more than once; the delivery
// ledger suppresses those copies. A record is replayable only after publication.
type Repository interface {
	Get(ctx context.Context, tenantID, key, fingerprint string) (Record, bool, error)
	Reserve(ctx context.Context, tenantID, key string, candidate Record) (Record, error)
	Complete(ctx context.Context, tenantID, key, batchID string) error
}

// ErrFull rejects new keys when the local store is at capacity.
var ErrFull = errors.New("idempotency store is full")

// ErrExpired means the retry window ended while a request was in flight.
var ErrExpired = errors.New("idempotency reservation expired")

// Get implements Repository for local development.
func (s *Store) Get(ctx context.Context, tenantID, key, fingerprint string) (Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, false, err
	}
	return s.Lookup(tenantID, key, fingerprint)
}

// Reserve atomically fixes the batch identity and response before publication.
func (s *Store) Reserve(ctx context.Context, tenantID, key string, candidate Record) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.sweepLocked(now)
	k := s.compositeKey(tenantID, key)
	if rec, ok := s.records[k]; ok && now.Sub(rec.StoredAt) <= s.ttl {
		if rec.Fingerprint != candidate.Fingerprint {
			return Record{}, ErrPayloadMismatch
		}
		return rec, nil
	}
	delete(s.records, k)
	s.bytes -= s.sizes[k]
	delete(s.sizes, k)
	if len(s.records) >= s.maxSize {
		return Record{}, ErrFull
	}
	// Bound bytes as well as keys: a batch can be several megabytes. This path
	// is local-only; production stores reservations in the shared database.
	data, err := json.Marshal(candidate)
	if err != nil {
		return Record{}, err
	}
	if s.bytes+len(data) > 64<<20 {
		return Record{}, ErrFull
	}
	candidate.StoredAt = now
	s.records[k] = candidate
	s.sizes[k] = len(data)
	s.bytes += len(data)
	return candidate, nil
}

// Complete records a confirmed broker acknowledgement without renewing TTL.
func (s *Store) Complete(ctx context.Context, tenantID, key, batchID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.compositeKey(tenantID, key)
	rec, ok := s.records[k]
	if !ok || rec.Batch.ID != batchID || s.now().Sub(rec.StoredAt) > s.ttl {
		return ErrExpired
	}
	rec.Published = true
	s.records[k] = rec
	return nil
}
