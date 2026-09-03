// Package ingest defines where accepted telemetry goes once the edge has
// validated it.
//
// The Sink interface is the seam between the synchronous HTTP path and the
// asynchronous delivery pipeline. Keeping it narrow means the Pub/Sub
// publisher, the in-memory test double and any future destination are
// interchangeable without the handler knowing which one it has.
package ingest

import (
	"context"
	"sync"

	"github.com/jon-jc/fluxgate/internal/telemetry"
)

// Sink accepts a validated batch for delivery.
//
// Implementations should return promptly: the caller is holding an open HTTP
// connection. Durable, slow or retrying work belongs behind a queue inside the
// implementation, not in the caller's request.
type Sink interface {
	// Publish hands off the batch. A returned error means the batch was not
	// accepted and the client should retry.
	Publish(ctx context.Context, batch telemetry.Batch) error
}

// SinkFunc adapts a function to the Sink interface.
type SinkFunc func(ctx context.Context, batch telemetry.Batch) error

// Publish implements Sink.
func (f SinkFunc) Publish(ctx context.Context, batch telemetry.Batch) error {
	return f(ctx, batch)
}

// MemorySink retains batches in memory. It backs tests and local development,
// where standing up a message broker to exercise a handler is not worth the
// friction.
//
// It is explicitly not durable: a restart discards everything.
type MemorySink struct {
	mu      sync.Mutex
	batches []telemetry.Batch
	points  int
}

// NewMemorySink returns an empty MemorySink.
func NewMemorySink() *MemorySink { return &MemorySink{} }

// Publish implements Sink.
func (s *MemorySink) Publish(_ context.Context, batch telemetry.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, batch)
	s.points += len(batch.Points)
	return nil
}

// Batches returns a copy of everything published so far.
func (s *MemorySink) Batches() []telemetry.Batch {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]telemetry.Batch, len(s.batches))
	copy(out, s.batches)
	return out
}

// PointCount reports how many individual points have been published.
func (s *MemorySink) PointCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.points
}

// Reset discards everything retained.
func (s *MemorySink) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = nil
	s.points = 0
}
