// Package pubsubx wraps the Google Cloud Pub/Sub client with the behaviour
// this platform needs on top of it: a versioned message envelope, publish-side
// circuit breaking and load shedding, and a subscriber runtime that knows the
// difference between an error worth retrying and one that never will be.
package pubsubx

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/jon-jc/fluxgate/internal/telemetry"
)

// SchemaVersion is the envelope format version.
//
// It is carried as a message attribute as well as in the body so a consumer
// can route or reject on it without deserialising anything. Every schema
// change bumps this, and consumers reject versions they do not understand
// rather than guessing at a payload they were not written for.
const SchemaVersion = "1"

// Message attribute keys.
//
// Attributes exist as a separate namespace from the body for a concrete
// reason: Pub/Sub subscription filters can only match on attributes. Putting
// the tenant here means a per-tenant subscription can be created server-side
// without every consumer deserialising every message to discard most of them.
const (
	AttrSchemaVersion = "schema_version"
	AttrTenantID      = "tenant_id"
	AttrBatchID       = "batch_id"
	AttrRequestID     = "request_id"
	AttrPointCount    = "point_count"
	AttrPublishedAt   = "published_at"
)

// Envelope is the wire format for a published batch.
//
// JSON is chosen over a binary encoding deliberately. At this message size the
// bandwidth difference is immaterial next to the operational cost of a format
// an engineer cannot read straight off a dead-letter queue at 3am. The schema
// version is the hedge: switching encodings later is a version bump, not a
// rewrite.
type Envelope struct {
	SchemaVersion string          `json:"schema_version"`
	BatchID       string          `json:"batch_id"`
	TenantID      string          `json:"tenant_id"`
	ReceivedAt    time.Time       `json:"received_at"`
	Points        []EnvelopePoint `json:"points"`
}

// EnvelopePoint is one observation on the wire.
type EnvelopePoint struct {
	Metric    string            `json:"metric"`
	Kind      string            `json:"kind"`
	Value     float64           `json:"value"`
	Timestamp time.Time         `json:"timestamp"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// NewEnvelope converts a validated batch into its wire form.
func NewEnvelope(batch telemetry.Batch) Envelope {
	points := make([]EnvelopePoint, len(batch.Points))
	for i, p := range batch.Points {
		points[i] = EnvelopePoint{
			Metric:    p.Metric,
			Kind:      string(p.Kind),
			Value:     p.Value,
			Timestamp: p.Timestamp,
			Labels:    p.Labels,
		}
	}

	return Envelope{
		SchemaVersion: SchemaVersion,
		BatchID:       batch.ID,
		TenantID:      batch.TenantID,
		ReceivedAt:    batch.ReceivedAt,
		Points:        points,
	}
}

// Batch converts an envelope back into the domain type.
func (e Envelope) Batch() telemetry.Batch {
	points := make([]telemetry.Point, len(e.Points))
	for i, p := range e.Points {
		points[i] = telemetry.Point{
			Metric:    p.Metric,
			Kind:      telemetry.Kind(p.Kind),
			Value:     p.Value,
			Timestamp: p.Timestamp,
			Labels:    p.Labels,
		}
	}

	return telemetry.Batch{
		ID:         e.BatchID,
		TenantID:   e.TenantID,
		ReceivedAt: e.ReceivedAt,
		Points:     points,
	}
}

// Attributes returns the message attributes for an envelope.
func (e Envelope) Attributes(requestID string, publishedAt time.Time) map[string]string {
	attrs := map[string]string{
		AttrSchemaVersion: e.SchemaVersion,
		AttrTenantID:      e.TenantID,
		AttrBatchID:       e.BatchID,
		AttrPointCount:    strconv.Itoa(len(e.Points)),
		AttrPublishedAt:   publishedAt.UTC().Format(time.RFC3339Nano),
	}
	// Carrying the request ID across the queue is what makes a trace continue
	// from the HTTP edge into an aggregator running minutes later on another
	// machine.
	if requestID != "" {
		attrs[AttrRequestID] = requestID
	}
	return attrs
}

// Encode serialises an envelope for transport.
func (e Envelope) Encode() ([]byte, error) {
	if err := e.validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode envelope %s: %w", e.BatchID, err)
	}
	if len(data) > maxEnvelopeBytes {
		return nil, fmt.Errorf("envelope exceeds %d bytes", maxEnvelopeBytes)
	}
	return data, nil
}

const maxEnvelopeBytes = 10_000_000

// DecodeEnvelope parses a message body.
//
// A body that fails to parse, or carries a schema version this build does not
// understand, is a permanent error: redelivering it will produce exactly the
// same failure forever. Marking it as such is what routes it to the
// dead-letter queue instead of into an infinite retry loop.
func DecodeEnvelope(data []byte) (Envelope, error) {
	if len(data) > maxEnvelopeBytes || !utf8.Valid(data) {
		return Envelope{}, Permanent(fmt.Errorf("envelope must be UTF-8 and at most %d bytes", maxEnvelopeBytes))
	}
	var e Envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return Envelope{}, Permanent(fmt.Errorf("decode envelope: %w", err))
	}
	// Zero is valid, so distinguish it from an absent/null value on the wire.
	var required struct {
		Points []struct {
			Value *float64 `json:"value"`
		} `json:"points"`
	}
	if err := json.Unmarshal(data, &required); err != nil {
		return Envelope{}, Permanent(err)
	}
	for i, p := range required.Points {
		if p.Value == nil {
			return Envelope{}, Permanent(fmt.Errorf("point %d is missing value", i))
		}
	}
	if err := e.validate(); err != nil {
		return Envelope{}, Permanent(err)
	}
	return e, nil
}

func (e Envelope) validate() error {
	if e.SchemaVersion != SchemaVersion {
		return fmt.Errorf(
			"unsupported schema version %q (this build understands %q)",
			e.SchemaVersion, SchemaVersion)
	}
	if !telemetry.ValidIdentity(e.BatchID) || !telemetry.ValidIdentity(e.TenantID) {
		return fmt.Errorf("envelope requires valid batch_id and tenant_id")
	}
	if e.ReceivedAt.IsZero() || !time.Unix(0, e.ReceivedAt.UnixNano()).Equal(e.ReceivedAt) {
		return fmt.Errorf("envelope requires a supported received_at timestamp")
	}
	v := telemetry.NewValidator()
	if len(e.Points) == 0 || len(e.Points) > v.Limits.MaxPointsPerBatch {
		return fmt.Errorf("envelope must contain 1 to %d points", v.Limits.MaxPointsPerBatch)
	}
	for i, p := range e.Batch().Points {
		if violations := v.ValidateStoredPoint(i, p); len(violations) > 0 {
			return fmt.Errorf("invalid envelope field %s: %s", violations[0].Field, violations[0].Message)
		}
	}
	return nil
}
