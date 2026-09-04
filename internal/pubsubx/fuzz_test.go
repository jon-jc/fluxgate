package pubsubx

import (
	"math"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jon-jc/fluxgate/internal/telemetry"
)

// FuzzDecodeEnvelope asserts the two properties a consumer's decode path must
// hold against a hostile message.
//
// Messages arrive from a broker, and the broker will hand over whatever was
// published -- including anything an earlier build, a misconfigured producer,
// or an attacker with publish rights put there. Two failures matter:
//
//   - A panic. The decode runs on a Receive goroutine, so a panic there takes
//     the process down, the message is redelivered to the replacement, and the
//     aggregator crash-loops on one payload.
//   - An error that is not marked permanent. Redelivering a message that can
//     never decode burns quota forever and never reaches the dead-letter queue,
//     where somebody could actually look at it.
func FuzzDecodeEnvelope(f *testing.F) {
	// Seeds: the shapes worth exploring outward from.
	valid, err := NewEnvelope(telemetry.Batch{
		ID:         "b1",
		TenantID:   "acme",
		ReceivedAt: time.Unix(0, 0).UTC(),
		Points: []telemetry.Point{{
			Metric:    "http.requests",
			Kind:      telemetry.KindCounter,
			Value:     1,
			Timestamp: time.Unix(0, 0).UTC(),
			Labels:    map[string]string{"service": "checkout"},
		}},
	}).Encode()
	if err != nil {
		f.Fatalf("encode seed: %v", err)
	}

	f.Add(valid)
	f.Add([]byte(`{}`))
	f.Add([]byte(`[]`))
	f.Add([]byte(``))
	f.Add([]byte(`null`))
	f.Add([]byte(`{"schema_version":"1"}`))
	f.Add([]byte(`{"schema_version":"1","batch_id":"b","tenant_id":"t","points":null}`))
	f.Add([]byte(`{"schema_version":"999","batch_id":"b","tenant_id":"t"}`))
	f.Add([]byte(`{"schema_version":"1","batch_id":"b","tenant_id":"t","points":[{"value":1e999}]}`))
	f.Add([]byte(`{"schema_version":"1","batch_id":"b","tenant_id":"t","received_at":"not-a-time"}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		envelope, err := DecodeEnvelope(data)
		if err != nil {
			if !IsPermanent(err) {
				t.Fatalf("decode failure is not marked permanent, so it would be "+
					"redelivered forever instead of dead-lettered: %v", err)
			}
			return
		}

		// A decode that succeeded must have produced something the rest of the
		// pipeline can rely on.
		if envelope.SchemaVersion != SchemaVersion {
			t.Fatalf("accepted schema version %q, want %q",
				envelope.SchemaVersion, SchemaVersion)
		}
		if envelope.BatchID == "" || envelope.TenantID == "" {
			t.Fatalf("accepted an envelope with no identity: batch=%q tenant=%q",
				envelope.BatchID, envelope.TenantID)
		}

		// Converting to the domain type must not panic either; the aggregator
		// does this on every message.
		batch := envelope.Batch()
		if len(batch.Points) != len(envelope.Points) {
			t.Fatalf("conversion changed the point count: %d then %d",
				len(envelope.Points), len(batch.Points))
		}
	})
}

// FuzzEnvelopeRoundTrip asserts that anything the edge can publish, a consumer
// can read back unchanged.
//
// The two halves live in different processes and are versioned together, so a
// divergence here is invisible until data is already flowing through a broker.
func FuzzEnvelopeRoundTrip(f *testing.F) {
	f.Add("b1", "acme", "http.requests", "counter", 1.5, int64(0), "service", "checkout")
	f.Add("", "", "", "", 0.0, int64(0), "", "")
	f.Add("b\x00", "t\n", "m\t", "gauge", math.Copysign(0, -1),
		int64(-62135596800), "k", "v")

	f.Fuzz(func(t *testing.T,
		batchID, tenantID, metric, kind string,
		value float64, unixNano int64,
		labelKey, labelValue string,
	) {
		// The edge only ever publishes a non-empty identity; anything else is
		// rejected before it reaches the publisher.
		if batchID == "" || tenantID == "" {
			t.Skip()
		}
		// NaN is not representable in JSON and is rejected at ingestion, so it
		// is out of scope for a round trip.
		if value != value {
			t.Skip()
		}
		// Invalid UTF-8 genuinely cannot survive a JSON round trip: the encoder
		// substitutes U+FFFD. This fuzz target found that, and the fix was not
		// to relax the property here but to reject such input at the edge --
		// see httpx.UnmarshalJSON, which now refuses a non-UTF-8 body outright
		// rather than letting the decoder silently rewrite it.
		//
		// So the precondition below is the guarantee that check establishes,
		// not an exception carved out to keep this test green. If the check is
		// ever removed, the corruption becomes reachable again.
		for _, s := range []string{batchID, tenantID, metric, kind, labelKey, labelValue} {
			if !utf8.ValidString(s) {
				t.Skip()
			}
		}

		original := telemetry.Batch{
			ID:         batchID,
			TenantID:   tenantID,
			ReceivedAt: time.Unix(0, unixNano).UTC(),
			Points: []telemetry.Point{{
				Metric:    metric,
				Kind:      telemetry.Kind(kind),
				Value:     value,
				Timestamp: time.Unix(0, unixNano).UTC(),
				Labels:    map[string]string{labelKey: labelValue},
			}},
		}

		data, err := NewEnvelope(original).Encode()
		if err != nil {
			// An unencodable value is a bug in the encoder, not a fuzz finding
			// about the decoder -- but it is still a bug.
			t.Fatalf("encode: %v", err)
		}

		decoded, err := DecodeEnvelope(data)
		if err != nil {
			t.Fatalf("a self-encoded envelope failed to decode: %v", err)
		}

		got := decoded.Batch()
		switch {
		case got.ID != original.ID:
			t.Errorf("batch ID: %q -> %q", original.ID, got.ID)
		case got.TenantID != original.TenantID:
			t.Errorf("tenant: %q -> %q", original.TenantID, got.TenantID)
		case len(got.Points) != 1:
			t.Fatalf("points: %d, want 1", len(got.Points))
		case got.Points[0].Metric != metric:
			t.Errorf("metric: %q -> %q", metric, got.Points[0].Metric)
		case got.Points[0].Value != value:
			t.Errorf("value: %v -> %v", value, got.Points[0].Value)
		case !got.Points[0].Timestamp.Equal(original.Points[0].Timestamp):
			t.Errorf("timestamp: %v -> %v",
				original.Points[0].Timestamp, got.Points[0].Timestamp)
		case got.Points[0].Labels[labelKey] != labelValue:
			t.Errorf("label %q: %q -> %q",
				labelKey, labelValue, got.Points[0].Labels[labelKey])
		}
	})
}
