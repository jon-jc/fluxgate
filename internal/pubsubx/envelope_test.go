package pubsubx

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/telemetry"
)

func testBatch() telemetry.Batch {
	ts := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	return telemetry.Batch{
		ID:         "batch-1",
		TenantID:   "acme",
		ReceivedAt: ts,
		Points: []telemetry.Point{
			{
				Metric:    "http.requests",
				Kind:      telemetry.KindCounter,
				Value:     1,
				Timestamp: ts,
				Labels:    map[string]string{"service": "checkout"},
			},
			{
				Metric:    "queue.depth",
				Kind:      telemetry.KindGauge,
				Value:     42.5,
				Timestamp: ts.Add(time.Second),
			},
		},
	}
}

// TestEnvelopeRoundTrip is the property everything downstream depends on: what
// the aggregator reconstructs must be what the edge accepted.
func TestEnvelopeRoundTrip(t *testing.T) {
	original := testBatch()

	data, err := NewEnvelope(original).Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	envelope, err := DecodeEnvelope(data)
	if err != nil {
		t.Fatalf("DecodeEnvelope: %v", err)
	}
	got := envelope.Batch()

	if got.ID != original.ID || got.TenantID != original.TenantID {
		t.Errorf("identity = %s/%s, want %s/%s",
			got.ID, got.TenantID, original.ID, original.TenantID)
	}
	if !got.ReceivedAt.Equal(original.ReceivedAt) {
		t.Errorf("ReceivedAt = %v, want %v", got.ReceivedAt, original.ReceivedAt)
	}
	if len(got.Points) != len(original.Points) {
		t.Fatalf("points = %d, want %d", len(got.Points), len(original.Points))
	}

	for i, want := range original.Points {
		gotPoint := got.Points[i]
		switch {
		case gotPoint.Metric != want.Metric:
			t.Errorf("point %d metric = %q, want %q", i, gotPoint.Metric, want.Metric)
		case gotPoint.Kind != want.Kind:
			t.Errorf("point %d kind = %q, want %q", i, gotPoint.Kind, want.Kind)
		case gotPoint.Value != want.Value:
			t.Errorf("point %d value = %v, want %v", i, gotPoint.Value, want.Value)
		case !gotPoint.Timestamp.Equal(want.Timestamp):
			t.Errorf("point %d timestamp = %v, want %v", i, gotPoint.Timestamp, want.Timestamp)
		case len(gotPoint.Labels) != len(want.Labels):
			t.Errorf("point %d labels = %v, want %v", i, gotPoint.Labels, want.Labels)
		}
	}
}

func TestAttributesCarryRoutingMetadata(t *testing.T) {
	envelope := NewEnvelope(testBatch())
	publishedAt := time.Date(2026, 9, 3, 12, 0, 5, 0, time.UTC)

	attrs := envelope.Attributes("req-123", publishedAt)

	// These exist so a Pub/Sub subscription filter can route without any
	// consumer deserialising a body it is going to discard.
	for key, want := range map[string]string{
		AttrSchemaVersion: SchemaVersion,
		AttrTenantID:      "acme",
		AttrBatchID:       "batch-1",
		AttrPointCount:    "2",
		AttrRequestID:     "req-123",
	} {
		if got := attrs[key]; got != want {
			t.Errorf("attribute %s = %q, want %q", key, got, want)
		}
	}

	if _, ok := attrs[AttrPublishedAt]; !ok {
		t.Error("published_at attribute is missing")
	}
}

// TestRequestIDIsOmittedWhenAbsent keeps an empty attribute out of the message
// rather than publishing a key whose value means nothing.
func TestRequestIDIsOmittedWhenAbsent(t *testing.T) {
	attrs := NewEnvelope(testBatch()).Attributes("", time.Now())

	if _, present := attrs[AttrRequestID]; present {
		t.Error("request_id was set despite there being no correlation ID")
	}
}

func TestPointCountAttributeMatchesTheBody(t *testing.T) {
	batch := testBatch()
	attrs := NewEnvelope(batch).Attributes("", time.Now())

	want := strconv.Itoa(len(batch.Points))
	if got := attrs[AttrPointCount]; got != want {
		t.Errorf("point_count = %q, want %q", got, want)
	}
}

// TestDecodeRejectsUnknownSchemaVersion is what makes the version field worth
// carrying: a consumer must refuse a payload it was not written for rather
// than guessing at its meaning.
func TestDecodeRejectsUnknownSchemaVersion(t *testing.T) {
	envelope := NewEnvelope(testBatch())
	envelope.SchemaVersion = "99"

	data, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	_, err = DecodeEnvelope(data)
	if err == nil {
		t.Fatal("an unknown schema version was accepted")
	}
	// Redelivering it would fail identically forever, so it must be permanent.
	if !IsPermanent(err) {
		t.Errorf("error = %v, want it marked permanent", err)
	}
	if !strings.Contains(err.Error(), "99") {
		t.Errorf("error does not name the offending version: %v", err)
	}
}

func TestDecodeRejectsMalformedMessages(t *testing.T) {
	tests := map[string]string{
		"not json":          `nonsense`,
		"truncated":         `{"schema_version":"1"`,
		"missing batch id":  `{"schema_version":"1","tenant_id":"acme"}`,
		"missing tenant id": `{"schema_version":"1","batch_id":"b1"}`,
		"no schema version": `{"batch_id":"b1","tenant_id":"acme"}`,
		"empty":             ``,
		"array not object":  `[]`,
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeEnvelope([]byte(body))
			if err == nil {
				t.Fatalf("DecodeEnvelope(%q) succeeded, want an error", body)
			}
			// Every one of these fails identically on every retry, so none of
			// them may be nacked back onto the subscription indefinitely.
			if !IsPermanent(err) {
				t.Errorf("error = %v, want it marked permanent", err)
			}
		})
	}
}

func TestPermanentWrapping(t *testing.T) {
	if IsPermanent(nil) {
		t.Error("nil was reported as permanent")
	}
	if Permanent(nil) != nil {
		t.Error("Permanent(nil) should stay nil")
	}

	base := DecodeEnvelopeErr()
	if !IsPermanent(base) {
		t.Error("a wrapped error was not detected as permanent")
	}
	if !strings.Contains(base.Error(), "permanent") {
		t.Errorf("Error() = %q, want it to say permanent", base.Error())
	}
}

// DecodeEnvelopeErr produces a permanent error for the wrapping test.
func DecodeEnvelopeErr() error {
	_, err := DecodeEnvelope([]byte(`nope`))
	return err
}

func TestEmptyBatchEncodesToAnEmptyPointList(t *testing.T) {
	// The edge never publishes an empty batch, but the encoding must not
	// produce a null that a consumer would then have to special-case.
	envelope := NewEnvelope(telemetry.Batch{ID: "b", TenantID: "t"})

	data, err := envelope.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !strings.Contains(string(data), `"points":[]`) {
		t.Errorf("encoded as %s, want an empty array rather than null", data)
	}
}

func TestTimestampsSurviveAsUTC(t *testing.T) {
	tokyo := time.FixedZone("JST", 9*60*60)
	ts := time.Date(2026, 9, 3, 21, 0, 0, 0, tokyo)

	batch := telemetry.Batch{
		ID:       "b",
		TenantID: "t",
		Points:   []telemetry.Point{{Metric: "a.b", Kind: telemetry.KindGauge, Timestamp: ts}},
	}

	data, err := NewEnvelope(batch).Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	envelope, err := DecodeEnvelope(data)
	if err != nil {
		t.Fatalf("DecodeEnvelope: %v", err)
	}

	if got := envelope.Batch().Points[0].Timestamp; !got.Equal(ts) {
		t.Errorf("timestamp = %v, want the same instant as %v", got, ts)
	}
}

func TestClampDeliveryAttempts(t *testing.T) {
	// Pub/Sub rejects a dead-letter policy outside 5..100 with an OutOfRange
	// error at subscription creation. Clamping turns a deployment failure into
	// a value the API will accept.
	for in, want := range map[int]int32{
		0:    minDeliveryAttempts,
		3:    minDeliveryAttempts,
		5:    5,
		20:   20,
		100:  100,
		1000: maxDeliveryAttempts,
		-1:   minDeliveryAttempts,
	} {
		if got := clampDeliveryAttempts(in); got != want {
			t.Errorf("clampDeliveryAttempts(%d) = %d, want %d", in, got, want)
		}
	}
}

func TestClampAckDeadline(t *testing.T) {
	// Pub/Sub accepts an ack deadline between 10 and 600 seconds.
	for in, want := range map[time.Duration]int32{
		0:                minAckDeadlineSeconds,
		time.Second:      minAckDeadlineSeconds,
		30 * time.Second: 30,
		10 * time.Minute: maxAckDeadlineSeconds,
		time.Hour:        maxAckDeadlineSeconds,
	} {
		if got := clampAckDeadline(in); got != want {
			t.Errorf("clampAckDeadline(%v) = %d, want %d", in, got, want)
		}
	}
}
