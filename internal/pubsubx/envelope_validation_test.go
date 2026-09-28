package pubsubx

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestBrokerMessagesEnforceDomainContract(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Envelope)
	}{
		{"kind", func(e *Envelope) { e.Points[0].Kind = "invalid" }},
		{"value", func(e *Envelope) { e.Points[0].Value = 1e101 }},
		{"negative counter", func(e *Envelope) { e.Points[0].Value = -1 }},
		{"zero timestamp", func(e *Envelope) { e.Points[0].Timestamp = time.Time{} }},
		{"timestamp range", func(e *Envelope) { e.Points[0].Timestamp = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{"tenant NUL", func(e *Envelope) { e.TenantID = "a\x00b" }},
		{"oversized ID", func(e *Envelope) { e.BatchID = strings.Repeat("a", 256) }},
		{"reserved label", func(e *Envelope) { e.Points[0].Labels = map[string]string{"__tenant": "other"} }},
		{"oversized batch", func(e *Envelope) { e.Points = make([]EnvelopePoint, 1001) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewEnvelope(testBatch())
			tc.mutate(&e)
			body, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeEnvelope(body); !IsPermanent(err) {
				t.Fatalf("malformed broker payload accepted: %v", err)
			}
		})
	}
}

func TestRetainedMessageDoesNotExpireAtDecode(t *testing.T) {
	e := NewEnvelope(testBatch())
	e.ReceivedAt = e.ReceivedAt.AddDate(-2, 0, 0)
	for i := range e.Points {
		e.Points[i].Timestamp = e.Points[i].Timestamp.AddDate(-2, 0, 0)
	}
	body, err := e.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeEnvelope(body); err != nil {
		t.Fatal(err)
	}
}

func TestEnvelopeRejectsMissingValueAndInvalidUTF8(t *testing.T) {
	body, err := NewEnvelope(testBatch()).Encode()
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{
		[]byte(strings.Replace(string(body), `"value":1,`, "", 1)),
		[]byte(strings.Replace(string(body), `"value":1,`, `"value":null,`, 1)),
		append(body, 0xff),
	} {
		if _, err := DecodeEnvelope(data); !IsPermanent(err) {
			t.Fatalf("invalid payload accepted: %v", err)
		}
	}
}
