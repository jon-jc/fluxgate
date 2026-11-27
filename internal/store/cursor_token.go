package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

// Resume tokens let a live-tail client continue from exactly where it stopped,
// carried in the server-sent events id field and returned as Last-Event-ID.
//
// A token is a position, not a credential. It names a commit revision and the
// last row read within it, and every read it positions is still scoped by the
// caller's authenticated tenant, so forging one can only move a client around
// inside data it may already read. It is therefore opaque but not signed. What
// a token must never do is resume silently in the wrong revision space -- which
// is what one reused across tenants would do, since revisions are per tenant --
// so each carries a tag derived from the tenant that issued it.

// MaxCursorTokenLen bounds a client-supplied token before any decoding.
const MaxCursorTokenLen = 512

var (
	// ErrCursorMalformed reports a token this server could not have issued.
	ErrCursorMalformed = errors.New("malformed stream cursor")
	// ErrCursorForeign reports a well-formed token issued for another tenant.
	ErrCursorForeign = errors.New("stream cursor was issued for another tenant")
)

const cursorTokenVersion = 1

type cursorToken struct {
	Version  int    `json:"v"`
	Tenant   string `json:"t"`
	Revision int64  `json:"r"`
	// The tie-breakers of a primed cursor. Metric is never empty on a stored
	// row, so its presence is what marks a token as primed.
	Kind      string `json:"k,omitempty"`
	Metric    string `json:"m,omitempty"`
	LabelHash string `json:"l,omitempty"`
	// WindowStart in Unix microseconds: PostgreSQL's own precision, so the
	// round trip is exact.
	WindowStart int64 `json:"w,omitempty"`
}

func tenantTag(tenantID string) string {
	sum := sha256.Sum256([]byte("fluxgate/stream-cursor/v1\x00" + tenantID))
	return hex.EncodeToString(sum[:8])
}

// Token encodes the cursor as an opaque resume token for tenantID.
func (c Cursor) Token(tenantID string) string {
	t := cursorToken{Version: cursorTokenVersion, Tenant: tenantTag(tenantID), Revision: c.Revision}
	if c.primed {
		t.Kind, t.Metric, t.LabelHash = c.Kind, c.Metric, c.LabelHash
		t.WindowStart = c.WindowStart.UnixMicro()
	}
	raw, err := json.Marshal(t)
	if err != nil {
		// A struct of strings and integers always marshals.
		panic("encode stream cursor: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// ParseCursorToken decodes a token issued by Token for tenantID.
//
// It fails closed: anything this server could not have produced is
// ErrCursorMalformed, and a valid token from another tenant is
// ErrCursorForeign, so the caller can tell a client bug from a cursor that is
// merely not resumable here.
func ParseCursorToken(tenantID, token string) (Cursor, error) {
	if token == "" || len(token) > MaxCursorTokenLen {
		return Cursor{}, ErrCursorMalformed
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil {
		return Cursor{}, ErrCursorMalformed
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var t cursorToken
	if err := dec.Decode(&t); err != nil || dec.More() {
		return Cursor{}, ErrCursorMalformed
	}

	primed := t.Metric != ""
	switch {
	case t.Version != cursorTokenVersion, t.Revision < 0, t.Tenant == "":
		return Cursor{}, ErrCursorMalformed
	case primed && t.Kind == "":
		return Cursor{}, ErrCursorMalformed
	case !primed && (t.Kind != "" || t.LabelHash != "" || t.WindowStart != 0):
		return Cursor{}, ErrCursorMalformed
	case t.Tenant != tenantTag(tenantID):
		return Cursor{}, ErrCursorForeign
	}

	if !primed {
		return Cursor{Revision: t.Revision}, nil
	}
	return Cursor{
		Revision:    t.Revision,
		Kind:        t.Kind,
		Metric:      t.Metric,
		LabelHash:   t.LabelHash,
		WindowStart: time.UnixMicro(t.WindowStart).UTC(),
		primed:      true,
	}, nil
}
