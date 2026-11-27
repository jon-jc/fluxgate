package store

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

func primedCursor() Cursor {
	return Cursor{}.After(StoredRollup{
		Kind:        "histogram",
		Metric:      "http.request.duration_ms",
		LabelHash:   "9f2c1b0a7e6d5c4b",
		WindowStart: time.Date(2026, 11, 27, 13, 4, 0, 123456000, time.UTC),
	}, 42)
}

func TestCursorTokensRoundTripExactly(t *testing.T) {
	for name, c := range map[string]Cursor{
		"fresh":         {Revision: 7},
		"zero":          {},
		"primed":        primedCursor(),
		"primed epoch":  Cursor{}.After(StoredRollup{Kind: "gauge", Metric: "m", WindowStart: time.Unix(0, 0)}, 1),
		"primed no lbl": Cursor{}.After(StoredRollup{Kind: "gauge", Metric: "m", WindowStart: time.Unix(60, 0)}, 3),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := ParseCursorToken("acme", c.Token("acme"))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got.Revision != c.Revision || got.Kind != c.Kind || got.Metric != c.Metric ||
				got.LabelHash != c.LabelHash || !got.WindowStart.Equal(c.WindowStart) || got.primed != c.primed {
				t.Fatalf("round trip changed the cursor:\n got %+v\nwant %+v", got, c)
			}
		})
	}
}

// A token is per-tenant because revisions are: one carried across a credential
// change must not resume silently at a position in someone else's history.
func TestCursorTokensAreBoundToTheirTenant(t *testing.T) {
	token := primedCursor().Token("acme")
	if _, err := ParseCursorToken("globex", token); !errors.Is(err, ErrCursorForeign) {
		t.Fatalf("token from acme parsed for globex: err = %v", err)
	}
}

func TestMalformedCursorTokensFailClosed(t *testing.T) {
	encode := func(raw string) string { return base64.RawURLEncoding.EncodeToString([]byte(raw)) }
	tag := tenantTag("acme")
	valid := primedCursor().Token("acme")
	for name, token := range map[string]string{
		"empty":             "",
		"oversized":         strings.Repeat("A", MaxCursorTokenLen+1),
		"not base64":        "!!!!",
		"padded base64":     valid + "==",
		"standard alphabet": strings.NewReplacer("-", "+", "_", "/").Replace(valid) + "+/",
		"not json":          encode("revision=5"),
		"trailing data":     encode(`{"v":1,"t":"` + tag + `","r":5}{}`),
		"unknown field":     encode(`{"v":1,"t":"` + tag + `","r":5,"admin":true}`),
		"future version":    encode(`{"v":2,"t":"` + tag + `","r":5}`),
		"no version":        encode(`{"t":"` + tag + `","r":5}`),
		"no tenant tag":     encode(`{"v":1,"r":5}`),
		"negative revision": encode(`{"v":1,"t":"` + tag + `","r":-1}`),
		"primed no kind":    encode(`{"v":1,"t":"` + tag + `","r":5,"m":"cpu"}`),
		"half primed":       encode(`{"v":1,"t":"` + tag + `","r":5,"k":"gauge"}`),
		"stray window":      encode(`{"v":1,"t":"` + tag + `","r":5,"w":60}`),
		"wrong types":       encode(`{"v":"1","t":"` + tag + `","r":5}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseCursorToken("acme", token); !errors.Is(err, ErrCursorMalformed) {
				t.Fatalf("err = %v, want ErrCursorMalformed", err)
			}
		})
	}
}

// A resume token is written into an SSE id field, and the SSE grammar ends a
// field at CR or LF. A token able to contain either could forge frames on the
// client, so the alphabet is asserted rather than assumed: base64url only.
func TestCursorTokensCannotBreakAnEventStreamFrame(t *testing.T) {
	cursors := []Cursor{{}, {Revision: 1 << 62}, primedCursor(),
		Cursor{}.After(StoredRollup{Kind: "k\r\n", Metric: "m\nid: forged\n\n", LabelHash: "\x00:"}, 3)}
	for _, c := range cursors {
		if token := c.Token("tenant\nwith\r\nbreaks"); !isBase64URL(token) {
			t.Fatalf("token %q can break an SSE frame", token)
		}
	}
}

func isBase64URL(s string) bool {
	return s != "" && strings.Trim(s,
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_") == ""
}

// FuzzParseCursorToken treats Last-Event-ID as what it is: a header any client
// can set to anything. Parsing must never panic, must classify every rejection,
// and anything it accepts must re-encode to a token that parses to the same
// position -- so an accepted cursor always means one thing.
func FuzzParseCursorToken(f *testing.F) {
	f.Add(primedCursor().Token("acme"))
	f.Add(Cursor{Revision: 9}.Token("acme"))
	f.Add(Cursor{Revision: 9}.Token("globex"))
	f.Add("")
	f.Add("eyJ2IjoxfQ")
	f.Add(strings.Repeat("A", MaxCursorTokenLen))

	f.Fuzz(func(t *testing.T, token string) {
		c, err := ParseCursorToken("acme", token)
		if err != nil {
			if !errors.Is(err, ErrCursorMalformed) && !errors.Is(err, ErrCursorForeign) {
				t.Fatalf("unclassified rejection: %v", err)
			}
			return
		}
		if c.Revision < 0 {
			t.Fatalf("accepted a negative revision: %+v", c)
		}
		if c.primed != (c.Metric != "") || (c.primed && c.Kind == "") {
			t.Fatalf("accepted an inconsistent cursor: %+v", c)
		}
		if !isBase64URL(c.Token("acme")) {
			t.Fatalf("an accepted cursor re-encodes outside base64url: %+v", c)
		}
		again, err := ParseCursorToken("acme", c.Token("acme"))
		if err != nil {
			t.Fatalf("re-encoded cursor no longer parses: %v", err)
		}
		if again.Revision != c.Revision || again.Kind != c.Kind || again.Metric != c.Metric ||
			again.LabelHash != c.LabelHash || !again.WindowStart.Equal(c.WindowStart) || again.primed != c.primed {
			t.Fatalf("one token, two positions:\n first %+v\nsecond %+v", c, again)
		}
	})
}
