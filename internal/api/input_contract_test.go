package api

import (
	"net/http"
	"strings"
	"testing"
)

func TestIngestRejectsAbsentValuesAndExplicitZeroTimestamp(t *testing.T) {
	for _, point := range []string{
		`{"metric":"requests","kind":"counter"}`,
		`{"metric":"requests","kind":"counter","value":null}`,
		`{"metric":"requests","kind":"counter","value":1e101}`,
		`{"metric":"requests","kind":"counter","value":1,"timestamp":"0001-01-01T00:00:00Z"}`,
	} {
		h := newHarness(t)
		r := h.post(t, `{"points":[`+point+`]}`, nil)
		if r.Code != http.StatusUnprocessableEntity {
			t.Fatalf("point %s: status=%d body=%s", point, r.Code, r.Body)
		}
	}
	h := newHarness(t)
	r := h.post(t, `{"points":[{"metric":"requests","kind":"counter","value":0}]}`, nil)
	if r.Code != http.StatusAccepted || !strings.Contains(r.Body.String(), `"accepted":1`) {
		t.Fatalf("explicit zero rejected: %d %s", r.Code, r.Body)
	}
}
