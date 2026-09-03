package telemetry

import (
	"math"
	"strings"
	"testing"
	"time"
)

// fixedNow anchors validation to a known instant so timestamp-window tests do
// not depend on how fast the suite runs.
var fixedNow = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

func testValidator() Validator {
	return Validator{
		Limits: DefaultLimits(),
		Clock:  func() time.Time { return fixedNow },
	}
}

// validPoint returns a point that passes every rule, for tests to spoil one
// field at a time.
func validPoint() Point {
	return Point{
		Metric:    "http.request.duration_ms",
		Kind:      KindHistogram,
		Value:     12.5,
		Timestamp: fixedNow.Add(-time.Second),
		Labels:    map[string]string{"service": "checkout"},
	}
}

func TestValidPointHasNoViolations(t *testing.T) {
	if got := testValidator().ValidatePoint(0, validPoint()); len(got) != 0 {
		t.Fatalf("valid point rejected: %+v", got)
	}
}

// fieldsOf collects the field paths from a violation set.
func fieldsOf(violations []Violation) []string {
	fields := make([]string, len(violations))
	for i, v := range violations {
		fields[i] = v.Field
	}
	return fields
}

func hasField(violations []Violation, field string) bool {
	for _, v := range violations {
		if v.Field == field {
			return true
		}
	}
	return false
}

func TestValidateMetricName(t *testing.T) {
	tests := []struct {
		name    string
		metric  string
		wantErr bool
	}{
		{name: "simple", metric: "requests"},
		{name: "dotted namespace", metric: "http.request.duration_ms"},
		{name: "leading underscore", metric: "_internal.count"},
		{name: "digits after the first character", metric: "http2.requests"},
		{name: "hyphen", metric: "cache-hit.rate"},
		{name: "empty", metric: "", wantErr: true},
		{name: "leading digit", metric: "5xx.count", wantErr: true},
		{name: "leading dot", metric: ".requests", wantErr: true},
		{name: "trailing dot", metric: "requests.", wantErr: true},
		{name: "doubled dot", metric: "http..requests", wantErr: true},
		{name: "space", metric: "http requests", wantErr: true},
		{name: "slash", metric: "http/requests", wantErr: true},
		{name: "unicode", metric: "requêtes", wantErr: true},
		{name: "too long", metric: "a" + strings.Repeat("b", 200), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := validPoint()
			p.Metric = tc.metric

			got := testValidator().ValidatePoint(0, p)
			if tc.wantErr && !hasField(got, "points.0.metric") {
				t.Errorf("metric %q accepted, want a violation (got %v)", tc.metric, fieldsOf(got))
			}
			if !tc.wantErr && hasField(got, "points.0.metric") {
				t.Errorf("metric %q rejected: %+v", tc.metric, got)
			}
		})
	}
}

func TestValidateKind(t *testing.T) {
	for _, tc := range []struct {
		kind    Kind
		wantErr bool
	}{
		{kind: KindGauge},
		{kind: KindCounter},
		{kind: KindHistogram},
		{kind: "", wantErr: true},
		{kind: "summary", wantErr: true},
		{kind: "Gauge", wantErr: true}, // kinds are lowercase; be strict
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			p := validPoint()
			p.Kind = tc.kind
			p.Value = 1 // keep counters non-negative

			got := testValidator().ValidatePoint(0, p)
			if tc.wantErr != hasField(got, "points.0.kind") {
				t.Errorf("kind %q: violations = %v, wantErr = %v",
					tc.kind, fieldsOf(got), tc.wantErr)
			}
		})
	}
}

// TestRejectsNonFiniteValues is the rule that protects every downstream
// aggregate: a single NaN turns a whole window's mean into NaN, with nothing
// left to say which point caused it.
func TestRejectsNonFiniteValues(t *testing.T) {
	for name, value := range map[string]float64{
		"NaN":               math.NaN(),
		"positive infinity": math.Inf(1),
		"negative infinity": math.Inf(-1),
	} {
		t.Run(name, func(t *testing.T) {
			p := validPoint()
			p.Value = value

			if got := testValidator().ValidatePoint(0, p); !hasField(got, "points.0.value") {
				t.Errorf("%s accepted, want a violation", name)
			}
		})
	}
}

func TestRejectsNegativeCounter(t *testing.T) {
	p := validPoint()
	p.Kind = KindCounter
	p.Value = -1

	if got := testValidator().ValidatePoint(0, p); !hasField(got, "points.0.value") {
		t.Error("negative counter accepted, want a violation")
	}

	// A gauge may legitimately be negative -- a temperature, a net flow.
	p.Kind = KindGauge
	if got := testValidator().ValidatePoint(0, p); hasField(got, "points.0.value") {
		t.Error("negative gauge rejected; only counters must be non-negative")
	}
}

func TestValidateTimestampWindow(t *testing.T) {
	limits := DefaultLimits()

	tests := []struct {
		name    string
		ts      time.Time
		wantErr bool
	}{
		{name: "now", ts: fixedNow},
		{name: "recent past", ts: fixedNow.Add(-time.Hour)},
		{name: "within clock skew", ts: fixedNow.Add(limits.MaxClockSkew - time.Second)},
		{name: "within backfill", ts: fixedNow.Add(-limits.MaxBackfill + time.Hour)},
		{name: "beyond clock skew", ts: fixedNow.Add(limits.MaxClockSkew + time.Minute), wantErr: true},
		{name: "beyond backfill", ts: fixedNow.Add(-limits.MaxBackfill - time.Hour), wantErr: true},
		{name: "zero", ts: time.Time{}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := validPoint()
			p.Timestamp = tc.ts

			got := testValidator().ValidatePoint(0, p)
			if tc.wantErr != hasField(got, "points.0.timestamp") {
				t.Errorf("violations = %v, wantErr = %v", fieldsOf(got), tc.wantErr)
			}
		})
	}
}

// TestClockSkewIsToleratedInBothDirections documents why the future window
// exists at all: client clocks drift, and rejecting everything a second ahead
// of the server would drop data from otherwise healthy senders.
func TestClockSkewIsToleratedInBothDirections(t *testing.T) {
	p := validPoint()
	p.Timestamp = fixedNow.Add(30 * time.Second)

	if got := testValidator().ValidatePoint(0, p); len(got) != 0 {
		t.Errorf("a sender 30s ahead was rejected: %+v", got)
	}
}

func TestValidateLabels(t *testing.T) {
	limits := DefaultLimits()

	tests := []struct {
		name    string
		labels  map[string]string
		wantErr bool
	}{
		{name: "none", labels: nil},
		{name: "simple", labels: map[string]string{"service": "checkout"}},
		{name: "underscore and digits", labels: map[string]string{"pod_id_2": "abc"}},
		{name: "empty value is allowed", labels: map[string]string{"region": ""}},
		{name: "empty key", labels: map[string]string{"": "x"}, wantErr: true},
		{name: "leading digit", labels: map[string]string{"2fast": "x"}, wantErr: true},
		{name: "hyphen in key", labels: map[string]string{"pod-id": "x"}, wantErr: true},
		{name: "reserved prefix", labels: map[string]string{"__tenant": "evil"}, wantErr: true},
		{
			name:    "oversized value",
			labels:  map[string]string{"trace": strings.Repeat("x", limits.MaxLabelValueLen+1)},
			wantErr: true,
		},
		{
			name:    "control character in value",
			labels:  map[string]string{"path": "/api\x00/admin"},
			wantErr: true,
		},
		{
			name:    "newline in value",
			labels:  map[string]string{"msg": "line one\nline two"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := validPoint()
			p.Labels = tc.labels

			got := testValidator().ValidatePoint(0, p)
			gotErr := len(got) > 0
			if gotErr != tc.wantErr {
				t.Errorf("violations = %+v, wantErr = %v", got, tc.wantErr)
			}
		})
	}
}

// TestRejectsReservedLabelPrefix covers a cross-tenant integrity rule: a tenant
// that could set __tenant itself would be able to write into another tenant's
// series.
func TestRejectsReservedLabelPrefix(t *testing.T) {
	p := validPoint()
	p.Labels = map[string]string{ReservedLabelPrefix + "tenant": "someone-else"}

	got := testValidator().ValidatePoint(0, p)
	if len(got) == 0 {
		t.Fatal("a reserved label was accepted")
	}
	if !strings.Contains(got[0].Message, "reserved") {
		t.Errorf("message = %q, want it to mention the reserved prefix", got[0].Message)
	}
}

// TestRejectsExcessiveLabelCardinality guards the limit that matters most:
// label cardinality is multiplicative, and an unbounded label set is the
// fastest way to make a time-series store unqueryable.
func TestRejectsExcessiveLabelCardinality(t *testing.T) {
	limits := DefaultLimits()

	labels := make(map[string]string, limits.MaxLabels+1)
	for i := range limits.MaxLabels + 1 {
		labels["label_"+string(rune('a'+i%26))+string(rune('a'+i/26))] = "v"
	}

	p := validPoint()
	p.Labels = labels

	got := testValidator().ValidatePoint(0, p)
	if !hasField(got, "points.0.labels") {
		t.Fatalf("%d labels accepted, limit is %d", len(labels), limits.MaxLabels)
	}
}

// TestReportsEveryViolationAtOnce keeps the client's round trips down: fixing
// a batch should not mean discovering the next problem only after a redeploy.
func TestReportsEveryViolationAtOnce(t *testing.T) {
	p := Point{
		Metric:    "9bad name",
		Kind:      "nonsense",
		Value:     math.NaN(),
		Timestamp: fixedNow.Add(-100 * 24 * time.Hour),
		Labels:    map[string]string{"__reserved": "x"},
	}

	got := testValidator().ValidatePoint(7, p)
	for _, want := range []string{
		"points.7.metric",
		"points.7.kind",
		"points.7.value",
		"points.7.timestamp",
		"points.7.labels.__reserved",
	} {
		if !hasField(got, want) {
			t.Errorf("missing violation for %s; got %v", want, fieldsOf(got))
		}
	}
}

// TestViolationFieldPathsCarryTheIndex is what makes a partial-success response
// actionable: the client has to know which element of its batch to fix.
func TestViolationFieldPathsCarryTheIndex(t *testing.T) {
	p := validPoint()
	p.Metric = ""

	got := testValidator().ValidatePoint(42, p)
	if len(got) == 0 {
		t.Fatal("expected a violation")
	}
	if !strings.HasPrefix(got[0].Field, "points.42.") {
		t.Errorf("field = %q, want it to start with points.42.", got[0].Field)
	}
}

func TestNormalizeFillsMissingTimestamp(t *testing.T) {
	arrival := fixedNow

	p := Point{Metric: "x", Kind: KindGauge}.Normalize(arrival)
	if !p.Timestamp.Equal(arrival) {
		t.Errorf("timestamp = %v, want the arrival time %v", p.Timestamp, arrival)
	}
}

// TestNormalizeConvertsToUTC keeps rollup keys stable: a window derived from a
// timestamp must not depend on where the point was produced.
func TestNormalizeConvertsToUTC(t *testing.T) {
	tokyo := time.FixedZone("JST", 9*60*60)
	local := time.Date(2026, 9, 3, 21, 0, 0, 0, tokyo)

	p := Point{Timestamp: local}.Normalize(fixedNow)

	if p.Timestamp.Location() != time.UTC {
		t.Errorf("location = %v, want UTC", p.Timestamp.Location())
	}
	if !p.Timestamp.Equal(local) {
		t.Errorf("normalising changed the instant: %v != %v", p.Timestamp, local)
	}
}

func TestNormalizeLeavesSuppliedTimestampAlone(t *testing.T) {
	supplied := fixedNow.Add(-time.Hour)

	p := Point{Timestamp: supplied}.Normalize(fixedNow)
	if !p.Timestamp.Equal(supplied) {
		t.Errorf("timestamp = %v, want the supplied %v", p.Timestamp, supplied)
	}
}

func TestKindValid(t *testing.T) {
	for kind, want := range map[Kind]bool{
		KindGauge:     true,
		KindCounter:   true,
		KindHistogram: true,
		"":            false,
		"summary":     false,
	} {
		if got := kind.Valid(); got != want {
			t.Errorf("Kind(%q).Valid() = %v, want %v", kind, got, want)
		}
	}
}

func TestValidatorDefaultsToWallClock(t *testing.T) {
	// A zero-value Now must not make every timestamp look like the far future.
	v := Validator{Limits: DefaultLimits()}

	p := validPoint()
	p.Timestamp = time.Now()

	if got := v.ValidatePoint(0, p); len(got) != 0 {
		t.Errorf("a present-time point was rejected by the default clock: %+v", got)
	}
}
