// Package telemetry defines the domain model for metric data and the rules
// that decide whether a submitted point is worth admitting to the pipeline.
//
// The package deliberately knows nothing about HTTP, Pub/Sub or storage. The
// validation rules here are the contract every ingestion path enforces, so
// there is exactly one definition of a valid point no matter how it arrives.
package telemetry

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// Kind classifies how a value should be interpreted downstream.
type Kind string

// Supported metric kinds.
const (
	// KindGauge is a point-in-time measurement that can move in any direction,
	// such as a queue depth.
	KindGauge Kind = "gauge"
	// KindCounter is a monotonically increasing total, such as requests served.
	KindCounter Kind = "counter"
	// KindHistogram is an observation to be bucketed for distribution
	// analysis, such as a request duration.
	KindHistogram Kind = "histogram"
)

// Valid reports whether k is a recognised kind.
func (k Kind) Valid() bool {
	switch k {
	case KindGauge, KindCounter, KindHistogram:
		return true
	default:
		return false
	}
}

// Point is a single metric observation.
type Point struct {
	// Metric is the dotted metric name, e.g. "http.request.duration_ms".
	Metric string
	// Kind classifies the value.
	Kind Kind
	// Value is the observation. It must be finite.
	Value float64
	// Timestamp is when the observation was taken, in UTC.
	Timestamp time.Time
	// Labels are the dimensions the point is broken down by.
	Labels map[string]string
}

// Batch is a set of points submitted together by one tenant.
type Batch struct {
	// ID uniquely identifies this batch for tracing and duplicate detection.
	ID string
	// TenantID owns the data.
	TenantID string
	// ReceivedAt is when the edge accepted the batch.
	ReceivedAt time.Time
	// Points are the accepted observations.
	Points []Point
}

// Violation describes one rejected field. It mirrors the shape of an API field
// error without binding the domain to a transport.
type Violation struct {
	// Field is a dotted path to the offending value, e.g. "points.3.value".
	Field string
	// Message explains what the submitter must change.
	Message string
}

// Limits bound what a single batch may contain.
//
// Every limit here exists to protect a specific downstream resource, and the
// tightest of them is MaxLabels: label cardinality is multiplicative, so an
// unbounded label set is the fastest way to turn a working time-series store
// into an unqueryable one.
type Limits struct {
	// MaxPointsPerBatch caps how many observations one request may carry.
	MaxPointsPerBatch int
	// MaxMetricNameLen caps the metric name length.
	MaxMetricNameLen int
	// MaxLabels caps the number of dimensions on a single point.
	MaxLabels int
	// MaxLabelKeyLen caps a label key's length.
	MaxLabelKeyLen int
	// MaxLabelValueLen caps a label value's length.
	MaxLabelValueLen int
	// MaxClockSkew is how far into the future a timestamp may be. Client
	// clocks drift; rejecting everything ahead of the server would drop
	// legitimate data from otherwise healthy senders.
	MaxClockSkew time.Duration
	// MaxBackfill is how far into the past a timestamp may be. Data older than
	// this arrives after its aggregation window has closed, so accepting it
	// would silently produce rollups nobody ever reads.
	MaxBackfill time.Duration
}

// DefaultLimits returns the limits the service enforces unless configured
// otherwise.
func DefaultLimits() Limits {
	return Limits{
		MaxPointsPerBatch: 1000,
		MaxMetricNameLen:  200,
		MaxLabels:         20,
		MaxLabelKeyLen:    64,
		MaxLabelValueLen:  256,
		MaxClockSkew:      5 * time.Minute,
		MaxBackfill:       7 * 24 * time.Hour,
	}
}

// ReservedLabelPrefix marks labels the platform owns. User-supplied labels
// using it are rejected so that a tenant cannot forge a system dimension such
// as __tenant and pollute another tenant's series.
const ReservedLabelPrefix = "__"

// Validator applies the ingestion rules.
//
// Clock is injectable so that timestamp-window tests are deterministic rather
// than dependent on how fast the suite happens to run. Callers that need the
// current time -- to stamp an arrival, say -- must read it through Now rather
// than calling time.Now themselves, so that validation and the values it
// validates can never disagree about what "now" is.
type Validator struct {
	Limits Limits
	Clock  func() time.Time
}

// NewValidator returns a Validator using the default limits and the wall
// clock.
func NewValidator() Validator {
	return Validator{Limits: DefaultLimits(), Clock: time.Now}
}

// Now returns the validator's current time, falling back to the wall clock
// when no clock was injected.
func (v Validator) Now() time.Time {
	if v.Clock != nil {
		return v.Clock()
	}
	return time.Now()
}

// ValidatePoint checks one point and returns every rule it breaks.
//
// All violations are collected rather than returning on the first: a client
// fixing a malformed batch should learn everything wrong with it in one round
// trip, not discover the next problem only after redeploying.
//
// index positions the point within its batch so the returned field paths point
// at the exact element the submitter needs to fix.
func (v Validator) ValidatePoint(index int, p Point) []Violation {
	prefix := fmt.Sprintf("points.%d", index)
	var violations []Violation

	add := func(field, msg string) {
		violations = append(violations, Violation{Field: prefix + "." + field, Message: msg})
	}

	if msg := v.validateMetricName(p.Metric); msg != "" {
		add("metric", msg)
	}

	if p.Kind == "" {
		add("kind", "is required (gauge, counter or histogram)")
	} else if !p.Kind.Valid() {
		add("kind", fmt.Sprintf("%q is not a valid kind (want gauge, counter or histogram)", p.Kind))
	}

	// NaN and the infinities survive a JSON round trip through some clients
	// and poison every aggregate they touch: one NaN turns a whole window's
	// mean into NaN, with no trace of which point caused it.
	switch {
	case math.IsNaN(p.Value):
		add("value", "must be a finite number, got NaN")
	case math.IsInf(p.Value, 0):
		add("value", "must be a finite number, got infinity")
	}

	if p.Kind == KindCounter && p.Value < 0 {
		add("value", "must not be negative for a counter")
	}

	violations = append(violations, v.validateTimestamp(prefix, p.Timestamp)...)
	violations = append(violations, v.validateLabels(prefix, p.Labels)...)

	return violations
}

func (v Validator) validateMetricName(name string) string {
	switch {
	case name == "":
		return "is required"
	case len(name) > v.Limits.MaxMetricNameLen:
		return fmt.Sprintf("must be at most %d characters", v.Limits.MaxMetricNameLen)
	}

	for i, c := range name {
		valid := c == '_' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(i > 0 && (c == '.' || c == '-' || (c >= '0' && c <= '9')))
		if !valid {
			return fmt.Sprintf(
				"contains %q at position %d; use letters, digits, '_', '.' and '-', starting with a letter or '_'",
				string(c), i)
		}
	}

	// A trailing or doubled separator produces an empty path segment, which
	// makes the name ambiguous to any consumer that splits on dots.
	if strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
		return "must not contain an empty path segment"
	}
	return ""
}

func (v Validator) validateTimestamp(prefix string, ts time.Time) []Violation {
	if ts.IsZero() {
		// An absent timestamp is filled in with the arrival time before
		// validation, so a zero value here means the caller sent something
		// that parsed to the zero instant.
		return []Violation{{Field: prefix + ".timestamp", Message: "is required"}}
	}

	now := v.Now()
	if ts.After(now.Add(v.Limits.MaxClockSkew)) {
		return []Violation{{
			Field: prefix + ".timestamp",
			Message: fmt.Sprintf("is more than %s in the future; check the sender's clock",
				v.Limits.MaxClockSkew),
		}}
	}
	if ts.Before(now.Add(-v.Limits.MaxBackfill)) {
		return []Violation{{
			Field: prefix + ".timestamp",
			Message: fmt.Sprintf("is more than %s old; its aggregation window has already closed",
				v.Limits.MaxBackfill),
		}}
	}
	return nil
}

func (v Validator) validateLabels(prefix string, labels map[string]string) []Violation {
	if len(labels) > v.Limits.MaxLabels {
		return []Violation{{
			Field: prefix + ".labels",
			Message: fmt.Sprintf("has %d labels, at most %d are allowed; high label cardinality is what makes a time-series store unqueryable",
				len(labels), v.Limits.MaxLabels),
		}}
	}

	var violations []Violation
	for key, value := range labels {
		field := prefix + ".labels." + key

		switch {
		case key == "":
			violations = append(violations, Violation{
				Field: prefix + ".labels", Message: "contains an empty label key"})
			continue
		case strings.HasPrefix(key, ReservedLabelPrefix):
			violations = append(violations, Violation{
				Field:   field,
				Message: fmt.Sprintf("uses the reserved %q prefix", ReservedLabelPrefix)})
			continue
		case len(key) > v.Limits.MaxLabelKeyLen:
			violations = append(violations, Violation{
				Field:   field,
				Message: fmt.Sprintf("key must be at most %d characters", v.Limits.MaxLabelKeyLen)})
			continue
		}

		if msg := validateLabelKey(key); msg != "" {
			violations = append(violations, Violation{Field: field, Message: msg})
			continue
		}

		if len(value) > v.Limits.MaxLabelValueLen {
			violations = append(violations, Violation{
				Field:   field,
				Message: fmt.Sprintf("value must be at most %d characters", v.Limits.MaxLabelValueLen)})
			continue
		}
		// Control characters survive JSON encoding and go on to corrupt log
		// lines, CSV exports and terminal output downstream.
		if i := strings.IndexFunc(value, isControl); i >= 0 {
			violations = append(violations, Violation{
				Field:   field,
				Message: fmt.Sprintf("value contains a control character at position %d", i)})
		}
	}
	return violations
}

func validateLabelKey(key string) string {
	for i, c := range key {
		valid := c == '_' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(i > 0 && c >= '0' && c <= '9')
		if !valid {
			return fmt.Sprintf(
				"key contains %q at position %d; use letters, digits and '_', starting with a letter or '_'",
				string(c), i)
		}
	}
	return ""
}

func isControl(r rune) bool {
	return r < 0x20 || r == 0x7f
}

// Normalize returns a copy of p with the conventions the pipeline relies on
// applied: timestamps in UTC, and an arrival time substituted when the sender
// omitted one.
//
// Storing everything in UTC means a rollup key derived from a timestamp is
// stable regardless of where the point was produced.
func (p Point) Normalize(receivedAt time.Time) Point {
	if p.Timestamp.IsZero() {
		p.Timestamp = receivedAt
	}
	p.Timestamp = p.Timestamp.UTC()
	return p
}
