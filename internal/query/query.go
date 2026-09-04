// Package query turns stored rollups into the time series a caller asked for.
//
// It sits between the HTTP layer and the store: request validation and result
// shaping live here so that both are testable without a database, and so the
// rules about what a caller may ask for are written down in one place rather
// than scattered through handlers.
package query

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

// Aggregation selects which statistic a query reads out of each rollup.
type Aggregation string

// Supported aggregations.
const (
	AggSum   Aggregation = "sum"
	AggCount Aggregation = "count"
	AggAvg   Aggregation = "avg"
	AggMin   Aggregation = "min"
	AggMax   Aggregation = "max"
	AggLast  Aggregation = "last"
	AggP50   Aggregation = "p50"
	AggP90   Aggregation = "p90"
	AggP95   Aggregation = "p95"
	AggP99   Aggregation = "p99"
)

// quantiles maps the percentile aggregations to their fraction.
var quantiles = map[Aggregation]float64{
	AggP50: 0.50,
	AggP90: 0.90,
	AggP95: 0.95,
	AggP99: 0.99,
}

// Aggregations lists every accepted value, for error messages and for the API
// description. It is sorted so the message a client sees is stable.
var Aggregations = []Aggregation{
	AggSum, AggCount, AggAvg, AggMin, AggMax, AggLast,
	AggP50, AggP90, AggP95, AggP99,
}

// Valid reports whether a is a recognised aggregation.
func (a Aggregation) Valid() bool {
	for _, known := range Aggregations {
		if a == known {
			return true
		}
	}
	return false
}

// IsQuantile reports whether a reads from the histogram rather than from a
// scalar field.
func (a Aggregation) IsQuantile() bool {
	_, ok := quantiles[a]
	return ok
}

// Limits bound what a single query may ask for.
type Limits struct {
	// MaxRange is the longest time span a query may cover.
	MaxRange time.Duration
	// MaxSeries caps how many distinct series one response may describe.
	MaxSeries int
	// MaxPoints caps the total points across all series in a response.
	MaxPoints int
	// DefaultRange is used when a caller supplies neither bound.
	DefaultRange time.Duration
}

// DefaultLimits returns the limits the query API enforces unless configured
// otherwise.
func DefaultLimits() Limits {
	return Limits{
		MaxRange:     31 * 24 * time.Hour,
		MaxSeries:    500,
		MaxPoints:    50_000,
		DefaultRange: time.Hour,
	}
}

// Request is a validated query.
type Request struct {
	TenantID    string
	Metric      string
	From        time.Time
	To          time.Time
	Aggregation Aggregation
	Labels      map[string]string
	MaxSeries   int
}

// Violation describes one rejected parameter, mirroring the domain's shape so
// the HTTP layer can render it without this package importing a transport.
type Violation struct {
	Field   string
	Message string
}

// Params is the unvalidated input, as it arrives on the wire.
type Params struct {
	Metric      string
	From        string
	To          string
	Aggregation string
	Labels      map[string]string
	Now         time.Time
}

// Parse validates raw parameters into a Request.
//
// Every problem is reported at once. A caller iterating on a query should learn
// that the range is too wide *and* the aggregation is misspelled in one
// response, not discover the second only after fixing the first.
func Parse(tenantID string, p Params, limits Limits) (Request, []Violation) {
	var violations []Violation

	now := p.Now
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()

	req := Request{
		TenantID:    tenantID,
		Metric:      strings.TrimSpace(p.Metric),
		Aggregation: AggAvg,
		Labels:      p.Labels,
		MaxSeries:   limits.MaxSeries,
	}

	if req.Metric == "" {
		violations = append(violations, Violation{
			Field: "metric", Message: "is required"})
	} else if msg := validateMetricName(req.Metric); msg != "" {
		violations = append(violations, Violation{Field: "metric", Message: msg})
	}

	if p.Aggregation != "" {
		agg := Aggregation(strings.ToLower(strings.TrimSpace(p.Aggregation)))
		if !agg.Valid() {
			violations = append(violations, Violation{
				Field:   "agg",
				Message: fmt.Sprintf("%q is not supported; use one of %s", p.Aggregation, aggList()),
			})
		} else {
			req.Aggregation = agg
		}
	}

	// `to` is relative to now; `from` is relative to `to`, so that
	// `from=-15m` means fifteen minutes before the end of the range rather
	// than fifteen minutes before some other anchor the caller cannot see.
	to, ok := parseTime("to", p.To, now, now, &violations)
	if !ok {
		to = now
	}
	// The fallback ends at the caller's `to`, not at now, so supplying only
	// `to` gives a window ending there rather than a range that ignores it.
	from, ok := parseTime("from", p.From, to.Add(-limits.DefaultRange), to, &violations)
	if !ok {
		from = to.Add(-limits.DefaultRange)
	}

	req.From, req.To = from, to

	switch {
	case !req.To.After(req.From):
		violations = append(violations, Violation{
			Field: "from", Message: "must be strictly before `to`"})
	case limits.MaxRange > 0 && req.To.Sub(req.From) > limits.MaxRange:
		violations = append(violations, Violation{
			Field: "from",
			Message: fmt.Sprintf("range of %s exceeds the maximum of %s",
				req.To.Sub(req.From).Round(time.Second), limits.MaxRange),
		})
	}

	for key, value := range p.Labels {
		if msg := validateLabelKey(key); msg != "" {
			violations = append(violations, Violation{
				Field: "label." + key, Message: msg})
		}
		if len(value) > telemetry.DefaultLimits().MaxLabelValueLen {
			violations = append(violations, Violation{
				Field:   "label." + key,
				Message: "value is longer than any stored label can be",
			})
		}
	}

	return req, violations
}

func aggList() string {
	names := make([]string, len(Aggregations))
	for i, a := range Aggregations {
		names[i] = string(a)
	}
	return strings.Join(names, ", ")
}

// parseTime accepts RFC 3339, or a negative duration relative to anchor.
//
// fallback is used when the value is absent; anchor is what a relative
// duration is measured from. They are separate parameters because they are
// genuinely different: `from` defaults to a default-range window but a relative
// `from` is measured from `to`, and conflating the two silently shifts every
// relative query by the default range.
//
// The relative form exists because "the last fifteen minutes" is what a human
// actually wants, and making them compute two timestamps to express it is how
// a dashboard ends up with a hard-coded range that silently goes stale.
func parseTime(
	field, raw string, fallback, anchor time.Time, violations *[]Violation,
) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, true
	}

	if strings.HasPrefix(raw, "-") {
		d, err := time.ParseDuration(raw)
		if err != nil {
			*violations = append(*violations, Violation{
				Field:   field,
				Message: fmt.Sprintf("%q is neither an RFC 3339 timestamp nor a relative duration such as -15m", raw),
			})
			return fallback, false
		}
		return anchor.Add(d).UTC(), true
	}

	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		*violations = append(*violations, Violation{
			Field:   field,
			Message: fmt.Sprintf("%q is not an RFC 3339 timestamp", raw),
		})
		return fallback, false
	}
	return t.UTC(), true
}

func validateMetricName(name string) string {
	if len(name) > telemetry.DefaultLimits().MaxMetricNameLen {
		return "is longer than any stored metric name can be"
	}
	for i, c := range name {
		valid := c == '_' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(i > 0 && (c == '.' || c == '-' || (c >= '0' && c <= '9')))
		if !valid {
			return fmt.Sprintf("contains %q at position %d", string(c), i)
		}
	}
	return ""
}

func validateLabelKey(key string) string {
	if key == "" {
		return "must not be empty"
	}
	for i, c := range key {
		valid := c == '_' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(i > 0 && c >= '0' && c <= '9')
		if !valid {
			return fmt.Sprintf("contains %q at position %d", string(c), i)
		}
	}
	return ""
}

// Point is one value at one window start.
type Point struct {
	Timestamp time.Time `json:"t"`
	Value     float64   `json:"v"`
}

// Series is one label combination's values over the queried range.
type Series struct {
	Labels map[string]string `json:"labels"`
	Points []Point           `json:"points"`
}

// Result is the answer to a query.
type Result struct {
	Metric      string   `json:"metric"`
	Kind        string   `json:"kind,omitempty"`
	Aggregation string   `json:"aggregation"`
	From        string   `json:"from"`
	To          string   `json:"to"`
	Series      []Series `json:"series"`
	// Truncated reports that the result was cut short by a limit, so a caller
	// can tell an incomplete answer from an empty one.
	Truncated bool `json:"truncated,omitempty"`
	// Warnings carry facts a caller needs but that are not errors, such as a
	// percentile requested of a metric that has no histogram.
	Warnings []string `json:"warnings,omitempty"`
}

// Build groups rollups into series and extracts the requested statistic.
//
// Rollups arrive newest-first from the store, because that is the order the
// index serves cheaply. Series are emitted oldest-first, because that is the
// order a chart draws.
func Build(req Request, rollups []store.StoredRollup, limits Limits) Result {
	result := Result{
		Metric:      req.Metric,
		Aggregation: string(req.Aggregation),
		From:        req.From.UTC().Format(time.RFC3339),
		To:          req.To.UTC().Format(time.RFC3339),
		Series:      []Series{},
	}

	type group struct {
		labels map[string]string
		points []Point
	}

	var (
		groups           = make(map[string]*group)
		order            []string
		total            int
		missingHistogram bool
	)

	for i := range rollups {
		r := &rollups[i]
		result.Kind = r.Kind

		value, ok := extract(r, req.Aggregation)
		if !ok {
			// A percentile on a series with no histogram: skip the point
			// rather than reporting a zero the data never contained.
			missingHistogram = true
			continue
		}

		key := store.SeriesFingerprint(r.Labels)
		g, exists := groups[key]
		if !exists {
			if len(groups) >= req.MaxSeries {
				result.Truncated = true
				continue
			}
			g = &group{labels: orEmptyLabels(r.Labels)}
			groups[key] = g
			order = append(order, key)
		}

		if limits.MaxPoints > 0 && total >= limits.MaxPoints {
			result.Truncated = true
			break
		}

		g.points = append(g.points, Point{Timestamp: r.WindowStart.UTC(), Value: value})
		total++
	}

	if missingHistogram && req.Aggregation.IsQuantile() {
		result.Warnings = append(result.Warnings, fmt.Sprintf(
			"%s is only available for histogram metrics; windows without a histogram were omitted",
			req.Aggregation))
	}

	// Stable output ordering: a client diffing two responses, or a test
	// asserting on one, should not see series shuffle between identical calls.
	sort.Strings(order)

	for _, key := range order {
		g := groups[key]
		sort.Slice(g.points, func(i, j int) bool {
			return g.points[i].Timestamp.Before(g.points[j].Timestamp)
		})
		result.Series = append(result.Series, Series{Labels: g.labels, Points: g.points})
	}

	return result
}

// extract reads one statistic out of a stored rollup.
func extract(r *store.StoredRollup, agg Aggregation) (float64, bool) {
	if q, ok := quantiles[agg]; ok {
		return aggregate.QuantileFromBuckets(r.Buckets, q)
	}

	switch agg {
	case AggSum:
		return r.Sum, true
	case AggCount:
		return float64(r.Count), true
	case AggAvg:
		return r.Mean(), true
	case AggMin:
		return r.Min, true
	case AggMax:
		return r.Max, true
	case AggLast:
		return r.Last, true
	default:
		// Unreachable: validation rejects anything else before this runs.
		return 0, false
	}
}

func orEmptyLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return map[string]string{}
	}
	return labels
}
