package query

import (
	"strings"
	"testing"
	"time"
)

// FuzzParse asserts that the query parser cannot be talked into producing a
// request the rest of the read path would then have to defend itself against.
//
// Parse is the only thing standing between a URL query string and a SQL query.
// Every field below is attacker-controlled, and Parse returning a Request with
// no violations is a promise to the caller that the request is safe to execute.
// Three ways that promise could break, all of which reach the database:
//
//   - A panic, which turns a malformed URL into a 500 and a stack trace.
//   - An inverted or absurd range, which becomes an index scan across the whole
//     retention period no matter what the range limit says.
//   - An aggregation the builder does not recognise, which silently produces an
//     empty series rather than an error the caller can act on.
func FuzzParse(f *testing.F) {
	f.Add("http.requests", "-1h", "", "sum", "service", "checkout")
	f.Add("", "", "", "", "", "")
	f.Add("m", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", "p99", "k", "v")
	f.Add("m", "-999999h", "", "AVG", "k", "v")
	f.Add("m", "-0s", "-0s", "p0.0000001", "", "")
	f.Add("m", "9999-12-31T23:59:59Z", "0001-01-01T00:00:00Z", "quantile", "k", "v")
	f.Add("m", "-1ns", "", "p100", "\x00", "\x00")

	limits := DefaultLimits()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)

	f.Fuzz(func(t *testing.T, metric, from, to, agg, labelKey, labelValue string) {
		params := Params{
			Metric:      metric,
			From:        from,
			To:          to,
			Aggregation: agg,
			Now:         now,
		}
		if labelKey != "" {
			params.Labels = map[string]string{labelKey: labelValue}
		}

		req, violations := Parse("acme", params, limits)
		if len(violations) > 0 {
			// A rejection must say which field to fix. A violation with an
			// empty field renders as an error the caller cannot act on.
			for _, v := range violations {
				if strings.TrimSpace(v.Field) == "" || strings.TrimSpace(v.Message) == "" {
					t.Fatalf("unactionable violation: %+v", v)
				}
			}
			return
		}

		// From here on, Parse has declared the request safe to execute.
		if req.TenantID != "acme" {
			t.Fatalf("tenant rewritten to %q -- a parsed request must stay "+
				"scoped to its caller", req.TenantID)
		}
		if req.Metric == "" {
			t.Fatal("accepted a request with no metric")
		}
		if !req.From.Before(req.To) {
			t.Fatalf("accepted an inverted or empty range: %s -> %s",
				req.From, req.To)
		}
		if span := req.To.Sub(req.From); span > limits.MaxRange {
			t.Fatalf("accepted a %s range, over the %s limit", span, limits.MaxRange)
		}
		if !req.Aggregation.Valid() {
			t.Fatalf("accepted aggregation %q, which Build does not implement",
				req.Aggregation)
		}
		if req.MaxSeries <= 0 {
			t.Fatalf("accepted MaxSeries=%d, which would return nothing",
				req.MaxSeries)
		}

		// Build must survive whatever Parse blessed, including with no rows.
		if got := Build(req, nil, limits); got.Series == nil {
			t.Fatal("Build produced a nil series slice, which marshals as " +
				"null rather than []")
		}
	})
}
