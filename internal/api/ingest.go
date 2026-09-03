package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jon-jc/fluxgate/internal/auth"
	"github.com/jon-jc/fluxgate/internal/httpx"
	"github.com/jon-jc/fluxgate/internal/idempotency"
	"github.com/jon-jc/fluxgate/internal/ingest"
	"github.com/jon-jc/fluxgate/internal/observability"
	"github.com/jon-jc/fluxgate/internal/ratelimit"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

// HeaderIdempotencyKey lets a client mark a retry as a repeat of an earlier
// request rather than a new one.
const HeaderIdempotencyKey = "Idempotency-Key"

// HeaderIdempotencyReplayed tells a client its request was recognised as a
// repeat and the original outcome was replayed, not recomputed.
const HeaderIdempotencyReplayed = "Idempotency-Replayed"

// maxReportedErrors caps how many per-point failures a response enumerates.
//
// A thousand-point batch of entirely malformed points would otherwise produce
// a response far larger than the request that caused it. The counts stay
// exact; only the enumeration is truncated.
const maxReportedErrors = 100

// maxIdempotencyKeyLen bounds a client-supplied key, which becomes part of a
// map key held in memory.
const maxIdempotencyKeyLen = 255

// pointPayload is the wire representation of one observation.
//
// Timestamp is a pointer so that "absent" and "explicitly null" are
// distinguishable from "the zero instant": an omitted timestamp is filled in
// with the arrival time, whereas a sender that transmits a zero timestamp has
// a bug worth reporting.
type pointPayload struct {
	Metric    string            `json:"metric"`
	Kind      string            `json:"kind"`
	Value     float64           `json:"value"`
	Timestamp *time.Time        `json:"timestamp,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// ingestRequest is the body of POST /v1/ingest.
type ingestRequest struct {
	Points []pointPayload `json:"points"`
}

// ingestResponse reports what happened to each point in a batch.
type ingestResponse struct {
	// BatchID identifies the accepted batch downstream. Quote it to trace a
	// point from submission to rollup.
	BatchID string `json:"batch_id"`
	// Accepted counts the points admitted to the pipeline.
	Accepted int `json:"accepted"`
	// Rejected counts the points that failed validation.
	Rejected int `json:"rejected"`
	// Errors enumerates the failures, truncated to maxReportedErrors.
	Errors []httpx.FieldError `json:"errors,omitempty"`
	// ErrorsTruncated reports that Errors lists fewer entries than Rejected.
	ErrorsTruncated bool `json:"errors_truncated,omitempty"`
}

// IngestDeps are the collaborators the ingest handler needs.
type IngestDeps struct {
	// Sink receives accepted batches.
	Sink ingest.Sink
	// Validator applies the domain rules.
	Validator telemetry.Validator
	// Limiter meters points per second per tenant.
	Limiter *ratelimit.Limiter
	// Idempotency replays outcomes for repeated requests. Optional.
	Idempotency *idempotency.Store
	// MaxRequestBytes caps the request body.
	MaxRequestBytes int64
}

// handleIngest accepts a batch of telemetry points.
//
// The endpoint is deliberately partial-success: a batch with one bad point
// admits the other 999 and reports the one failure. Rejecting the whole batch
// would mean a single misbehaving call site in a client can silently blind an
// entire service's telemetry -- and telemetry is exactly what you need working
// when something is going wrong.
func handleIngest(deps IngestDeps) httpx.Handler {
	return func(w http.ResponseWriter, r *http.Request) error {
		principal, ok := auth.PrincipalFromContext(r.Context())
		if !ok {
			// Reaching here means the route was mounted outside the
			// authenticated group -- a wiring bug, not a client error.
			return httpx.Internal(errors.New("ingest handler reached without authentication"))
		}

		if err := httpx.RequireJSONContentType(r); err != nil {
			return err
		}

		body, err := httpx.ReadBody(w, r, deps.MaxRequestBytes)
		if err != nil {
			return err
		}

		idemKey, err := idempotencyKey(r)
		if err != nil {
			return err
		}

		fingerprint := idempotency.Fingerprint(body)
		if replayed, err := deps.replay(w, r, principal.TenantID, idemKey, fingerprint); err != nil || replayed {
			return err
		}

		var req ingestRequest
		if err := httpx.UnmarshalJSON(body, &req); err != nil {
			return err
		}

		limits := deps.Validator.Limits
		switch {
		case len(req.Points) == 0:
			return httpx.Invalid("A batch must contain at least one point.",
				httpx.FieldError{Field: "points", Message: "must not be empty"})
		case len(req.Points) > limits.MaxPointsPerBatch:
			return httpx.Invalid(
				fmt.Sprintf("A batch may contain at most %d points.", limits.MaxPointsPerBatch),
				httpx.FieldError{
					Field: "points",
					Message: fmt.Sprintf("has %d points, the limit is %d",
						len(req.Points), limits.MaxPointsPerBatch),
				})
		}

		// Metering happens after decoding because the cost is the point count,
		// and the point count is only knowable once the body is parsed. The
		// body size cap is what bounds the work an unmetered caller can force
		// before reaching this line.
		if err := deps.meter(w, principal, len(req.Points)); err != nil {
			return err
		}

		// The arrival stamp comes from the validator's clock, not time.Now:
		// a point stamped by one clock and validated against another can be
		// judged "in the future" by the service that just created it.
		receivedAt := deps.Validator.Now().UTC()
		accepted, fieldErrors := deps.validate(req.Points, receivedAt)

		// Nothing survived validation, so there is nothing to publish and the
		// whole request is a client error.
		if len(accepted) == 0 {
			return httpx.Invalid("No point in the batch passed validation.",
				truncateErrors(fieldErrors)...)
		}

		batch := telemetry.Batch{
			ID:         httpx.NewRequestID(),
			TenantID:   principal.TenantID,
			ReceivedAt: receivedAt,
			Points:     accepted,
		}

		if err := deps.Sink.Publish(r.Context(), batch); err != nil {
			// The batch was never durably accepted, so the client must retry.
			// Saying so plainly is what keeps their data from being lost.
			return httpx.Unavailable(
				"The batch could not be accepted for delivery. Retry with the same Idempotency-Key.").
				WithCause(fmt.Errorf("publish batch %s: %w", batch.ID, err))
		}

		resp := ingestResponse{
			BatchID:         batch.ID,
			Accepted:        len(accepted),
			Rejected:        len(req.Points) - len(accepted),
			Errors:          truncateErrors(fieldErrors),
			ErrorsTruncated: len(fieldErrors) > maxReportedErrors,
		}

		observability.LoggerFromContext(r.Context()).Info("batch accepted",
			slog.String("batch_id", batch.ID),
			slog.Int("accepted", resp.Accepted),
			slog.Int("rejected", resp.Rejected))

		deps.remember(r, principal.TenantID, idemKey, fingerprint, resp)

		// 202 rather than 201: delivery is asynchronous, and no addressable
		// resource exists yet at the moment this returns.
		return httpx.WriteJSON(w, r, http.StatusAccepted, resp)
	}
}

// validate normalises and checks every submitted point, returning those that
// passed alongside the failures.
func (deps IngestDeps) validate(
	payloads []pointPayload, receivedAt time.Time,
) ([]telemetry.Point, []httpx.FieldError) {
	accepted := make([]telemetry.Point, 0, len(payloads))
	var fieldErrors []httpx.FieldError

	for i, p := range payloads {
		point := telemetry.Point{
			Metric: p.Metric,
			Kind:   telemetry.Kind(p.Kind),
			Value:  p.Value,
			Labels: p.Labels,
		}
		if p.Timestamp != nil {
			point.Timestamp = *p.Timestamp
		}
		point = point.Normalize(receivedAt)

		violations := deps.Validator.ValidatePoint(i, point)
		if len(violations) == 0 {
			accepted = append(accepted, point)
			continue
		}
		for _, v := range violations {
			fieldErrors = append(fieldErrors,
				httpx.FieldError{Field: v.Field, Message: v.Message})
		}
	}
	return accepted, fieldErrors
}

// meter applies the tenant's quota and sets the RateLimit response headers.
func (deps IngestDeps) meter(w http.ResponseWriter, p auth.Principal, cost int) error {
	if deps.Limiter == nil {
		return nil
	}

	decision := deps.Limiter.AllowN(p.TenantID, cost, ratelimit.Limits{
		Rate:  p.RateLimitPerSecond,
		Burst: p.Burst,
	})

	h := w.Header()
	h.Set("X-RateLimit-Limit", strconv.FormatFloat(decision.Limit.Rate, 'f', -1, 64))
	h.Set("X-RateLimit-Remaining", strconv.Itoa(decision.Remaining))
	h.Set("X-RateLimit-Reset", strconv.Itoa(int(decision.ResetAfter.Seconds()+0.999)))

	if decision.Allowed {
		return nil
	}

	if decision.RetryAfter > 0 {
		// Round up: a client that retries a fraction of a second early is
		// simply denied again, which helps nobody.
		h.Set("Retry-After", strconv.Itoa(int(decision.RetryAfter.Seconds()+0.999)))
		return httpx.RateLimited(fmt.Sprintf(
			"Rate limit of %g points per second exceeded. Retry after %d seconds.",
			decision.Limit.Rate, int(decision.RetryAfter.Seconds()+0.999)))
	}

	// No retry delay means waiting cannot help: the batch is larger than the
	// burst ceiling will ever hold. Tell the client to split it instead.
	return httpx.RateLimited(fmt.Sprintf(
		"This batch of %d points exceeds the burst allowance of %d. Split it into smaller batches.",
		cost, decision.Limit.Burst))
}

// replay returns the stored outcome for a repeated request, if there is one.
// It reports whether a response was written.
func (deps IngestDeps) replay(
	w http.ResponseWriter, r *http.Request, tenantID, key, fingerprint string,
) (bool, error) {
	if deps.Idempotency == nil || key == "" {
		return false, nil
	}

	rec, found, err := deps.Idempotency.Lookup(tenantID, key, fingerprint)
	if errors.Is(err, idempotency.ErrPayloadMismatch) {
		return false, httpx.Conflict(
			"This Idempotency-Key was already used for a different payload. Use a new key for new data.").
			WithCause(err)
	}
	if err != nil || !found {
		return false, err
	}

	observability.LoggerFromContext(r.Context()).Info("replaying idempotent response",
		slog.String("idempotency_key", key))

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set(HeaderIdempotencyReplayed, "true")
	w.WriteHeader(rec.Status)
	_, _ = w.Write(rec.Body)
	return true, nil
}

// remember stores the outcome so a retry of the same request replays it.
func (deps IngestDeps) remember(
	r *http.Request, tenantID, key, fingerprint string, resp ingestResponse,
) {
	if deps.Idempotency == nil || key == "" {
		return
	}

	body, err := marshalResponse(resp)
	if err != nil {
		// Losing the record only costs duplicate protection on a retry, which
		// is not worth failing an otherwise successful request over.
		observability.LoggerFromContext(r.Context()).Warn(
			"could not store idempotency record", slog.Any("error", err))
		return
	}

	deps.Idempotency.Save(tenantID, key, idempotency.Record{
		Status:      http.StatusAccepted,
		Body:        body,
		Fingerprint: fingerprint,
	})
}

// idempotencyKey extracts and validates the client-supplied key.
func idempotencyKey(r *http.Request) (string, error) {
	key := r.Header.Get(HeaderIdempotencyKey)
	if key == "" {
		return "", nil
	}
	if len(key) > maxIdempotencyKeyLen {
		return "", httpx.BadRequest(fmt.Sprintf(
			"%s must be at most %d characters.", HeaderIdempotencyKey, maxIdempotencyKeyLen))
	}
	for i, c := range key {
		// Restrict to printable ASCII: the key is echoed into logs, and a
		// control character there corrupts every downstream consumer of them.
		if c < 0x21 || c > 0x7e {
			return "", httpx.BadRequest(fmt.Sprintf(
				"%s contains an unsupported character at position %d; use printable ASCII.",
				HeaderIdempotencyKey, i))
		}
	}
	return key, nil
}

// truncateErrors bounds the enumerated failures in a response.
func truncateErrors(errs []httpx.FieldError) []httpx.FieldError {
	if len(errs) <= maxReportedErrors {
		return errs
	}
	return errs[:maxReportedErrors]
}

// marshalResponse renders the response body exactly as WriteJSON would, so a
// replayed record is byte-identical to the original response.
func marshalResponse(resp ingestResponse) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(resp); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
