package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jon-jc/fluxgate/internal/auth"
	"github.com/jon-jc/fluxgate/internal/httpx"
	"github.com/jon-jc/fluxgate/internal/observability"
	"github.com/jon-jc/fluxgate/internal/query"
	"github.com/jon-jc/fluxgate/internal/store"
)

// Reader is the read side of the store. It is an interface so the handlers can
// be tested without a database.
type Reader interface {
	Query(ctx context.Context, f store.QueryFilter) ([]store.StoredRollup, error)
	Changed(ctx context.Context, tenantID, metric string, cursor store.Cursor, limit int) ([]store.StoredRollup, store.Cursor, error)
	NewestWriteTime(ctx context.Context, tenantID string) (time.Time, error)
	Metrics(ctx context.Context, tenantID string, limit int) ([]store.MetricSummary, error)
	LabelKeys(ctx context.Context, tenantID, metric string, limit int) ([]string, error)
	LabelValues(ctx context.Context, tenantID, metric, label string, limit int) ([]string, error)
}

// QueryDeps are the collaborators the read endpoints need.
type QueryDeps struct {
	// Reader supplies stored rollups.
	Reader Reader
	// Limits bound what one query may ask for.
	Limits query.Limits
	// Stream configures the live tail.
	Stream StreamOptions
	// Now is the clock, injectable so relative ranges are testable.
	Now func() time.Time
}

func (d QueryDeps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// StreamOptions configures the server-sent events endpoint.
type StreamOptions struct {
	// PollInterval is how often the tail checks for newly written rollups.
	PollInterval time.Duration
	// HeartbeatInterval is how often a comment is sent on an idle stream, to
	// keep intermediaries from closing a connection they believe is dead.
	HeartbeatInterval time.Duration
	// MaxDuration bounds a single connection, so a forgotten browser tab does
	// not hold a database poller open indefinitely.
	MaxDuration time.Duration
}

func (o *StreamOptions) applyDefaults() {
	if o.PollInterval <= 0 {
		o.PollInterval = 2 * time.Second
	}
	if o.HeartbeatInterval <= 0 {
		o.HeartbeatInterval = 20 * time.Second
	}
	if o.MaxDuration <= 0 {
		o.MaxDuration = 30 * time.Minute
	}
}

// labelParamPrefix marks a query parameter as a label filter, so label names
// can never collide with the API's own parameters. A metric labelled "agg"
// would otherwise be unqueryable.
const labelParamPrefix = "label."

// handleQuery reads a metric's rollups as time series.
func handleQuery(deps QueryDeps) httpx.Handler {
	return func(w http.ResponseWriter, r *http.Request) error {
		principal, ok := auth.PrincipalFromContext(r.Context())
		if !ok {
			return httpx.Internal(errors.New("query handler reached without authentication"))
		}

		params := r.URL.Query()
		req, violations := query.Parse(principal.TenantID, query.Params{
			Metric:      params.Get("metric"),
			From:        params.Get("from"),
			To:          params.Get("to"),
			Aggregation: params.Get("agg"),
			Labels:      labelFilters(params),
			Now:         deps.now(),
		}, deps.Limits)

		if len(violations) > 0 {
			return httpx.Invalid("The query is not valid.", fieldErrors(violations)...)
		}

		rollups, err := deps.Reader.Query(r.Context(), store.QueryFilter{
			// The tenant comes from the credential, never from a parameter: a
			// caller able to name someone else's tenant would be able to read
			// their data.
			TenantID: req.TenantID,
			Metric:   req.Metric,
			From:     req.From,
			To:       req.To,
			Labels:   req.Labels,
			Limit:    deps.Limits.MaxPoints,
		})
		if err != nil {
			return httpx.Internal(fmt.Errorf("read rollups: %w", err))
		}

		result := query.Build(req, rollups, deps.Limits)
		return httpx.WriteJSON(w, r, http.StatusOK, result)
	}
}

// handleMetrics lists the metrics a tenant has data for.
func handleMetrics(deps QueryDeps) httpx.Handler {
	return func(w http.ResponseWriter, r *http.Request) error {
		principal, ok := auth.PrincipalFromContext(r.Context())
		if !ok {
			return httpx.Internal(errors.New("metrics handler reached without authentication"))
		}

		metrics, err := deps.Reader.Metrics(r.Context(), principal.TenantID, parseLimit(r, 500))
		if err != nil {
			return httpx.Internal(fmt.Errorf("list metrics: %w", err))
		}
		if metrics == nil {
			// An empty array rather than null: a client should be able to
			// iterate the response without a nil check.
			metrics = []store.MetricSummary{}
		}

		return httpx.WriteJSON(w, r, http.StatusOK, map[string]any{"metrics": metrics})
	}
}

// handleLabels lists the label keys, or one key's values, for a metric.
func handleLabels(deps QueryDeps) httpx.Handler {
	return func(w http.ResponseWriter, r *http.Request) error {
		principal, ok := auth.PrincipalFromContext(r.Context())
		if !ok {
			return httpx.Internal(errors.New("labels handler reached without authentication"))
		}

		metric := strings.TrimSpace(r.URL.Query().Get("metric"))
		if metric == "" {
			return httpx.Invalid("A metric is required.",
				httpx.FieldError{Field: "metric", Message: "is required"})
		}

		limit := parseLimit(r, 500)

		// With a label named, return its values; otherwise the available keys.
		if label := strings.TrimSpace(r.URL.Query().Get("label")); label != "" {
			values, err := deps.Reader.LabelValues(
				r.Context(), principal.TenantID, metric, label, limit)
			if err != nil {
				return httpx.Internal(fmt.Errorf("read label values: %w", err))
			}
			return httpx.WriteJSON(w, r, http.StatusOK, map[string]any{
				"metric": metric,
				"label":  label,
				"values": orEmptyStrings(values),
			})
		}

		keys, err := deps.Reader.LabelKeys(r.Context(), principal.TenantID, metric, limit)
		if err != nil {
			return httpx.Internal(fmt.Errorf("read label keys: %w", err))
		}
		return httpx.WriteJSON(w, r, http.StatusOK, map[string]any{
			"metric": metric,
			"labels": orEmptyStrings(keys),
		})
	}
}

// streamEvent is one server-sent event payload.
type streamEvent struct {
	Metric      string            `json:"metric"`
	Labels      map[string]string `json:"labels"`
	WindowStart time.Time         `json:"window_start"`
	WindowEnd   time.Time         `json:"window_end"`
	Count       int64             `json:"count"`
	Sum         float64           `json:"sum"`
	Min         float64           `json:"min"`
	Max         float64           `json:"max"`
	Last        float64           `json:"last"`
}

// handleStream tails newly written rollups over server-sent events.
//
// SSE rather than WebSocket: the traffic is one-directional, every browser and
// HTTP client already speaks it, it survives proxies that mangle upgrades, and
// reconnection is built into the protocol rather than into every client.
func handleStream(deps QueryDeps) httpx.Handler {
	opts := deps.Stream
	opts.applyDefaults()

	return func(w http.ResponseWriter, r *http.Request) error {
		principal, ok := auth.PrincipalFromContext(r.Context())
		if !ok {
			return httpx.Internal(errors.New("stream handler reached without authentication"))
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			// Without flushing, every event would sit in a buffer until the
			// response ended -- which for a stream is never.
			return httpx.Internal(errors.New("response writer does not support flushing"))
		}

		metric := strings.TrimSpace(r.URL.Query().Get("metric"))
		if metric != "" {
			if _, violations := query.Parse(principal.TenantID,
				query.Params{Metric: metric, Now: deps.now()}, deps.Limits); len(violations) > 0 {
				return httpx.Invalid("The stream request is not valid.",
					fieldErrors(violations)...)
			}
		}

		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-store")
		// Proxies that buffer a response would defeat the whole endpoint;
		// nginx and several CDNs honour this hint to stream it through.
		h.Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)

		// Tell the client how long to wait before reconnecting. Without it,
		// browsers default to three seconds, and a thousand tabs dropped by one
		// deploy would all return within the same instant.
		//
		// A failure to write the preamble means the client disconnected before
		// the stream even began; there is nobody left to report it to.
		if _, err := fmt.Fprintf(
			w, "retry: %d\n\n", opts.PollInterval.Milliseconds()*2,
		); err != nil {
			return nil
		}
		flusher.Flush()

		ctx, cancel := context.WithTimeout(r.Context(), opts.MaxDuration)
		defer cancel()

		return streamRollups(ctx, w, flusher, deps, opts, principal.TenantID, metric)
	}
}

func streamRollups(
	ctx context.Context, w http.ResponseWriter, flusher http.Flusher,
	deps QueryDeps, opts StreamOptions, tenantID, metric string,
) error {
	log := observability.LoggerFromContext(ctx)

	// Start from now: a tail is for watching what happens next, and replaying
	// history on connect would flood a client that only wanted the live edge.
	//
	// "Now" is asked of the database, not of this process. Rows are stamped by
	// the database clock, so any skew between the two would either hide events
	// or replay history -- both silently.
	seed, err := deps.Reader.NewestWriteTime(ctx, tenantID)
	if err != nil {
		// Falling back to the local clock keeps the stream working through a
		// blip; the cost is at most a little skew on the first poll.
		log.Warn("could not seed the stream cursor from the database",
			slog.Any("error", err))
		seed = deps.now().UTC()
	}
	cursor := store.Cursor{Since: seed}

	poll := time.NewTicker(opts.PollInterval)
	defer poll.Stop()

	heartbeat := time.NewTicker(opts.HeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			// A cancelled stream is how this endpoint always ends: the client
			// navigated away, or the connection hit its bound.
			return nil

		case <-heartbeat.C:
			// A comment, which the protocol ignores but which keeps an idle
			// connection from being reaped by an intermediary.
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return nil
			}
			flusher.Flush()

		case <-poll.C:
			changed, next, err := deps.Reader.Changed(ctx, tenantID, metric, cursor, 500)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				// The stream is long-lived; one failed poll should not end it.
				log.Warn("stream poll failed", slog.Any("error", err))
				continue
			}
			cursor = next

			for i := range changed {
				if err := writeEvent(w, &changed[i]); err != nil {
					// A write failure on a stream means the client
					// disconnected, which is how every stream ends. Reporting
					// it as an error would fill the logs with the ordinary.
					log.Debug("stream client disconnected", slog.Any("error", err))
					return nil //nolint:nilerr // disconnection is the normal exit
				}
			}
			if len(changed) > 0 {
				flusher.Flush()
			}
		}
	}
}

func writeEvent(w http.ResponseWriter, r *store.StoredRollup) error {
	payload, err := json.Marshal(streamEvent{
		Metric:      r.Metric,
		Labels:      orEmptyLabels(r.Labels),
		WindowStart: r.WindowStart.UTC(),
		WindowEnd:   r.WindowEnd.UTC(),
		Count:       r.Count,
		Sum:         r.Sum,
		Min:         r.Min,
		Max:         r.Max,
		Last:        r.Last,
	})
	if err != nil {
		// The payload is fixed-shape data, so this cannot fail in practice.
		// Dropping one event is strictly better than tearing down a live
		// stream over an encoding problem the client cannot act on.
		return nil //nolint:nilerr // deliberate: skip the event, keep the stream
	}

	_, err = fmt.Fprintf(w, "event: rollup\ndata: %s\n\n", payload)
	return err
}

// labelFilters extracts the label.* parameters.
func labelFilters(params map[string][]string) map[string]string {
	var labels map[string]string
	for key, values := range params {
		if !strings.HasPrefix(key, labelParamPrefix) || len(values) == 0 {
			continue
		}
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[strings.TrimPrefix(key, labelParamPrefix)] = values[0]
	}
	return labels
}

func fieldErrors(violations []query.Violation) []httpx.FieldError {
	out := make([]httpx.FieldError, len(violations))
	for i, v := range violations {
		out[i] = httpx.FieldError{Field: v.Field, Message: v.Message}
	}
	return out
}

func parseLimit(r *http.Request, fallback int) int {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		// A malformed limit is not worth failing a read over; the default is
		// already a safe answer.
		return fallback
	}
	return n
}

func orEmptyStrings(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}

func orEmptyLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return map[string]string{}
	}
	return labels
}
