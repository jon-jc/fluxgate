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
	"sync"
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
	ChangedPage(ctx context.Context, tenantID, metric string, cursor store.Cursor, limit int) (store.ChangePage, error)
	NewestRevision(ctx context.Context, tenantID string) (int64, error)
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
	// Bound connection count, individual polls, and writes to slow clients.
	MaxConcurrent int
	MaxPerTenant  int
	QueryTimeout  time.Duration
	WriteTimeout  time.Duration
	// PollInterval is how often the tail checks for newly written rollups.
	PollInterval time.Duration
	// HeartbeatInterval is how often a comment is sent on an idle stream, to
	// keep intermediaries from closing a connection they believe is dead.
	HeartbeatInterval time.Duration
	// MaxDuration bounds a single connection, so a forgotten browser tab does
	// not hold a database poller open indefinitely.
	MaxDuration time.Duration
	// MaxResumeRevisions is how many tenant commits a resumed stream may span.
	// A Last-Event-ID further behind than this, or a stream that falls this far
	// behind while connected, gets a reset event instead of a replay: past
	// some point a client is better served by one REST query than by a
	// trickle of history.
	MaxResumeRevisions int64
	// MaxCatchUpPages bounds how many full pages one poll reads back to back
	// before yielding, so a stream that is behind catches up quickly without
	// monopolising the database pool.
	MaxCatchUpPages int
}

func (o *StreamOptions) applyDefaults() {
	if o.MaxConcurrent <= 0 {
		o.MaxConcurrent = 100
	}
	if o.MaxPerTenant <= 0 {
		o.MaxPerTenant = 8
	}
	o.MaxPerTenant = min(o.MaxPerTenant, o.MaxConcurrent)
	if o.QueryTimeout <= 0 {
		o.QueryTimeout = 5 * time.Second
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 10 * time.Second
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 2 * time.Second
	}
	if o.HeartbeatInterval <= 0 {
		o.HeartbeatInterval = 20 * time.Second
	}
	if o.MaxDuration <= 0 {
		o.MaxDuration = 30 * time.Minute
	}
	if o.MaxResumeRevisions <= 0 {
		o.MaxResumeRevisions = 10_000
	}
	if o.MaxCatchUpPages <= 0 {
		o.MaxCatchUpPages = 20
	}
}

// HeaderLastEventID is sent by a reconnecting SSE client with the id of the
// last event it received.
const HeaderLastEventID = "Last-Event-ID"

// streamPageRows is the row bound of one live-tail read.
const streamPageRows = 500

// Reasons carried by a reset event. Each means the stream cannot account for
// every change since the client's position, so the client should refetch the
// range it displays; the stream itself carries on from the latest revision.
const (
	resetStale   = "stale_cursor"   // further behind than MaxResumeRevisions
	resetFuture  = "future_cursor"  // ahead of every commit, e.g. after a restore
	resetForeign = "foreign_cursor" // issued for another tenant
	resetLagging = "lagging"        // fell too far behind while connected
)

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
			Limit:    deps.Limits.MaxPoints + 1,
		})
		if err != nil {
			if errors.Is(err, store.ErrReadBudget) {
				return httpx.Invalid("The query result is too large. Narrow the time range or add label filters.")
			}
			return httpx.Internal(fmt.Errorf("read rollups: %w", err))
		}

		truncated := len(rollups) > deps.Limits.MaxPoints
		if truncated {
			rollups = rollups[:deps.Limits.MaxPoints]
		}
		result := query.Build(req, rollups, deps.Limits)
		result.Truncated = result.Truncated || truncated
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
	Kind        string            `json:"kind"`
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
	slots := make(chan struct{}, opts.MaxConcurrent)
	var admission sync.Mutex
	perTenant := make(map[string]int)

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

		// Validated before admission: a malformed cursor is a client bug and
		// should not cost a stream slot to report.
		var resume *store.Cursor
		foreign := false
		if token := r.Header.Get(HeaderLastEventID); token != "" {
			cursor, err := store.ParseCursorToken(principal.TenantID, token)
			switch {
			case errors.Is(err, store.ErrCursorForeign):
				foreign = true
			case err != nil:
				return httpx.BadRequest("Last-Event-ID is not a position issued by this stream. " +
					"Reconnect without it to start from the latest revision.")
			default:
				resume = &cursor
			}
		}

		admission.Lock()
		if perTenant[principal.TenantID] >= opts.MaxPerTenant {
			admission.Unlock()
			w.Header().Set("Retry-After", "5")
			return httpx.RateLimited("This tenant has too many live streams. Close a stream before retrying.")
		}
		select {
		case slots <- struct{}{}:
			perTenant[principal.TenantID]++
			admission.Unlock()
			defer func() {
				admission.Lock()
				defer admission.Unlock()
				<-slots
				perTenant[principal.TenantID]--
				if perTenant[principal.TenantID] == 0 {
					delete(perTenant, principal.TenantID)
				}
			}()
		default:
			admission.Unlock()
			w.Header().Set("Retry-After", "5")
			return httpx.Unavailable("The live stream is at capacity. Retry shortly.")
		}
		seedCtx, seedCancel := context.WithTimeout(r.Context(), opts.QueryTimeout)
		seed, seedErr := deps.Reader.NewestRevision(seedCtx, principal.TenantID)
		seedCancel()
		if seedErr != nil {
			return httpx.Internal(fmt.Errorf("seed live stream: %w", seedErr))
		}
		if err := streamWriteDeadline(w, opts.WriteTimeout); err != nil {
			return httpx.Internal(err)
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

		// Either confirm the starting position or say why the client's own
		// cannot be honoured. The id-only frame updates the client's
		// Last-Event-ID without dispatching an event, so a stream that ends
		// before anything changes still resumes from here rather than from
		// whatever the latest revision is by the time it reconnects.
		start, reason := resumeFrom(seed, resume, foreign, opts.MaxResumeRevisions)
		var preambleErr error
		if reason != "" {
			preambleErr = writeReset(w, principal.TenantID, start, reason)
		} else {
			// The id is re-encoded by Token, never the client's header echoed
			// back, and its base64url alphabet cannot end an SSE field (see
			// TestCursorTokensCannotBreakAnEventStreamFrame).
			_, preambleErr = fmt.Fprintf(w, "id: %s\n\n", start.Token(principal.TenantID)) //nolint:gosec // G705: base64url token in text/event-stream
		}
		if preambleErr != nil {
			return nil
		}
		flusher.Flush()

		ctx, cancel := context.WithTimeout(r.Context(), opts.MaxDuration)
		defer cancel()

		return streamRollups(ctx, w, flusher, deps, opts, principal.TenantID, metric, start)
	}
}

// resumeFrom decides where a stream starts. A client position is honoured when
// it is within reach; otherwise the stream starts at the latest revision and
// the returned reason is sent as a reset event.
func resumeFrom(newest int64, resume *store.Cursor, foreign bool, maxLag int64) (start store.Cursor, reason string) {
	latest := store.Cursor{Revision: newest}
	switch {
	case foreign:
		return latest, resetForeign
	case resume == nil:
		return latest, ""
	case resume.Revision > newest:
		// Without this check the stream would wait, silently, for a revision
		// this database may never reach -- the situation after a restore to
		// an earlier point.
		return latest, resetFuture
	case newest-resume.Revision > maxLag:
		return latest, resetStale
	}
	return *resume, ""
}

func streamRollups(
	ctx context.Context, w http.ResponseWriter, flusher http.Flusher,
	deps QueryDeps, opts StreamOptions, tenantID, metric string, cursor store.Cursor,
) error {

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
			if err := streamWriteDeadline(w, opts.WriteTimeout); err != nil {
				return nil
			}
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return nil
			}
			flusher.Flush()

		case <-poll.C:
			next, open := catchUp(ctx, w, flusher, deps, opts, tenantID, metric, cursor)
			if !open {
				return nil
			}
			cursor = next
		}
	}
}

// catchUp sends every change since cursor, reading up to MaxCatchUpPages full
// pages back to back, and reports where it stopped and whether the stream is
// still open.
//
// One page per poll interval capped a stream at 250 rows a second by default,
// below what a busy tenant writes: such a stream fell further behind on every
// tick until its connection expired, and the client, reconnecting at the
// latest revision, never learned what it had missed. Reading on while pages
// come back full keeps up, and a stream still too far behind after a full
// burst says so with a reset event rather than lagging silently.
func catchUp(
	ctx context.Context, w http.ResponseWriter, flusher http.Flusher,
	deps QueryDeps, opts StreamOptions, tenantID, metric string, cursor store.Cursor,
) (store.Cursor, bool) {
	log := observability.LoggerFromContext(ctx)
	for pages := 1; ; pages++ {
		pollCtx, pollCancel := context.WithTimeout(ctx, opts.QueryTimeout)
		page, err := deps.Reader.ChangedPage(pollCtx, tenantID, metric, cursor, streamPageRows)
		pollCancel()
		if err != nil {
			if ctx.Err() != nil {
				return cursor, false
			}
			// The stream is long-lived; one failed poll should not end it.
			log.Warn("stream poll failed", slog.Any("error", err))
			return cursor, true
		}
		cursor = page.Next

		if len(page.Rows) > 0 {
			if err = streamWriteDeadline(w, opts.WriteTimeout); err != nil {
				return cursor, false
			}
			for i := range page.Rows {
				if err = writeEvent(w, tenantID, &page.Rows[i]); err != nil {
					// A write failure on a stream means the client
					// disconnected, which is how every stream ends. Reporting
					// it as an error would fill the logs with the ordinary.
					log.Debug("stream client disconnected", slog.Any("error", err))
					return cursor, false
				}
			}
			flusher.Flush()
		}

		if !page.More {
			return cursor, true
		}
		if pages < opts.MaxCatchUpPages {
			continue
		}

		// A full burst and still more: find out how far behind this is.
		lagCtx, lagCancel := context.WithTimeout(ctx, opts.QueryTimeout)
		newest, err := deps.Reader.NewestRevision(lagCtx, tenantID)
		lagCancel()
		if err != nil || newest-cursor.Revision <= opts.MaxResumeRevisions {
			// Within reach, or unknown: carry on from here at the next tick.
			return cursor, ctx.Err() == nil
		}
		latest := store.Cursor{Revision: newest}
		if err := streamWriteDeadline(w, opts.WriteTimeout); err != nil {
			return cursor, false
		}
		if err := writeReset(w, tenantID, latest, resetLagging); err != nil {
			return cursor, false
		}
		flusher.Flush()
		log.Info("stream reset after falling behind",
			slog.Int64("revision", cursor.Revision), slog.Int64("newest", newest))
		return latest, true
	}
}

// Refresh a bounded write deadline for each SSE frame instead of inheriting
// the server's absolute HTTP response deadline. The context bounds total life.
func streamWriteDeadline(w http.ResponseWriter, timeout time.Duration) error {
	err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(timeout))
	if errors.Is(err, http.ErrNotSupported) {
		return nil
	} // e.g. in-memory test recorder
	return err
}

// writeReset tells the client its position cannot be honoured and the stream
// continues from latest. The id moves the client's Last-Event-ID there too, so
// a reconnect after the reset does not repeat it.
func writeReset(w http.ResponseWriter, tenantID string, latest store.Cursor, reason string) error {
	payload, err := json.Marshal(map[string]any{"reason": reason, "revision": latest.Revision})
	if err != nil {
		return fmt.Errorf("encode reset event: %w", err)
	}
	_, err = fmt.Fprintf(w, "event: reset\nid: %s\ndata: %s\n\n", latest.Token(tenantID), payload) //nolint:gosec // G705: base64url token and JSON in text/event-stream
	return err
}

// writeEvent sends one rollup. Its id is the position immediately after the
// row, so a client that reconnects with it resumes with the next change.
func writeEvent(w http.ResponseWriter, tenantID string, r *store.StoredRollup) error {
	payload, err := json.Marshal(streamEvent{
		Kind:        r.Kind,
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
		return fmt.Errorf("encode stream event: %w", err)
	}

	id := store.Cursor{}.After(*r, r.Revision).Token(tenantID)
	_, err = fmt.Fprintf(w, "event: rollup\nid: %s\ndata: %s\n\n", id, payload)
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
