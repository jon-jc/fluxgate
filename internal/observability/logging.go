// Package observability wires the cross-cutting concerns every Fluxgate
// process needs: structured logging, liveness and readiness reporting, and
// (from the telemetry milestone onward) traces and metrics.
package observability

import (
	"context"
	"io"
	"log/slog"
	"strings"

	"github.com/jon-jc/fluxgate/internal/config"
	"github.com/jon-jc/fluxgate/internal/version"
)

// Keys used consistently across every service so that a single log query can
// follow a request from the edge through to a Pub/Sub consumer.
const (
	KeyRequestID = "request_id"
	KeyTraceID   = "trace_id"
	KeyTenantID  = "tenant_id"
	KeyService   = "service"
)

// NewLogger builds the process-wide logger.
//
// Production tiers emit JSON, which Cloud Logging parses into structured
// fields; local development gets human-readable text. Every record carries the
// service name and build revision so logs from a canary can be told apart from
// logs from the stable revision without deploying anything extra.
func NewLogger(w io.Writer, cfg config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:       parseLevel(cfg.Log.Level),
		AddSource:   cfg.Log.AddSource,
		ReplaceAttr: replaceAttr,
	}

	var h slog.Handler
	if cfg.Log.Format == "text" {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}

	return slog.New(h).With(
		slog.String(KeyService, cfg.Service),
		slog.String("env", string(cfg.Environment)),
		slog.String("version", version.Get().Version),
		slog.String("commit", shortCommit(version.Get().Commit)),
	)
}

func shortCommit(c string) string {
	if len(c) > 12 {
		return c[:12]
	}
	return c
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// replaceAttr renames slog's defaults to the field names Cloud Logging
// promotes automatically, so severity-based filtering and alerting work
// without a log-router transformation in between.
func replaceAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) > 0 {
		return a
	}
	switch a.Key {
	case slog.LevelKey:
		a.Key = "severity"
		if lvl, ok := a.Value.Any().(slog.Level); ok {
			a.Value = slog.StringValue(severity(lvl))
		}
	case slog.MessageKey:
		a.Key = "message"
	case slog.TimeKey:
		a.Key = "timestamp"
	}
	return a
}

// severity maps slog levels onto the names Cloud Logging recognises.
func severity(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return "ERROR"
	case l >= slog.LevelWarn:
		return "WARNING"
	case l >= slog.LevelInfo:
		return "INFO"
	default:
		return "DEBUG"
	}
}

// loggerKey is the context key for a request-scoped logger. It is an unexported
// zero-size type so no other package can collide with it.
type loggerKey struct{}

// ContextWithLogger returns a copy of ctx carrying the given logger. Middleware
// uses it to attach per-request fields once, so downstream code logs them
// without having to thread them through every call signature.
func ContextWithLogger(ctx context.Context, l *slog.Logger) context.Context {
	if l == nil {
		return ctx
	}
	return context.WithValue(ctx, loggerKey{}, l)
}

// LoggerFromContext returns the request-scoped logger, or the default logger
// when none was attached. It never returns nil, so callers can log
// unconditionally.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}
