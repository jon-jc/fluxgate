package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/jon-jc/fluxgate/internal/config"
	"github.com/jon-jc/fluxgate/internal/version"
)

// TracingConfig configures the tracer provider.
type TracingConfig struct {
	// Enabled turns tracing on. When false, a no-op tracer is installed and
	// every instrumentation call becomes close to free.
	Enabled bool
	// Endpoint is the OTLP gRPC collector address, e.g. "localhost:4317".
	Endpoint string
	// Insecure disables transport security, which is appropriate for a
	// collector reached over a loopback or a private mesh and nowhere else.
	Insecure bool
	// SampleRatio is the fraction of traces recorded, between 0 and 1.
	SampleRatio float64
	// ExportTimeout bounds a single batch export.
	ExportTimeout time.Duration
}

// Tracing is an installed tracer provider.
type Tracing struct {
	provider *sdktrace.TracerProvider
	log      *slog.Logger
}

// InitTracing installs a global tracer provider and propagator.
//
// The propagator is set even when tracing is disabled. Context propagation is
// how a downstream service joins a trace, and a service that drops the header
// because it is not itself sampling would silently break every trace passing
// through it -- turning one unsampled hop into a permanently broken chain.
func InitTracing(ctx context.Context, cfg config.Config, tracing TracingConfig, log *slog.Logger) (*Tracing, error) {
	if log == nil {
		log = slog.Default()
	}

	// W3C trace context, plus baggage for the few attributes worth carrying
	// across a service boundary.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Instrumentation errors must never become application errors. A collector
	// that is down should cost visibility, not availability.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		log.Warn("opentelemetry error", slog.Any("error", err))
	}))

	if !tracing.Enabled || tracing.Endpoint == "" {
		log.Info("tracing disabled")
		return &Tracing{log: log}, nil
	}

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(tracing.Endpoint),
		otlptracegrpc.WithTimeout(tracing.ExportTimeout),
	}
	if tracing.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("tracing: create OTLP exporter: %w", err)
	}

	// Schemaless rather than pinned to a semconv URL. resource.Merge refuses to
	// combine two resources whose schema URLs differ, so pinning one here
	// breaks the build every time the SDK moves to a newer semconv -- a
	// versioning detail that has nothing to do with what this service wants to
	// say about itself.
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(cfg.Service),
		semconv.ServiceVersion(version.Get().Version),
		semconv.DeploymentEnvironmentNameKey.String(string(cfg.Environment)),
		attribute.String("service.commit", version.Get().Commit),
	))
	if err != nil {
		return nil, fmt.Errorf("tracing: build resource: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			// Batching bounds the cost of export. A synchronous exporter would
			// put the collector's latency on the request path, which is
			// exactly backwards: telemetry must not be able to slow the thing
			// it observes.
			sdktrace.WithBatchTimeout(5*time.Second),
			sdktrace.WithMaxExportBatchSize(512),
		),
		sdktrace.WithResource(res),
		// ParentBased keeps a trace intact across services: once the edge has
		// decided to sample a request, every downstream hop honours that
		// decision rather than re-rolling the dice and producing a trace with
		// holes in it.
		sdktrace.WithSampler(sdktrace.ParentBased(
			sdktrace.TraceIDRatioBased(tracing.SampleRatio))),
	)
	otel.SetTracerProvider(provider)

	log.Info("tracing enabled",
		slog.String("endpoint", tracing.Endpoint),
		slog.Float64("sample_ratio", tracing.SampleRatio))

	return &Tracing{provider: provider, log: log}, nil
}

// Shutdown flushes pending spans.
//
// It runs on a fresh deadline rather than the cancelled shutdown context: the
// spans describing why the process is shutting down are the ones most worth
// keeping, and inheriting the cancellation would discard them.
func (t *Tracing) Shutdown(ctx context.Context) {
	if t == nil || t.provider == nil {
		return
	}

	flushCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := t.provider.Shutdown(flushCtx); err != nil && !errors.Is(err, context.Canceled) {
		t.log.Warn("flushing traces", slog.Any("error", err))
	}
}

// Tracer returns a named tracer from the global provider.
func Tracer(name string) trace.Tracer { return otel.Tracer(name) }

// TraceIDFromContext returns the current trace ID, or "" when the span is not
// recording.
//
// It exists so a log line can carry the trace it belongs to. Without that
// join, an engineer holding a trace has to guess at which logs relate to it,
// and an engineer holding a log line cannot find the trace at all.
func TraceIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// LoggerWithTrace returns a logger carrying the current trace and span IDs.
func LoggerWithTrace(ctx context.Context) *slog.Logger {
	log := LoggerFromContext(ctx)

	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return log
	}
	return log.With(
		slog.String(KeyTraceID, sc.TraceID().String()),
		slog.String("span_id", sc.SpanID().String()),
	)
}

// MapCarrier adapts a plain string map to the OpenTelemetry propagation
// interface.
//
// Pub/Sub message attributes are exactly such a map, which is what lets a trace
// continue across the broker: the publisher writes the trace context into the
// attributes, and the consumer reads it back out minutes later on another
// machine.
type MapCarrier map[string]string

// Get implements propagation.TextMapCarrier.
func (c MapCarrier) Get(key string) string { return c[key] }

// Set implements propagation.TextMapCarrier.
func (c MapCarrier) Set(key, value string) { c[key] = value }

// Keys implements propagation.TextMapCarrier.
func (c MapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// InjectTrace writes the current trace context into a carrier.
func InjectTrace(ctx context.Context, carrier propagation.TextMapCarrier) {
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// ExtractTrace reads a trace context out of a carrier, returning a context
// linked to the upstream trace.
func ExtractTrace(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// HeaderCarrier adapts HTTP headers for trace propagation.
//
// It is not the same as MapCarrier over the header map. W3C trace context
// names its header "traceparent" in lower case, while Go canonicalises header
// keys to "Traceparent" -- so a plain map lookup misses it, silently, and every
// cross-service trace starts a fresh root instead of continuing. Going through
// http.Header's own accessors makes the lookup case-insensitive, as HTTP
// requires.
func HeaderCarrier(h http.Header) propagation.TextMapCarrier {
	return propagation.HeaderCarrier(h)
}
