package observability

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// withTracing installs a recording provider and the W3C propagator for the
// duration of a test.
func withTracing(t *testing.T) trace.Tracer {
	t.Helper()

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()))

	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
		_ = provider.Shutdown(context.Background())
	})

	return provider.Tracer("test")
}

// TestTraceSurvivesAMessageCarrier is the property that makes one trace span
// the broker: a consumer minutes later on another machine must join the
// producer's trace rather than starting an orphan.
func TestTraceSurvivesAMessageCarrier(t *testing.T) {
	tracer := withTracing(t)

	ctx, span := tracer.Start(context.Background(), "publish")
	wantTrace := span.SpanContext().TraceID()
	wantSpan := span.SpanContext().SpanID()

	// The publisher writes the context into the message attributes.
	attributes := MapCarrier{"tenant_id": "acme"}
	InjectTrace(ctx, attributes)
	span.End()

	if _, ok := attributes["traceparent"]; !ok {
		t.Fatalf("no traceparent was injected; attributes = %v", attributes)
	}
	// Injection must not disturb the message's own attributes.
	if attributes["tenant_id"] != "acme" {
		t.Errorf("injection clobbered an existing attribute: %v", attributes)
	}

	// The consumer, with no shared memory, reads it back out.
	consumerCtx := ExtractTrace(context.Background(), attributes)
	_, child := tracer.Start(consumerCtx, "consume")
	defer child.End()

	if got := child.SpanContext().TraceID(); got != wantTrace {
		t.Errorf("trace ID = %s, want %s; the trace did not survive the carrier",
			got, wantTrace)
	}
	// A child, not a sibling: the consumer's work belongs under the publish.
	if got := trace.SpanContextFromContext(consumerCtx).SpanID(); got != wantSpan {
		t.Errorf("parent span = %s, want %s", got, wantSpan)
	}
}

// TestTraceSurvivesHTTPHeaderCanonicalisation guards a bug that is silent and
// total: W3C names its header "traceparent" in lower case, Go canonicalises to
// "Traceparent", and a plain map lookup therefore misses it. Every
// cross-service trace would start a fresh root instead of continuing, with
// nothing in the logs to say so.
func TestTraceSurvivesHTTPHeaderCanonicalisation(t *testing.T) {
	tracer := withTracing(t)

	ctx, span := tracer.Start(context.Background(), "client")
	want := span.SpanContext().TraceID()

	header := http.Header{}
	InjectTrace(ctx, HeaderCarrier(header))
	span.End()

	// Go stored it canonicalised, which is exactly the trap.
	if _, canonical := header["Traceparent"]; !canonical {
		t.Fatalf("expected the header to be canonicalised; got %v", header)
	}

	serverCtx := ExtractTrace(context.Background(), HeaderCarrier(header))
	if got := trace.SpanContextFromContext(serverCtx).TraceID(); got != want {
		t.Errorf("trace ID = %s, want %s; canonicalisation broke extraction", got, want)
	}
}

func TestExtractWithNoTraceStartsFresh(t *testing.T) {
	tracer := withTracing(t)

	// A message published before tracing was enabled carries no context. The
	// consumer must still work, with a root span of its own.
	ctx := ExtractTrace(context.Background(), MapCarrier{"tenant_id": "acme"})
	_, span := tracer.Start(ctx, "consume")
	defer span.End()

	if !span.SpanContext().IsValid() {
		t.Error("no span was started for an untraced message")
	}
}

func TestExtractIgnoresAMalformedTraceparent(t *testing.T) {
	tracer := withTracing(t)

	// A corrupt header must not take the consumer down; it starts a new trace.
	ctx := ExtractTrace(context.Background(), MapCarrier{"traceparent": "garbage"})
	_, span := tracer.Start(ctx, "consume")
	defer span.End()

	if !span.SpanContext().IsValid() {
		t.Error("a malformed traceparent prevented any span from being started")
	}
}

func TestTraceIDFromContext(t *testing.T) {
	tracer := withTracing(t)

	if got := TraceIDFromContext(context.Background()); got != "" {
		t.Errorf("TraceIDFromContext with no span = %q, want empty", got)
	}

	ctx, span := tracer.Start(context.Background(), "work")
	defer span.End()

	got := TraceIDFromContext(ctx)
	if got != span.SpanContext().TraceID().String() {
		t.Errorf("TraceIDFromContext = %q, want the span's trace ID", got)
	}
	if len(got) != 32 {
		t.Errorf("trace ID = %q, want 32 hex characters", got)
	}
}

// TestLoggerWithTraceJoinsLogsToSpans: without this join an engineer holding a
// trace has to guess which logs relate to it, and one holding a log line cannot
// find the trace at all.
func TestLoggerWithTraceJoinsLogsToSpans(t *testing.T) {
	tracer := withTracing(t)

	ctx, span := tracer.Start(context.Background(), "work")
	defer span.End()

	if LoggerWithTrace(ctx) == nil {
		t.Fatal("LoggerWithTrace returned nil")
	}
	// It never returns nil, even with no span, so callers can log
	// unconditionally.
	if LoggerWithTrace(context.Background()) == nil {
		t.Error("LoggerWithTrace with no span returned nil")
	}
}

func TestMapCarrier(t *testing.T) {
	c := MapCarrier{"a": "1", "b": "2"}

	if c.Get("a") != "1" {
		t.Errorf("Get(a) = %q", c.Get("a"))
	}
	if c.Get("absent") != "" {
		t.Errorf("Get(absent) = %q, want empty", c.Get("absent"))
	}

	c.Set("c", "3")
	if c.Get("c") != "3" {
		t.Error("Set did not take effect")
	}

	keys := strings.Join(c.Keys(), ",")
	for _, want := range []string{"a", "b", "c"} {
		if !strings.Contains(keys, want) {
			t.Errorf("Keys() = %v, missing %s", c.Keys(), want)
		}
	}
}

// TestPropagatorIsInstalledEvenWhenTracingIsDisabled: a service that dropped
// the header because it is not itself sampling would silently break every trace
// passing through it, turning one unsampled hop into a permanently broken
// chain.
func TestPropagatorIsInstalledEvenWhenTracingIsDisabled(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })

	// Deliberately clear it, so the test proves InitTracing sets it rather
	// than inheriting whatever an earlier test left behind.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	tracing, err := InitTracing(context.Background(), testConfig(), TracingConfig{}, nil)
	if err != nil {
		t.Fatalf("InitTracing: %v", err)
	}
	t.Cleanup(func() { tracing.Shutdown(context.Background()) })

	fields := otel.GetTextMapPropagator().Fields()
	var sawTraceparent bool
	for _, f := range fields {
		if f == "traceparent" {
			sawTraceparent = true
		}
	}
	if !sawTraceparent {
		t.Errorf("propagator fields = %v, want traceparent even with tracing off", fields)
	}
}

func TestShutdownIsSafeWithoutAProvider(t *testing.T) {
	// A service that never enabled tracing still calls Shutdown on the way out.
	var tracing *Tracing
	tracing.Shutdown(context.Background())

	disabled, err := InitTracing(context.Background(), testConfig(), TracingConfig{}, nil)
	if err != nil {
		t.Fatalf("InitTracing: %v", err)
	}
	disabled.Shutdown(context.Background())
}
