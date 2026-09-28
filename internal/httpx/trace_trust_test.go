package httpx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestPublicTraceCannotForceSamplingOrPropagateBaggage(t *testing.T) {
	previous := otel.GetTracerProvider()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.NeverSample())))
	otel.SetTracerProvider(provider)
	defer func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) }()
	for _, trusted := range []bool{false, true} {
		var recording bool
		var child trace.SpanContext
		var leaked string
		h := Trace(nil, TelemetryOptions{TrustTraceParent: trusted})(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			span := trace.SpanFromContext(r.Context())
			recording, child = span.IsRecording(), span.SpanContext()
			leaked = baggage.FromContext(r.Context()).Member("secret").Value()
		}))
		r := httptest.NewRequest("GET", "/", http.NoBody)
		r.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
		r.Header.Set("baggage", "secret=client-controlled")
		h.ServeHTTP(httptest.NewRecorder(), r)
		if recording != trusted {
			t.Fatalf("trusted=%v: recording=%v", trusted, recording)
		}
		if (child.TraceID().String() == "11111111111111111111111111111111") != trusted {
			t.Fatalf("trusted=%v: trace ID=%s", trusted, child.TraceID())
		}
		if leaked != "" {
			t.Fatal("HTTP baggage escaped the public boundary")
		}
	}
}
