package pubsubx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	pubsub "cloud.google.com/go/pubsub/v2"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/jon-jc/fluxgate/internal/httpx"
	"github.com/jon-jc/fluxgate/internal/observability"
	"github.com/jon-jc/fluxgate/internal/resilience"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

// PublisherOptions configures a Publisher.
type PublisherOptions struct {
	// Topic is the topic name or fully qualified path to publish to.
	Topic string
	// PublishTimeout bounds a single publish, including retries inside the
	// client.
	PublishTimeout time.Duration
	// BatchDelay is how long the client waits to accumulate a batch.
	BatchDelay time.Duration
	// BatchCount is how many messages accumulate before a batch is sent.
	BatchCount int
	// BatchBytes is the size at which a batch is sent.
	BatchBytes int
	// MaxOutstandingMessages caps messages buffered awaiting publish.
	MaxOutstandingMessages int
	// MaxOutstandingBytes caps the memory held by buffered messages.
	MaxOutstandingBytes int
	// Breaker configures failing fast when Pub/Sub looks unhealthy.
	Breaker resilience.Options
	// Metrics records publish outcomes. Optional; a nil value disables
	// instrumentation rather than panicking.
	Metrics *observability.Metrics
	// Logger receives lifecycle events.
	Logger *slog.Logger
}

func (o *PublisherOptions) applyDefaults() {
	if o.PublishTimeout <= 0 {
		o.PublishTimeout = 10 * time.Second
	}
	if o.BatchDelay <= 0 {
		o.BatchDelay = 10 * time.Millisecond
	}
	if o.BatchCount <= 0 {
		o.BatchCount = 100
	}
	if o.BatchBytes <= 0 {
		o.BatchBytes = 1 << 20
	}
	if o.MaxOutstandingMessages <= 0 {
		o.MaxOutstandingMessages = 1000
	}
	if o.MaxOutstandingBytes <= 0 {
		o.MaxOutstandingBytes = 64 << 20
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Publisher delivers batches to a Pub/Sub topic.
//
// It implements ingest.Sink, so the HTTP handler is unaware it is talking to a
// message broker at all.
//
// Publish waits for the broker to acknowledge the message before returning.
// The alternative -- accepting into an in-process buffer and replying 202
// immediately -- is faster and quietly dishonest: it reports success for data
// that a crash, a deploy or an OOM kill would silently discard. Waiting means a
// 202 is a real durability guarantee, and the client-side batching below is
// what keeps that from costing throughput.
type Publisher struct {
	pub     *pubsub.Publisher
	topic   string
	timeout time.Duration
	breaker *resilience.Breaker
	metrics *observability.Metrics
	log     *slog.Logger
}

// NewPublisher builds a Publisher over an existing client.
func NewPublisher(client *pubsub.Client, opts PublisherOptions) (*Publisher, error) {
	if client == nil {
		return nil, errors.New("pubsub: client is required")
	}
	if opts.Topic == "" {
		return nil, errors.New("pubsub: topic is required")
	}
	opts.applyDefaults()

	pub := client.Publisher(opts.Topic)
	pub.PublishSettings = pubsub.PublishSettings{
		DelayThreshold: opts.BatchDelay,
		CountThreshold: opts.BatchCount,
		ByteThreshold:  opts.BatchBytes,
		Timeout:        opts.PublishTimeout,
		FlowControlSettings: pubsub.FlowControlSettings{
			MaxOutstandingMessages: opts.MaxOutstandingMessages,
			MaxOutstandingBytes:    opts.MaxOutstandingBytes,
			// SignalError rather than Block: a blocked publish holds an HTTP
			// connection open with no upper bound, so a slow broker silently
			// converts into an exhausted server. Shedding the request lets the
			// client retry against an instance that has capacity.
			LimitExceededBehavior: pubsub.FlowControlSignalError,
		},
		// Compression pays for itself on batches of repetitive JSON, which is
		// exactly what a telemetry envelope is.
		EnableCompression:         true,
		CompressionBytesThreshold: 4 << 10,
	}

	// Ordering is deliberately left off. It would pin publishing to a single
	// region and serialise delivery per key, and it buys nothing here: the
	// aggregations downstream are sum, count, min and max, all of which are
	// commutative. Paying for ordering that no consumer needs is a throughput
	// ceiling in exchange for nothing.
	pub.EnableMessageOrdering = false

	return &Publisher{
		pub:     pub,
		topic:   opts.Topic,
		timeout: opts.PublishTimeout,
		breaker: resilience.New(opts.Breaker),
		metrics: opts.Metrics,
		log:     opts.Logger,
	}, nil
}

// tracerName identifies this instrumentation in the collected spans.
const tracerName = "github.com/jon-jc/fluxgate/internal/pubsubx"

// Publish implements ingest.Sink.
func (p *Publisher) Publish(ctx context.Context, batch telemetry.Batch) error {
	ctx, span := observability.Tracer(tracerName).Start(ctx, "publish "+p.topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "gcp_pubsub"),
			attribute.String("messaging.destination.name", p.topic),
			attribute.String("fluxgate.batch_id", batch.ID),
			attribute.String("fluxgate.tenant_id", batch.TenantID),
			attribute.Int("fluxgate.points", len(batch.Points)),
		))
	defer span.End()

	started := time.Now()

	token, err := p.breaker.Allow()
	if err != nil {
		// Failing fast here is the whole point: without it, every request
		// waits out the full publish timeout while holding a connection, and
		// a broker outage becomes an exhausted server.
		span.SetStatus(codes.Error, "circuit breaker open")
		p.metrics.ObservePublish("breaker_open", time.Since(started))
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}

	envelope := NewEnvelope(batch)
	data, err := envelope.Encode()
	if err != nil {
		// An unencodable envelope is our own bug, not the broker's; it must
		// not count against the dependency's health.
		token.Success()
		return err
	}

	msg := &pubsub.Message{
		Data:       data,
		Attributes: envelope.Attributes(httpx.RequestIDFromContext(ctx), time.Now()),
	}

	// Write the trace context into the message attributes. This is what lets
	// one trace span the broker: the consumer reads it back out minutes later,
	// on another machine, and its work appears as a child of the request that
	// produced the data rather than as an unattributable orphan.
	observability.InjectTrace(ctx, observability.MapCarrier(msg.Attributes))

	// Bound the wait independently of the caller's deadline so a client with a
	// generous timeout cannot pin a publish slot indefinitely.
	publishCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	result := p.pub.Publish(publishCtx, msg)

	serverID, err := result.Get(publishCtx)
	if err != nil {
		token.Failure()
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")

		if errors.Is(err, pubsub.ErrFlowControllerMaxOutstandingMessages) ||
			errors.Is(err, pubsub.ErrFlowControllerMaxOutstandingBytes) {
			p.metrics.ObservePublish("overloaded", time.Since(started))
			return fmt.Errorf("%w: %w", ErrOverloaded, err)
		}
		p.metrics.ObservePublish("error", time.Since(started))
		return wrapPublishError(p.topic, err)
	}
	token.Success()

	span.SetAttributes(attribute.String("messaging.message.id", serverID))
	p.metrics.ObservePublish("ok", time.Since(started))
	p.metrics.SetBreakerState("pubsub-publisher", int(p.breaker.State()))

	observability.LoggerFromContext(ctx).Debug("batch published",
		slog.String("batch_id", batch.ID),
		slog.String("message_id", serverID),
		slog.Int("points", len(batch.Points)))

	return nil
}

// Check reports whether the publisher believes Pub/Sub is usable. It is
// registered as a readiness check.
//
// It reads the breaker rather than making a call of its own: an active probe
// on every scrape adds load to a dependency that may already be struggling,
// and the breaker's view is built from real traffic, which is a better signal
// than a synthetic one.
func (p *Publisher) Check(context.Context) error {
	if state := p.breaker.State(); state == resilience.StateOpen {
		return fmt.Errorf("publish circuit breaker is %s", state)
	}
	return nil
}

// Name implements observability.Checker.
func (p *Publisher) Name() string { return "pubsub-publisher" }

// Close flushes buffered messages and releases the publisher's goroutines.
//
// It is called during drain, after the listener has stopped accepting work, so
// that a batch accepted a moment before shutdown is not lost with the process.
func (p *Publisher) Close() {
	p.log.Info("flushing publisher", slog.String("topic", p.topic))
	p.pub.Stop()
}
