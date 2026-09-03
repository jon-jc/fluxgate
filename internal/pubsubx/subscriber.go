package pubsubx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	pubsub "cloud.google.com/go/pubsub/v2"

	"github.com/jon-jc/fluxgate/internal/httpx"
	"github.com/jon-jc/fluxgate/internal/observability"
)

// Delivery is one message handed to a Handler.
type Delivery struct {
	// Envelope is the decoded payload.
	Envelope Envelope
	// Attributes are the message attributes as published.
	Attributes map[string]string
	// PublishTime is when Pub/Sub accepted the message.
	PublishTime time.Time
	// DeliveryAttempt is how many times this message has been delivered,
	// starting at 1. It is only populated on a subscription with a dead-letter
	// policy, which is the only situation where the count is actionable.
	DeliveryAttempt int
	// MessageID is the broker's identifier, for correlating with Pub/Sub's own
	// metrics and logs.
	MessageID string
}

// RequestID returns the correlation ID propagated from the HTTP edge, so a
// trace can continue across the queue.
func (d Delivery) RequestID() string { return d.Attributes[AttrRequestID] }

// Handler processes one delivery.
//
// Returning nil acknowledges the message. Returning an error nacks it for
// redelivery -- unless the error is marked with Permanent, in which case
// retrying is known to be futile.
type Handler func(ctx context.Context, d Delivery) error

// SubscriberOptions configures a Subscriber.
type SubscriberOptions struct {
	// Subscription is the subscription name or fully qualified path.
	Subscription string
	// MaxOutstandingMessages caps unacknowledged messages held in memory. It
	// is the primary throughput and memory dial.
	MaxOutstandingMessages int
	// MaxOutstandingBytes caps the memory held by unacknowledged messages.
	MaxOutstandingBytes int
	// NumGoroutines is how many streaming pull connections to open.
	NumGoroutines int
	// MaxExtension bounds how long the client keeps extending a message's ack
	// deadline before giving up and letting it be redelivered.
	MaxExtension time.Duration
	// HandlerTimeout bounds a single handler invocation. A handler that hangs
	// would otherwise occupy an outstanding-message slot indefinitely.
	HandlerTimeout time.Duration
	// Logger receives lifecycle events.
	Logger *slog.Logger
}

func (o *SubscriberOptions) applyDefaults() {
	if o.MaxOutstandingMessages == 0 {
		o.MaxOutstandingMessages = 1000
	}
	if o.MaxOutstandingBytes == 0 {
		o.MaxOutstandingBytes = 128 << 20
	}
	if o.NumGoroutines <= 0 {
		o.NumGoroutines = 1
	}
	if o.MaxExtension <= 0 {
		o.MaxExtension = 10 * time.Minute
	}
	if o.HandlerTimeout <= 0 {
		o.HandlerTimeout = 30 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Subscriber pulls messages from a subscription and runs a Handler for each.
type Subscriber struct {
	sub     *pubsub.Subscriber
	name    string
	handler Handler
	timeout time.Duration
	log     *slog.Logger
}

// NewSubscriber builds a Subscriber over an existing client.
func NewSubscriber(client *pubsub.Client, handler Handler, opts SubscriberOptions) (*Subscriber, error) {
	switch {
	case client == nil:
		return nil, errors.New("pubsub: client is required")
	case opts.Subscription == "":
		return nil, errors.New("pubsub: subscription is required")
	case handler == nil:
		return nil, errors.New("pubsub: handler is required")
	}
	opts.applyDefaults()

	sub := client.Subscriber(opts.Subscription)
	sub.ReceiveSettings = pubsub.ReceiveSettings{
		MaxOutstandingMessages: opts.MaxOutstandingMessages,
		MaxOutstandingBytes:    opts.MaxOutstandingBytes,
		NumGoroutines:          opts.NumGoroutines,
		MaxExtension:           opts.MaxExtension,
	}

	return &Subscriber{
		sub:     sub,
		name:    opts.Subscription,
		handler: handler,
		timeout: opts.HandlerTimeout,
		log:     opts.Logger,
	}, nil
}

// Run consumes until ctx is cancelled.
//
// Receive dispatches messages concurrently up to the outstanding-message
// limit, so the handler must be safe for concurrent use. Cancelling ctx stops
// new deliveries and waits for in-flight handlers to finish, which is what
// makes shutdown lose nothing: an unacknowledged message is simply redelivered
// to whichever instance is still running.
func (s *Subscriber) Run(ctx context.Context) error {
	s.log.Info("subscriber starting", slog.String("subscription", s.name))

	err := s.sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		s.dispatch(ctx, msg)
	})

	// A cancelled context is the expected way to stop, not a failure.
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("receive on %s: %w", s.name, err)
	}

	s.log.Info("subscriber stopped", slog.String("subscription", s.name))
	return nil
}

func (s *Subscriber) dispatch(ctx context.Context, msg *pubsub.Message) {
	// Re-attach the correlation ID published at the HTTP edge so a log line
	// here can be joined to the request that produced the data, however many
	// minutes and machines ago that was.
	if requestID := msg.Attributes[AttrRequestID]; requestID != "" {
		ctx = httpx.ContextWithRequestID(ctx, requestID)
		ctx = observability.ContextWithLogger(ctx,
			observability.LoggerFromContext(ctx).With(
				slog.String(observability.KeyRequestID, requestID)))
	}

	log := observability.LoggerFromContext(ctx).With(
		slog.String("message_id", msg.ID),
		slog.String("subscription", s.name))

	envelope, err := DecodeEnvelope(msg.Data)
	if err != nil {
		s.reject(log, msg, err, "envelope could not be decoded")
		return
	}

	log = log.With(
		slog.String("batch_id", envelope.BatchID),
		slog.String(observability.KeyTenantID, envelope.TenantID))
	ctx = observability.ContextWithLogger(ctx, log)

	delivery := Delivery{
		Envelope:        envelope,
		Attributes:      msg.Attributes,
		PublishTime:     msg.PublishTime,
		DeliveryAttempt: deliveryAttempt(msg),
		MessageID:       msg.ID,
	}

	handlerCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	if err := s.invoke(handlerCtx, delivery); err != nil {
		if IsPermanent(err) {
			s.reject(log, msg, err, "handler reported a permanent failure")
			return
		}

		// Transient: nack so the message comes back promptly rather than
		// waiting out its ack deadline.
		log.Warn("handler failed; message will be redelivered",
			slog.Any("error", err),
			slog.Int("delivery_attempt", delivery.DeliveryAttempt))
		msg.Nack()
		return
	}

	msg.Ack()
}

// invoke runs the handler, converting a panic into a permanent failure.
//
// A panicking handler on a Receive goroutine would otherwise take down the
// whole process, and the message that caused it would be redelivered to the
// replacement -- a crash loop driven by one poisoned payload.
func (s *Subscriber) invoke(ctx context.Context, d Delivery) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = Permanent(fmt.Errorf("handler panicked: %v", rec))
		}
	}()
	return s.handler(ctx, d)
}

// reject nacks a message that cannot succeed, so the subscription's
// dead-letter policy captures it after the configured number of attempts.
//
// Nacking rather than acking is deliberate: acking would discard the payload
// silently, leaving nothing to diagnose. The dead-letter queue is where a
// poisoned message belongs -- preserved, out of the way, and inspectable.
// A subscription without a dead-letter policy will redeliver it forever, which
// is why the bootstrap in this package always attaches one.
func (s *Subscriber) reject(log *slog.Logger, msg *pubsub.Message, cause error, reason string) {
	log.Error("rejecting message to the dead-letter queue",
		slog.String("reason", reason),
		slog.Any("error", cause),
		slog.Int("delivery_attempt", deliveryAttempt(msg)),
		slog.Int("bytes", len(msg.Data)))
	msg.Nack()
}

func deliveryAttempt(msg *pubsub.Message) int {
	if msg.DeliveryAttempt == nil {
		return 0
	}
	return *msg.DeliveryAttempt
}
