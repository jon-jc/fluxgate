package pubsubx

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pubsub "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Topology describes the topics and subscriptions a deployment needs.
type Topology struct {
	// ProjectID owns the resources.
	ProjectID string
	// Topics are created if absent.
	Topics []string
	// Subscriptions are created if absent.
	Subscriptions []SubscriptionSpec
}

// SubscriptionSpec describes one subscription.
type SubscriptionSpec struct {
	// Name is the subscription's short name.
	Name string
	// Topic is the topic it consumes.
	Topic string
	// AckDeadline is how long a consumer has to acknowledge before the message
	// is redelivered. The client extends this automatically while a handler
	// runs, so it need not cover the slowest possible handler.
	AckDeadline time.Duration
	// RetentionDuration is how long unacknowledged messages are kept.
	RetentionDuration time.Duration
	// DeadLetterTopic receives messages that exhaust MaxDeliveryAttempts.
	// Leaving it empty means a poisoned message is redelivered forever.
	DeadLetterTopic string
	// MaxDeliveryAttempts is how many times a message is redelivered before it
	// is dead-lettered.
	MaxDeliveryAttempts int
	// MinBackoff and MaxBackoff bound the exponential redelivery delay.
	MinBackoff time.Duration
	MaxBackoff time.Duration
	// Filter is a Pub/Sub filter expression over message attributes.
	Filter string
}

// Ensure creates any missing topics and subscriptions, and is safe to run
// repeatedly.
//
// This exists for local development and integration tests, where a fresh
// emulator starts with nothing. Deployed environments get their topology from
// Terraform instead: a service that can create its own infrastructure needs
// admin permissions at runtime, which is a much larger blast radius than a
// service that can only publish and subscribe.
func Ensure(ctx context.Context, client *pubsub.Client, topo Topology, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}

	for _, topic := range topo.Topics {
		if err := ensureTopic(ctx, client, topo.ProjectID, topic, log); err != nil {
			return err
		}
	}

	for _, spec := range topo.Subscriptions {
		if err := ensureSubscription(ctx, client, topo.ProjectID, spec, log); err != nil {
			return err
		}
	}
	return nil
}

func ensureTopic(ctx context.Context, client *pubsub.Client, projectID, name string, log *slog.Logger) error {
	path := TopicPath(projectID, name)

	_, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: path})
	switch {
	case err == nil:
		log.Info("created topic", slog.String("topic", path))
		return nil
	case isAlreadyExists(err):
		// Concurrent instances racing to bootstrap is the normal case, not an
		// error: whichever lost the race still ends up with the topic it needs.
		return nil
	default:
		return fmt.Errorf("create topic %s: %w", path, err)
	}
}

func ensureSubscription(
	ctx context.Context, client *pubsub.Client, projectID string,
	spec SubscriptionSpec, log *slog.Logger,
) error {
	path := SubscriptionPath(projectID, spec.Name)

	sub := &pubsubpb.Subscription{
		Name:   path,
		Topic:  TopicPath(projectID, spec.Topic),
		Filter: spec.Filter,
	}

	if spec.AckDeadline > 0 {
		sub.AckDeadlineSeconds = clampAckDeadline(spec.AckDeadline)
	}
	if spec.RetentionDuration > 0 {
		sub.MessageRetentionDuration = durationpb.New(spec.RetentionDuration)
	}

	if spec.DeadLetterTopic != "" {
		sub.DeadLetterPolicy = &pubsubpb.DeadLetterPolicy{
			DeadLetterTopic:     TopicPath(projectID, spec.DeadLetterTopic),
			MaxDeliveryAttempts: clampDeliveryAttempts(spec.MaxDeliveryAttempts),
		}
	}

	if spec.MinBackoff > 0 || spec.MaxBackoff > 0 {
		// Without an explicit retry policy, a nacked message comes straight
		// back. A consumer failing because its database is down would then
		// hammer that database as fast as it can fail, turning a brief outage
		// into a sustained one.
		retry := &pubsubpb.RetryPolicy{}
		if spec.MinBackoff > 0 {
			retry.MinimumBackoff = durationpb.New(spec.MinBackoff)
		}
		if spec.MaxBackoff > 0 {
			retry.MaximumBackoff = durationpb.New(spec.MaxBackoff)
		}
		sub.RetryPolicy = retry
	}

	_, err := client.SubscriptionAdminClient.CreateSubscription(ctx, sub)
	switch {
	case err == nil:
		log.Info("created subscription",
			slog.String("subscription", path),
			slog.String("topic", sub.GetTopic()),
			slog.String("dead_letter_topic", sub.GetDeadLetterPolicy().GetDeadLetterTopic()))
		return nil
	case isAlreadyExists(err):
		return nil
	default:
		return fmt.Errorf("create subscription %s: %w", path, err)
	}
}

func isAlreadyExists(err error) bool {
	return status.Code(err) == codes.AlreadyExists
}

// DefaultTopology returns the standard Fluxgate topology for a project.
//
// Every working subscription is paired with a dead-letter topic. A poisoned
// message on a subscription without one is redelivered indefinitely: it blocks
// nothing by itself, but it burns quota forever and buries real failures in
// the logs.
func DefaultTopology(projectID, rawTopic, dlqTopic, aggregatorSub string) Topology {
	return Topology{
		ProjectID: projectID,
		Topics:    []string{rawTopic, dlqTopic},
		Subscriptions: []SubscriptionSpec{
			{
				Name:  aggregatorSub,
				Topic: rawTopic,
				// Long enough for a slow flush, short enough that a crashed
				// consumer's messages come back promptly.
				AckDeadline:       60 * time.Second,
				RetentionDuration: 24 * time.Hour,
				DeadLetterTopic:   dlqTopic,
				// Five attempts distinguishes a transient blip from a genuinely
				// undeliverable message without retrying a hopeless one for
				// hours.
				MaxDeliveryAttempts: 5,
				MinBackoff:          time.Second,
				MaxBackoff:          time.Minute,
			},
			{
				// The dead-letter queue gets its own subscription so that
				// messages there can be inspected and replayed, rather than
				// silently ageing out of retention unexamined.
				Name:              dlqTopic + "-inspect",
				Topic:             dlqTopic,
				AckDeadline:       60 * time.Second,
				RetentionDuration: 7 * 24 * time.Hour,
			},
		},
	}
}

// Pub/Sub's own bounds. Values outside these ranges are rejected by the API
// with an OutOfRange error at subscription creation, which is a poor place to
// discover a limit -- so a caller's intent is clamped into range here rather
// than failing a deployment over it. Clamping to int32 also keeps the
// conversion to the protobuf field provably in range.
const (
	minDeliveryAttempts int32 = 5
	maxDeliveryAttempts int32 = 100

	minAckDeadlineSeconds int32 = 10
	maxAckDeadlineSeconds int32 = 600
)

func clampDeliveryAttempts(attempts int) int32 {
	switch {
	case attempts < int(minDeliveryAttempts):
		return minDeliveryAttempts
	case attempts > int(maxDeliveryAttempts):
		return maxDeliveryAttempts
	default:
		return int32(attempts)
	}
}

func clampAckDeadline(d time.Duration) int32 {
	seconds := d.Round(time.Second).Seconds()
	switch {
	case seconds < float64(minAckDeadlineSeconds):
		return minAckDeadlineSeconds
	case seconds > float64(maxAckDeadlineSeconds):
		return maxAckDeadlineSeconds
	default:
		return int32(seconds)
	}
}
