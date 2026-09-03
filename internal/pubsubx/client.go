package pubsubx

import (
	"context"
	"fmt"
	"os"
	"strings"

	pubsub "cloud.google.com/go/pubsub/v2"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// EmulatorEnvVar is the variable the Google client libraries look at to decide
// whether to talk to a local emulator instead of the real service.
const EmulatorEnvVar = "PUBSUB_EMULATOR_HOST"

// Config describes how to reach Pub/Sub.
type Config struct {
	// ProjectID is the GCP project owning the topics and subscriptions.
	ProjectID string
	// EmulatorHost points at a local emulator. When empty, the value of
	// PUBSUB_EMULATOR_HOST is used, and failing that the real service.
	EmulatorHost string
}

// EmulatorTarget returns the emulator address in effect, or "" when the client
// will talk to the real service.
func (c Config) EmulatorTarget() string {
	if c.EmulatorHost != "" {
		return c.EmulatorHost
	}
	return os.Getenv(EmulatorEnvVar)
}

// NewClient connects to Pub/Sub.
//
// Against the emulator the connection is explicitly insecure and carries no
// credentials. That is configured here rather than left to the library's own
// environment sniffing so the choice is visible in code review: an unencrypted,
// unauthenticated connection is exactly right for a container on localhost and
// exactly wrong for anything else, and it should be obvious which one is in
// play.
func NewClient(ctx context.Context, cfg Config) (*pubsub.Client, error) {
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("pubsub: project ID is required")
	}

	if target := cfg.EmulatorTarget(); target != "" {
		return pubsub.NewClient(ctx, cfg.ProjectID,
			option.WithEndpoint(target),
			option.WithoutAuthentication(),
			option.WithGRPCDialOption(
				grpc.WithTransportCredentials(insecure.NewCredentials())),
		)
	}

	client, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub: connect to project %s: %w", cfg.ProjectID, err)
	}
	return client, nil
}

// TopicPath returns the fully qualified name of a topic.
func TopicPath(projectID, topic string) string {
	if strings.HasPrefix(topic, "projects/") {
		return topic
	}
	return fmt.Sprintf("projects/%s/topics/%s", projectID, topic)
}

// SubscriptionPath returns the fully qualified name of a subscription.
func SubscriptionPath(projectID, subscription string) string {
	if strings.HasPrefix(subscription, "projects/") {
		return subscription
	}
	return fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subscription)
}
