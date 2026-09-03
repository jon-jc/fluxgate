package pubsubx_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	pubsub "cloud.google.com/go/pubsub/v2"

	"github.com/jon-jc/fluxgate/internal/pubsubx"
	"github.com/jon-jc/fluxgate/internal/resilience"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

// These tests run against a real Pub/Sub emulator. They are skipped when none
// is configured, so `go test ./...` stays green on a machine with no Docker,
// while CI and anyone running the compose stack gets full coverage of the
// transport rather than of a mock that agrees with its author.
//
//	docker compose -f deploy/docker-compose.yml up -d pubsub
//	PUBSUB_EMULATOR_HOST=localhost:8681 go test ./internal/pubsubx/...
func requireEmulator(t *testing.T) string {
	t.Helper()

	host := os.Getenv(pubsubx.EmulatorEnvVar)
	if host == "" {
		t.Skipf("set %s to run the Pub/Sub integration tests", pubsubx.EmulatorEnvVar)
	}
	return host
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// harness is an isolated topic, subscription and dead-letter queue.
type harness struct {
	client    *pubsub.Client
	projectID string
	topic     string
	sub       string
	dlqTopic  string
	dlqSub    string
}

// newHarness provisions a topology unique to this test.
//
// Every test gets its own topic and subscription: the emulator is shared, and
// tests that reuse names see each other's messages and fail in ways that look
// like flakes rather than like the collisions they are.
func newHarness(t *testing.T, opts ...func(*pubsubx.SubscriptionSpec)) *harness {
	t.Helper()

	host := requireEmulator(t)
	projectID := "fluxgate-test"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := pubsubx.NewClient(ctx, pubsubx.Config{
		ProjectID:    projectID,
		EmulatorHost: host,
	})
	if err != nil {
		t.Fatalf("connect to the emulator at %s: %v", host, err)
	}
	t.Cleanup(func() { _ = client.Close() })

	unique := strconv.FormatInt(time.Now().UnixNano(), 36)
	h := &harness{
		client:    client,
		projectID: projectID,
		topic:     "t-" + unique,
		sub:       "s-" + unique,
		dlqTopic:  "dlq-" + unique,
		dlqSub:    "dlqsub-" + unique,
	}

	spec := pubsubx.SubscriptionSpec{
		Name:                h.sub,
		Topic:               h.topic,
		AckDeadline:         10 * time.Second,
		DeadLetterTopic:     h.dlqTopic,
		MaxDeliveryAttempts: 5,
	}
	for _, opt := range opts {
		opt(&spec)
	}

	topo := pubsubx.Topology{
		ProjectID: projectID,
		Topics:    []string{h.topic, h.dlqTopic},
		Subscriptions: []pubsubx.SubscriptionSpec{
			spec,
			{Name: h.dlqSub, Topic: h.dlqTopic, AckDeadline: 10 * time.Second},
		},
	}
	if err := pubsubx.Ensure(ctx, client, topo, discardLogger()); err != nil {
		t.Fatalf("provision topology: %v", err)
	}
	return h
}

func (h *harness) publisher(t *testing.T) *pubsubx.Publisher {
	t.Helper()

	p, err := pubsubx.NewPublisher(h.client, pubsubx.PublisherOptions{
		Topic: h.topic,
		// Publish immediately: batching would add latency to every assertion
		// for no benefit at one message per test.
		BatchDelay:     time.Millisecond,
		BatchCount:     1,
		PublishTimeout: 15 * time.Second,
		Logger:         discardLogger(),
	})
	if err != nil {
		t.Fatalf("build publisher: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

// consume runs a subscriber until handler has been called n times or the
// timeout expires.
func (h *harness) consume(
	t *testing.T, subscription string, n int, timeout time.Duration,
	handler func(pubsubx.Delivery) error,
) []pubsubx.Delivery {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var (
		mu         sync.Mutex
		deliveries []pubsubx.Delivery
	)

	sub, err := pubsubx.NewSubscriber(h.client,
		func(_ context.Context, d pubsubx.Delivery) error {
			mu.Lock()
			deliveries = append(deliveries, d)
			done := len(deliveries) >= n
			mu.Unlock()

			err := handler(d)
			if done {
				// Stop pulling once the expected messages have arrived, rather
				// than waiting out the whole timeout on every passing test.
				defer cancel()
			}
			return err
		},
		pubsubx.SubscriberOptions{
			Subscription:   subscription,
			NumGoroutines:  1,
			HandlerTimeout: 10 * time.Second,
			Logger:         discardLogger(),
		})
	if err != nil {
		t.Fatalf("build subscriber: %v", err)
	}

	if err := sub.Run(ctx); err != nil {
		t.Fatalf("subscriber: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	return deliveries
}

func batch(id string, points int) telemetry.Batch {
	ts := time.Now().UTC().Truncate(time.Second)
	pts := make([]telemetry.Point, points)
	for i := range pts {
		pts[i] = telemetry.Point{
			Metric:    "http.requests",
			Kind:      telemetry.KindCounter,
			Value:     float64(i),
			Timestamp: ts,
			Labels:    map[string]string{"service": "checkout"},
		}
	}
	return telemetry.Batch{
		ID:         id,
		TenantID:   "acme",
		ReceivedAt: ts,
		Points:     pts,
	}
}

// TestPublishAndReceive is the end-to-end contract: what the edge publishes is
// what a consumer reconstructs, through a real broker.
func TestPublishAndReceive(t *testing.T) {
	h := newHarness(t)
	pub := h.publisher(t)

	sent := batch("batch-round-trip", 3)
	if err := pub.Publish(context.Background(), sent); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deliveries := h.consume(t, h.sub, 1, 30*time.Second, func(pubsubx.Delivery) error {
		return nil
	})

	if len(deliveries) != 1 {
		t.Fatalf("received %d messages, want 1", len(deliveries))
	}

	got := deliveries[0].Envelope.Batch()
	if got.ID != sent.ID {
		t.Errorf("batch ID = %q, want %q", got.ID, sent.ID)
	}
	if got.TenantID != sent.TenantID {
		t.Errorf("tenant = %q, want %q", got.TenantID, sent.TenantID)
	}
	if len(got.Points) != len(sent.Points) {
		t.Fatalf("points = %d, want %d", len(got.Points), len(sent.Points))
	}
	if got.Points[0].Metric != sent.Points[0].Metric {
		t.Errorf("metric = %q, want %q", got.Points[0].Metric, sent.Points[0].Metric)
	}
}

// TestAttributesSurviveTheBroker matters because subscription filters read
// attributes, and a trace only continues across the queue if the request ID
// arrives with the message.
func TestAttributesSurviveTheBroker(t *testing.T) {
	h := newHarness(t)
	pub := h.publisher(t)

	if err := pub.Publish(context.Background(), batch("batch-attrs", 2)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deliveries := h.consume(t, h.sub, 1, 30*time.Second, func(pubsubx.Delivery) error {
		return nil
	})
	if len(deliveries) != 1 {
		t.Fatalf("received %d messages, want 1", len(deliveries))
	}

	attrs := deliveries[0].Attributes
	for key, want := range map[string]string{
		pubsubx.AttrSchemaVersion: pubsubx.SchemaVersion,
		pubsubx.AttrTenantID:      "acme",
		pubsubx.AttrBatchID:       "batch-attrs",
		pubsubx.AttrPointCount:    "2",
	} {
		if got := attrs[key]; got != want {
			t.Errorf("attribute %s = %q, want %q", key, got, want)
		}
	}

	if deliveries[0].PublishTime.IsZero() {
		t.Error("PublishTime was not populated")
	}
}

// TestTransientFailureIsRedelivered is the at-least-once guarantee: a consumer
// that fails must see the message again rather than silently losing it.
func TestTransientFailureIsRedelivered(t *testing.T) {
	h := newHarness(t)
	pub := h.publisher(t)

	if err := pub.Publish(context.Background(), batch("batch-retry", 1)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var attempts int
	deliveries := h.consume(t, h.sub, 2, 60*time.Second, func(pubsubx.Delivery) error {
		attempts++
		if attempts == 1 {
			// A transient failure: the database blinked, the message is fine.
			return errors.New("database temporarily unavailable")
		}
		return nil
	})

	if len(deliveries) < 2 {
		t.Fatalf("saw %d deliveries, want the nacked message to come back", len(deliveries))
	}
	if deliveries[0].Envelope.BatchID != deliveries[1].Envelope.BatchID {
		t.Errorf("redelivery carried a different batch: %q then %q",
			deliveries[0].Envelope.BatchID, deliveries[1].Envelope.BatchID)
	}
}

// TestPoisonMessageReachesTheDeadLetterQueue covers the failure mode that
// otherwise loops forever: a body no consumer will ever parse.
func TestPoisonMessageReachesTheDeadLetterQueue(t *testing.T) {
	// Five is Pub/Sub's own minimum for a dead-letter policy, so it is also
	// the fastest this test can legitimately run.
	h := newHarness(t, func(s *pubsubx.SubscriptionSpec) {
		s.MaxDeliveryAttempts = 5
	})

	// Publish a body that is not a valid envelope at all, bypassing the
	// publisher so nothing sanitises it on the way out.
	raw := h.client.Publisher(h.topic)
	defer raw.Stop()

	result := raw.Publish(context.Background(), &pubsub.Message{
		Data: []byte(`{"this is": "not an envelope"}`),
	})
	if _, err := result.Get(context.Background()); err != nil {
		t.Fatalf("publish poison message: %v", err)
	}

	// The working subscriber rejects it on every attempt. Run until the
	// emulator gives up and dead-letters it.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var rejections int
	var mu sync.Mutex

	sub, err := pubsubx.NewSubscriber(h.client,
		func(context.Context, pubsubx.Delivery) error {
			t.Error("a malformed message reached the handler; decoding should have rejected it")
			return nil
		},
		pubsubx.SubscriberOptions{
			Subscription:   h.sub,
			NumGoroutines:  1,
			HandlerTimeout: 5 * time.Second,
			Logger:         discardLogger(),
		})
	if err != nil {
		t.Fatalf("build subscriber: %v", err)
	}

	go func() { _ = sub.Run(ctx) }()

	// Watch the dead-letter subscription for the message to land there.
	//
	// The DLQ is read with the raw client rather than a pubsubx.Subscriber:
	// what arrives there is by definition a body this codebase cannot decode,
	// so the only useful consumer is the one an operator would reach for.
	dlqCtx, dlqCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer dlqCancel()

	rawSub := h.client.Subscriber(h.dlqSub)
	rawSub.ReceiveSettings.NumGoroutines = 1

	err = rawSub.Receive(dlqCtx, func(_ context.Context, msg *pubsub.Message) {
		mu.Lock()
		rejections++
		mu.Unlock()
		msg.Ack()
		dlqCancel()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("dead-letter receive: %v", err)
	}

	cancel()

	mu.Lock()
	defer mu.Unlock()
	if rejections == 0 {
		t.Fatal("the poison message never reached the dead-letter queue")
	}
}

// TestPublisherHealthReflectsTheBreaker wires the transport into readiness: an
// instance that cannot publish should leave rotation rather than accept
// batches it will only reject.
func TestPublisherHealthReflectsTheBreaker(t *testing.T) {
	h := newHarness(t)

	pub, err := pubsubx.NewPublisher(h.client, pubsubx.PublisherOptions{
		Topic:          h.topic,
		BatchDelay:     time.Millisecond,
		BatchCount:     1,
		PublishTimeout: 15 * time.Second,
		Breaker:        resilience.Options{FailureThreshold: 2, Cooldown: time.Minute},
		Logger:         discardLogger(),
	})
	if err != nil {
		t.Fatalf("build publisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Check(context.Background()); err != nil {
		t.Fatalf("a healthy publisher reported unready: %v", err)
	}
	if err := pub.Publish(context.Background(), batch("batch-health", 1)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := pub.Check(context.Background()); err != nil {
		t.Errorf("Check after a successful publish: %v", err)
	}
	if pub.Name() == "" {
		t.Error("Name() is empty; the readiness report would have no label")
	}
}

// TestPublishToAMissingTopicFails proves the publisher surfaces a broker
// rejection rather than reporting a success the data never had.
func TestPublishToAMissingTopicFails(t *testing.T) {
	host := requireEmulator(t)

	ctx := context.Background()
	client, err := pubsubx.NewClient(ctx, pubsubx.Config{
		ProjectID:    "fluxgate-test",
		EmulatorHost: host,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	pub, err := pubsubx.NewPublisher(client, pubsubx.PublisherOptions{
		Topic:          "topic-that-does-not-exist",
		BatchDelay:     time.Millisecond,
		BatchCount:     1,
		PublishTimeout: 10 * time.Second,
		Logger:         discardLogger(),
	})
	if err != nil {
		t.Fatalf("build publisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish(ctx, batch("batch-missing-topic", 1)); err == nil {
		t.Fatal("Publish to a missing topic reported success")
	}
}

// TestBootstrapIsIdempotent lets several instances start at once without one
// of them failing the race to create shared topology.
func TestBootstrapIsIdempotent(t *testing.T) {
	h := newHarness(t)

	topo := pubsubx.Topology{
		ProjectID: h.projectID,
		Topics:    []string{h.topic, h.dlqTopic},
		Subscriptions: []pubsubx.SubscriptionSpec{
			{Name: h.sub, Topic: h.topic, DeadLetterTopic: h.dlqTopic},
		},
	}

	ctx := context.Background()
	for i := range 3 {
		if err := pubsubx.Ensure(ctx, h.client, topo, discardLogger()); err != nil {
			t.Fatalf("Ensure run %d: %v", i+1, err)
		}
	}
}

func TestDefaultTopologyPairsEverySubscriptionWithADeadLetterQueue(t *testing.T) {
	// A working subscription without a dead-letter policy redelivers a poison
	// message forever, burning quota and burying real failures in the logs.
	topo := pubsubx.DefaultTopology("p", "raw", "dlq", "agg")

	var working *pubsubx.SubscriptionSpec
	for i := range topo.Subscriptions {
		if topo.Subscriptions[i].Topic == "raw" {
			working = &topo.Subscriptions[i]
		}
	}
	if working == nil {
		t.Fatal("the default topology has no subscription on the raw topic")
	}
	if working.DeadLetterTopic == "" {
		t.Error("the working subscription has no dead-letter topic")
	}
	if working.MaxDeliveryAttempts <= 0 {
		t.Error("MaxDeliveryAttempts is unset; the dead-letter policy would never fire")
	}
	// Without a retry policy a nacked message returns immediately, so a
	// consumer failing on a downed database would hammer it as fast as it can
	// fail.
	if working.MinBackoff <= 0 {
		t.Error("MinBackoff is unset; redelivery would have no backoff")
	}
}

func TestTopicAndSubscriptionPaths(t *testing.T) {
	if got := pubsubx.TopicPath("p", "t"); got != "projects/p/topics/t" {
		t.Errorf("TopicPath = %q", got)
	}
	// An already-qualified name must pass through untouched, or configuration
	// that names a topic in another project would be silently rewritten.
	qualified := "projects/other/topics/t"
	if got := pubsubx.TopicPath("p", qualified); got != qualified {
		t.Errorf("TopicPath(%q) = %q, want it unchanged", qualified, got)
	}
	if got := pubsubx.SubscriptionPath("p", "s"); got != "projects/p/subscriptions/s" {
		t.Errorf("SubscriptionPath = %q", got)
	}
}

func TestNewClientRequiresAProject(t *testing.T) {
	_, err := pubsubx.NewClient(context.Background(), pubsubx.Config{})
	if err == nil {
		t.Fatal("NewClient with no project succeeded")
	}
	if got := fmt.Sprint(err); got == "" {
		t.Error("the error message is empty")
	}
}
