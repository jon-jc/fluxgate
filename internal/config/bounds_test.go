package config

import (
	"strings"
	"testing"
)

func TestRejectUnsafeRuntimeBounds(t *testing.T) {
	for key, value := range map[string]string{
		"HTTP_HANDLER_TIMEOUT": "0s", "HTTP_READ_HEADER_TIMEOUT": "0s",
		"HTTP_READ_TIMEOUT": "0s", "HTTP_IDLE_TIMEOUT": "0s",
		"HTTP_MAX_CONCURRENT":        "0",
		"QUERY_STREAM_POLL_INTERVAL": "0s", "QUERY_STREAM_HEARTBEAT": "0s",
		"QUERY_STREAM_MAX_CONCURRENT": "0", "QUERY_DEFAULT_RANGE": "0s",
		"AGGREGATOR_CONCURRENCY": "0", "AGGREGATOR_IDLE_TIMEOUT": "0s",
		"AGGREGATOR_WINDOW_SIZE": "1.000000001s", "AGGREGATOR_MAX_OUTSTANDING_MESSAGES": "0",
		"DATABASE_MIN_CONNS": "-1", "DATABASE_CONNECT_TIMEOUT": "0s",
		"INGEST_MAX_POINTS_PER_BATCH": "1001", "METRICS_PATH": "/healthz",
		"PUBSUB_BATCH_COUNT": "0", "PUBSUB_RETENTION": "0s", "PUBSUB_BREAKER_COOLDOWN": "0s",
	} {
		t.Run(key, func(t *testing.T) {
			values := map[string]string{"DATABASE_URL": "postgres://db/test", "PUBSUB_ENABLED": "true", "GCP_PROJECT_ID": "test"}
			values[key] = value
			_, err := load(env(values), "test", Requirements{Database: true, Aggregator: true, PubSub: true})
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("wanted rejection of %s, got %v", key, err)
			}
		})
	}
}
