package config

import (
	"strings"
	"testing"
)

func TestRejectUnsafeRuntimeBounds(t *testing.T) {
	for key, value := range map[string]string{
		"QUERY_STREAM_MAX_PER_TENANT":   "0",
		"AGGREGATOR_MAX_BUFFERED_BYTES": "1MB", "AGGREGATOR_MAX_OUTSTANDING_BYTES": "1MB",
		"HTTP_HANDLER_TIMEOUT": "0s", "HTTP_READ_HEADER_TIMEOUT": "0s",
		"HTTP_READ_TIMEOUT": "0s", "HTTP_IDLE_TIMEOUT": "0s",
		"HTTP_MAX_CONCURRENT":        "0",
		"QUERY_STREAM_POLL_INTERVAL": "0s", "QUERY_STREAM_HEARTBEAT": "0s",
		"QUERY_STREAM_MAX_CONCURRENT": "0", "QUERY_DEFAULT_RANGE": "0s",
		"QUERY_STREAM_RESUME_REVISIONS": "0",
		"RATE_LIMIT_LEASE":              "5ms", "RATE_LIMIT_SCOPE": "global",
		"AGGREGATOR_CONCURRENCY": "0", "AGGREGATOR_IDLE_TIMEOUT": "0s",
		"AGGREGATOR_STORAGE_TIMEOUT":   "0s",
		"AGGREGATOR_FLUSH_CONCURRENCY": "0",
		"AGGREGATOR_WINDOW_SIZE":       "1.000000001s", "AGGREGATOR_MAX_OUTSTANDING_MESSAGES": "0",
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

// Tenant-wide limits need somewhere shared to keep each allowance, so the scope
// follows the database by default, and asking for tenant scope without one is
// refused at startup rather than silently degraded to per-process limits.
func TestRateLimitScopeFollowsTheDatabase(t *testing.T) {
	const dsn = "postgres://db/test"
	for _, tc := range []struct {
		name   string
		values map[string]string
		want   string
	}{
		{"no database", map[string]string{}, RateLimitScopeInstance},
		{"database", map[string]string{"DATABASE_URL": dsn}, RateLimitScopeTenant},
		{"explicit instance", map[string]string{"DATABASE_URL": dsn, "RATE_LIMIT_SCOPE": "instance"}, RateLimitScopeInstance},
		{"tenant without a database", map[string]string{"RATE_LIMIT_SCOPE": "tenant"}, ""},
		{"lease too long", map[string]string{"DATABASE_URL": dsn, "RATE_LIMIT_LEASE": "6s"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := load(env(tc.values), "test", Requirements{})
			if tc.want == "" {
				if err == nil || !strings.Contains(err.Error(), "RATE_LIMIT_") {
					t.Fatalf("want a RATE_LIMIT_ rejection, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Ingest.RateLimitScope != tc.want {
				t.Fatalf("scope = %q, want %q", cfg.Ingest.RateLimitScope, tc.want)
			}
		})
	}
}
