package config

import (
	"strings"
	"testing"
	"time"
)

// env builds a lookupFunc backed by a map, standing in for the process
// environment.
func env(kv map[string]string) lookupFunc {
	return func(key string) (string, bool) {
		v, ok := kv[key]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := load(env(nil))
	if err != nil {
		t.Fatalf("load with empty environment: %v", err)
	}

	if cfg.Service != "fluxgate-ingest-api" {
		t.Errorf("Service = %q, want fluxgate-ingest-api", cfg.Service)
	}
	if cfg.Environment != EnvLocal {
		t.Errorf("Environment = %q, want local", cfg.Environment)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("HTTP.Addr = %q, want :8080", cfg.HTTP.Addr)
	}
	if cfg.HTTP.MaxRequestBytes != 4<<20 {
		t.Errorf("HTTP.MaxRequestBytes = %d, want %d", cfg.HTTP.MaxRequestBytes, 4<<20)
	}
	if cfg.HTTP.ReadHeaderTimeout != 5*time.Second {
		t.Errorf("HTTP.ReadHeaderTimeout = %v, want 5s", cfg.HTTP.ReadHeaderTimeout)
	}
	if cfg.Log.Format != "json" {
		t.Errorf("Log.Format = %q, want json", cfg.Log.Format)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := load(env(map[string]string{
		"SERVICE_NAME":             "fluxgate-test",
		"ENVIRONMENT":              "prod",
		"API_KEYS":                 `[{"key_id":"k1","tenant_id":"t","secret_sha256":"x"}]`,
		"GCP_PROJECT_ID":           "fluxgate-test",
		"HTTP_ADDR":                ":9999",
		"HTTP_READ_HEADER_TIMEOUT": "2s",
		"HTTP_HANDLER_TIMEOUT":     "3s",
		"HTTP_MAX_REQUEST_BYTES":   "8MB",
		"HTTP_TRUST_PROXY_HEADER":  "true",
		"LOG_LEVEL":                "DEBUG",
		"LOG_FORMAT":               "TEXT",
		"SHUTDOWN_DRAIN_TIMEOUT":   "45s",
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.Service != "fluxgate-test" {
		t.Errorf("Service = %q", cfg.Service)
	}
	if !cfg.Environment.IsProduction() {
		t.Error("Environment.IsProduction() = false, want true for prod")
	}
	if cfg.HTTP.Addr != ":9999" {
		t.Errorf("HTTP.Addr = %q, want :9999", cfg.HTTP.Addr)
	}
	if cfg.HTTP.MaxRequestBytes != 8<<20 {
		t.Errorf("HTTP.MaxRequestBytes = %d, want %d", cfg.HTTP.MaxRequestBytes, 8<<20)
	}
	if !cfg.HTTP.TrustedProxyHeader {
		t.Error("HTTP.TrustedProxyHeader = false, want true")
	}
	// Level and format are case-insensitive so that operators are not tripped
	// up by shouting an environment variable.
	if cfg.Log.Level != "debug" {
		t.Errorf("Log.Level = %q, want debug", cfg.Log.Level)
	}
	if cfg.Log.Format != "text" {
		t.Errorf("Log.Format = %q, want text", cfg.Log.Format)
	}
	if cfg.Shutdown.DrainTimeout != 45*time.Second {
		t.Errorf("Shutdown.DrainTimeout = %v, want 45s", cfg.Shutdown.DrainTimeout)
	}
}

func TestLoadPortOverridesAddr(t *testing.T) {
	// Managed platforms such as Cloud Run dictate the port; it must win.
	cfg, err := load(env(map[string]string{
		"HTTP_ADDR": ":8080",
		"PORT":      "8081",
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.HTTP.Addr != ":8081" {
		t.Errorf("HTTP.Addr = %q, want :8081 (PORT must win)", cfg.HTTP.Addr)
	}
}

func TestLoadBlankValueFallsBackToDefault(t *testing.T) {
	// An unset variable and one exported as the empty string are the same
	// thing in practice; both must yield the default.
	cfg, err := load(env(map[string]string{"HTTP_ADDR": "   "}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("HTTP.Addr = %q, want :8080", cfg.HTTP.Addr)
	}
}

func TestLoadReportsEveryProblemAtOnce(t *testing.T) {
	_, err := load(env(map[string]string{
		"ENVIRONMENT":            "banana",
		"LOG_LEVEL":              "loud",
		"HTTP_READ_TIMEOUT":      "soon",
		"HTTP_MAX_REQUEST_BYTES": "-1",
	}))
	if err == nil {
		t.Fatal("load: expected an error, got nil")
	}

	// A single boot attempt should surface every misconfiguration rather than
	// forcing an operator through one redeploy per typo.
	for _, want := range []string{
		"ENVIRONMENT",
		"LOG_LEVEL",
		"HTTP_READ_TIMEOUT",
		"HTTP_MAX_REQUEST_BYTES",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s:\n%v", want, err)
		}
	}
}

func TestLoadRejectsHandlerTimeoutAtOrAboveWriteTimeout(t *testing.T) {
	for _, tc := range []struct{ name, handler, write string }{
		{"equal", "10s", "10s"},
		{"longer", "30s", "10s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(env(map[string]string{
				"HTTP_HANDLER_TIMEOUT": tc.handler,
				"HTTP_WRITE_TIMEOUT":   tc.write,
			}))
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), "HTTP_HANDLER_TIMEOUT") {
				t.Errorf("error does not mention HTTP_HANDLER_TIMEOUT:\n%v", err)
			}
		})
	}
}

func TestLoaderBytes(t *testing.T) {
	tests := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{in: "1024", want: 1024},
		{in: "1KB", want: 1 << 10},
		{in: "4MB", want: 4 << 20},
		{in: "2gb", want: 2 << 30},
		{in: "16m", want: 16 << 20},
		{in: "0", wantErr: true},
		{in: "-5", wantErr: true},
		{in: "big", wantErr: true},
		{in: "12TB", wantErr: true},
		{in: "9223372036854775807MB", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			l := &loader{lookup: env(map[string]string{"SIZE": tc.in})}
			got := l.bytes("SIZE", -1)

			if tc.wantErr {
				if l.err() == nil {
					t.Fatalf("bytes(%q) = %d, want an error", tc.in, got)
				}
				return
			}
			if l.err() != nil {
				t.Fatalf("bytes(%q): unexpected error: %v", tc.in, l.err())
			}
			if got != tc.want {
				t.Errorf("bytes(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestEnvironmentIsProduction(t *testing.T) {
	for env, want := range map[Environment]bool{
		EnvLocal:   false,
		EnvDev:     false,
		EnvStaging: true,
		EnvProd:    true,
	} {
		if got := env.IsProduction(); got != want {
			t.Errorf("%s.IsProduction() = %v, want %v", env, got, want)
		}
	}
}

// TestAuthDefaultsOffOnlyLocally keeps a fresh clone runnable with no setup,
// without letting that convenience follow the code to a deployed tier.
func TestAuthDefaultsOffOnlyLocally(t *testing.T) {
	local, err := load(env(map[string]string{"ENVIRONMENT": "local"}))
	if err != nil {
		t.Fatalf("local: %v", err)
	}
	if !local.Auth.Disabled {
		t.Error("local tier requires credentials by default; a fresh clone should just run")
	}

	// Every other tier defaults to requiring credentials, so omitting them is
	// a boot failure rather than a silently open endpoint.
	if _, err := load(env(map[string]string{"ENVIRONMENT": "dev"})); err == nil {
		t.Error("dev tier booted with no API keys and no explicit AUTH_DISABLED")
	}
}

// TestAuthCannotBeDisabledInProduction is the safeguard that matters most: an
// unauthenticated ingest endpoint would let anyone write into any tenant's
// data, so it must be impossible to configure rather than merely discouraged.
func TestAuthCannotBeDisabledInProduction(t *testing.T) {
	for _, tier := range []string{"staging", "prod"} {
		t.Run(tier, func(t *testing.T) {
			_, err := load(env(map[string]string{
				"ENVIRONMENT":   tier,
				"AUTH_DISABLED": "true",
			}))
			if err == nil {
				t.Fatalf("AUTH_DISABLED=true was accepted on the %s tier", tier)
			}
			if !strings.Contains(err.Error(), "AUTH_DISABLED") {
				t.Errorf("error does not mention AUTH_DISABLED:\n%v", err)
			}
		})
	}
}

func TestAuthRequiresAKeySourceWhenEnabled(t *testing.T) {
	_, err := load(env(map[string]string{"ENVIRONMENT": "prod"}))
	if err == nil {
		t.Fatal("expected an error when authentication is on but no keys are configured")
	}
	if !strings.Contains(err.Error(), "API_KEYS") {
		t.Errorf("error does not mention API_KEYS:\n%v", err)
	}

	// Either source satisfies the requirement.
	for _, source := range []string{"API_KEYS", "API_KEYS_FILE"} {
		t.Run(source, func(t *testing.T) {
			if _, err := load(env(map[string]string{
				"ENVIRONMENT":    "prod",
				"GCP_PROJECT_ID": "fluxgate-test",
				source:           "value",
			})); err != nil {
				t.Errorf("load with %s set: %v", source, err)
			}
		})
	}
}

// TestBurstMustAdmitAFullBatch rejects a quota that would refuse every
// conforming request no matter how long the client waited.
func TestBurstMustAdmitAFullBatch(t *testing.T) {
	_, err := load(env(map[string]string{
		"INGEST_MAX_POINTS_PER_BATCH": "1000",
		"RATE_LIMIT_BURST":            "500",
	}))
	if err == nil {
		t.Fatal("a burst smaller than one batch was accepted")
	}
	if !strings.Contains(err.Error(), "RATE_LIMIT_BURST") {
		t.Errorf("error does not mention RATE_LIMIT_BURST:\n%v", err)
	}
}

func TestIngestDefaults(t *testing.T) {
	cfg, err := load(env(nil))
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.Ingest.MaxPointsPerBatch != 1000 {
		t.Errorf("MaxPointsPerBatch = %d, want 1000", cfg.Ingest.MaxPointsPerBatch)
	}
	if cfg.Ingest.MaxClockSkew != 5*time.Minute {
		t.Errorf("MaxClockSkew = %v, want 5m", cfg.Ingest.MaxClockSkew)
	}
	if cfg.Ingest.RateLimitPointsPerSecond != 10_000 {
		t.Errorf("RateLimitPointsPerSecond = %g, want 10000", cfg.Ingest.RateLimitPointsPerSecond)
	}
	if cfg.Ingest.IdempotencyTTL != 24*time.Hour {
		t.Errorf("IdempotencyTTL = %v, want 24h", cfg.Ingest.IdempotencyTTL)
	}
}

func TestLoaderNumericParsing(t *testing.T) {
	_, err := load(env(map[string]string{
		"INGEST_MAX_POINTS_PER_BATCH":  "lots",
		"RATE_LIMIT_POINTS_PER_SECOND": "fast",
	}))
	if err == nil {
		t.Fatal("expected an error for unparseable numbers")
	}
	for _, want := range []string{"INGEST_MAX_POINTS_PER_BATCH", "RATE_LIMIT_POINTS_PER_SECOND"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s:\n%v", want, err)
		}
	}
}
