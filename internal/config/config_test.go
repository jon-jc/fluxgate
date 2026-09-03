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
