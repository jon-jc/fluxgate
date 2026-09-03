// Package config loads strongly-typed process configuration from the
// environment.
//
// Configuration is resolved once at startup and treated as immutable
// thereafter. Every field is validated eagerly so that a misconfigured
// deployment fails during boot -- while an orchestrator can still roll back --
// rather than at the moment the first request arrives.
package config

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Environment identifies the deployment tier the process is running in.
type Environment string

// Recognised deployment tiers.
const (
	EnvLocal   Environment = "local"
	EnvDev     Environment = "dev"
	EnvStaging Environment = "staging"
	EnvProd    Environment = "prod"
)

// IsProduction reports whether the tier warrants production safeguards such as
// JSON logging and stricter timeouts.
func (e Environment) IsProduction() bool { return e == EnvProd || e == EnvStaging }

func (e Environment) valid() bool {
	switch e {
	case EnvLocal, EnvDev, EnvStaging, EnvProd:
		return true
	default:
		return false
	}
}

// Config is the fully resolved configuration for a Fluxgate process.
type Config struct {
	// Service is the logical name reported in logs, traces and metrics.
	Service string
	// Environment is the deployment tier.
	Environment Environment
	// HTTP holds the public API listener settings.
	HTTP HTTPConfig
	// Log holds logging settings.
	Log LogConfig
	// Shutdown holds graceful-termination settings.
	Shutdown ShutdownConfig
}

// HTTPConfig configures the public HTTP listener.
type HTTPConfig struct {
	// Addr is the TCP address to listen on, e.g. ":8080".
	Addr string
	// ReadHeaderTimeout bounds the time spent reading request headers and is
	// the primary defence against Slowloris-style attacks.
	ReadHeaderTimeout time.Duration
	// ReadTimeout bounds the time spent reading the entire request.
	ReadTimeout time.Duration
	// WriteTimeout bounds the time spent writing the response.
	WriteTimeout time.Duration
	// IdleTimeout bounds how long a keep-alive connection may sit idle.
	IdleTimeout time.Duration
	// HandlerTimeout bounds the time an individual handler may run. It must be
	// shorter than WriteTimeout so the timeout response can still be flushed.
	HandlerTimeout time.Duration
	// MaxRequestBytes caps the size of a request body accepted by the server.
	MaxRequestBytes int64
	// TrustedProxyHeader, when true, allows client IPs to be read from
	// X-Forwarded-For. Only enable behind a proxy that rewrites that header.
	TrustedProxyHeader bool
}

// LogConfig configures structured logging.
type LogConfig struct {
	// Level is one of debug, info, warn, error.
	Level string
	// Format is either json or text.
	Format string
	// AddSource attaches source file and line to every record.
	AddSource bool
}

// ShutdownConfig configures graceful termination.
type ShutdownConfig struct {
	// GracePeriod is how long the process keeps failing readiness probes
	// before it stops accepting new connections. It gives load balancers time
	// to remove the instance from rotation, which is what actually prevents
	// dropped requests during a rolling deploy.
	GracePeriod time.Duration
	// DrainTimeout bounds how long in-flight requests may take to complete
	// once the listener has been closed.
	DrainTimeout time.Duration
}

// Load reads configuration from the process environment, applying defaults for
// every optional value. All problems are collected and reported together so a
// single boot attempt surfaces every misconfiguration.
func Load() (Config, error) {
	return load(os.LookupEnv)
}

// lookupFunc mirrors os.LookupEnv so tests can supply a fake environment.
type lookupFunc func(string) (string, bool)

func load(lookup lookupFunc) (Config, error) {
	l := &loader{lookup: lookup}

	cfg := Config{
		Service:     l.str("SERVICE_NAME", "fluxgate-ingest-api"),
		Environment: Environment(l.str("ENVIRONMENT", string(EnvLocal))),
		HTTP: HTTPConfig{
			Addr:               l.str("HTTP_ADDR", ":8080"),
			ReadHeaderTimeout:  l.duration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
			ReadTimeout:        l.duration("HTTP_READ_TIMEOUT", 15*time.Second),
			WriteTimeout:       l.duration("HTTP_WRITE_TIMEOUT", 20*time.Second),
			IdleTimeout:        l.duration("HTTP_IDLE_TIMEOUT", 120*time.Second),
			HandlerTimeout:     l.duration("HTTP_HANDLER_TIMEOUT", 10*time.Second),
			MaxRequestBytes:    l.bytes("HTTP_MAX_REQUEST_BYTES", 4<<20),
			TrustedProxyHeader: l.boolean("HTTP_TRUST_PROXY_HEADER", false),
		},
		Log: LogConfig{
			Level:     strings.ToLower(l.str("LOG_LEVEL", "info")),
			Format:    strings.ToLower(l.str("LOG_FORMAT", "json")),
			AddSource: l.boolean("LOG_ADD_SOURCE", false),
		},
		Shutdown: ShutdownConfig{
			GracePeriod:  l.duration("SHUTDOWN_GRACE_PERIOD", 5*time.Second),
			DrainTimeout: l.duration("SHUTDOWN_DRAIN_TIMEOUT", 25*time.Second),
		},
	}

	// Cloud Run and several other managed platforms inject the listener port
	// via PORT and ignore anything else, so it wins over HTTP_ADDR.
	if port, ok := lookup("PORT"); ok && strings.TrimSpace(port) != "" {
		cfg.HTTP.Addr = ":" + strings.TrimSpace(port)
	}

	cfg.validate(l)

	if err := l.err(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate(l *loader) {
	if c.Service == "" {
		l.reject("SERVICE_NAME", "must not be empty")
	}
	if !c.Environment.valid() {
		l.reject("ENVIRONMENT", "must be one of local, dev, staging, prod")
	}
	if c.HTTP.Addr == "" {
		l.reject("HTTP_ADDR", "must not be empty")
	}
	if c.HTTP.MaxRequestBytes <= 0 {
		l.reject("HTTP_MAX_REQUEST_BYTES", "must be greater than zero")
	}
	// A handler that outlives the write timeout can never deliver its 503, so
	// the client sees a truncated connection instead of a clean error.
	if c.HTTP.HandlerTimeout >= c.HTTP.WriteTimeout {
		l.reject("HTTP_HANDLER_TIMEOUT", "must be shorter than HTTP_WRITE_TIMEOUT")
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		l.reject("LOG_LEVEL", "must be one of debug, info, warn, error")
	}
	switch c.Log.Format {
	case "json", "text":
	default:
		l.reject("LOG_FORMAT", "must be one of json, text")
	}
	if c.Shutdown.DrainTimeout <= 0 {
		l.reject("SHUTDOWN_DRAIN_TIMEOUT", "must be greater than zero")
	}
}

// loader reads typed values from an environment lookup, accumulating problems
// rather than failing on the first one.
type loader struct {
	lookup lookupFunc
	errs   map[string]string
}

func (l *loader) reject(key, reason string) {
	if l.errs == nil {
		l.errs = make(map[string]string)
	}
	// Keep the first reason recorded for a key: a parse failure is more
	// actionable than the validation error it inevitably cascades into.
	if _, seen := l.errs[key]; !seen {
		l.errs[key] = reason
	}
}

func (l *loader) err() error {
	if len(l.errs) == 0 {
		return nil
	}
	keys := make([]string, 0, len(l.errs))
	for k := range l.errs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("invalid configuration:")
	for _, k := range keys {
		fmt.Fprintf(&b, "\n  %s: %s", k, l.errs[k])
	}
	return errors.New(b.String())
}

func (l *loader) raw(key string) (string, bool) {
	v, ok := l.lookup(key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return "", false
	}
	return v, true
}

func (l *loader) str(key, def string) string {
	if v, ok := l.raw(key); ok {
		return v
	}
	return def
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.reject(key, fmt.Sprintf("%q is not a valid duration (want e.g. 250ms, 30s, 5m)", v))
		return def
	}
	if d < 0 {
		l.reject(key, "must not be negative")
		return def
	}
	return d
}

func (l *loader) boolean(key string, def bool) bool {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.reject(key, fmt.Sprintf("%q is not a valid boolean (want true or false)", v))
		return def
	}
	return b
}

// byteUnits are matched longest-suffix-first so that "KB" is not mistaken for
// a bare "B" and "K" is only reached once the two-letter forms have failed.
var byteUnits = []struct {
	suffix string
	scale  int64
}{
	{"KB", 1 << 10}, {"MB", 1 << 20}, {"GB", 1 << 30},
	{"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30},
}

// bytes parses a byte count, accepting a plain integer or a KB/MB/GB suffix so
// that operators can write 4MB instead of counting zeroes.
func (l *loader) bytes(key string, def int64) int64 {
	v, ok := l.raw(key)
	if !ok {
		return def
	}

	digits := strings.ToUpper(v)
	multiplier := int64(1)
	for _, unit := range byteUnits {
		if strings.HasSuffix(digits, unit.suffix) {
			multiplier = unit.scale
			digits = strings.TrimSpace(strings.TrimSuffix(digits, unit.suffix))
			break
		}
	}

	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		l.reject(key, fmt.Sprintf("%q is not a valid byte size (want e.g. 4194304 or 4MB)", v))
		return def
	}
	if n <= 0 {
		l.reject(key, "must be greater than zero")
		return def
	}
	// Reject sizes that would overflow once scaled rather than silently
	// wrapping to a negative limit that disables the body cap entirely.
	if n > (1<<62)/multiplier {
		l.reject(key, fmt.Sprintf("%q is too large", v))
		return def
	}
	return n * multiplier
}
