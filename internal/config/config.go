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
	"math"
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
	// Auth holds credential settings.
	Auth AuthConfig
	// Ingest holds the ingestion limits and quotas.
	Ingest IngestConfig
	// PubSub holds the event transport settings.
	PubSub PubSubConfig
	// Database holds the Postgres settings.
	Database DatabaseConfig
	// Aggregator holds the windowing and flush settings.
	Aggregator AggregatorConfig
	// Query holds the read API's limits.
	Query QueryConfig
}

// QueryConfig bounds what the read API will do for one caller.
//
// Every limit here exists because the read path is the easy way to hurt a
// telemetry system: a single unbounded query over a year of one-minute windows
// across a thousand series is half a billion rows, and the client that asked
// for it is usually a dashboard that will ask again in thirty seconds.
type QueryConfig struct {
	// MaxRange is the longest time span one query may cover.
	MaxRange time.Duration
	// MaxSeries caps the distinct series in one response.
	MaxSeries int
	// MaxPoints caps the total points across all series in one response.
	MaxPoints int
	// DefaultRange applies when the caller supplies neither bound.
	DefaultRange time.Duration

	// StreamPollInterval is how often the live tail checks for new rollups.
	StreamPollInterval time.Duration
	// StreamHeartbeat is how often an idle stream sends a keep-alive, so an
	// intermediary does not close a connection it believes is dead.
	StreamHeartbeat time.Duration
	// StreamMaxDuration bounds one streaming connection, so a forgotten
	// browser tab does not hold a database poller open indefinitely.
	StreamMaxDuration time.Duration
}

// DatabaseConfig configures Postgres.
type DatabaseConfig struct {
	// DSN is the connection string.
	DSN string
	// MaxConns caps the pool. Size it against the database's own connection
	// limit divided by the replica count, not against how much concurrency
	// this process would like to have.
	MaxConns int32
	// MinConns keeps a floor of warm connections so a traffic spike does not
	// pay connection setup costs on every request.
	MinConns int32
	// MaxConnLifetime recycles connections, so a failover or a rolling
	// database upgrade is picked up without restarting the service.
	MaxConnLifetime time.Duration
	// ConnectTimeout bounds the initial connection.
	ConnectTimeout time.Duration
	// Migrate applies pending migrations at startup.
	Migrate bool
}

// AggregatorConfig configures windowed aggregation.
type AggregatorConfig struct {
	// WindowSize is the tumbling window width. Every rollup covers exactly
	// this much event time.
	WindowSize time.Duration
	// AllowedLateness is how far the watermark trails the highest observed
	// event time. It trades freshness for tolerance of out-of-order arrival:
	// too small and legitimate stragglers are discarded, too large and every
	// rollup is delayed by that much before anyone can read it.
	AllowedLateness time.Duration
	// MaxSeries caps distinct series held across all open windows.
	MaxSeries int
	// IdleTimeout is how long a producer may be silent before the watermark
	// advances on processing time, so a stream that stops does not strand its
	// last window unwritten.
	IdleTimeout time.Duration
	// FlushInterval is how often closed windows are drained even with no new
	// data. Without it a producer that goes quiet leaves its last window
	// unwritten and its messages unacknowledged.
	FlushInterval time.Duration
	// MaxOutstandingMessages caps unacknowledged messages held in memory.
	MaxOutstandingMessages int
	// Concurrency is how many streaming pull connections to open.
	Concurrency int
	// RollupRetention is how long rollups are kept before pruning.
	RollupRetention time.Duration
	// LedgerRetention is how long delivery-ledger entries are kept. It only
	// has to outlive the longest redelivery Pub/Sub could produce; keeping it
	// longer grows a table forever to guard against a duplicate that can no
	// longer arrive.
	LedgerRetention time.Duration
	// PruneInterval is how often retention runs.
	PruneInterval time.Duration
}

// PubSubConfig configures the Pub/Sub transport.
type PubSubConfig struct {
	// Enabled selects the durable transport over the in-memory sink. Defaults
	// to false only on the local tier with no emulator configured.
	Enabled bool
	// ProjectID owns the topics and subscriptions.
	ProjectID string
	// EmulatorHost points at a local emulator instead of the real service.
	// Empty falls back to PUBSUB_EMULATOR_HOST.
	EmulatorHost string
	// Bootstrap creates any missing topics and subscriptions at startup. It is
	// for local development and tests; deployed topology comes from Terraform.
	Bootstrap bool

	// RawTopic carries accepted batches from the edge.
	RawTopic string
	// DeadLetterTopic receives messages that exhaust their delivery attempts.
	DeadLetterTopic string
	// AggregatorSubscription is the working subscription on RawTopic.
	AggregatorSubscription string

	// PublishTimeout bounds a single publish including client-side retries.
	PublishTimeout time.Duration
	// BatchDelay is how long the client accumulates messages before sending.
	BatchDelay time.Duration
	// BatchCount is how many messages accumulate before sending.
	BatchCount int
	// MaxOutstandingMessages caps buffered messages awaiting publish; exceeding
	// it sheds load rather than growing without bound.
	MaxOutstandingMessages int
	// MaxOutstandingBytes caps the memory held by buffered messages.
	MaxOutstandingBytes int

	// BreakerFailureThreshold is how many consecutive publish failures trip
	// the circuit breaker.
	BreakerFailureThreshold int
	// BreakerCooldown is how long the breaker fails fast before probing.
	BreakerCooldown time.Duration
}

// AuthConfig configures API key authentication.
type AuthConfig struct {
	// Disabled turns authentication off entirely. Only permitted on the local
	// and dev tiers; validation rejects it anywhere else.
	Disabled bool
	// Keys is the raw JSON key document. It takes precedence over KeysFile.
	Keys string
	// KeysFile is a path to the key document, which is how a secret manager
	// mounts credentials into a container.
	KeysFile string
}

// IngestConfig configures the ingestion endpoint.
type IngestConfig struct {
	// MaxPointsPerBatch caps how many observations one request may carry.
	MaxPointsPerBatch int
	// MaxClockSkew is how far into the future a timestamp may be.
	MaxClockSkew time.Duration
	// MaxBackfill is how far into the past a timestamp may be.
	MaxBackfill time.Duration
	// RateLimitPointsPerSecond is the default sustained per-tenant rate. A key
	// may override it.
	RateLimitPointsPerSecond float64
	// RateLimitBurst is the default per-tenant burst allowance.
	RateLimitBurst int
	// IdempotencyTTL is how long a completed outcome is replayable. Zero
	// disables idempotency handling.
	IdempotencyTTL time.Duration
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

// Requirements declares which subsystems a binary cannot run without.
//
// Passing them in rather than having each service check its own dependencies
// after Load returns keeps every failure in one report: an aggregator started
// with neither a database nor credentials should be told both at once, not one
// per redeploy.
type Requirements struct {
	// Auth means the process serves an authenticated API, so credentials are
	// mandatory. A background consumer declares false: it has no callers to
	// authenticate, and demanding an API key from it would be a boot failure
	// over a setting it never reads.
	Auth bool
	// Database means the process needs Postgres.
	Database bool
	// PubSub means the process publishes or subscribes. A read-only service
	// touches neither, and requiring a project of it would be a boot failure
	// over a setting it never reads.
	PubSub bool
}

// Load reads configuration for the named service from the process environment,
// applying defaults for every optional value. All problems are collected and
// reported together so a single boot attempt surfaces every misconfiguration.
//
// The service name is passed by the binary rather than guessed, because a
// wrong default is worse than none: logs and metrics from an aggregator
// labelled as the ingest API are actively misleading during an incident.
func Load(service string, req Requirements) (Config, error) {
	return load(os.LookupEnv, service, req)
}

// lookupFunc mirrors os.LookupEnv so tests can supply a fake environment.
type lookupFunc func(string) (string, bool)

func load(lookup lookupFunc, service string, req Requirements) (Config, error) {
	l := &loader{lookup: lookup, requirements: req}
	if service == "" {
		service = "fluxgate"
	}

	// The tier is resolved first because a few defaults depend on it.
	env := Environment(l.str("ENVIRONMENT", string(EnvLocal)))

	// PUBSUB_EMULATOR_HOST is the variable the Google client libraries already
	// read, so it is honoured here rather than inventing a second name that
	// could disagree with it.
	emulator := l.str("PUBSUB_EMULATOR_HOST", "")

	cfg := Config{
		Service:     l.str("SERVICE_NAME", service),
		Environment: env,
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
		Auth: AuthConfig{
			// Local development defaults to no authentication so that a fresh
			// clone runs with no setup. Every other tier defaults to requiring
			// it, and validation makes disabling it impossible on staging and
			// prod regardless of what the environment says.
			Disabled: l.boolean("AUTH_DISABLED", env == EnvLocal),
			Keys:     l.str("API_KEYS", ""),
			KeysFile: l.str("API_KEYS_FILE", ""),
		},
		Ingest: IngestConfig{
			MaxPointsPerBatch:        l.integer("INGEST_MAX_POINTS_PER_BATCH", 1000),
			MaxClockSkew:             l.duration("INGEST_MAX_CLOCK_SKEW", 5*time.Minute),
			MaxBackfill:              l.duration("INGEST_MAX_BACKFILL", 7*24*time.Hour),
			RateLimitPointsPerSecond: l.float("RATE_LIMIT_POINTS_PER_SECOND", 10_000),
			RateLimitBurst:           l.integer("RATE_LIMIT_BURST", 20_000),
			IdempotencyTTL:           l.duration("IDEMPOTENCY_TTL", 24*time.Hour),
		},
		PubSub: PubSubConfig{
			// Anything past the local tier publishes for real. An emulator
			// host is taken as an explicit request for the durable path, since
			// nobody points at an emulator expecting the in-memory sink.
			Enabled:      l.boolean("PUBSUB_ENABLED", env != EnvLocal || emulator != ""),
			ProjectID:    l.str("GCP_PROJECT_ID", ""),
			EmulatorHost: emulator,
			// Creating topology needs admin permissions at runtime, which is a
			// far larger blast radius than publish-and-subscribe. Allowed only
			// where an emulator is in play.
			Bootstrap: l.boolean("PUBSUB_BOOTSTRAP", emulator != ""),

			RawTopic:               l.str("PUBSUB_TOPIC_RAW", "telemetry-raw"),
			DeadLetterTopic:        l.str("PUBSUB_TOPIC_DLQ", "telemetry-dlq"),
			AggregatorSubscription: l.str("PUBSUB_SUBSCRIPTION_AGGREGATOR", "telemetry-aggregator"),

			PublishTimeout:         l.duration("PUBSUB_PUBLISH_TIMEOUT", 10*time.Second),
			BatchDelay:             l.duration("PUBSUB_BATCH_DELAY", 10*time.Millisecond),
			BatchCount:             l.integer("PUBSUB_BATCH_COUNT", 100),
			MaxOutstandingMessages: l.integer("PUBSUB_MAX_OUTSTANDING_MESSAGES", 1000),
			MaxOutstandingBytes:    int(l.bytes("PUBSUB_MAX_OUTSTANDING_BYTES", 64<<20)),

			BreakerFailureThreshold: l.integer("PUBSUB_BREAKER_FAILURE_THRESHOLD", 5),
			BreakerCooldown:         l.duration("PUBSUB_BREAKER_COOLDOWN", 10*time.Second),
		},
		Database: DatabaseConfig{
			DSN:             l.str("DATABASE_URL", ""),
			MaxConns:        l.int32("DATABASE_MAX_CONNS", 10),
			MinConns:        l.int32("DATABASE_MIN_CONNS", 2),
			MaxConnLifetime: l.duration("DATABASE_MAX_CONN_LIFETIME", time.Hour),
			ConnectTimeout:  l.duration("DATABASE_CONNECT_TIMEOUT", 10*time.Second),
			Migrate:         l.boolean("DATABASE_MIGRATE", true),
		},
		Aggregator: AggregatorConfig{
			WindowSize:             l.duration("AGGREGATOR_WINDOW_SIZE", time.Minute),
			AllowedLateness:        l.duration("AGGREGATOR_ALLOWED_LATENESS", 30*time.Second),
			MaxSeries:              l.integer("AGGREGATOR_MAX_SERIES", 100_000),
			IdleTimeout:            l.duration("AGGREGATOR_IDLE_TIMEOUT", 30*time.Second),
			FlushInterval:          l.duration("AGGREGATOR_FLUSH_INTERVAL", 15*time.Second),
			MaxOutstandingMessages: l.integer("AGGREGATOR_MAX_OUTSTANDING_MESSAGES", 1000),
			Concurrency:            l.integer("AGGREGATOR_CONCURRENCY", 2),
			RollupRetention:        l.duration("ROLLUP_RETENTION", 30*24*time.Hour),
			LedgerRetention:        l.duration("LEDGER_RETENTION", 24*time.Hour),
			PruneInterval:          l.duration("PRUNE_INTERVAL", time.Hour),
		},
		Query: QueryConfig{
			MaxRange:     l.duration("QUERY_MAX_RANGE", 31*24*time.Hour),
			MaxSeries:    l.integer("QUERY_MAX_SERIES", 500),
			MaxPoints:    l.integer("QUERY_MAX_POINTS", 50_000),
			DefaultRange: l.duration("QUERY_DEFAULT_RANGE", time.Hour),

			StreamPollInterval: l.duration("QUERY_STREAM_POLL_INTERVAL", 2*time.Second),
			StreamHeartbeat:    l.duration("QUERY_STREAM_HEARTBEAT", 20*time.Second),
			StreamMaxDuration:  l.duration("QUERY_STREAM_MAX_DURATION", 30*time.Minute),
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

	c.validateAuth(l)
	c.validateIngest(l)
	c.validatePubSub(l)
	c.validateDatabase(l)
	c.validateAggregator(l)
	c.validateQuery(l)
}

func (c Config) validateQuery(l *loader) {
	if c.Query.MaxSeries <= 0 {
		l.reject("QUERY_MAX_SERIES", "must be greater than zero")
	}
	if c.Query.MaxPoints <= 0 {
		l.reject("QUERY_MAX_POINTS", "must be greater than zero")
	}
	if c.Query.MaxRange <= 0 {
		l.reject("QUERY_MAX_RANGE", "must be greater than zero")
	}
	// A default wider than the maximum would make every parameterless request
	// fail its own validation, which is a confusing way to learn about a
	// misconfiguration.
	if c.Query.DefaultRange > c.Query.MaxRange {
		l.reject("QUERY_DEFAULT_RANGE", fmt.Sprintf(
			"must not exceed QUERY_MAX_RANGE (%s), or a request with no range would always be rejected",
			c.Query.MaxRange))
	}
	// A heartbeat that never fires before the connection is closed is the same
	// as having none, and the whole point is to keep intermediaries from
	// reaping an idle stream.
	if c.Query.StreamHeartbeat >= c.Query.StreamMaxDuration {
		l.reject("QUERY_STREAM_HEARTBEAT",
			"must be shorter than QUERY_STREAM_MAX_DURATION, or it would never fire")
	}
}

func (c Config) validateDatabase(l *loader) {
	if c.Database.DSN == "" {
		if l.requirements.Database {
			l.reject("DATABASE_URL", "is required by this service")
		}
		return
	}
	if c.Database.MaxConns <= 0 {
		l.reject("DATABASE_MAX_CONNS", "must be greater than zero")
	}
	if c.Database.MinConns > c.Database.MaxConns {
		l.reject("DATABASE_MIN_CONNS", "must not exceed DATABASE_MAX_CONNS")
	}
}

func (c Config) validateAggregator(l *loader) {
	if !l.requirements.Database {
		// Only the aggregator uses these; validating them everywhere would
		// reject an ingest API over settings it never reads.
		return
	}

	if c.Aggregator.WindowSize <= 0 {
		l.reject("AGGREGATOR_WINDOW_SIZE", "must be greater than zero")
	}
	if c.Aggregator.MaxSeries <= 0 {
		l.reject("AGGREGATOR_MAX_SERIES", "must be greater than zero")
	}
	if c.Aggregator.FlushInterval <= 0 {
		l.reject("AGGREGATOR_FLUSH_INTERVAL", "must be greater than zero")
	}
	// A flush interval longer than the window means a closed window waits for
	// the timer rather than being written promptly, so every rollup is stale by
	// up to that difference before anyone can read it.
	if c.Aggregator.WindowSize > 0 && c.Aggregator.FlushInterval > c.Aggregator.WindowSize {
		l.reject("AGGREGATOR_FLUSH_INTERVAL", fmt.Sprintf(
			"must not exceed AGGREGATOR_WINDOW_SIZE (%s), or rollups are stale by the difference",
			c.Aggregator.WindowSize))
	}
	// The ledger must outlive the redeliveries it exists to suppress. Pruning
	// it sooner would let a redelivered batch be counted a second time.
	if c.Aggregator.LedgerRetention < c.Aggregator.WindowSize {
		l.reject("LEDGER_RETENTION",
			"must be at least AGGREGATOR_WINDOW_SIZE, or a redelivery could be counted twice")
	}
	if c.Aggregator.RollupRetention <= 0 {
		l.reject("ROLLUP_RETENTION", "must be greater than zero")
	}
	if c.Aggregator.PruneInterval <= 0 {
		l.reject("PRUNE_INTERVAL", "must be greater than zero")
	}
}

func (c Config) validatePubSub(l *loader) {
	if !l.requirements.PubSub {
		return
	}

	// The in-memory sink accepts a batch, answers 202 and then discards it on
	// the next restart. That is fine for a laptop and catastrophic in
	// production, where it would report success for data nobody ever receives.
	if !c.PubSub.Enabled {
		if c.Environment.IsProduction() {
			l.reject("PUBSUB_ENABLED",
				"must not be false on the "+string(c.Environment)+
					" tier; the in-memory sink acknowledges batches it then discards")
		}
		return
	}

	if c.PubSub.ProjectID == "" {
		l.reject("GCP_PROJECT_ID", "is required when PUBSUB_ENABLED is true")
	}
	if c.PubSub.RawTopic == "" {
		l.reject("PUBSUB_TOPIC_RAW", "must not be empty")
	}
	if c.PubSub.PublishTimeout <= 0 {
		l.reject("PUBSUB_PUBLISH_TIMEOUT", "must be greater than zero")
	}
	if c.PubSub.MaxOutstandingMessages <= 0 {
		l.reject("PUBSUB_MAX_OUTSTANDING_MESSAGES", "must be greater than zero")
	}
	if c.PubSub.BreakerFailureThreshold <= 0 {
		l.reject("PUBSUB_BREAKER_FAILURE_THRESHOLD", "must be greater than zero")
	}
	// Creating topics and subscriptions requires admin credentials at runtime.
	// Granting those to a request-serving process is a blast radius nobody
	// should accept in exchange for saving a Terraform apply.
	if c.PubSub.Bootstrap && c.Environment.IsProduction() {
		l.reject("PUBSUB_BOOTSTRAP",
			"must not be true on the "+string(c.Environment)+
				" tier; deployed topology belongs in Terraform, not in a runtime admin call")
	}
}

func (c Config) validateAuth(l *loader) {
	if !l.requirements.Auth {
		return
	}

	// Shipping an unauthenticated ingest endpoint would let anyone write into
	// any tenant's data. Making that impossible to configure is worth more than
	// any amount of documentation warning against it.
	if c.Auth.Disabled && c.Environment.IsProduction() {
		l.reject("AUTH_DISABLED",
			"must not be true on the "+string(c.Environment)+" tier; authentication is mandatory outside local and dev")
		return
	}
	if c.Auth.Disabled {
		return
	}

	if c.Auth.Keys == "" && c.Auth.KeysFile == "" {
		l.reject("API_KEYS",
			"is required (or set API_KEYS_FILE) unless AUTH_DISABLED is true")
	}
}

func (c Config) validateIngest(l *loader) {
	if c.Ingest.MaxPointsPerBatch <= 0 {
		l.reject("INGEST_MAX_POINTS_PER_BATCH", "must be greater than zero")
	}
	if c.Ingest.RateLimitPointsPerSecond <= 0 {
		l.reject("RATE_LIMIT_POINTS_PER_SECOND", "must be greater than zero")
	}
	if c.Ingest.RateLimitBurst <= 0 {
		l.reject("RATE_LIMIT_BURST", "must be greater than zero")
	}
	// A burst below one batch means a full-sized batch can never be admitted,
	// no matter how long the client waits -- a quota that rejects every
	// conforming request is a misconfiguration, not a policy.
	if c.Ingest.RateLimitBurst > 0 && c.Ingest.MaxPointsPerBatch > c.Ingest.RateLimitBurst {
		l.reject("RATE_LIMIT_BURST", fmt.Sprintf(
			"must be at least INGEST_MAX_POINTS_PER_BATCH (%d), or a full batch can never be admitted",
			c.Ingest.MaxPointsPerBatch))
	}
	if c.Ingest.MaxBackfill <= 0 {
		l.reject("INGEST_MAX_BACKFILL", "must be greater than zero")
	}
}

// loader reads typed values from an environment lookup, accumulating problems
// rather than failing on the first one.
type loader struct {
	lookup       lookupFunc
	requirements Requirements
	errs         map[string]string
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

func (l *loader) integer(key string, def int) int {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.reject(key, fmt.Sprintf("%q is not a valid integer", v))
		return def
	}
	return n
}

// int32 parses a value that must fit a 32-bit field, rejecting anything that
// would silently wrap into a negative connection count.
func (l *loader) int32(key string, def int32) int32 {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		l.reject(key, fmt.Sprintf("%q is not a valid 32-bit integer", v))
		return def
	}
	return int32(n)
}

func (l *loader) float(key string, def float64) float64 {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		l.reject(key, fmt.Sprintf("%q is not a valid number", v))
		return def
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		l.reject(key, fmt.Sprintf("%q is not a finite number", v))
		return def
	}
	return f
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
