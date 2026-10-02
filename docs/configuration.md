# Configuration reference

[Documentation index](README.md) · [Terraform inputs](terraform-reference.md)

This is the runtime environment reference for
[`internal/config/config.go`](../internal/config/config.go). Defaults below are
binary defaults, not the values injected by Compose or Terraform. Configuration
is read once at startup and validated before serving traffic.

## Loading and precedence

Fluxgate reads the process environment; it does **not** load `.env` files. Treat
[`.env.example`](../.env.example) as a local template and export selected values
with your shell or service manager. Docker Compose uses its own explicit
`environment` entries; a root `.env` file does not automatically override those
entries or get passed to containers.

Leading/trailing whitespace is trimmed. An unset or empty variable uses its
default. `PORT` overrides `HTTP_ADDR`. Inline `API_KEYS` overrides `API_KEYS_FILE`.
The service name defaults to `fluxgate-ingest-api`, `fluxgate-aggregator`,
`fluxgate-query-api`, or `migrate`. Restart or roll a new revision
after changing configuration or the key document; there is no live reload.

Durations use Go syntax (`250ms`, `30s`, `5m`, `24h`), not `1d`. Negative durations
are rejected. Byte sizes accept integers or case-insensitive `K`, `KB`, `M`, `MB`,
`G`, `GB` suffixes. These are powers of 1,024: `4MB` is 4 MiB. Use whole numbers,
not `1.5MB` or `4MiB`. Prefer literal `true`/`false` for booleans. Numeric floats
must be finite.

| Service | Required dependencies |
| --- | --- |
| `ingest-api` | Credentials unless auth disabled; project when Pub/Sub enabled; PostgreSQL required on staging/prod for retry reservations |
| `aggregator` | PostgreSQL and a functioning Pub/Sub transport; no application API keys |
| `query-api` | PostgreSQL and credentials unless auth disabled; no Pub/Sub requirement |
| `migrate` | Owner database connection; optional Cloud SQL connector; no application API keys |
| `loadgen` | Uses command-line flags, not the shared configuration loader |

The shared loader parses all configuration groups. Many validation rules apply
even when a binary does not use that group; pass only relevant overrides. A
configuration failure lists all invalid keys together.

## Identity, HTTP, logs, and shutdown

| Variable | Default | Meaning / constraint |
| --- | --- | --- |
| `ENVIRONMENT` | `local` | `local`, `dev`, `staging`, or `prod`; staging/prod enable runtime safeguards |
| `SERVICE_NAME` | Service identity above | Log/trace/metric identity; normally leave unchanged |
| `HTTP_ADDR` | `:8080` | Listener address; use `127.0.0.1:8080` for a native loopback-only process |
| `PORT` | Unset | When nonempty, listener becomes `:<PORT>` and overrides `HTTP_ADDR` |
| `HTTP_READ_HEADER_TIMEOUT` | `5s` | Positive header read deadline |
| `HTTP_READ_TIMEOUT` | `15s` | Positive request read deadline |
| `HTTP_WRITE_TIMEOUT` | `20s` | Positive ordinary response deadline |
| `HTTP_IDLE_TIMEOUT` | `120s` | Positive keep-alive idle deadline |
| `HTTP_HANDLER_TIMEOUT` | `10s` | Positive request context deadline; must be shorter than write timeout; live stream uses separate bounds |
| `HTTP_MAX_REQUEST_BYTES` | `4MB` | Positive body limit |
| `HTTP_MAX_CONCURRENT` | `4` | Positive per-instance active non-stream data-handler limit; excess requests receive 503 |
| `HTTP_TRUST_PROXY_HEADER` | `false` | Trust rewritten `X-Forwarded-For` for client address only behind a controlled proxy |
| `HTTP_TRUST_TRACE_PARENT` | `false` | Permit incoming trace context to control parent sampling only behind a trusted gateway |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `LOG_FORMAT` | `json` | `json` or `text` |
| `LOG_ADD_SOURCE` | `false` | Include source file/line in logs |
| `SHUTDOWN_GRACE_PERIOD` | `5s` | Time between failing readiness and closing HTTP admission; zero allowed |
| `SHUTDOWN_DRAIN_TIMEOUT` | `25s` | Positive bound for draining in-flight work |

Do not raise request concurrency independently of the maximum body size,
container memory, publisher buffering, and database pool. A client timeout does
not cancel an already committed publish. Use [safe retries](api.md#safe-retries).

## Authentication and ingestion

| Variable | Default | Meaning / constraint |
| --- | --- | --- |
| `AUTH_DISABLED` | `true` only in local; otherwise `false` | Attributes unauthenticated requests to `anonymous`; forbidden for staging/prod data APIs |
| `API_KEYS` | Empty | JSON array of credential records; required when auth enabled unless file supplied |
| `API_KEYS_FILE` | Empty | File containing the same JSON array; inline value wins |
| `INGEST_MAX_POINTS_PER_BATCH` | `1000` | 1–1,000 points |
| `INGEST_MAX_CLOCK_SKEW` | `5m` | Nonnegative allowed future timestamp offset |
| `INGEST_MAX_BACKFILL` | `168h` | Positive allowed age at HTTP admission |
| `RATE_LIMIT_POINTS_PER_SECOND` | `10000` | Positive default sustained tenant rate per instance |
| `RATE_LIMIT_BURST` | `20000` | Positive default burst; at least maximum batch size |
| `IDEMPOTENCY_TTL` | `24h` | Positive for ingest; at most `168h`; reservation expiry is measured from initial reservation |

Metric/label length, value magnitude, and label-count bounds are fixed domain
limits, documented in [API ingestion](api.md#ingestion). They are not environment
knobs. API key fields and rotation are documented in [security](security.md).

## Pub/Sub transport

| Variable | Default | Meaning / constraint |
| --- | --- | --- |
| `PUBSUB_ENABLED` | `false` for local without emulator, otherwise `true` | Select broker transport over the development sink; required on staging/prod transport services |
| `GCP_PROJECT_ID` | Empty | Required when transport is enabled |
| `PUBSUB_EMULATOR_HOST` | Empty | `host:port`; also read by Google's client; forbidden on staging/prod |
| `PUBSUB_BOOTSTRAP` | `true` if emulator configured, otherwise `false` | Create development topology; forbidden on staging/prod |
| `PUBSUB_TOPIC_RAW` | `telemetry-raw` | Accepted batch topic |
| `PUBSUB_TOPIC_DLQ` | `telemetry-dlq` | Dead-letter topic |
| `PUBSUB_SUBSCRIPTION_AGGREGATOR` | `telemetry-aggregator` | Shared worker subscription |
| `PUBSUB_RETENTION` | `24h` | Source replay horizon used for topology/retention validation; 10m–744h |
| `PUBSUB_DLQ_RETENTION` | `168h` | Dead-letter horizon; 10m–744h |
| `PUBSUB_PUBLISH_TIMEOUT` | `10s` | Positive publish confirmation deadline |
| `PUBSUB_BATCH_DELAY` | `10ms` | Positive client-side publish batching delay |
| `PUBSUB_BATCH_COUNT` | `100` | 1–1,000 broker messages per client batch; not telemetry points |
| `PUBSUB_MAX_OUTSTANDING_MESSAGES` | `1000` | Positive publisher buffering ceiling |
| `PUBSUB_MAX_OUTSTANDING_BYTES` | `64MB` | Publisher buffered wire-byte budget |
| `PUBSUB_BREAKER_FAILURE_THRESHOLD` | `5` | Positive consecutive failures before opening publisher circuit |
| `PUBSUB_BREAKER_COOLDOWN` | `10s` | Positive delay before a recovery probe |

Environment retention values do not update an existing managed subscription.
Change the infrastructure configuration too, and keep worker validation consistent
with the actual replay horizon. Production topology comes from Terraform.

## Database

| Variable | Default | Meaning / constraint |
| --- | --- | --- |
| `DATABASE_URL` | Empty | PostgreSQL DSN; use the service-specific runtime role outside development |
| `CLOUD_SQL_INSTANCE` | Empty | Optional `project:region:instance`; enables private-IP Cloud SQL connector |
| `DATABASE_MAX_CONNS` | `10` | Positive pool ceiling per process |
| `DATABASE_MIN_CONNS` | `2` | Nonnegative warm pool floor, no greater than maximum |
| `DATABASE_MAX_CONN_LIFETIME` | `1h` | Positive connection recycling interval when DSN supplied |
| `DATABASE_CONNECT_TIMEOUT` | `10s` | Positive individual connection/startup probe timeout |
| `DATABASE_MIGRATE` | `true` in local/dev, `false` in staging/prod | Ingest/aggregator development startup migrations; forbidden on staging/prod with a DSN |

The query binary never applies migrations. The migration command explicitly
applies them regardless of `DATABASE_MIGRATE`, with a separate owner identity,
one connection, and a five-minute operation deadline. The Cloud Run migration
job has its own ten-minute platform timeout and no automatic retries.

With `CLOUD_SQL_INSTANCE`, the connector handles IAM connection authorization and
TLS over private IP; the database still uses the username/password in the DSN.
This is not IAM database authentication. The Terraform DSN's `sslmode=disable`
is appropriate only inside that connector transport. For direct remote
PostgreSQL connections, configure certificate-verifying TLS in the DSN.

Budget every pool across maximum replicas and overlapping deployment revisions.
Worker ledger lookups, checkpoints, and cleanup share the same pool. The
[Terraform budget](terraform-reference.md#deployment-gates) reserves two full
revisions plus 20 administrative connections.

## Aggregation and retention

| Variable | Default | Meaning / constraint |
| --- | --- | --- |
| `AGGREGATOR_WINDOW_SIZE` | `1m` | Tumbling event-time window; 1s–24h, whole microseconds |
| `AGGREGATOR_ALLOWED_LATENESS` | `30s` | Nonnegative event-time watermark lag; not a finality or deletion deadline |
| `AGGREGATOR_IDLE_TIMEOUT` | `30s` | Positive quiet period before processing-time watermark advancement |
| `AGGREGATOR_MAX_SERIES` | `100000` | Positive limit on tracked series entries across open windows |
| `AGGREGATOR_MAX_BUFFERED_BYTES` | `128MB` | Estimated retained engine budget, at least 32MB |
| `AGGREGATOR_MAX_OUTSTANDING_BYTES` | `16MB` | Subscriber wire bytes retained until settlement, at least 10MB |
| `AGGREGATOR_MAX_OUTSTANDING_MESSAGES` | `1000` | Positive subscriber message bound |
| `AGGREGATOR_CONCURRENCY` | `2` | Positive number of streaming pull connections; not checkpoint workers |
| `AGGREGATOR_FLUSH_INTERVAL` | `15s` | Positive partial-window checkpoint interval, no greater than window size |
| `AGGREGATOR_FLUSH_CONCURRENCY` | `4` | 1–16 simultaneous tenant transactions per worker |
| `AGGREGATOR_STORAGE_TIMEOUT` | `1m` | 1s–2m; bounds one ledger lookup or the entire checkpoint, including queued tenants |
| `ROLLUP_RETENTION` | `720h` | Positive age horizon for pruning rollups by window end |
| `LEDGER_RETENTION` | `768h` | At least one window; staging/prod also require the replay-horizon equation below |
| `PRUNE_INTERVAL` | `5m` | Positive cleanup cadence |

On staging/prod the worker requires:

```text
LEDGER_RETENTION >= PUBSUB_RETENTION + PUBSUB_DLQ_RETENTION + IDEMPOTENCY_TTL + 24h
```

Every worker must agree on window size and retention policy. Changing window
width can change delivery identities and mix incompatible rollups; treat it as a
data migration with a replay/cutover plan, not an ordinary rolling knob change.
Existing stored histogram layout also cannot be changed through configuration.

Estimated buffered bytes and subscriber wire bytes are not RSS limits. Decoded
envelopes, flush snapshots, histogram arrays, SQL serialization, connection pools,
and the Go runtime need headroom. `GOMEMLIMIT` is a Go runtime soft memory target,
not a replacement for application admission or the container hard limit.

## Query and stream bounds

| Variable | Default | Meaning / constraint |
| --- | --- | --- |
| `QUERY_MAX_RANGE` | `744h` | Positive maximum resolved range |
| `QUERY_ALLOWED_ORIGINS` | empty | Comma-separated allowlist of at most 16 exact browser origins; HTTPS on staging/prod; no wildcards, paths, or credentials. Same-origin dashboards need no CORS setting. See [dashboard connections](dashboard.md#connect-your-data). |
| `QUERY_DEFAULT_RANGE` | `1h` | Positive default range, no greater than maximum |
| `QUERY_MAX_SERIES` | `500` | Positive maximum returned kind/label series |
| `QUERY_MAX_POINTS` | `50000` | 1–50,000 returned points across all series |
| `QUERY_STREAM_MAX_CONCURRENT` | `100` | Positive total streams per instance |
| `QUERY_STREAM_MAX_PER_TENANT` | `8` | Positive per-tenant streams; effective limit capped by instance total |
| `QUERY_STREAM_POLL_INTERVAL` | `2s` | Positive update-poll cadence |
| `QUERY_STREAM_HEARTBEAT` | `20s` | Positive idle comment cadence, shorter than stream duration |
| `QUERY_STREAM_MAX_DURATION` | `30m` | Positive connection lifetime; clients reconnect and reconcile |

Fixed implementation bounds include 8 MiB per materialized query read, 256 KiB
per live-update page, 500 rows per live poll, 5s per stream database operation,
and 10s per stream write. Discovery list limits are described in the
[API reference](api.md#discovery).

## Observability

| Variable | Default | Meaning / constraint |
| --- | --- | --- |
| `TRACING_ENABLED` | `true` when OTLP endpoint nonempty, otherwise `false` | Requires an endpoint when enabled |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Empty | OTLP gRPC collector address, e.g. `collector:4317` |
| `OTEL_EXPORTER_OTLP_INSECURE` | `true` local/dev, `false` staging/prod | Disable collector TLS only on a deliberately trusted transport |
| `TRACE_SAMPLE_RATIO` | `1` local/dev, `0.05` staging/prod | Fraction between 0 and 1 |
| `TRACE_EXPORT_TIMEOUT` | `10s` | Positive exporter deadline |
| `METRICS_ENABLED` | `true` local/dev, `false` staging/prod | Public ingest/query APIs reject `true` on staging/prod; internal aggregator can expose it |
| `METRICS_PATH` | `/metrics` | Literal slash-prefixed path using letters/digits/underscore/hyphen/slash; cannot occupy `/v1*`, `/healthz`, or `/readyz` |

The internal aggregator's metrics need an explicitly configured private scraper
in GCP. Terraform supplies platform alerts but does not install a Prometheus
scraping agent or an OTLP collector. See [operations](operations.md).

## Compose and Terraform overrides

| Setting | Local Compose | Supplied Terraform runtime |
| --- | --- | --- |
| Environment | `dev` | `environment` input |
| Auth | Shared local key for tenant `acme` | Pinned Secret Manager version |
| Database users | Shared local owner | Separate ingest/aggregator/query users |
| Migrations | Development startup | Separate migration job; runtime disabled |
| Window / checkpoint / lateness | 10s / 2s / 2s | 1m / 15s / 30s binary defaults |
| Idle advancement | 5s | 30s binary default |
| Ledger retention | 1h, for disposable local data | Source retention + 7d DLQ + 2d retry/safety |
| Pool max / min | 10 / 2 | `database_pool_connections` (default 5) / 0 |
| Query stream poll | 1s | 2s binary default |
| Publish / handler / write deadlines | 10s / 10s / 20s | 4s / 6s / 10s |
| Shutdown grace / drain | 5s / 25s | 0s / 5s |
| Traces | Jaeger, sample 1, insecure local transport | Disabled until collector provided; TLS, sample 0.05 |
| Metrics | Enabled on all three services | Enabled only on internal aggregator |
| `GOMEMLIMIT` | Unset | Worker `700MiB`, APIs `350MiB` |

Compose's one-hour ledger is intentionally unsuitable for a durable replay policy.
Do not transfer development overrides into production. Review the actual
[Compose](../deploy/docker-compose.yml) and [Cloud Run configuration](../deploy/terraform/cloudrun.tf)
when changing defaults.
