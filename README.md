# Fluxgate

[![CI](https://github.com/jon-jc/fluxgate/actions/workflows/ci.yml/badge.svg)](https://github.com/jon-jc/fluxgate/actions/workflows/ci.yml)

A Go telemetry pipeline built on Google Cloud Pub/Sub and PostgreSQL. Fluxgate
accepts metric batches over HTTP, aggregates them into event-time windows, and
serves the results through REST queries and server-sent events.

The pipeline handles retries across replicas, duplicate deliveries, late data,
bounded resource use, and recovery after process or database failures. Deployment
configuration targets Cloud Run and private Cloud SQL, with separate runtime and
migration identities.

**Status:** production hardening is implemented and exercised in local/CI tests.
Real GCP staging, capacity, IAM, alerting, and recovery validation remain release
gates. See [production deployment](#production-deployment) before serving live traffic.

[Quick start](#quick-start) · [Architecture](#architecture) · [API](#api) ·
[Verification](#verification) · [Configuration](#configuration) ·
[Production deployment](#production-deployment) · [Development](#development)

## Documentation

Read the **[hosted documentation](https://fluxgate-docs.vercel.app)** for searchable
guides and the **[API reference](https://fluxgate-docs.vercel.app/api-reference/)**
with endpoint examples, response schemas, and OpenAPI downloads.

The [documentation hub](docs/README.md) contains the complete guides and references:

| For | Read |
| --- | --- |
| First-time users | [Getting started](docs/getting-started.md) |
| Client developers | [API, retries, queries, and live streams](docs/api.md) · [OpenAPI](api/openapi.yaml) |
| System design | [Architecture and data guarantees](docs/architecture.md) · [Decisions](docs/adr/README.md) |
| Operators | [Configuration](docs/configuration.md) · [Security and key rotation](docs/security.md) · [Operations](docs/operations.md) · [Recovery](docs/recovery.md) |
| Release owners | [Deployment and acceptance](docs/deployment.md) · [Terraform reference](docs/terraform-reference.md) · [Capacity](docs/capacity.md) · [Measured results](docs/capacity-results.md) |
| Contributors | [Development, command reference, and verification](docs/development.md) |

## Quick start

Requires Docker with Compose. The stack includes the three services, PostgreSQL,
the Pub/Sub emulator, Jaeger, Prometheus, and Grafana. No GCP project is needed.

```sh
git clone https://github.com/jon-jc/fluxgate.git
cd fluxgate
docker compose -f deploy/docker-compose.yml up -d --build
```

`make up` runs the same command if Make and Bash are available. Published ports
bind to loopback; the example credential belongs only to this local stack.

| Service | Local address |
| --- | --- |
| Ingest API | http://localhost:8080 |
| Aggregator probes | http://localhost:8081 |
| Query API | http://localhost:8082 |
| Jaeger | http://localhost:16686 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 — preconfigured dashboard, no login |
| Pub/Sub emulator | `localhost:8681` |
| PostgreSQL | `localhost:5442` — database/user/password: `fluxgate` |

Send a batch, using Bash/curl syntax:

```sh
curl -sS http://localhost:8080/v1/ingest \
  -H 'Authorization: Bearer fxg_local_local-dev-secret' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: quickstart-001' \
  -d '{"points":[
    {"metric":"queue.depth","kind":"gauge","value":42},
    {"metric":"http.request.duration_ms","kind":"histogram","value":12.5,
     "labels":{"service":"checkout"}}
  ]}'
```

A successful response reports the batch identity and per-point outcome:

```json
{"batch_id":"<generated-id>","accepted":2,"rejected":0}
```

After a local checkpoint (normally within a few seconds), query the result.
Windows can be visible before they close and receive later corrections:

```sh
curl -sS -H 'Authorization: Bearer fxg_local_local-dev-secret' \
  'http://localhost:8082/v1/query?metric=queue.depth&from=-15m&agg=sum'
```

Repeat the same body and idempotency key to replay the original outcome. Use a
new key for a new observation. In PowerShell, the equivalent ingestion call is:

```powershell
$headers = @{ Authorization = 'Bearer fxg_local_local-dev-secret'; 'Idempotency-Key' = 'quickstart-powershell-001' }
$body = '{"points":[{"metric":"queue.depth","kind":"gauge","value":42}]}'
Invoke-RestMethod -Uri http://localhost:8080/v1/ingest -Method Post -Headers $headers -ContentType 'application/json' -Body $body
```

Check `/healthz`, `/readyz`, and `/v1/version` on each service. Readiness reports
dependency status and keeps error details in logs. Stop the stack while retaining
its PostgreSQL volume with:

```sh
docker compose -f deploy/docker-compose.yml down
```

The emulator's state is ephemeral. `make down` also removes the PostgreSQL volume
and discards local database data.

### Ingest-only development

CI and container builds use Go 1.27; the module minimum is Go 1.26. With no
configuration overrides, this starts the local ingest process:

```sh
go run ./cmd/ingest-api
```

Local defaults disable authentication and use an in-memory sink. This mode is
useful for handler development; it does not persist telemetry or run the query
pipeline. Use Compose or the packaged verification below for the complete flow.

To build and run that same local-only mode in a container:

```sh
docker build -f build/docker/Dockerfile --build-arg SERVICE=ingest-api -t fluxgate/ingest-api:local .
docker run --rm -p 127.0.0.1:8080:8080 -e ENVIRONMENT=local fluxgate/ingest-api:local
```

## Architecture

```mermaid
flowchart LR
    C[Clients] -->|HTTP batches| I[ingest-api]
    I -->|retry reservations| P[(PostgreSQL)]
    I -->|confirmed publish| T((Pub/Sub raw topic))
    T --> A[aggregator]
    T -.->|dead-letter policy| D((Dead-letter topic))
    A -->|rollups + delivery ledger| P
    P --> Q[query-api]
    Q -->|REST + SSE| C
    M[migrate job] -->|schema + runtime grants| P
```

| Component | Responsibility |
| --- | --- |
| `ingest-api` | Authenticate, validate, meter points, reserve retry identity, and publish |
| `aggregator` | Consume, buffer bounded windows, persist rollups, acknowledge durable work, and prune retained data |
| `query-api` | Read tenant-scoped rollups, metric/label metadata, and committed live updates |
| `migrate` | Apply schema migrations and provision restricted database roles |
| `loadgen` | Generate synthetic HTTP traffic for development and measurements |

### Delivery and aggregation guarantees

With Pub/Sub enabled, a `202` follows broker-confirmed publication, or replays a
previously confirmed outcome. Aggregation is asynchronous: acceptance does not
mean a rollup is immediately queryable. A timeout or `503` can leave publication
ambiguous, so retry with the same idempotency key and identical body.

PostgreSQL reserves the batch identity, server timestamps, and response before
publication. Shared retry outcomes survive replica changes and restarts within
`IDEMPOTENCY_TTL` (24 hours by default). A changed body with the same key returns
`409`; without a key, each HTTP attempt is a new batch. Do not retry an old
operation after its key expires. Completed reservations release their point
payload; pending publications retain it for safe retry.

Pub/Sub delivers at least once. Fluxgate suppresses duplicate accumulation using
transactional claims keyed by **tenant, batch, and window**. Rollups and claims
commit together, and the message is acknowledged only after every contributing
window is durable. Overlapping claims roll back the flush for redelivery.

| Failure point | Recovery |
| --- | --- |
| Before commit | Unacknowledged work is redelivered and accumulated again |
| After commit, before acknowledgment | The persisted ledger suppresses the duplicate |
| During an overlapping replica flush | The transaction rolls back; retry excludes contributions already committed |

This guarantee lasts only as long as the delivery ledger is retained. Archive
replay beyond that horizon needs the ledger preserved or rebuilt first.

Workers checkpoint buffered data every flush interval and earlier when admitted
deliveries consume half the configured receive byte/message budget. This frees
broker receive slots without waiting for newer events to close a window. Queries
can therefore see partial windows. Late accepted data produces additive
corrections, including after a window has
already committed. Admission is atomic for each batch: capacity exhaustion
returns the whole batch for redelivery. Sustained overload can reach the
configured dead-letter policy and requires operator attention.

Delivery-ledger reads run concurrently within the receive and database-pool
limits. A slow lookup does not hold the shared admission lock or delay collecting
a checkpoint. If a checkpoint resolves during a lookup, the worker refreshes
that snapshot before admission, within the original storage deadline. This keeps
duplicate suppression intact across concurrent reads, commits and shutdown.

Checkpoints use independent transactions per tenant, with up to four running at
once per aggregator. A blocked tenant or conflicting delivery does not roll back
another tenant's committed work. Each transaction retains atomic rollups, delivery
claims and commit-ordered live-query revisions. All queued tenant work shares one
checkpoint deadline; failed tenants return for redelivery independently.

Each rollup is keyed by tenant, metric name, kind, labels, and window. Stored
statistics are count, sum, min, max, and last, plus fixed exponential buckets for
histograms. Last-value ordering uses event timestamps at PostgreSQL microsecond
precision; equal timestamps choose the larger value. Histogram percentiles are
estimates derived from the stored buckets.

## API

Data routes require `Authorization: Bearer fxg_<key_id>_<secret>` when
authentication is enabled. The credential selects the tenant; clients cannot
select another tenant through a request parameter. Only SHA-256 secret digests
are stored. Credential identifiers and secret lengths are bounded before lookup.

See [the OpenAPI contract](api/openapi.yaml) for request/response schemas and
error codes. Errors use `application/problem+json` with a stable `code` field;
internal causes are logged rather than exposed.

| Endpoint | Service | Purpose |
| --- | --- | --- |
| `POST /v1/ingest` | Ingest | Submit a batch of points |
| `GET /v1/query` | Query | Read a metric's aggregated series |
| `GET /v1/metrics` | Query | List stored metric metadata |
| `GET /v1/labels` | Query | Discover label keys or values |
| `GET /v1/stream` | Query | Stream committed rollup updates over SSE |
| `GET /healthz`, `GET /readyz`, `GET /v1/version` | All three services | Process health, dependencies, and build identity |

### Input and retry rules

- Batches contain at most 1,000 points. Valid points can be accepted while invalid
  points are rejected; always inspect `accepted`, `rejected`, and `errors` on a
  `202` response. Error lists may be truncated while counts remain exact.
- Kinds are `gauge`, `counter`, and `histogram`. Counters are nonnegative
  increments, not cumulative totals.
- `value` is required, finite, and bounded to ±1e100. Zero is valid; missing or
  null values are rejected.
- Omitted/null timestamps use arrival time; explicit zero timestamps are invalid.
  The default HTTP acceptance range is
  seven days of backfill and five minutes of future clock skew. Broker delivery
  validates structure without rejecting retained messages for their age.
- A point has at most 20 labels. Names, label sizes, and UTF-8 are validated;
  labels beginning with `__` are reserved.
- Point-rate limits and stream quotas apply per tenant **per instance**. Scaling
  replicas multiplies the allowance. Hard tenant-wide quotas require a shared
  gateway or another coordinated mechanism.
- Honor `Retry-After` on `429`/`503` responses. Preserve the same body and
  idempotency key across retries, including ambiguous publication failures.

### Queries and live updates

Queries accept RFC 3339 timestamps or relative ranges such as `from=-15m`.
Relative `from` is measured from `to`; responses echo the resolved range. Label
filters use `label.<name>`, for example `label.service=checkout`.

```sh
curl -sS -H 'Authorization: Bearer fxg_local_local-dev-secret' \
  'http://localhost:8082/v1/query?metric=http.request.duration_ms&from=-15m&agg=p95&label.service=checkout'

curl -N -H 'Authorization: Bearer fxg_local_local-dev-secret' \
  'http://localhost:8082/v1/stream?metric=queue.depth'
```

Percentiles require histogram data; unsupported percentile queries return
warnings rather than fabricated values. Row/series limits set `truncated` when
results are cut short. Reads that exceed the materialization byte budget return
`422`; narrow the time range or add label filters.

SSE follows committed tenant revisions, so a slow transaction cannot commit
behind an already advanced cursor. Updates to the same rollup between polls may
coalesce. Reconnects begin at the current revision; refresh REST queries to
reconcile missed updates. Streams have separate connection quotas, bounded
polls, socket-write deadlines, and maximum lifetimes.

## Verification

Requires Docker and Python 3. Run the same packaged workload used by CI:

```sh
python scripts/verify_pipeline.py --load-batches 200
python scripts/verify_build_context.py
python scripts/audit_images.py --output-dir ../image-audit/reports --cache-dir ../image-audit/cache
```

The pipeline script builds all four release images, creates disposable containers
on a unique network with random loopback ports, and removes its test resources
when finished. Use `--skip-build` to reuse local validation images, or
`--load-batches 2000` for one million points. No cloud resources are used.

The check covers:

- Two ingest replicas, persisted retry identities, changed-body conflicts, and
  tenant isolation.
- Sixteen concurrent clients submitting 500-point batches, with every batch
  replayed through the other replica. All 600 series reconcile count, sum, min,
  max, and last across counter, gauge, and histogram data.
- Rate/concurrency backpressure, liveness during load, sampled resident memory,
  and container restart/OOM status. APIs have 512 MiB limits; the aggregator has
  1 GiB. Services run as nonroot with read-only filesystems and dropped capabilities.
- Aggregator termination before commit, broker redelivery, database outages, and
  retention spanning more than one 10,000-row cleanup transaction.
- A quiesced PostgreSQL backup restored into a separate database. All five
  application tables are compared before restricted services verify HTTP retry
  identity, broker duplicate suppression, and a fresh write.

### Recorded local results

The September 28, 2026 million-point fixture reconciled **1,000,000 points across
600 series in 49.1 seconds**, including cross-replica replays. It handled 658
rate-limit and 24 concurrency-limit rejections. Sampled aggregator RSS peaked at
116.94 MiB. The fixture used fixed timestamps and eight admitted requests per API
replica; the default is four. These are local observations, not GCP capacity or
latency guarantees. The final crypto dependency refresh was subsequently checked
with the complete 100,000-point load and recovery run.

For sustained traffic with current timestamps, multiple tenants, higher series
counts and concurrent queries, use the [capacity measurement guide](docs/capacity.md).
Its open-loop generator records offered versus accepted rate, missed work,
durable visibility delay, backlog, memory and exact per-series/window totals.
The on-demand [Capacity workflow](.github/workflows/capacity.yml) saves the JSON
evidence; CI runs a smaller sustained correctness profile on every PR.

The September 29 sustained tests exposed and fixed a receive-buffer stall. At
3,000 points/s across 40,000 series, p95 durable visibility fell from 149.9s to
13.9s. Bulk SQL then brought a 100,000-series run to 9,806 accepted points/s with
6.47s p95 visibility. A two-worker run reconciled 1.721 million points, but its
growing backlog showed that its 20,000 points/s offer exceeded sustainable
capacity on the tested resources. See [measured profiles, missed submissions and
resource limits](docs/capacity-results.md) before sizing a deployment.
The 3,000 points/s, 100,000-series bulk baseline accepted every offered point
without HTTP rejections; this is a 90-second local baseline, not a production SLO.

After concurrent ledger reads and independent tenant checkpoints, a hosted
two-worker run accepted and reconciled **1.2 million points at an offered
10,000 points/s for 120 seconds across 100,000 active series**, with no missed
submissions or HTTP rejections. Achieved throughput was 9,998 points/s, visibility
p95 was 9.1s and query p95 about 124ms. Hardware and run
duration differ from the earlier local profiles; this is a measured workload,
not a production rate guarantee. [Hosted evidence](https://github.com/jon-jc/fluxgate/actions/runs/37056416156).

CI also applies an explicit [workload and latency budget](deploy/capacity/ci.json).
Use [capacity qualification](docs/capacity.md#qualifying-a-measured-profile) to fail
a release profile that reconciles its data but misses traffic or exceeds its
visibility, query or drain limits. The checker works offline on saved evidence.

### CI and release scanning

[CI](.github/workflows/ci.yml) runs race-enabled Go tests, PostgreSQL/Pub/Sub
integration, lint, reachable-code vulnerability checks, Terraform validation and
mocked plans, container builds, and the packaged load/recovery check. Integration
checks fail if their required fixtures silently skip.

Compiler/runtime base images and GitHub Actions use immutable pins. Docker's
allowlist admits only compilation inputs, with a regression check that excludes
dummy credential files at the root and inside source directories. Images use a
nonroot distroless runtime.

The pinned Trivy scanner uses a current advisory database and blocks MEDIUM,
HIGH, and CRITICAL findings, including unfixed ones. Scanner failures fail CI.
JSON reports and CycloneDX inventories are retained for 30 days. LOW/UNKNOWN
findings remain visible for review; [dated advisory notes](docs/security-scanning.md)
explain the current OpenPGP and timezone-data findings. A passing scan is not a
claim that the service is free of vulnerabilities.

## Configuration

Settings are read from the environment and validated at startup. See
[.env.example](.env.example) for examples and
[the configuration loader](internal/config/config.go) for defaults and constraints.
The application does not automatically load `.env` files. Local example values
are not production credentials or deployment settings.

| Setting | Default / behavior |
| --- | --- |
| `ENVIRONMENT` | `local`; also supports `dev`, `staging`, `prod` |
| `API_KEYS` / `API_KEYS_FILE` | JSON key document; required when authentication is enabled |
| `AUTH_DISABLED` | True locally; forbidden on staging/prod |
| `PUBSUB_ENABLED` | False locally unless an emulator is configured; required on staging/prod |
| `GCP_PROJECT_ID` | Required when Pub/Sub is enabled |
| `DATABASE_URL` | Required for aggregator/query and for shared ingest retries on staging/prod |
| `CLOUD_SQL_INSTANCE` | Enables the IAM-authorized private-IP Cloud SQL connector |
| `DATABASE_MIGRATE` | False and required to remain false on staging/prod; use the migration job |
| `IDEMPOTENCY_TTL` | `24h`; retry identities must survive the client's retry horizon |
| `RATE_LIMIT_POINTS_PER_SECOND` / `RATE_LIMIT_BURST` | `10000` / `20000` per tenant per instance; key configuration can override |
| `HTTP_MAX_CONCURRENT` | 4 active non-stream data requests per API instance; excess receives `503` |
| `AGGREGATOR_WINDOW_SIZE` / `AGGREGATOR_ALLOWED_LATENESS` | `1m` / `30s` |
| `AGGREGATOR_MAX_SERIES` | 100,000 tracked series |
| `AGGREGATOR_MAX_BUFFERED_BYTES` | 128 MiB estimated accumulation budget |
| `AGGREGATOR_MAX_OUTSTANDING_BYTES` | 16 MiB subscriber wire-data budget |
| `QUERY_MAX_RANGE` / `QUERY_MAX_SERIES` / `QUERY_MAX_POINTS` | `744h` / 500 / 50,000 |
| `QUERY_STREAM_MAX_CONCURRENT` / `QUERY_STREAM_MAX_PER_TENANT` | 100 / 8 per instance; tenant exhaustion returns `429`, total exhaustion `503` |
| `ROLLUP_RETENTION` / `LEDGER_RETENTION` | `720h` / `768h`; align the ledger with raw, dead-letter, and retry retention |
| `PRUNE_INTERVAL` | `5m`; cleanup uses bounded chunks and per-table deadlines |
| `AGGREGATOR_FLUSH_CONCURRENCY` | 4 tenant transactions per aggregator, configurable from 1 to 16; uses the existing database pool |
| `HTTP_TRUST_PROXY_HEADER` / `HTTP_TRUST_TRACE_PARENT` | False; enable only behind a gateway that rewrites/authenticates the corresponding headers |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | A configured endpoint enables tracing; production requires a real TLS collector |
| `TRACE_SAMPLE_RATIO` | 1 on local/dev; 0.05 on staging/prod |

Additional fixed bounds include an 8 MiB query materialization budget, 256 KiB
live-update pages, a 32 KiB HTTP header setting, and at most 256 rows
per bulk SQL write within one tenant transaction. Ledger lookups and whole checkpoints,
including queued tenant transactions, have a one-minute deadline (`AGGREGATOR_STORAGE_TIMEOUT`, configurable
from one second to two minutes); timed-out work returns for redelivery. Memory budgets estimate admitted data;
they are not RSS limits. Measure actual container memory before increasing them.

### Issuing an API key

Generate a secret and its SHA-256 digest, for example in Bash:

```sh
SECRET=$(openssl rand -hex 32)
echo "client credential: fxg_k1_$SECRET"
printf %s "$SECRET" | sha256sum | cut -d' ' -f1
```

Place the digest in `API_KEYS`, a file referenced by `API_KEYS_FILE`, or the
Secret Manager document used by the deployment:

```json
[{"key_id":"k1","tenant_id":"acme","secret_sha256":"<digest>",
  "rate_limit_per_second":5000,"burst":10000}]
```

Keep the plaintext credential in the client's secret store. The document must be
one complete JSON array with valid tenant/key identifiers. See the deployment
runbook for secret versions, rotation, and service identities.

## Observability

Local Compose includes a provisioned Grafana dashboard, Prometheus, and Jaeger.
Each service exposes `/metrics` when enabled. The GCP configuration disables
public application metrics and uses native Cloud Run, SQL, and Pub/Sub metrics
for its four operational alert policies.

| Signal | Purpose |
| --- | --- |
| `fluxgate_aggregate_watermark_lag_seconds` | Event-time progress relative to wall clock |
| `fluxgate_aggregate_tracked_series` | Cardinality pressure |
| `fluxgate_aggregate_buffered_bytes` | Estimated accumulation memory |
| `fluxgate_publish_batches_total` | Successful, failed, or shed publishes |
| `fluxgate_consume_messages_total` | Accepted, retried, or rejected deliveries |
| `fluxgate_resilience_breaker_state` | Closed, half-open, or open publish circuit |

Metrics use bounded route patterns, method labels, and status classes. Unknown
methods share `_OTHER`. Request IDs, trace IDs, and span IDs connect structured
logs with HTTP and broker work.

Public HTTP trace parents are linked to a fresh locally sampled trace; arbitrary
baggage is discarded. Trusted gateways can opt into parent continuation.
Internal Pub/Sub propagation preserves the edge's sampling decision. Span data
and export queues are bounded; collector failures are handled without making
export success a request-path dependency.

Liveness and readiness are separate. Shutdown fails readiness, allows a grace
period, and drains work within a deadline. Uncommitted broker deliveries remain
available for retry if the process cannot finish.

## Production deployment

Use the [GCP deployment and recovery runbook](deploy/terraform/README.md).
Terraform provisions Pub/Sub and dead-letter topology, private Cloud SQL,
service-specific IAM and database roles, Secret Manager, Cloud Run services, a
migration job, and operational alerts.

Deployment is staged: bootstrap with `deploy_services = false`, publish reviewed
images and record their digests, select an explicit API-key secret version, run
the migration job with runtime-role provisioning, and only then enable services.
Runtime processes cannot migrate the deployed schema or create broker topology.
The aggregator uses a fixed instance count; aggregator/query CPU remains
allocated between requests. Connection budgets account for overlapping revisions.

Before production traffic, validate in a real staging project:

1. Private SQL connectivity, Cloud Run lifecycle behavior, and actual IAM/database
   permission boundaries.
2. Broker redelivery, dead-letter handling, and alert delivery.
3. Expected peak traffic, cardinality, query mix, and retention throughput, with
   agreed latency, memory, backlog, and connection headroom.
4. Backup/PITR restore, replay, client cutover, key rotation, and rollback, with
   measured recovery objectives and operational ownership.

For existing deployments, take a verified backup and stop old aggregators before
migrations **0003 and 0005**. Migration **0007** corrects the retention-index
collision in 0006; index creation can block writes, so schedule a maintenance
window. Rerun runtime-role provisioning. Changing an image does not undo an
incompatible schema change.

A database restore does not rewind Pub/Sub acknowledgments. Restore rollups,
delivery claims, retry reservations, stream revisions, and migration records
consistently, then coordinate replay. If retry reservations were lost, recover
them or coordinate affected clients before reopening ingestion: broker replay
alone cannot reconstruct HTTP retry outcomes. The local restore check uses
quiesced writers and does not certify Cloud SQL PITR or production recovery time.

Tenant rule evaluation and notification delivery are not implemented. The
supplied alerts cover the pipeline's own operational health; see
[ADR 7](docs/adr/0007-deferred-alerting.md).

## Development

Make targets require Make and Bash. Useful commands include:

```sh
make help              # available targets
make build             # build every command
make test              # Go tests with the race detector; requires cgo
make test-short        # Go tests without the race detector
make test-integration  # start local dependencies and run integration tests
make lint              # requires golangci-lint
make vulncheck         # Go reachable-code vulnerability analysis
make load              # synthetic traffic against the local stack
make psql              # local database shell
make tf-check          # Terraform formatting and validation
```

Tests needing external services skip locally when `PUBSUB_EMULATOR_HOST` and
`TEST_DATABASE_URL` are unset. CI supplies both. `make ci` covers module tidiness,
vet, and Go tests; the complete release checks also include the CI jobs and
packaged verification described above.

[Fuzzing](.github/workflows/fuzz.yml) runs nightly across envelope parsing,
query parsing, label identity, window boundaries, credentials, key documents,
and point validation. Committed seeds and crash reproducers run with ordinary
tests on every PR. To fuzz one boundary locally:

```sh
go test -run '^$' -fuzz FuzzValidatePoint -fuzztime 60s ./internal/telemetry
```

## Design notes and repository layout

[Architecture decisions](docs/adr) cover the asynchronous pipeline, delivery
ledger, JSON envelopes, fixed histograms, PostgreSQL, separate read/write
services, and deferred tenant alerting.

| Path | Contents |
| --- | --- |
| `cmd/` | Ingest, aggregator, query, migration, and load-generator commands |
| `internal/aggregate/`, `internal/aggregator/` | Windowing, accumulation, flush and acknowledgment lifecycle |
| `internal/api/`, `internal/httpx/` | Routes, handlers, middleware, admission, and server lifecycle |
| `internal/auth/`, `internal/idempotency/`, `internal/ratelimit/` | Credentials, retry storage, and quotas |
| `internal/pubsubx/`, `internal/ingest/`, `internal/resilience/` | Broker transport, sink interfaces, and circuit breaker |
| `internal/store/`, `internal/query/`, `internal/telemetry/` | Schema/migrations, read shaping, and metric validation |
| `internal/config/`, `internal/observability/`, `internal/version/` | Runtime configuration, instrumentation, and build identity |
| `scripts/` | Packaged load/recovery verification, build-context checks, and image audits |
| `api/openapi.yaml` | Public API contract |
| `build/docker/`, `deploy/` | Shared service image, local Compose stack, dashboards, and Terraform |
| `docs/` | User/client/operator guides, configuration and deployment references, decision records, capacity evidence, and security assessments |

## License

MIT. See [LICENSE](LICENSE).
