# Fluxgate

A distributed, event-driven telemetry ingestion and stream-aggregation platform
written in Go and built for Google Cloud Pub/Sub.

Fluxgate accepts high-volume metric points over HTTP, publishes them onto a
durable event bus, aggregates them into time windows across a fleet of stateless
workers, and serves the rollups back over a query API. It is built to make the
hard parts of a streaming pipeline explicit: backpressure, at-least-once
delivery, duplicate suppression, late-arriving data, and clean shutdown.

```mermaid
flowchart LR
    C[Clients] -->|POST /v1/ingest| I[ingest-api]
    I -->|bounded queue<br/>batched publish| T((telemetry.raw))
    T --> A[aggregator]
    A -->|windowed rollups| P[(Postgres)]
    A --> AG((telemetry.aggregates))
    AG --> AL[alerter]
    AL --> N((telemetry.alerts))
    A -.->|poison messages| D((telemetry.dlq))
    P --> Q[query-api]
    Q -->|REST + SSE| C
```

## Status

| Milestone | Scope | State |
| --------- | ----- | ----- |
| 1 | Service foundation: config, logging, HTTP middleware, probes, graceful shutdown, CI | Merged |
| 2 | Ingest API: metric model, validation, API-key auth, per-tenant rate limiting, OpenAPI | In review |
| 3 | Pub/Sub layer: batched publisher, subscriber runtime, retries, DLQ, emulator harness | Planned |
| 4 | Aggregator: windowing, watermarks, duplicate suppression, Postgres rollups | Planned |
| 5 | Query API and alerting: time-series queries, SSE live tail, rule evaluation | Planned |
| 6 | Observability and resilience: OpenTelemetry, Prometheus, circuit breaking, load shedding | Planned |
| 7 | Deployment: Terraform for Pub/Sub and Cloud Run, runbooks, architecture decision records | Planned |

Each milestone lands as its own reviewed pull request.

## The event transport

Accepted batches are published to Pub/Sub and consumed asynchronously. The
whole path -- validation, publish, broker, consume -- is exercised in CI against
a real emulator rather than a mock.

**A 202 is a durability guarantee, not a hopeful one.** `Publish` waits for the
broker to acknowledge the message before the handler returns. The faster
alternative, accepting into an in-process buffer and replying 202 immediately,
is quietly dishonest: it reports success for data that a crash, a deploy or an
OOM kill silently discards. Client-side batching is what keeps that honesty
from costing throughput.

**Load is shed, not buffered.** Publisher flow control signals an error rather
than blocking. A blocked publish holds an HTTP connection open with no upper
bound, so a slow broker quietly converts into an exhausted server; rejecting
the request lets the client retry against an instance that has capacity.

**A circuit breaker turns an outage into a fast 503.** The problem with a
dependency being down is not the failure, it is the queue behind it: every
publish waits out its full timeout holding a connection, a goroutine and a
request worth of memory. After a few consecutive failures the breaker opens and
fails immediately, then admits a limited number of probes after a cooldown --
enough to notice recovery, not enough to knock over a broker that is only just
coming back. Readiness reads the breaker rather than making its own probe call,
so an instance that cannot publish leaves rotation instead of accepting batches
it will only reject.

**Message ordering is deliberately off.** It would pin publishing to one region
and serialise delivery per key. Every aggregation downstream is sum, count, min
or max, all commutative, so ordering would be a throughput ceiling bought in
exchange for nothing.

**Retryable and permanent failures are different things.** A consumer whose
database blinked must retry; a consumer handed a body that is not valid JSON
must not, because the thousandth attempt fails exactly like the first while
burning quota. Permanent failures are nacked straight through to a dead-letter
queue, where the payload is preserved and inspectable. Every working
subscription the bootstrap creates has a dead-letter policy attached, because
one without it redelivers a poisoned message forever.

**Attributes are a routing surface, not decoration.** Pub/Sub subscription
filters can only match on attributes, so tenant, schema version, batch ID and
point count travel there as well as in the body: a per-tenant subscription can
be filtered server-side without every consumer deserialising messages it is
about to discard. The request ID rides along too, which is what lets a trace
continue from the HTTP edge into an aggregator running minutes later on a
different machine.

**The envelope is versioned JSON.** At this message size the bandwidth saving
of a binary format is immaterial next to the operational cost of a payload an
engineer cannot read straight off a dead-letter queue at 3am. The schema
version is the hedge: consumers reject a version they were not written for
rather than guessing, so changing encodings later is a version bump, not a
rewrite.

## The ingest endpoint

`POST /v1/ingest` takes a batch of metric points. The full contract is in
[api/openapi.yaml](api/openapi.yaml); the decisions behind it are below.

```bash
curl -X POST localhost:8080/v1/ingest   -H 'Authorization: Bearer fxg_k1_<secret>'   -H 'Content-Type: application/json'   -d '{"points":[
        {"metric":"http.request.duration_ms","kind":"histogram","value":12.5,
         "labels":{"service":"checkout","region":"us-central1"}},
        {"metric":"queue.depth","kind":"gauge","value":42}
      ]}'
```

```json
{"batch_id":"9f2c1a4be6d84f0fa1c3e7b25d908146","accepted":2,"rejected":0}
```

**Partial success is the point.** A batch with one bad point admits the other
999 and reports the failure with its exact index. Rejecting the whole batch
would let a single misbehaving call site in a client silently blind an entire
service's telemetry — and telemetry is exactly what you need working when
something is going wrong. A 202 does not mean everything was accepted; check
`rejected`.

```json
{"batch_id":"47f5...","accepted":1,"rejected":2,"errors":[
  {"field":"points.1.metric","message":"contains \"9\" at position 0; use letters, digits, '_', '.' and '-', starting with a letter or '_'"},
  {"field":"points.2.labels.__tenant","message":"uses the reserved \"__\" prefix"}
]}
```

**Validation protects specific downstream resources**, and the tightest rule is
the 20-label ceiling: cardinality is multiplicative, and an unbounded label set
is the fastest way to make a time-series store unqueryable. Non-finite values
are rejected because a single NaN turns a whole window's mean into NaN with
nothing left to identify which point caused it. Timestamps may run 5 minutes
ahead — client clocks drift, and rejecting those senders loses real data — but
not 7 days behind, since that data arrives after its aggregation window has
closed. Labels prefixed `__` are reserved, so a tenant cannot forge a system
dimension and write into another tenant's series.

**Quota is metered in points per second, not requests per second.** A thousand
points in one batch and a thousand single-point requests place identical load
on everything downstream, so charging per request would let a caller evade its
allowance simply by batching. The limiter is a sharded token bucket: a fixed
window would admit a double-rate spike across its boundary, which is precisely
the traffic shape that overwhelms a downstream service. Denials carry
`Retry-After` — unless waiting cannot help, because the batch is larger than the
burst will ever hold, in which case the response says to split it instead.

**Retries are safe.** A client that times out cannot tell whether its batch
landed. Send `Idempotency-Key` and repeat the identical body: the original
response is replayed rather than the data being counted twice. Reusing a key
with a *different* body is a 409 — replaying the first response there would
silently discard the second batch.

**Authentication is bearer API keys** in the form `fxg_<key id>_<secret>`. Only
a SHA-256 digest is stored, so a leaked configuration file does not hand anyone
working credentials. Every failure returns a byte-identical 401 regardless of
cause: a client that could distinguish "unknown key" from "bad secret" has an
oracle for enumerating valid key IDs. Verification runs a constant-time
comparison against a placeholder digest even when the key ID does not exist, so
timing does not leak what the response body refuses to.

The tenant always comes from the credential, never from the request body.

## The foundation

Milestone 1 is the substrate every service in the platform is built on.

**Configuration** (`internal/config`) is resolved once from the environment and
validated eagerly, so a bad deployment fails during boot — while an orchestrator
can still roll it back — rather than on the first request. Every problem is
reported at once, so one boot attempt surfaces every typo instead of one
redeploy per typo. Byte sizes accept `4MB` as readily as `4194304`, and `PORT`
overrides the listener address because that is what Cloud Run injects.

**Structured logging** (`internal/observability`) emits JSON keyed the way Cloud
Logging expects (`severity`, `message`, `timestamp`), so severity-based alerting
works without a log-router transformation in between. A request-scoped logger is
bound into the context once, so every downstream record carries the request ID
without a single handler having to remember it.

**HTTP middleware** (`internal/httpx`) covers correlation IDs, client IP
resolution, panic recovery, access logging, request deadlines, body size limits,
and security headers. A few decisions worth calling out:

- **Correlation IDs are sanitised, not trusted.** An inbound `X-Request-Id` is
  reused so a trace survives across service hops, but only if it is short and
  alphanumeric. Without that check, a client could inject CRLF into a response
  header or forge structure into every JSON log line the request produces.
- **`X-Forwarded-For` is off by default.** Honouring it without a proxy in front
  lets any caller spoof its address and walk straight through per-IP limits.
- **Request timeouts use context, not `http.TimeoutHandler`.** The standard
  handler buffers the entire response in memory to be able to discard it, which
  breaks the streaming endpoints milestone 5 adds.

**Error handling** is total and uniform. Handlers return errors; a single
adapter decides the status code and renders an RFC 9457 `application/problem+json`
document — including the 404 from the router itself, so there is no endpoint in
the API that speaks a different dialect on failure. Internal causes are logged
and never serialised: a client sees `internal_error`, not a database address.
The JSON decoder rejects unknown fields and names the offending key, so a client
that sends `timestmap` hears about the typo immediately instead of debugging why
its data silently vanished.

**Health probes** separate liveness from readiness, because they answer
different questions. Liveness asks whether the process is wedged and needs a
restart; readiness asks whether this instance should receive traffic. A database
blip should drain an instance, not kill it — restarting does not fix the
database, and a restart loop across the fleet turns a dependency blip into an
outage. Dependency checks run concurrently, are bounded by a probe timeout, and
a panic in one is contained rather than taking the process down.

**Graceful shutdown** is three-phase, which is what keeps a rolling deploy from
shedding requests:

1. Fail readiness immediately, so the load balancer stops routing new work here.
2. Keep serving for a grace period, because the balancer has not noticed yet and
   requests arriving in that window should be served, not refused.
3. Close the listener and drain in-flight requests under a bounded timeout,
   forcing connections closed if a handler ignores its context.

Skipping step 2 is the usual cause of a handful of 502s on every deploy.

## Quick start

The full stack -- Pub/Sub emulator and the ingest API -- comes up with one
command. Requires Docker.

```bash
git clone https://github.com/jon-jc/fluxgate.git
cd fluxgate
make up
```

Send a batch through the real broker:

```bash
curl -X POST localhost:8080/v1/ingest \
  -H 'Authorization: Bearer fxg_local_local-dev-secret' \
  -H 'Content-Type: application/json' \
  -d '{"points":[{"metric":"queue.depth","kind":"gauge","value":42}]}'
```

```json
{"batch_id":"7f0c8e8327d609ba5917480caeea6a7b","accepted":1,"rejected":0}
```

Readiness reports the transport too:

```bash
curl -s localhost:8080/readyz
# {"status":"ok","checks":{"pubsub-publisher":"ok"}}
```

`make down` discards the stack and its state.

### Without Docker

Requires Go 1.25 or newer. Runs against the in-memory sink with authentication
off:

```bash
make run
```

In another shell:

```bash
curl -s localhost:8080/healthz
curl -s localhost:8080/readyz
curl -s localhost:8080/v1/version
```

Every response carries an `X-Request-Id`. Quote it when reporting a problem —
it is in the logs too.

To see the error envelope:

```bash
curl -s -i localhost:8080/v1/nope
```

### Container

```bash
docker build -f build/docker/Dockerfile --build-arg SERVICE=ingest-api -t fluxgate/ingest-api .
docker run --rm -p 8080:8080 -e ENVIRONMENT=dev fluxgate/ingest-api
```

The runtime image is `distroless/static` running as a non-root user: no shell,
no package manager, nothing for an attacker who achieves code execution to
pivot with.

## Development

```bash
make help              # list every target
make test              # unit tests with the race detector
make test-integration  # tests that need a real broker (starts the emulator)
make cover             # coverage profile and per-package summary
make lint              # golangci-lint
make vulncheck         # known vulnerabilities in dependencies
make ci                # what the pipeline enforces
```

The Pub/Sub tests skip themselves when `PUBSUB_EMULATOR_HOST` is unset, so
`go test ./...` stays green without Docker. CI runs them against a real
emulator under the race detector, and fails the build if they report a skip --
a suite that silently stops covering anything is worse than one that fails.

`make test` needs cgo for the race detector. On a machine without a C toolchain,
use `make test-short` locally — CI runs the race detector on Linux regardless.

## Configuration

Every setting has a working default; an empty environment boots correctly. See
[.env.example](.env.example) for the full list with defaults and constraints.

The ones that most often need changing:

| Variable | Default | Notes |
| -------- | ------- | ----- |
| `ENVIRONMENT` | `local` | `local`, `dev`, `staging` or `prod` |
| `AUTH_DISABLED` | `true` on `local`, else `false` | Validation **refuses** `true` on staging and prod |
| `API_KEYS` / `API_KEYS_FILE` | — | Required whenever authentication is on |
| `RATE_LIMIT_POINTS_PER_SECOND` | `10000` | Per tenant; a key may override it |
| `PUBSUB_ENABLED` | `false` on `local`, else `true` | Validation **refuses** `false` on staging and prod |
| `GCP_PROJECT_ID` | -- | Required when the transport is enabled |
| `PUBSUB_EMULATOR_HOST` | -- | Setting it also enables the transport and topology bootstrap |
| `HTTP_TRUST_PROXY_HEADER` | `false` | Enable only behind a proxy that rewrites `X-Forwarded-For` |

Two settings are boot failures rather than documented warnings, because both
would be silently catastrophic. `AUTH_DISABLED=true` on a deployed tier lets
anyone write into any tenant's data, and `PUBSUB_ENABLED=false` there means the
service acknowledges batches it then discards. `PUBSUB_BOOTSTRAP=true` is
refused for a third reason: creating topology needs runtime admin credentials,
a far larger blast radius than publish-and-subscribe, and deployed topology
belongs in Terraform.

### Issuing an API key

The service stores only the digest, so generate the secret yourself and keep it:

```bash
SECRET=$(openssl rand -hex 32)
echo "give this to the client: fxg_k1_$SECRET"
printf %s "$SECRET" | sha256sum | cut -d' ' -f1
```

Put the digest in the key document:

```json
[{"key_id":"k1","tenant_id":"acme","secret_sha256":"<digest>",
  "rate_limit_per_second":5000,"burst":10000}]
```

## Repository layout

```
api/openapi.yaml        the public API contract
cmd/ingest-api/         service entrypoint
internal/api/           route table and request handlers
internal/auth/          API key verification and tenant resolution
internal/config/        environment configuration and validation
internal/httpx/         handler contract, error envelope, middleware, server
internal/idempotency/   replaying outcomes for retried requests
internal/ingest/        the sink seam between HTTP and the delivery pipeline
internal/observability/ logging and health probes
internal/pubsubx/       envelope, publisher, subscriber runtime, topology
internal/ratelimit/     sharded token-bucket throttling
internal/resilience/    circuit breaker
internal/telemetry/     the metric domain model and its validation rules
internal/version/       build provenance
build/docker/           multi-stage Dockerfile shared by every service
deploy/                 docker-compose stack for local development
```

## License

MIT. See [LICENSE](LICENSE).
