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
| 2 | Ingest API: metric model, validation, API-key auth, per-tenant rate limiting, OpenAPI | Merged |
| 3 | Pub/Sub layer: batched publisher, subscriber runtime, retries, DLQ, emulator harness | Merged |
| 4 | Aggregator: windowing, watermarks, duplicate suppression, Postgres rollups | Merged |
| 5 | Query API: time-series queries, label filtering, percentiles, SSE live tail | Merged |
| 6 | Observability: distributed tracing across the broker, Prometheus metrics, dashboards | Merged |
| 7 | Deployment: Terraform for GCP, least-privilege IAM, alert policies, decision records | Merged |

All seven milestones are merged. Each landed as its own reviewed pull request,
so the history reads as a sequence of decisions rather than one large drop.

Alerting was deliberately dropped from the plan rather than quietly left
promised; [ADR 7](docs/adr/0007-deferred-alerting.md) records why.

## Deploying

[`deploy/terraform`](deploy/terraform) provisions the whole platform on GCP:
Pub/Sub topology with dead-lettering, Cloud SQL on a private IP, three Cloud Run
services, one service account per service, secrets in Secret Manager, and four
alert policies. See [its README](deploy/terraform/README.md) for the apply, and
for the parts that bite.

Three decisions in there are worth calling out, because each is a mistake that
produces no error message:

**Dead lettering silently does nothing without two IAM bindings.** Pub/Sub's own
service agent performs the dead-letter publish — not the consumer — so it needs
publish rights on the dead-letter topic and subscribe rights on the source
subscription. Without them there is no failure at apply time and none at run
time: messages simply keep being redelivered forever.

**The aggregator is not autoscaled.** Each instance holds open windows in memory
and writes them only when they close, so scaling in mid-window hands the
survivors a redelivery of everything the departing instance had not yet flushed.
Correct, thanks to the delivery ledger, and pure rework.

**CPU stays allocated on the aggregator and the query API.** Both work between
requests — flushing on a timer, polling for a live tail. With Cloud Run's idle
CPU throttling the flush timer would be throttled to a stop, and windows would
only be written when a message happened to arrive.

Permissions are split by what each service actually does. The ingest API holds
no database credentials at all; the query API holds no Pub/Sub permissions. A
compromise of one does not reach what the other can touch, and that is enforced
by IAM rather than by discipline.

## Decision records

The choices that were genuinely contested, each with what it cost and what would
make us revisit it — [`docs/adr`](docs/adr):

| # | Decision |
| --- | --- |
| [1](docs/adr/0001-event-driven-pipeline.md) | Asynchronous pipeline rather than synchronous writes |
| [2](docs/adr/0002-exactly-once-accumulation.md) | Exactly-once accumulation via a per-window delivery ledger |
| [3](docs/adr/0003-json-envelope.md) | Versioned JSON on the wire rather than protobuf |
| [4](docs/adr/0004-fixed-histogram-layout.md) | Fixed-layout histograms rather than adaptive sketches |
| [5](docs/adr/0005-postgres-not-a-tsdb.md) | Postgres rather than a purpose-built time-series database |
| [6](docs/adr/0006-separate-read-and-write-services.md) | Separate read and write services |
| [7](docs/adr/0007-deferred-alerting.md) | Alerting deferred, and why |

## Observability

The pipeline is asynchronous and spans three processes, which makes the obvious
question — *what happened to this point?* — the hard one. Tracing answers it.

### One trace across the broker

A request to the ingest API, the publish it triggers, and the aggregation that
happens seconds later in another process are all one trace:

```
SERVICE                  SPAN                           KIND       DURATION
fluxgate-ingest-api      POST /v1/ingest                server      12.1ms
fluxgate-ingest-api      publish telemetry-raw          producer    12.0ms
fluxgate-aggregator      consume telemetry-aggregator   consumer     0.6ms
```

The publisher writes W3C trace context into the Pub/Sub message attributes; the
consumer reads it back out and starts its span as a child. Nothing is shared
between the processes but the message itself.

The propagator is installed **even when tracing is disabled**. A service that
dropped the header because it was not itself sampling would silently break
every trace passing through it, turning one unsampled hop into a permanently
broken chain.

Sampling is `ParentBased`: once the edge decides to record a request, every
downstream hop honours that decision instead of re-rolling the dice and
producing a trace with holes in it. The default ratio follows the tier — 1 on
local and dev, 0.05 on staging and prod — because tracing every request is
affordable while developing and ruinous at ingest volumes.

**Telemetry can never take the service down.** OpenTelemetry errors are handled,
not returned; the exporter batches rather than blocking the request path; and
nothing in the compose stack gates a service on the collector being healthy.
Telemetry being down costs visibility, never availability.

### Metrics

Every service exposes `/metrics` on its own listener. The instruments worth
knowing about:

| Metric | Why it matters |
| --- | --- |
| `fluxgate_aggregate_watermark_lag_seconds` | How far event time trails the wall clock. A rising line means the pipeline is falling behind, long before a queue-depth alarm would notice. |
| `fluxgate_aggregate_tracked_series` | The number the cardinality bound applies to. Rising without traffic rising is the shape of a cardinality problem. |
| `fluxgate_resilience_breaker_state` | 0 closed, 1 half-open, 2 open. Above zero means the edge is failing fast rather than waiting out timeouts. |
| `fluxgate_publish_batches_total` | By outcome. Anything but `ok` means shedding, or an unhealthy broker. |
| `fluxgate_consume_messages_total` | By outcome. `rejected` means something is heading for the dead-letter queue. |

**Labels are bounded by construction.** Requests are labelled by *route pattern*
— `POST /v1/ingest` — never by path, and every unmatched request shares one
`unmatched` label so a scan for URLs that do not exist cannot mint a series per
probe. Status codes are bucketed by class, because alerts are written against
classes and the exact code is already in the access log where it can be read in
context.

That is not an incidental choice. This is a telemetry system: shipping one with
an unbounded metric label would be a bad joke.

### Logs, traces and metrics agree

A request ID is generated at the edge, bound to the logger, carried in the
response header, published as a message attribute, and re-attached by the
consumer. Trace and span IDs are bound to the same logger. So a log line names
the trace it belongs to, a trace names the request that produced it, and a
metric shares the route label with both — which is what makes it possible to
start anywhere and reach the other two.

### Seeing it

`make up` brings up Jaeger, Prometheus and a provisioned Grafana alongside the
services:

```
Jaeger      http://localhost:16686   traces spanning all three services
Prometheus  http://localhost:9090
Grafana     http://localhost:3000    the "Fluxgate pipeline" dashboard, no login
```

## The query API

Rollups are read back through a separate process on a separate port. Reads and
writes scale on different axes and fail in different ways: an expensive
dashboard query should not be able to slow telemetry ingestion, and an ingest
spike should not make dashboards unreadable. The read path also holds only a
read-shaped database pool and no publisher at all.

```bash
curl -H 'Authorization: Bearer fxg_local_local-dev-secret' \
  'localhost:8082/v1/query?metric=http.request.duration_ms&from=-15m&agg=p95&label.status=500'
```

```json
{
  "metric": "http.request.duration_ms",
  "kind": "histogram",
  "aggregation": "p95",
  "from": "2026-09-04T01:21:24Z",
  "to": "2026-09-04T01:36:24Z",
  "series": [
    {
      "labels": {"service": "checkout", "status": "500"},
      "points": [{"t": "2026-09-04T01:36:00Z", "v": 219.48}]
    }
  ]
}
```

**Percentiles come from the stored histogram buckets**, not from a recomputation
over raw points that no longer exist. Asking for `p99` of a gauge returns an
empty series *and a warning*, rather than a zero: a fabricated percentile
rendered on a dashboard is worse than a gap, because nobody can tell it is
wrong.

**Relative ranges are first class.** `from=-15m` is what a human actually
wants, and forcing them to compute two timestamps is how a dashboard ends up
with a hard-coded range that silently goes stale. `from` is measured from `to`,
not from now, so the two compose. The response echoes the resolved range, so a
caller can see what a relative range actually meant.

**Label filters are namespaced** as `label.<key>`. Without the prefix, a metric
labelled `agg` or `from` would be unqueryable. Filtering uses JSONB containment
against a GIN index rather than a join through a normalised label table: the
rollup already carries its labels, so containment answers the question in one
index lookup.

**Every limit is a bound on a real failure.** A single unbounded query over a
year of one-minute windows across a thousand series is half a billion rows, and
the client that asked for it is usually a dashboard that will ask again in
thirty seconds. Responses that hit a limit are marked `truncated`, so a caller
can tell an incomplete answer from an empty one.

**The tenant always comes from the credential.** There is no parameter for
selecting one — a caller able to name someone else's tenant could read their
data.

### Live tail

`GET /v1/stream` pushes rollups over server-sent events as the aggregator
commits them:

```
retry: 2000

event: rollup
data: {"metric":"live.demo","labels":{},"window_start":"2026-09-04T01:36:30Z","count":3,"sum":600,"min":100,"max":300,"last":300}

: keep-alive
```

SSE rather than WebSocket: the traffic is one-directional, every HTTP client
already speaks it, it survives proxies that mangle upgrades, and reconnection is
part of the protocol rather than of every client.

The tail polls an indexed column rather than subscribing to a topic. A
per-instance Pub/Sub subscription would deliver changes sooner, but every
replica would need one created and torn down with the instance — runtime
topology management, for a feature whose usable latency floor is a human
looking at a screen.

The cursor advances on **write** time, not event time. A late arrival updates a
window that closed minutes ago, and a tail ordered by event time would never
show it.

One routing detail worth noting: the request timeout is applied to every
endpoint *except* the stream. A blanket timeout would sever each stream at the
deadline, which a client cannot distinguish from a server fault — it would
reconnect, be cut off again, and settle into a reconnect loop that looks exactly
like an outage. The stream bounds itself with its own, much longer, budget.

## The aggregator

The aggregator consumes batches, folds their points into tumbling windows keyed
by event time, and commits each closed window to Postgres. It is a separate
binary from the ingest API, because the two scale on different axes: the edge
scales with request rate, the aggregator with series cardinality.

### Exactly-once, and how it is actually achieved

Pub/Sub delivers at least once. Counting a redelivered batch twice would
silently corrupt every aggregate it touches, and nothing downstream could
detect it. Three things together make accumulation exactly-once:

1. **The message is acknowledged only when its data is durable.** Acknowledging
   on receipt would mean a crash between accepting a point and writing its
   window loses the point while the broker believes it was delivered. The
   subscriber runs in manual-acknowledgement mode and the runner settles each
   message once every window it fed has committed.
2. **Rollups and a delivery ledger commit in one transaction.** There is no
   interval where the data is stored but the batch is not recorded, or the
   reverse.
3. **The ledger is keyed on (batch, window), not on the batch.** A batch that
   straddles a boundary feeds two windows that flush at different times. Keyed
   on the batch alone, one whose first window committed and whose second failed
   would be recorded as fully processed -- and the retry that should have
   rebuilt the second window would be skipped, losing it silently.

The two crash windows both come out correct:

| Crash point | Ledger | Acknowledged | On redelivery |
| --- | --- | --- | --- |
| Before commit | absent | no | Re-accumulated; correct |
| After commit, before ack | present | no | Skipped; correct |

Duplicate suppression has an in-memory half and a durable half, and both are
needed: a redelivery arriving *before* the flush is not in the database yet, and
one arriving *after a restart* is not in memory. A third state exists for a
redelivery that lands *during* a write -- the outcome is not knowable yet, so
the message is handed back rather than guessed at.

### Watermarks and late data

Windows close on event time, not wall-clock time. A watermark trails the highest
observed timestamp by the lateness allowance, and a window is emitted once the
watermark passes its end. That is what makes replay meaningful: feeding a day of
history through the aggregator produces exactly the rollups it produced live,
because nothing depends on when the process happened to run.

Collecting a window does not close it. The engine hands the rollups over, but
only marks the window flushed when the caller confirms the write -- because a
window whose write failed has to be rebuildable from a redelivery, and it cannot
be if the engine has already decided that anything arriving for it is late.

Event-time watermarks have one structural weakness: they only advance when data
arrives, so a producer that stops sending strands its final window one
observation short of closing, permanently. After a configurable silence the
watermark is allowed to advance on processing time instead, far enough to close
the oldest open window and no further -- jumping it to the wall clock would slam
every window shut at once, including ones a resuming producer could still fill.

### What is stored

One row per series per window: count, sum, min, max, last, and for histogram
series a fixed-layout bucket vector. The upsert merges additively, so a window
written in two pieces -- by two replicas, or by one across a restart -- totals
the same as if it had been written once. That is sound only because every
statistic is associative, which is also why `last` is resolved by event time
rather than by whichever write arrived second.

Histogram buckets are exponential with a fixed layout, so absolute error grows
with the value: a 1ms measurement is resolved far more finely than a 10s one,
which is the right trade when the question is "is p99 2ms or 20ms". The layout
is fixed rather than adaptive precisely so two accumulators can always be merged
by adding their bucket counts -- including inside SQL, via a small immutable
function, which keeps the upsert a single statement instead of a
read-modify-write race between replicas.

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

### Under load

`make load` drives synthetic telemetry at a running stack. On a laptop, against
the full compose stack — every service, the emulator and Postgres sharing one
machine with the load generator:

```
  batches         897 (45/s)
  points          179400 (8970/s)
  status
    202           897
  latency (client-side, includes the broker acknowledgement)
    p50           55.5ms
    p90           57.9ms
    p99           60.7ms
```

Those 179,400 points became **240 rollup rows** — four metrics across ten hosts
over the run's windows — and the percentiles were queryable per series
immediately afterwards. Watermark lag held at 4.2s against a 10s window.

The latency is dominated by the broker acknowledgement, which is the honest
cost of a 202 that means *durable* rather than *buffered*. See
[ADR 1](docs/adr/0001-event-driven-pipeline.md) for why that trade is made
deliberately.

Removing the `-rate` cap is also worth doing once: the run saturates, and 2163
of 3261 batches come back 429 with `Retry-After`. That is the rate limiter
working — quota is metered in points per second, so a bigger batch consumes
proportionally more of it.

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

Within a few seconds the aggregator closes the window, and the rollup is
readable:

```bash
curl -H 'Authorization: Bearer fxg_local_local-dev-secret' \
  'localhost:8082/v1/query?metric=queue.depth&from=-15m&agg=sum'
```

```json
{"metric":"queue.depth","aggregation":"sum",
 "series":[{"labels":{},"points":[{"t":"2026-09-04T01:05:40Z","v":42}]}]}
```

Readiness reports each service's own dependencies:

```bash
curl -s localhost:8080/readyz   # {"status":"ok","checks":{"pubsub-publisher":"ok"}}
curl -s localhost:8081/readyz   # {"status":"ok","checks":{"postgres":"ok"}}
curl -s localhost:8082/readyz   # {"status":"ok","checks":{"postgres":"ok"}}
```

`make psql` opens a shell on the database; `make down` discards the stack and
its state.

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
make test-integration  # tests needing a real broker and database
make psql              # a shell on the local database
make cover             # coverage profile and per-package summary
make lint              # golangci-lint
make vulncheck         # known vulnerabilities in dependencies
make load              # drive synthetic telemetry at a running stack
make tf-check          # format and validate the Terraform
make ci                # what the pipeline enforces
```

The integration tests skip themselves when `PUBSUB_EMULATOR_HOST` and
`TEST_DATABASE_URL` are unset, so `go test ./...` stays green without Docker.
CI runs them against a real emulator and a real Postgres under the race
detector, and fails the build if they report a skip -- a suite that silently
stops covering anything is worse than one that fails.

Using a real database rather than a mock is deliberate: the additive upsert, the
histogram merge function and the ledger's composite key are exactly the parts a
mock would get wrong in agreement with its author.

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
| `DATABASE_URL` | -- | Required by the aggregator; unused by the ingest API |
| `AGGREGATOR_WINDOW_SIZE` | `1m` | Event time covered by each rollup |
| `AGGREGATOR_ALLOWED_LATENESS` | `30s` | Freshness traded for out-of-order tolerance |
| `QUERY_MAX_RANGE` | `744h` | Longest span one query may cover |
| `QUERY_MAX_SERIES` | `500` | Distinct series in one response |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | -- | Setting it also enables tracing |
| `TRACE_SAMPLE_RATIO` | `1` local/dev, `0.05` prod | Completeness traded for cost |
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
cmd/ingest-api/         the edge: validates and publishes
cmd/aggregator/         the consumer: windows, aggregates and persists
cmd/query-api/          the read path: queries and the live tail
cmd/loadgen/            synthetic load, for seeing what the pipeline does
internal/aggregate/     windowing, watermarks, accumulators, histograms
internal/aggregator/    delivery, flushing and acknowledgement lifecycle
internal/api/           route table and request handlers
internal/auth/          API key verification and tenant resolution
internal/config/        environment configuration and validation
internal/httpx/         handler contract, error envelope, middleware, server
internal/idempotency/   replaying outcomes for retried requests
internal/ingest/        the sink seam between HTTP and the delivery pipeline
internal/observability/ logging, probes, tracing and metrics
internal/pubsubx/       envelope, publisher, subscriber runtime, topology
internal/query/         query parsing, limits and result shaping
internal/ratelimit/     sharded token-bucket throttling
internal/resilience/    circuit breaker
internal/store/         Postgres schema, migrations and the delivery ledger
internal/telemetry/     the metric domain model and its validation rules
internal/version/       build provenance
build/docker/           multi-stage Dockerfile shared by every service
deploy/                 docker-compose stack, Prometheus scrape config,
                        provisioned Grafana dashboards
deploy/terraform/       GCP infrastructure
docs/adr/               architecture decision records
```

## License

MIT. See [LICENSE](LICENSE).
