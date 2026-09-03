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
| 1 | Service foundation: config, logging, HTTP middleware, probes, graceful shutdown, CI | In review |
| 2 | Ingest API: metric model, validation, API-key auth, per-tenant rate limiting, OpenAPI | Planned |
| 3 | Pub/Sub layer: batched publisher, subscriber runtime, retries, DLQ, emulator harness | Planned |
| 4 | Aggregator: windowing, watermarks, duplicate suppression, Postgres rollups | Planned |
| 5 | Query API and alerting: time-series queries, SSE live tail, rule evaluation | Planned |
| 6 | Observability and resilience: OpenTelemetry, Prometheus, circuit breaking, load shedding | Planned |
| 7 | Deployment: Terraform for Pub/Sub and Cloud Run, runbooks, architecture decision records | Planned |

Each milestone lands as its own reviewed pull request.

## What is implemented today

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

Requires Go 1.25 or newer.

```bash
git clone https://github.com/jon-jc/fluxgate.git
cd fluxgate
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
make help        # list every target
make test        # unit tests with the race detector
make cover       # coverage profile and per-package summary
make lint        # golangci-lint
make vulncheck   # known vulnerabilities in dependencies
make ci          # what the pipeline enforces
```

`make test` needs cgo for the race detector. On a machine without a C toolchain,
use `make test-short` locally — CI runs the race detector on Linux regardless.

## Configuration

Every setting has a working default; an empty environment boots correctly. See
[.env.example](.env.example) for the full list with defaults and constraints.

The two that most often need changing:

| Variable | Default | Notes |
| -------- | ------- | ----- |
| `ENVIRONMENT` | `local` | `local`, `dev`, `staging` or `prod` |
| `HTTP_TRUST_PROXY_HEADER` | `false` | Enable only behind a proxy that rewrites `X-Forwarded-For` |

## Repository layout

```
cmd/ingest-api/        service entrypoint
internal/api/          route table and handler wiring
internal/config/       environment configuration and validation
internal/httpx/        handler contract, error envelope, middleware, server
internal/observability/ logging and health probes
internal/version/      build provenance
build/docker/          multi-stage Dockerfile shared by every service
```

## License

MIT. See [LICENSE](LICENSE).
