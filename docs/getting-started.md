# Getting started

[Documentation index](README.md)

This walkthrough runs the complete pipeline on your computer. It needs Git and
Docker with Compose and Linux containers. Go is needed only for native development
and the optional load generator. Builds and CI use Go 1.27; `go.mod` declares a
minimum of Go 1.26. Use Python 3.12 or newer for the verification scripts.

## Start the stack

```sh
git clone https://github.com/jon-jc/fluxgate.git
cd fluxgate
docker compose -f deploy/docker-compose.yml up -d --build
docker compose -f deploy/docker-compose.yml ps
```

Wait for PostgreSQL, the emulator, and the three application services to become
healthy. Inspect startup logs if they do not:

```sh
docker compose -f deploy/docker-compose.yml logs --tail=100 ingest-api aggregator query-api
```

Compose sets all required service configuration. You do not need a `.env` file,
GCP account, or cloud credentials. Copying `.env.example` does not configure native
Go processes automatically; see [configuration loading](configuration.md#loading-and-precedence).

| Local address | Purpose |
| --- | --- |
| `http://localhost:8080` | Ingest API |
| `http://localhost:8081` | Aggregator health, build metadata, and local metrics |
| `http://localhost:8082` | Query API and live stream |
| `http://localhost:16686` | Jaeger trace viewer |
| `http://localhost:9090` | Prometheus |
| `http://localhost:3000` | Grafana with the provisioned pipeline dashboard |
| `localhost:8681` | Pub/Sub emulator |
| `localhost:5442` | PostgreSQL; database/user/password are `fluxgate` |

Published ports bind to loopback. Grafana allows anonymous administration and
database services use shared development credentials. This stack is for local
use; the [GCP deployment](deployment.md) has different access boundaries.

## Submit a batch

The Compose credential is `fxg_local_local-dev-secret`, belonging to tenant
`acme`. Use a new idempotency key for each new batch. These examples omit timestamps
so they remain valid whenever you run them.

Bash:

```sh
KEY="quickstart-$(date +%s)-$RANDOM"
curl -i http://localhost:8080/v1/ingest \
  -H 'Authorization: Bearer fxg_local_local-dev-secret' \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $KEY" \
  --data-binary '{"points":[{"metric":"queue.depth","kind":"gauge","value":42,"labels":{"service":"checkout"}},{"metric":"http.requests","kind":"counter","value":1,"labels":{"service":"checkout"}},{"metric":"http.request.duration_ms","kind":"histogram","value":12.5,"labels":{"service":"checkout"}}]}'
```

PowerShell:

```powershell
$headers = @{
    Authorization = 'Bearer fxg_local_local-dev-secret'
    'Idempotency-Key' = [guid]::NewGuid().ToString()
}
$body = '{"points":[{"metric":"queue.depth","kind":"gauge","value":42,"labels":{"service":"checkout"}},{"metric":"http.requests","kind":"counter","value":1,"labels":{"service":"checkout"}},{"metric":"http.request.duration_ms","kind":"histogram","value":12.5,"labels":{"service":"checkout"}}]}'
Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/ingest `
    -Headers $headers -ContentType 'application/json' -Body $body
```

Expect HTTP `202` with `accepted: 3`, `rejected: 0`, and a generated `batch_id`.
This confirms publication, not that a query can already see the data. Repeating
the exact body and key returns the same batch identity. Changing the body while
reusing that key returns `409`.

## Read the result

Compose uses 10-second windows and a 2-second checkpoint timer. After a checkpoint,
query the latest gauge sample in each window:

```sh
curl -sS -H 'Authorization: Bearer fxg_local_local-dev-secret' \
  'http://localhost:8082/v1/query?metric=queue.depth&from=-15m&agg=last&label.service=checkout'
```

PowerShell:

```powershell
Invoke-RestMethod -Headers $headers -Uri 'http://localhost:8082/v1/query?metric=queue.depth&from=-15m&agg=last&label.service=checkout' |
    ConvertTo-Json -Depth 10
```

Expect a series with kind `gauge`, label `service=checkout`, and a point whose
`v` is `42`. Its `t` is the window start, not the original sample timestamp. An
initial empty `series` is normal before the checkpoint; poll again after a few
seconds. Query totals may change as later observations arrive in the same window.

Explore metric names and label values:

```sh
curl -sS -H 'Authorization: Bearer fxg_local_local-dev-secret' http://localhost:8082/v1/metrics
curl -sS -H 'Authorization: Bearer fxg_local_local-dev-secret' 'http://localhost:8082/v1/labels?metric=queue.depth'
curl -sS -H 'Authorization: Bearer fxg_local_local-dev-secret' 'http://localhost:8082/v1/labels?metric=queue.depth&label=service'
```

In a second terminal, start a stream **before sending another batch**:

```sh
curl -N -H 'Authorization: Bearer fxg_local_local-dev-secret' \
  'http://localhost:8082/v1/stream?metric=queue.depth'
```

On Windows use `curl.exe` for the streaming example. The stream emits future
committed updates; it does not send historical points. Stop it with Ctrl+C.
See the [stream client contract](api.md#live-stream) before building a dashboard.

## Inspect health and traces

```sh
curl -sS http://localhost:8080/readyz
curl -sS http://localhost:8081/readyz
curl -sS http://localhost:8082/readyz
curl -sS http://localhost:8080/v1/version
```

Open Grafana's provisioned pipeline dashboard and Jaeger to inspect the request
and broker/consumer spans. Local tracing samples every request. Metrics and
readiness have specific limits described in [operations](operations.md).

Optional synthetic traffic, with Go installed:

```sh
go run ./cmd/loadgen -duration 30s -workers 8 -batch 200 -rate 45 -idempotency
```

`-rate` means batches per second across all workers, so this offers about 9,000
points/s. Start lower on a constrained machine. This exercises the API; use the
[capacity tool](capacity.md) for durable reconciliation and a measured baseline.

## Stop, preserve, or reset

```sh
docker compose -f deploy/docker-compose.yml down
```

This preserves the named PostgreSQL volume. The emulator is memory-only: stopping
it loses queued messages regardless of whether the SQL volume remains. The local
stack is not a durable environment for valuable data.

To deliberately discard the local database as well:

```sh
docker compose -f deploy/docker-compose.yml down -v
```

`make down` performs that destructive local reset. `make up` starts the stack;
Make targets require Bash and Make.

## Native ingest-only mode

```sh
go run ./cmd/ingest-api
```

With a clean environment, this uses `ENVIRONMENT=local`, disables authentication,
and writes to an in-memory sink. It is useful for handler development and does
not produce PostgreSQL rollups. For real integration use Compose or configure
the broker and database explicitly. Native multi-service development is covered
in [development](development.md#run-services-natively).

For startup failures, empty queries, or rejected requests, use the
[troubleshooting table](operations.md#troubleshooting).
