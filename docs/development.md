# Development and verification

[Documentation index](README.md) · [Architecture](architecture.md)

## Toolchain and repository map

Builds and CI use Go 1.27; the module minimum is 1.26. Docker with Linux containers
is needed for broker/database integration and image checks. Python 3.12 or newer
runs the standard-library verification scripts. Terraform requires 1.9 or newer.
Make targets require Bash, Make, and ordinary Unix utilities; native Go/Python
commands also work from PowerShell.

| Path | Responsibility |
| --- | --- |
| `cmd/ingest-api`, `cmd/aggregator`, `cmd/query-api` | Service wiring, dependency lifecycle, health, shutdown |
| `cmd/migrate`, `cmd/loadgen` | Schema/grants job and synthetic producer |
| `internal/api`, `internal/httpx` | HTTP contract, middleware, admission, errors |
| `internal/auth`, `internal/ratelimit`, `internal/idempotency` | Credential/tenant identity, point quotas, retry reservations |
| `internal/telemetry`, `internal/aggregate` | Validation, series identity, windows, histograms |
| `internal/aggregator` | Delivery ownership, checkpoints, durable settlement |
| `internal/pubsubx`, `internal/ingest`, `internal/resilience` | Broker envelope/publish/receive, sinks, circuit breaker |
| `internal/store`, `internal/query` | Embedded migrations, grants, transactional writes, bounded reads and response shaping |
| `internal/config`, `internal/observability`, `internal/version` | Startup configuration, logs/metrics/traces, provenance |
| `api`, `docs` | OpenAPI, guides, references, decisions, recorded evidence |
| `build/docker`, `deploy` | Service images, local stack, dashboards, capacity policy, Terraform |
| `scripts`, `.github/workflows` | Packaged verification, capacity, security audits, CI/fuzz workflows |

## Build and unit tests

```sh
go build ./cmd/...
go test -count=1 -timeout 5m ./...
go vet ./...
python -m unittest discover -s scripts -p 'test_*.py'
```

Run the race detector on a cgo-capable toolchain (Linux CI provides one):

```sh
go test -race -count=1 -timeout 10m ./...
```

On a memory-constrained Windows host, reduce local Go parallelism:

```powershell
$env:GOMAXPROCS = '2'
$env:GOMEMLIMIT = '768MiB'
go test -p 1 -count=1 -timeout 5m ./...
```

Dependency tests skip if their required environment variables are absent. A green
unit run with skipped integration tests is not equivalent to the complete CI
pipeline. Avoid running heavy local capacity tests alongside memory-intensive
builds on the same host.

## Run services natively

Start only disposable local dependencies:

```sh
docker compose -f deploy/docker-compose.yml up -d pubsub postgres
```

In each Bash terminal used for a native service, set:

```sh
export ENVIRONMENT=dev
export GCP_PROJECT_ID=fluxgate-local
export PUBSUB_EMULATOR_HOST=localhost:8681
export DATABASE_URL='postgres://fluxgate:fluxgate@localhost:5442/fluxgate?sslmode=disable'
export API_KEYS='[{"key_id":"local","tenant_id":"acme","secret_sha256":"eb26d4a9a0ef04727f52fb5acc8936196ae90e23003fc5117ce8e3e28c49363f"}]'
```

Run `go run ./cmd/migrate` once before starting the query service. Then run each
service in its own terminal:

```sh
HTTP_ADDR=127.0.0.1:8080 go run ./cmd/ingest-api
HTTP_ADDR=127.0.0.1:8081 go run ./cmd/aggregator
HTTP_ADDR=127.0.0.1:8082 go run ./cmd/query-api
```

For PowerShell, assign the same values through `$env:NAME = 'value'`, set
`$env:HTTP_ADDR` before each service command, and run `go run` normally. Do not
start native listeners on ports already used by the full Compose services.

Native workers use one-minute windows and 15-second checkpoints unless explicitly
overridden; Compose uses shorter development windows. The query service does not
own migrations. A database role fixture shared by local development is different
from the restricted roles tested by the packaged verifier.

## Broker and database integration

Use a disposable test database: integration tests apply migrations and create,
update, and delete fixture data. Never point them at a production database.

Bash after starting local dependencies:

```sh
PUBSUB_EMULATOR_HOST=localhost:8681 \
TEST_DATABASE_URL='postgres://fluxgate:fluxgate@localhost:5442/fluxgate?sslmode=disable' \
  go test -count=1 -timeout 15m ./internal/pubsubx/... ./internal/store/... ./internal/aggregator/...
```

PowerShell:

```powershell
$env:PUBSUB_EMULATOR_HOST = 'localhost:8681'
$env:TEST_DATABASE_URL = 'postgres://fluxgate:fluxgate@localhost:5442/fluxgate?sslmode=disable'
go test -count=1 -timeout 15m ./internal/pubsubx/... ./internal/store/... ./internal/aggregator/...
```

CI verifies that representative integration tests actually ran rather than
silently skipping. Relevant coverage includes tenant isolation, claim conflicts,
atomic rollback, revision ordering, query budgets, role denials, checkpoint
deadlines, and concurrent ledger admission.

## Packaged end-to-end and recovery tests

```sh
python scripts/verify_build_context.py
python scripts/verify_pipeline.py --load-batches 200
```

The verifier builds `ingest-api`, `aggregator`, `query-api`, and `migrate`, then
starts isolated Docker dependencies and services with random names/loopback
ports. It provisions restricted database roles, uses read-only application
containers with dropped capabilities, and tears down its disposable resources.
The optional 200 batches add 100,000 points to mixed-kind correctness checks.

Coverage includes authentication, concurrent requests, shared retries across
replicas and restarts, exact tenant/window totals, worker crash before commit,
database outage/retry, bounded retention beyond one cleanup chunk, and quiesced
backup/restore with exact table comparison and post-restore writes. This checks
the packaged application, not real cloud IAM, PITR, or capacity.

| `verify_pipeline.py` option | Default / meaning |
| --- | --- |
| `--skip-build` | Reuse already-built images |
| `--image-prefix` | `fluxgate-validation` |
| `--tag` | `local` |
| `--load-batches` | `0`; optional batches of 500 mixed-kind points |

The script needs working Docker and enough memory for dependencies and replicas.
Host or emulator failure invalidates a capacity claim even if some requests
succeeded. Preserve failure diagnostics before rerunning.

## Capacity tools

Use [capacity.md](capacity.md) for the full method and interpretation. The
sustained tool reuses the verified images and creates its own isolated stack:

```sh
python scripts/measure_capacity.py --points-per-second 3000 --duration 120 \
  --series 10000 --tenants 4 --aggregators 2 --output capacity.json
python scripts/check_capacity.py capacity.json \
  --policy deploy/capacity/ci.json --output qualification.json
```

| `measure_capacity.py` option | Default | Bound / unit |
| --- | --- | --- |
| `--points-per-second` | 10000 | 100–1,000,000 offered points/s |
| `--duration` | 120 | 5–600 seconds |
| `--series` | 10000 | 1–100,000 series per tenant |
| `--tenants` | 4 | 1–32 |
| `--aggregators` | 1 | 1–4 |
| `--flush-concurrency` | 4 | 1–16 tenant checkpoint workers per aggregator |
| `--batch-size` | 500 | 1–1,000 points |
| `--clients` | 16 | 1–128 concurrent producers |
| `--window-seconds` | 60 | 1–60 seconds |
| `--flush-seconds` | 15 | 1–60 seconds, no greater than window |
| `--drain-timeout` | 180 | 10–600 seconds |
| `--broker-memory-mib` | 1024 | 512–8,192 MiB, Java heap explicitly 75% |
| `--image-prefix`, `--tag` | `fluxgate-validation`, `local` | Existing image names |
| `--output` | Required | Raw JSON evidence file |

The run is also limited to 10 million offered points to bound evidence memory.
`passed` in raw evidence means correctness/health passed, not that an SLO was met.
The offline qualifier exits 0 for qualification, 1 for failed budgets/correctness,
and 2 for invalid input; it writes a failed result even on invalid input when an
output path is requested. See [qualification policies](capacity.md#qualifying-a-measured-profile).

## Command-line programs

The three long-running service binaries accept `-healthcheck`, which probes their
configured local readiness address and exits 0 on success. Service settings
otherwise come from the [environment reference](configuration.md).

`migrate` applies embedded migrations with an owner connection. Its optional
`-grant-runtime-roles` restricts/provisions the three predefined runtime users.
It is intended for the isolated deployment database, not arbitrary shared roles.

`loadgen` is a lightweight synthetic API driver:

| Flag | Default | Meaning |
| --- | --- | --- |
| `-target` | `http://localhost:8080` | Ingest base URL |
| `-key` | Local Compose key | Bearer token; avoid exposing real keys in shared process/log output |
| `-workers` | 8 | Concurrent senders |
| `-batch` | 100 | Points per batch |
| `-duration` | `30s` | Run duration |
| `-rate` | 0 | Total batches/s across workers; zero sends as fast as possible |
| `-metrics` | 4 | Distinct metric names |
| `-hosts` | 10 | Distinct host label values |
| `-idempotency` | false | Add a batch retry key |

`loadgen` is not the durable capacity/reconciliation tool. Use its output to
exercise the HTTP surface; use `measure_capacity.py` to qualify a workload.

## Fuzzing, benchmarks, and security

Committed fuzz seeds run with ordinary Go tests. To fuzz a validation boundary:

```sh
go test -run '^$' -fuzz FuzzValidatePoint -fuzztime 60s ./internal/telemetry
```

The [fuzz workflow](../.github/workflows/fuzz.yml) runs nightly and manually across
point validation, credentials, key documents, envelopes, queries, label identity,
and windows. Preserve minimized reproducers as regression seeds.

```sh
go test -run '^$' -bench . -benchmem ./internal/aggregate/...
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
golangci-lint run
```

The database flush benchmark requires `TEST_DATABASE_URL`; see
[capacity](capacity.md). Benchmark results need hardware, workload, repeat count,
and build identity to be meaningful.

Image audit after building the four verifier images:

```sh
python scripts/audit_images.py --output-dir image-audit --cache-dir trivy-cache
```

It uses Docker, exports vulnerability reports and SBOMs, and supports
`--image-prefix`/`--tag` overrides. Review [current findings](security-scanning.md)
and retain the actual scan artifacts. Do not commit local credentials, state,
cache directories, generated binaries, or large evidence dumps.

## CI and change workflow

The [CI workflow](../.github/workflows/ci.yml) runs six jobs on PRs and main:
build/unit/race/Python checks; broker/database integration; Terraform formatting,
validation and mocked tests; lint; Go vulnerability analysis; and container
builds/audits/packaged recovery/sustained capacity qualification. The manual
[Capacity workflow](../.github/workflows/capacity.yml) accepts workload inputs and
an optional qualification policy and retains evidence artifacts.

`make ci` covers module tidiness, vet, and Go tests. It is only a local subset;
the complete workflow also checks images, integration, Terraform, security, and
capacity. `make help` lists other targets, including build, lint, coverage,
benchmarks, and local services. `make down` deletes the local database volume.

For a change:

1. Work on a focused branch and reproduce the affected behavior.
2. Preserve identity, durability, admission, and tenant boundaries. Add meaningful
   regression coverage when behavior changes; use a new migration for schema
   changes instead of editing an already-applied migration.
3. Update the relevant guide and OpenAPI for public contract changes. Update
   configuration/default tables and `.env.example` together. Document any
   maintenance or replay compatibility requirements.
4. Run checks appropriate to the change, then require the complete PR checks
   before release. Compare measured evidence for performance-sensitive changes.
5. Write the PR around the final behavior, validation, and limitations. Record
   any real staging gates separately from local/CI success.

Documentation is versioned with the implementation. Use repository-relative
links, make shell assumptions explicit, verify snippets against the actual API,
and avoid presenting examples or emulator measurements as production guarantees.
