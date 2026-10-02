# Measuring capacity

See [recorded measurements](capacity-results.md) for the observed improvement,
tested profiles, resource limits and saturation evidence.

Capacity is a measured workload envelope: point rate, active series, tenant
distribution, payload size, visibility delay and resources. An HTTP 202 proves
broker acceptance; it does not prove the database can keep up. Run the same
profile before and after a change, retain the JSON reports, and compare durable
visibility and backlog as well as HTTP throughput.

## Reproduce a sustained local run

Build and verify the images first, then run the capacity tool:

```sh
python scripts/verify_pipeline.py
python scripts/measure_capacity.py --points-per-second 3000 --duration 120 \
  --series 10000 --tenants 4 --output /tmp/fluxgate-capacity.json
```

The **Capacity** GitHub Actions workflow runs the same procedure on demand and
uploads the report. Every PR also runs a smaller sustained correctness profile.
Use `--aggregators 2` to test shared-subscription scaling and concurrent database
writers (1..4 replicas supported). CI uses two workers for its sustained profile.
Runtime services use their restricted database roles, one CPU per instance,
512 MiB per API and 1 GiB for the aggregator. Two ingest replicas share one
aggregator, one query service, a 2 CPU/2 GiB PostgreSQL container and a 2 CPU/1 GiB
Pub/Sub emulator. These are ceilings, not reserved cores: all containers and the
load generator share the host. The report records its total CPU/memory and exact
service image IDs.

The emulator's Java heap is explicitly 75% of its container memory. For higher
volumes, pass `--broker-memory-mib 4096` (3 GiB heap) and record that change in
comparisons. An initial 900,000-point attempt with an implicit ~256 MiB Java heap
failed from emulator heap exhaustion; that run is not evidence of Fluxgate's
sustainable throughput. Failure reports retain dependency logs and container
states to distinguish fixture failures from application bottlenecks.

The default 60 second windows, 30 second lateness, 30 second idle timeout,
15 second flush interval, four concurrent HTTP requests per replica, 16 MiB
subscriber buffer and 1,000 outstanding-message limit remain in force. Tenant
rate limits are explicitly raised to 1,000,000 points/second so that a quota
does not masquerade as processing capacity. A production quota must instead
come from measured safe capacity and tenant policy.

The worker checkpoints partial windows at each flush interval and under receive
pressure (half the byte/message limit), so window size no longer sets a minimum
time to release broker receive credits. Checkpoint writes still wait for durable
commit. `fluxgate_aggregate_pending_messages` and
`fluxgate_aggregate_pending_encoded_bytes` include writes in progress; compare
these with broker backlog to distinguish a full receiver from a slow publisher.

Storage checkpoints use bounded 256-row bulk writes and claims in **one
transaction per tenant**, including that tenant's revision lock. Up to four
tenant transactions run concurrently per aggregator (`AGGREGATOR_FLUSH_CONCURRENCY`,
range 1..16), within the existing database connection pool. A blocked tenant or
claim conflict does not roll back other tenants. `AGGREGATOR_STORAGE_TIMEOUT`
defaults to one minute (valid range 1s..2m), shared by the whole checkpoint,
including queued tenant work. A blocked ledger lookup declines admission;
failed tenant transactions roll back and nack only their deliveries.
Changing it cannot make storage faster: repeated timeouts require reducing
load, investigating locks, or increasing database capacity.

Flush counts, duration and committed-window metrics describe tenant transactions.
Several tenants committing the same time window count as separate checkpoints.
Pending delivery gauges fall as each tenant commits, even while another is
blocked. More concurrency does not increase the database's connection limit or
remove serialization for one hot tenant; budget connections across all replicas
and leave room for ledger reads. Existing images and the new worker can share
the same schema and per-tenant revision protocol during a rolling upgrade.
Isolation is bounded: blocked transactions consume worker slots, queued tenants
can exhaust the shared deadline, and the next collection waits for the current
checkpoint to resolve. Use the capacity tool's `--flush-concurrency` option to
measure this tradeoff on the intended database tier.

Ledger lookups do not hold the shared admission lock. Receive limits and the
connection pool bound concurrent reads; the engine and delivery bookkeeping
still change atomically. A checkpoint completing during a lookup invalidates
that snapshot and causes a fresh read under the same timeout. This prevents a
slow lookup from blocking checkpoint collection or unrelated deliveries without
allowing a stale read to accumulate a committed contribution again.

For database-only comparisons against a disposable PostgreSQL instance:

```sh
TEST_DATABASE_URL='postgres://fluxgate:fluxgate@localhost:5442/fluxgate?sslmode=disable' \
  go test ./internal/store -run '^$' -bench BenchmarkFlush -benchtime=3x -count=3
```

The benchmark includes transaction commit, delivery claims and real indexes,
with both initial inserts and subsequent additive updates. It leaves isolated
tenant data in the database. Use the complete pipeline test for correctness
and the sustained capacity tool for end-to-end performance.

The generator offers 500 point batches on a fixed schedule, with 16 clients
and no unbounded waiting queue. Points use current event times, deterministic
values, 128 byte detail labels and independently cycling host series per tenant.
It retries 429/503 responses with the original body and idempotency key for up
to 30 seconds. Unresolved requests fail the run; they are never silently removed
from expected totals. Missed scheduled work is reported separately. Client
saturation therefore cannot silently lower the offered rate or create a catch-up
burst. The accepted rate includes any time spent finishing requests after the
offered-load period.
Reports distinguish scheduling lag from exhausted client slots, and preserve
retry reasons and recent dependency/service logs after 503 responses.

Every two seconds, the observer checks service health and memory, reads committed
batch identities and issues a bounded query while writes continue. It records
the first observed durable visibility of each accepted batch. The reported lag
is an upper-bound sample, including observer delay, rather than a precise commit
timestamp. Final reconciliation compares **every tenant/series/window** count,
sum, minimum, maximum and last value with accepted input. Containers must remain
running without OOMs or restarts. Timestamp tie behavior, histogram buckets,
cross-replica retries and crash/outage recovery remain covered by the separate
pipeline and Go tests.

## Interpreting results

`passed` means the correctness and health checks passed. It does **not** mean
the offered rate is sustainable or that a latency SLO was met. Inspect:

- `generator_missed_points` and `accepted_points_per_second`: did the generator
  and API sustain the offered rate?
- `accepted_to_visible_p95_seconds`, `accepted_to_visible_max_seconds` and
  `drain_seconds`: did storage keep up, or merely finish after traffic stopped?
- `samples`: compare accepted batches against visible batch identities over
  time; growing separation means growing backlog. Inspect RSS and CPU alongside
  flush count/time and tracked series.
- `query_probe_p95_ms` and `errors`: did readers and health checks remain usable?
- `active_series` and `reconciled_series_windows`: did the run actually exercise
  the intended cardinality and window turnover?

Increase rate and cardinality separately. Repeat runs and use longer soak tests
after locating saturation. Do not extrapolate this single-host emulator result
to GCP: repeat against the intended Cloud SQL tier and storage, real Pub/Sub,
network path, instance count, tenant skew and production query mix. Verify
backlog recovery after a replica loss and database interruption, then select a
working rate below saturation with a stated visibility SLO and operational
headroom. The repository's deployment and recovery release gates still apply.
