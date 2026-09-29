# Measuring capacity

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
Runtime services use their restricted database roles, one CPU per instance,
512 MiB per API and 1 GiB for the aggregator. Two ingest replicas share one
aggregator, one query service, a 2 CPU/2 GiB PostgreSQL container and a 2 CPU/1 GiB
Pub/Sub emulator. These are ceilings, not reserved cores: all containers and the
load generator share the host. The report records its total CPU/memory and exact
service image IDs.

The default 60 second windows, 30 second lateness, 30 second idle timeout,
15 second flush interval, four concurrent HTTP requests per replica, 16 MiB
subscriber buffer and 1,000 outstanding-message limit remain in force. Tenant
rate limits are explicitly raised to 1,000,000 points/second so that a quota
does not masquerade as processing capacity. A production quota must instead
come from measured safe capacity and tenant policy.

The generator offers 500 point batches on a fixed schedule, with 16 clients
and no unbounded waiting queue. Points use current event times, deterministic
values, 128 byte detail labels and independently cycling host series per tenant.
It retries 429/503 responses with the original body and idempotency key for up
to 30 seconds. Unresolved requests fail the run; they are never silently removed
from expected totals. Missed scheduled work is reported separately. Client
saturation therefore cannot silently lower the offered rate or create a catch-up
burst. The accepted rate includes any time spent finishing requests after the
offered-load period.

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
