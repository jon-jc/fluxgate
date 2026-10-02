# Recorded capacity measurements

Measured September 29 and October 2, 2026. These are Docker/Pub/Sub emulator
results from local and hosted CI machines, not a GCP throughput certification
or a production SLO. The workload and
measurement procedure are in the [capacity guide](capacity.md).

## What changed

- [PR #26](https://github.com/jon-jc/fluxgate/pull/26) added sustained offered-load
  measurement, durable-visibility sampling and exact series/window reconciliation.
- [PR #27](https://github.com/jon-jc/fluxgate/pull/27) removed the dependency between
  event-time window closure and releasing broker receive credits. Partial windows
  checkpoint on a timer or receive pressure; acknowledgment still follows commit.
- [PR #28](https://github.com/jon-jc/fluxgate/pull/28) replaced per-row execution
  with bounded bulk SQL, reduced allocations, and bounded storage operations.
- [PR #30](https://github.com/jon-jc/fluxgate/pull/30) moved ledger reads outside
  the admission lock, with stale-snapshot invalidation across checkpoint commits.
- [PR #31](https://github.com/jon-jc/fluxgate/pull/31) made tenant checkpoints
  independent, with bounded concurrency and one shared checkpoint deadline.

## Observed profiles

Every row below reconciled every accepted point by tenant, series, window, count,
sum, minimum, maximum and last value. All application containers stayed within
their memory limits without restarts. Success includes correctness, not necessarily
sustaining the offered rate: missed submissions and growing backlog remain visible.

| Runtime | Offered points/s | Active series | Workers | Accepted points | Achieved points/s | p95 durable visibility | Drain after submissions | Peak worker RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Before checkpoints | 3,000 | 40,000 | 1 | 270,000 | 3,000 | 149.89s | 120.11s | 196.97 MiB |
| Partial-window checkpoints | 3,000 | 40,000 | 1 | 270,000 | 3,000 | 13.88s | 16.52s | 104.27 MiB |
| Checkpoints, per-row SQL | 10,000 | 100,000 | 1 | 872,500 | 9,421 | 32.86s | 27.03s | 157.97 MiB |
| Checkpoints, bulk SQL | 10,000 | 100,000 | 1 | 882,500 | 9,806 | 6.47s | 3.00s | 116.21 MiB |
| Bulk SQL, lower-load baseline | 3,000 | 100,000 | 1 | 270,000 | 3,000 | 13.00s | 14.02s | 93.26 MiB |
| Bulk SQL, three-minute run | 5,000 | 100,000 | 1 | 890,500 | 4,947 | 8.61s | 14.52s | 96.89 MiB |
| Bulk SQL, two workers | 20,000 | 100,000 | 2 | 1,721,000 | 18,656 | 33.86s | 31.02s | 178.38 / 177.64 MiB |

These runs offered traffic for 90 seconds (180 seconds for the three-minute run), through two ingest replicas, four
tenants and one concurrently queried read service. Each application container
had a one CPU ceiling; APIs had 512 MiB and workers 1 GiB memory ceilings.
PostgreSQL had two CPUs and 2 GiB. The Docker Desktop host exposed 24 CPUs and
about 15.3 GiB memory. The higher-volume profiles used a 4 GiB emulator with an
explicit 3 GiB Java heap; the first two used a 1 GiB emulator. Resource ceilings
are shared-host limits, not dedicated hardware.

All use 500-point gauge batches, current timestamps, 60-second windows, the
15-second flush timer, 128-byte detail labels and the default receive/HTTP
admission bounds. Tenant rate limits were raised solely for capacity measurement.
Different wall-clock window alignment produces different counts of series/window
rows. There was no warm-up period. Local comparisons are individual runs, not
statistical confidence intervals; background host activity and storage cache can
affect timings. Visibility sampling includes observer delay (roughly two seconds
plus probe work), and sampled memory can miss brief peaks.

At the 10,000-point/s offer, the bulk profile recorded **17,500 missed offered
points** and **34 retryable 503 responses**, all resolved. At 20,000 points/s,
there were **79,000 missed offered points** and **71 retryable 503 responses**.
The latter accumulated about 322,500 accepted-but-not-yet-visible points by
72 seconds, versus 108,000 at 23 seconds. Adding a worker did not remove the
shared database limit. **Do not treat 20,000 points/s as sustainable capacity
for these resources.** The concurrent query p95 was 62ms in the bulk 10,000/s
profile and 94ms in the two-worker profile.

The three-minute 5,000/s profile also reconciled 400,000 series/window rows,
with a 47ms query p95. It recorded 9,500 missed offered points and 30 retryable
503 responses. This is a useful lower-load observation, not an error-free
5,000/s guarantee. Reports now distinguish generator scheduling misses from
busy-client misses and retain retry reasons and diagnostic logs after backpressure.

The 3,000/s, 100,000-series bulk baseline completed with **zero missed offered
points and zero HTTP rejections** (540 accepted batches), 31ms query p95 and
190,000 reconciled series/window rows. It is a conservative measured starting
profile for these resources, still limited to a 90-second local run. Its
13-second p95 visibility is consistent with the 15-second checkpoint timer at
lower traffic. A quota or SLO for production requires the longer cloud tests below.

An earlier high-volume attempt exhausted the emulator's implicit ~256 MiB Java
heap. The tool failed that run despite reconciling accepted work; unresolved
publication errors cannot be silently excluded. Subsequent runs explicitly
budgeted emulator heap and retained failure logs/container state in the JSON
evidence. That fixture failure is distinct from application capacity.

## Hosted tenant-checkpoint profiles (October 2)

The [hosted 5,000/s run](https://github.com/jon-jc/fluxgate/actions/runs/37055705604)
accepted and reconciled **600,000 points for 120 seconds across 100,000 active
series**, with **zero missed submissions and zero HTTP rejections**. Durable
visibility p95 was 14.026s, drain was 15.504s, and concurrent query p95 was
59.826ms. Sampled aggregator RSS peaked at 106.33 and 104.61 MiB.

This used two aggregators, four concurrent tenant transactions per aggregator,
two ingest replicas, four tenants and one query service on an Ubuntu runner with
four CPUs and approximately 15.6 GiB available to Docker. Application ceilings
were one CPU each, 1 GiB per aggregator and 512 MiB per API. PostgreSQL had two
CPUs/2 GiB; the emulator had two CPUs/3 GiB and a 2.25 GiB Java heap. The gauge
workload, 500-point batches, 60-second windows and 15-second timer matched the
earlier measurement method. Hosted hardware and run duration differ from the
local results, so this is an independently measured profile, not a controlled
before/after throughput multiplier.

The subsequent [10,000/s run](https://github.com/jon-jc/fluxgate/actions/runs/37056416156)
accepted all **1,200,000 offered points**, again with **zero missed submissions
and zero HTTP rejections**, reconciling 294,500 series/window rows across 100,000
active series. Achieved rate was 9,998.1 points/s (submission completion extended
slightly past 120 seconds). Visibility p95 was 9.089s and maximum 15.411s; drain
was 15.505s and query p95 124.145ms. Worker RSS peaks were 115.16 and 114.88 MiB.
Its emulator limit was 4 GiB with a 3 GiB heap; the other settings were unchanged.
Receive-pressure checkpoints can lower visibility delay at higher traffic, but
one two-minute run is not a sustainable-throughput or latency guarantee.

A new local 20,000/s comparison attempt failed when Windows reported exhausted
paging memory and all test containers stopped. Its unresolved requests and
missing drain invalidate it as a throughput comparison. Heavy validation then
moved to the hosted runner; the failure is not counted as successful evidence.

The [qualification checker](capacity.md#qualifying-a-measured-profile) applies
explicit workload and latency limits to saved reports. Passing reconciliation
alone cannot qualify a profile that violates those limits.

## Database benchmark

Against a disposable PostgreSQL 17 container limited to two CPUs/2 GiB, the
10,000-row benchmark's median of three runs (three operations per run) changed:

| Measure | Per-row execution | Bulk execution |
| --- | ---: | ---: |
| Transaction time | 994.5ms | 515.0ms |
| Allocated bytes per transaction | 25.54 MB | 15.60 MB |
| Allocations per transaction | 311,896 | 40,967 |

The benchmark includes delivery claims, tenant revisions, real indexes, initial
inserts, subsequent additive updates and commit. It does not predict throughput
under an arbitrary production workload. Regression tests separately prove
rollback across chunks, conflict handling, blocked-write timeout/retry, precision
above 2^53, histogram buckets and microsecond event timestamps.

## Reproduce and qualify a release

### Admission latency isolation (October 2, 2026)

The aggregator previously held its shared admission lock during each durable
ledger lookup. Reads now run concurrently and refresh their snapshot if a local
checkpoint resolves before admission. Regression tests cover delayed reads
across commits, concurrent duplicate delivery, shutdown and deadline exhaustion.

`BenchmarkLedgerAdmission` isolates this coordination overhead with a simulated
2ms ledger latency, 32 concurrent one-point batches, exact total reconciliation
and a checkpoint per operation. On the same Windows host, three runs of ten
operations each had a median of **79.65ms before** and **2.75ms after**. This is
about 29 times faster for that latency-bound microbenchmark; it does **not**
multiply the measured end-to-end capacity above or model database saturation.

```sh
go test ./internal/aggregator -run '^$' -bench '^BenchmarkLedgerAdmission$' \
  -benchtime=10x -count=3
```

### Full pipeline

Build the images with `python scripts/verify_pipeline.py`, then:

```sh
python scripts/measure_capacity.py --points-per-second 10000 --duration 90 \
  --series 25000 --tenants 4 --broker-memory-mib 4096 --output /tmp/one-worker.json
python scripts/measure_capacity.py --points-per-second 20000 --duration 90 \
  --series 25000 --tenants 4 --aggregators 2 --broker-memory-mib 4096 \
  --output /tmp/two-workers.json
```

The [Capacity workflow](../.github/workflows/capacity.yml) exposes the same
controls and saves JSON evidence. Hosted runner hardware differs from the local
host. The [PR #28 CI run](https://github.com/jon-jc/fluxgate/actions/runs/36643282943)
also passed all six checks: race-enabled Go tests, real PostgreSQL/Pub/Sub
integration, lint, mocked Terraform, vulnerability checks, packaged image scans
and recovery/load tests. CI is evidence of the tested profile, not a peak-rate SLO.

Production sign-off still requires an agreed workload/latency target, repeated
longer soak runs with representative metric kinds/labels/query mix, and the
[real GCP staging and recovery gates](../deploy/terraform/README.md#release-checks-requiring-a-real-staging-project).
Select quotas and replica/database sizes with headroom below measured saturation.
No cloud infrastructure was deployed for these measurements.
