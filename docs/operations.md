# Operations and troubleshooting

[Documentation index](README.md) · [Recovery runbooks](recovery.md)

Monitor the path from **accepted input to committed, queryable output**. Healthy
HTTP responses alone do not show whether aggregation, retention, or recovery can
keep up. Record a workload-specific visibility SLO and operating headroom using
the [capacity guide](capacity.md).

## Health and readiness

| Endpoint | Meaning |
| --- | --- |
| `/healthz` | Process is alive; no dependency checks |
| `/readyz` | Readiness flag and registered dependency checks; 200 `ok`, or 503 `draining`/`degraded` |
| `/v1/version` | Version, commit, build date, Go version, platform |

Ingest registers its publisher and, when configured, retry database. Its publisher
check reads circuit-breaker state; it does not actively publish a probe. Query and
aggregator register PostgreSQL. The aggregator readiness probe does not prove
that the broker subscription is receiving or that backlog is falling. Database
connectivity does not prove every required table/write permission or a healthy
query plan. Use functional probes and lag measurements alongside readiness.

A failing dependency is reported as `unavailable` in the `checks` map; inspect
server logs for the cause. Readiness responses deliberately omit internal error
details.

Do not restart every process just because a shared database is down. Liveness
stays independent to avoid turning a dependency outage into restart churn. On
shutdown, readiness fails before HTTP draining. Uncommitted worker deliveries
remain recoverable through broker redelivery; forced termination can still
increase rework and visibility delay.

## Metrics

Compose provisions [Prometheus](../deploy/prometheus.yml) and a
[Grafana dashboard](../deploy/grafana/provisioning/dashboards/pipeline.json).
Each service uses its own registry, with Go/process collectors and a constant
`service` label. Request route/method labels are bounded; HTTP status uses classes
such as `2xx` or `5xx`, not exact codes. Use logs for individual statuses.

| Metric family | Use and interpretation |
| --- | --- |
| `fluxgate_http_requests_total` | Requests by route, method, and status class |
| `fluxgate_http_request_duration_seconds` | Request latency histogram; stream duration includes connection lifetime |
| `fluxgate_http_requests_in_flight` | In-flight HTTP requests |
| `fluxgate_publish_batches_total` | Publish attempts by outcome; not distinct logical batches |
| `fluxgate_publish_duration_seconds` | Time to broker confirmation |
| `fluxgate_resilience_breaker_state` | 0 closed, 1 half-open, 2 open |
| `fluxgate_consume_messages_total` | Callback outcomes (`ok`, `retried`, `rejected`); `ok` is admission, not proof of commit/ack |
| `fluxgate_aggregate_windows_flushed_total` | Tenant windows committed; partial checkpoints can revisit a time window |
| `fluxgate_aggregate_flush_duration_seconds` | Duration of one successful tenant transaction |
| `fluxgate_aggregate_rollups_written_total` | Series/window writes, including updates; not unique stored rows |
| `fluxgate_aggregate_open_windows` | In-memory window count |
| `fluxgate_aggregate_tracked_series` | Series entries held across open windows |
| `fluxgate_aggregate_buffered_bytes` | Estimated engine admission accounting, not process RSS |
| `fluxgate_aggregate_pending_messages` | Admitted deliveries awaiting durable settlement, including writes in progress |
| `fluxgate_aggregate_pending_encoded_bytes` | Wire payload bytes awaiting durable settlement |
| `fluxgate_aggregate_watermark_lag_seconds` | Wall clock minus engine watermark; not end-to-end durable visibility |

The registry also defines `fluxgate_ingest_points_accepted_total`,
`fluxgate_ingest_points_rejected_total`, and `fluxgate_ingest_batch_points`, but
the current ingest handler does not call their recording hook. Do not use those
families as a production point-throughput or loss SLI. Client counters, reconciled
capacity evidence, and broker/storage observations provide the current evidence.
Adding bounded runtime point instrumentation is a separate implementation task.

Useful local PromQL examples:

```promql
sum by (service) (rate(fluxgate_http_requests_total{status="5xx"}[5m]))
```

```promql
histogram_quantile(0.95,
  sum by (le, service) (rate(fluxgate_aggregate_flush_duration_seconds_bucket[5m])))
```

```promql
sum by (service) (fluxgate_aggregate_pending_encoded_bytes)
```

Histograms have the usual `_bucket`, `_sum`, and `_count` series. Establish
thresholds from measured workload/latency budgets; a low successful-flush latency
alone cannot reveal transactions that failed and are retrying.

## GCP monitoring

Terraform supplies native monitoring policies for dead-letter backlog, consumer
lag, ingest Cloud Run 5xx, and Cloud SQL disk pressure. Production service creation
requires notification channels. Verify actual delivery to the on-call destination;
a policy existing in state is not an alert-delivery test.

Public APIs disable Prometheus scraping on staging/prod. The internal aggregator
enables it, but the module does not deploy a scraper. Optional tracing requires
an external TLS OTLP collector. Cloud Run does not automatically provide one for
the application's exporter.

Track at least accepted workload, oldest broker backlog age, undelivered messages,
DLQ depth, database CPU/I/O/connections/locks, storage growth, query latency,
container RSS/restarts, and functional ingest-to-query visibility. Database
vacuum and retention progress matter during a long soak. Infrastructure health
alerts are separate from the unimplemented tenant rule/notification engine.

## Logs and tracing

JSON is the default log format. Use `request_id` to join API and broker activity,
`batch_id` for accepted batch identity, and service/revision metadata to identify
the build. Access logs carry exact response status; failure logs retain internal
causes that are intentionally absent from public problem documents.

Trace context propagates through Pub/Sub. Public inbound trace context is linked
to a locally sampled trace by default, rather than allowing a client to force
sampling. Local/dev samples all traces when tracing is enabled; staging/prod
defaults to 5%. Failure to export traces should reduce observability, not block
the data path. Missing sampled spans do not prove a request was not processed.

## Routine checks

1. Verify the deployed commit/image digests and key version against the release
   record. Check all three services and the last migration result.
2. Compare offered/accepted workload with committed visibility and backlog age.
   Verify query latency under the actual read mix, not only idle probes.
3. Watch retention deletion errors, live/dead table growth, disk headroom, and
   vacuum progress. Ledger expiry must remain beyond every supported replay path.
4. Exercise a tenant-scoped synthetic write/query with a unique identity. Track
   its visibility delay and distinguish ingestion failures from read failures.
5. Verify backup/PITR coverage and recent restore rehearsal evidence. Keep enough
   broker retention to recover the selected database point.

Read-only SQL diagnostics, for an operator connected to the intended database:

```sql
SELECT relname, n_live_tup, n_dead_tup, last_autovacuum
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;

SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC;

SELECT state, wait_event_type, wait_event, count(*)
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY state, wait_event_type, wait_event
ORDER BY count(*) DESC;
```

Tuple counts are estimates. Avoid adding repeated full-table counts or unbounded
debug queries to an already overloaded database. Collect a bounded evidence
sample before changing concurrency or restarting workers.

## Troubleshooting

| Symptom | Check | Response |
| --- | --- | --- |
| Startup rejects configuration | Full `invalid configuration` report, effective environment, service dependency requirements | Correct the named values; do not disable production safeguards |
| 401 for every request | Correct API URL, Bearer format, enabled key ID/hash, secret version on every revision | Verify local fixture or rotate/fix deployed document; never log the token |
| Ingest 409 | Client key/body persistence and JSON reserialization | Recover original bytes; investigate before assigning a new identity |
| Ingest 422 | Field errors, point count, timestamps, metric/label bounds | Correct invalid observations; preserve accepted subsets on partial responses |
| Ingest 429 | Rate headers and presence of Retry-After | Back off, or split a batch larger than burst; account for per-instance quota |
| Ingest 503 | Admission concurrency, publisher circuit, Pub/Sub permissions, SQL retry-store health | Retry same bytes/key; reduce pressure and repair the failing dependency |
| 202 but query is empty | Same tenant/key, correct query service, checkpoint delay, window-start range, label filters, worker logs/backlog/DLQ | Wait for normal checkpoint, then locate the stalled stage; do not republish under a new key |
| Query 200 is incomplete | `truncated`, series/point limits | Narrow range and labels; aggregate only complete responses |
| Query 422 for a large read | Materialized byte budget or range validation | Narrow the read; raising HTTP body size does not raise query budget |
| Query 500/504 | SQL reachability, schema/grants, locks, pool pressure, handler deadline | Fix the dependency or reduce read cost; persistent timeouts are not solved by retries alone |
| Stream opens but shows no historical points | Stream starts at current revision | Fetch history via REST and watch future updates |
| Stream has heartbeats but stale data | Worker backlog and logged stream poll errors | Check durable progress and reconcile REST; heartbeat is only connection activity |
| Stream 429/503 or frequent reconnects | Tenant/instance slots, slow reader, proxy buffering/timeouts, maximum duration | Close unused streams, back off, disable proxy buffering, reconcile on reconnect |
| Aggregate memory/receive budget fills | Pending encoded bytes vs buffered estimate vs process RSS, flush durations, database locks | Reduce load/cardinality, inspect database bottleneck; measure before raising bounds |
| Ledger/retention failures | Schema 0007, runtime grants, lock waits, cleanup duration, disk growth | Run reviewed migration/provisioning; protect replay coverage while fixing cleanup |
| Duplicate-looking totals | New HTTP keys, expired reservation/ledger, changed window policy, adding SSE totals as deltas | Trace original identities and client behavior; stop unsafe replay until reconciled |
| Local containers exit together | Docker daemon/host memory and emulator heap, not just application logs | Restore host resources; discard failed capacity claims and rerun with recorded limits |
| Local ports unavailable | Existing listeners or another Compose stack | Resolve the collision or adjust host port mappings consistently |

For dependency outages, dead letters, database restore, and rollback, follow
[recovery](recovery.md). Save exact revision, timestamps, request/batch IDs,
sanitized configuration, response codes, and relevant metrics/log samples in
the incident record. Do not include credentials or raw sensitive labels.
