# API and client integration

[Documentation index](README.md) · [OpenAPI contract](../api/openapi.yaml)

The ingest and query APIs use separate base URLs. Local Compose exposes ingest
on port `8080` and query on `8082`. Production URLs come from Terraform outputs
`ingest_url` and `query_url`; use HTTPS. All data routes require
`Authorization: Bearer fxg_<key-id>_<secret>` unless authentication is explicitly
disabled in local/dev. The credential determines the tenant; request bodies and
query parameters cannot select another tenant.

## Routes

| Service | Method and path | Result |
| --- | --- | --- |
| Ingest | `POST /v1/ingest` | Accepted batch identity and validation counts |
| Query | `GET /v1/query` | One metric's per-series, per-window values |
| Query | `GET /v1/metrics` | Available metric/kind combinations |
| Query | `GET /v1/labels` | Label keys or values for a metric |
| Query | `GET /v1/stream` | Future rollup updates as SSE |
| All three | `GET /healthz`, `GET /readyz` | Liveness and readiness; no application API key |
| All three | `GET /v1/version` | Build metadata; no application API key |

The optional Prometheus endpoint is operational and separate from
`/v1/metrics`. It is disabled on public staging/prod APIs. Wrong-service routes
return `404`; wrong methods on known routes return `405` with `Allow`.

## Metric model

A series is identified by **tenant + metric name + kind + complete label set**.
Label order does not affect identity. Two kinds with the same metric name and
labels remain separate series. Use consistent names, kinds, units, and label
sets to keep clients understandable; units are a naming convention, not a
separate field or automatic conversion.

| Kind | Send | Useful window aggregations |
| --- | --- | --- |
| `counter` | Nonnegative increments, e.g. `1` per request or `12` for twelve requests | `sum` gives total increments; `count` gives number of observations |
| `gauge` | A finite sampled value, positive or negative | `last`, `avg`, `min`, `max` |
| `histogram` | Individual finite observations in a consistent unit | `p50`, `p90`, `p95`, `p99`, plus scalar statistics |

Do not submit a cumulative counter total as repeated increments: sending `100`
then `110` produces a sum of `210`, not `10`. Gauge `avg` is sample-weighted
(`sum/count`), not time-weighted. `last` follows event time, not arrival order;
timestamps normalize to PostgreSQL microsecond precision and equal timestamps
resolve to the larger value. Counts are stored as integers but `/v1/query`
returns numeric values through `float64`; exact integer precision beyond 2^53
is not guaranteed on that endpoint.

Histogram quantiles use fixed exponential buckets with upper-bound estimates,
not exact observations or interpolated percentiles. Negative values share one
bucket (reported quantile `0`); overflow reports the highest supported boundary.
Choose units that place important observations inside the useful range. See
[histogram storage](architecture.md#histograms) for the layout and limitations.

## Ingestion

Send one UTF-8 JSON object with `Content-Type: application/json`:

```json
{
  "points": [
    {"metric": "http.requests", "kind": "counter", "value": 1,
     "labels": {"service": "checkout", "status": "200"}},
    {"metric": "http.request.duration_ms", "kind": "histogram", "value": 12.5,
     "labels": {"service": "checkout"}}
  ]
}
```

| Field or limit | Contract |
| --- | --- |
| Body | Default 4 MiB maximum; exactly one JSON document; unknown fields rejected |
| `points` | 1–1,000 entries by default; operator may lower the maximum |
| `metric` | Required, at most 200 bytes, ASCII letter/underscore first, then letters/digits/underscore/dot/hyphen; no trailing dot or doubled dot |
| `kind` | Required: `counter`, `gauge`, or `histogram` |
| `value` | Required number; null is invalid; finite and absolute value at most `1e100`; counter values must be nonnegative |
| `timestamp` | Optional RFC 3339 timestamp; omitted or null currently uses request arrival time; default age range is 7 days back to 5 minutes ahead |
| `labels` | Optional object of strings; omitted or null is an empty label set |
| Label count | At most 20 per observation |
| Label key | At most 64 bytes; ASCII letter/underscore then letters/digits/underscores; `__` prefix reserved |
| Label value | At most 256 bytes of valid UTF-8, with control characters rejected |

Lengths enforced by Go are byte lengths. For non-ASCII label values, a JSON
schema character count alone does not capture the byte limit.

Envelope/JSON decoding failures reject the whole request. After successful
decoding, domain validation is per point: one bad metric name can be rejected
while a valid sibling is admitted. Unknown fields or a field with an incompatible
JSON type can prevent decoding and therefore reject the entire batch.

Successful response:

```json
{"batch_id":"generated-batch-id","accepted":2,"rejected":0}
```

HTTP `202` means the valid points were confirmed published. It does not mean
aggregation is complete. Always inspect `rejected`, `errors`, and
`errors_truncated`. `rejected` counts points; `errors` contains field violations
and may contain several for one point. At most 100 violations are returned.
`errors_truncated: true` means additional field violations were omitted.
If all points fail, the response is `422` instead of `202`.

Quota is charged for the decoded point count, including invalid points, before
point validation. Already reserved retries bypass that meter. The default tenant
token bucket permits 10,000 points/s with a 20,000-point burst **per ingest
instance**. Keys may override these values. Read
`X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset` when present;
reset is seconds until the burst bucket refills, not a Unix timestamp. Replayed
responses need not contain rate-limit headers. Quotas are not coordinated across
replicas; see [tenant access](security.md#quotas-and-access-scope).

## Safe retries

1. Assign a fresh `Idempotency-Key` to each logical batch. Use a UUID or another
   identifier unique within the tenant. It may contain 1–255 printable ASCII
   characters, excluding spaces.
2. Serialize the body once and retain those exact bytes and the key until the
   outcome is resolved. Whitespace, field order, or timestamp changes produce a
   different fingerprint even when the JSON means the same thing.
3. Retry network failures, ambiguous timeouts, and retryable server responses
   with the **same bytes and key**. Respect `Retry-After`; otherwise use bounded
   exponential backoff with jitter. Bound concurrent requests and the local
   retry queue so an outage cannot exhaust the producer's memory.
4. On `202`, retain the batch ID for correlation and handle any rejected points.
   If you repair rejected points, send only those repaired observations in a new
   batch with a new key. Do not resend the accepted subset under a new identity.
5. Stop and investigate `409`. Do not silently choose a new key for an ambiguous
   earlier publish. A new key can double-count data.

The default retry reservation lasts **24 hours from initial reservation**, not
24 hours from each retry. Staging/prod uses PostgreSQL, shared across replicas
and restarts. A pending reservation includes the original batch and timestamps;
an uncertain publish is retried under the same batch identity. Confirmed outcomes
replay the original response with `Idempotency-Replayed: true`. An already pending
retry can also set that header while completing the reserved publication.

After expiry a reused key can become a new batch. Clients must resolve or escalate
unknown outcomes before the configured TTL; retries after expiry are not protected
by the HTTP reservation. Omitting the header creates a new batch on every request.
The downstream ledger suppresses broker redelivery of a batch identity; it cannot
recognize independently submitted HTTP batches with new identities.

There is no batch-status HTTP endpoint. Application correlation logs and eventual
queries help diagnose progress, but absence from a query is not evidence that an
ambiguous publish failed. Preserve the original retry identity.

## Querying rollups

```sh
curl -sS -H "Authorization: Bearer $FLUXGATE_API_KEY" \
  "$FLUXGATE_QUERY_URL/v1/query?metric=http.request.duration_ms&from=-15m&agg=p95&label.service=checkout"
```

| Parameter | Meaning |
| --- | --- |
| `metric` | Required exact metric name |
| `to` | RFC 3339 or negative Go duration relative to now; defaults to now |
| `from` | RFC 3339 or negative Go duration relative to resolved `to`; defaults to one hour before `to` |
| `agg` | `sum`, `count`, `avg` (default), `min`, `max`, `last`, `p50`, `p90`, `p95`, `p99` |
| `label.<key>` | Exact-match filter; all supplied labels must match; extra stored labels are allowed |

Use durations such as `-15m` or `-24h`; `-1d` is not a Go duration. Encode label
values as URL query values. Repeating a parameter uses its first value; avoid
duplicate keys. There is no regex matching, group-by, step/resampling, rate
function, or pagination cursor.

The interval is **`[from, to)` on window starts**. A window starting before `from`
is excluded even if some of its observations fall after `from`. A matching window
returns its whole stored aggregate, not just observations before `to`. Align
historical range boundaries to the aggregation window when that distinction
matters. The current window can be partial and older windows can be corrected.

Example response shape (timestamps are illustrative):

```json
{
  "metric": "queue.depth", "kind": "gauge", "aggregation": "last",
  "from": "2026-10-02T12:00:00Z", "to": "2026-10-02T12:02:00Z",
  "series": [{"kind": "gauge", "labels": {"service": "checkout"},
              "points": [{"t": "2026-10-02T12:01:00Z", "v": 42}]}]
}
```

Each kind/label combination has its own series; mixed kinds set top-level
`kind: "mixed"`. Series order is stable; points are oldest-first within a series.
There is no zero-fill for missing windows. Empty results use `series: []`.
Requesting a percentile omits non-histogram windows and supplies a warning.

Default bounds are 744 hours, 500 series, and 50,000 returned points. Series/point
limits return `200` with `truncated: true`; do not treat that as a complete total.
The underlying scan favors newest windows before response sorting. An additional
8 MiB materialized-read budget returns `422`; it is an internal allocation budget,
not a promised maximum HTTP content length. Narrow the time range and add labels
to obtain complete results.

## Discovery

`GET /v1/metrics?limit=500` returns:

```json
{"metrics":[{"metric":"queue.depth","kind":"gauge","series_count":1,"oldest_window":"2026-10-02T12:01:00Z","newest_window":"2026-10-02T12:01:00Z"}]}
```

Entries group by metric **and kind**, and reflect retained rollups. The metrics
limit defaults to 500 and is capped at 1,000.

`GET /v1/labels?metric=queue.depth` returns `{"metric":"queue.depth","labels":["service"]}`.
Adding `&label=service` returns `{"metric":"queue.depth","label":"service","values":["checkout"]}`.
Both accept `limit`, default 500; keys cap at 500 and values at 1,000. Missing,
non-integer, zero, or negative limits use the default. Lists are sorted, bounded,
and have no pagination or truncation indicator. They are discovery aids, not a
complete inventory API for arbitrarily large datasets.

## Live stream

```sh
curl -N -H "Authorization: Bearer $FLUXGATE_API_KEY" \
  "$FLUXGATE_QUERY_URL/v1/stream?metric=queue.depth"
```

Omit `metric` to watch all metrics in the authenticated tenant. A normal stream
uses `text/event-stream`, starts with `retry: 4000` at the default 2-second poll
interval (Compose uses `retry: 2000`), and emits:

```text
event: rollup
data: {"kind":"gauge","metric":"queue.depth","labels":{"service":"checkout"},"window_start":"2026-10-02T12:01:00Z","window_end":"2026-10-02T12:02:00Z","count":2,"sum":84,"min":42,"max":42,"last":42}

: keep-alive
```

The payload is the **current total** of that series/window. Replace the dashboard
entry keyed by metric, kind, labels, and window start; adding the payload to the
previous value would double-count. Streams omit histogram bucket vectors and
percentiles. Use `/v1/query` for percentile reads.

Changes follow per-tenant committed revision order, including corrections to old
windows. Updates between polls may coalesce. The cursor is internal: events have
no `id`, `Last-Event-ID` is not a replay mechanism, and a new connection starts at
the current revision. Reconnect with backoff and periodically reconcile visible
windows through `/v1/query`. This is a live dashboard feed, not a durable change
log or an exactly-once event subscription.

Default limits per query instance: 100 streams total, 8 per tenant, 30 minutes per
connection, a heartbeat every 20 seconds, 5-second database polls, and 10-second
write deadlines. Each poll reads at most 500 rows under a 256 KiB page budget.
Tenant capacity returns `429`; instance capacity returns `503`; both supply
`Retry-After: 5`. Failed database polls are logged and retried on the open stream;
heartbeats alone do not prove fresh data. After headers are sent, disconnects
cannot be represented as a fresh JSON error response.

Browser-native `EventSource` cannot attach an arbitrary bearer header. Use a
fetch-based SSE client or an authenticated backend proxy. Fluxgate does not
provide cookie sessions, query-string credentials, or a built-in CORS policy;
keep API keys out of URLs and deploy browser access through an appropriate
same-origin backend/gateway.

## Errors and response handling

Application failures use `application/problem+json`:

```json
{"type":"https://docs.fluxgate.dev/errors/validation_failed","title":"Unprocessable Entity","status":422,"code":"validation_failed","detail":"The query is not valid.","request_id":"request-id","errors":[{"field":"metric","message":"is required"}]}
```

Branch on `code` and status; `detail` is human-readable and may change. The `type`
is a problem identifier, not a requirement for clients to fetch a hosted page.
Keep `X-Request-Id` with incident reports. Probes use their own JSON status shape;
upstream proxies and transport failures may return other formats or no response.

| Status / code | Client action |
| --- | --- |
| `400 bad_request` | Fix malformed JSON, invalid UTF-8, or idempotency header |
| `401 unauthorized` | Supply a valid enabled credential; response deliberately does not reveal which part failed |
| `403 forbidden` | Check the access policy; included in the shared error vocabulary |
| `404 not_found`, `405 method_not_allowed` | Verify service base URL, path, and method |
| `409 conflict` | Investigate a key reused with different bytes |
| `413 payload_too_large` | Reduce body size before retrying new batches |
| `415 unsupported_media_type` | Send JSON content type |
| `422 validation_failed` | Fix fields/limits or narrow an oversized read; no automatic unchanged retry |
| `429 rate_limited` | Respect `Retry-After`; ingest without it means batch exceeds burst and must be split |
| `503 service_unavailable` | Back off; a publish may already have happened, so preserve write identity |
| `504 timeout`, `500 internal_error` | Preserve identity on write retries; use bounded retries and investigate persistent failures |

HTTP admission defaults to four concurrent non-stream data handlers per instance;
excess requests receive `503` and `Retry-After`. This is separate from point quotas
and stream limits. A graceful shutdown, network reset, or platform timeout can
leave an unknown write outcome even if no structured response arrives.

Implementation: [ingest handler](../internal/api/ingest.go),
[query/stream handlers](../internal/api/query.go), [query shaping](../internal/query/query.go),
[validation](../internal/telemetry/point.go), and [error adapter](../internal/httpx/response.go).
