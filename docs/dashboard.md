# Metrics dashboard

The [hosted metrics workspace](https://fluxgate-docs.vercel.app/dashboard/)
includes an explicit synthetic demo and can connect directly to your Fluxgate
query API. The same application is embedded in the query binary at `/dashboard/`.
It needs no Node runtime, extra server, or database of its own.

## Connect your data

For the local Compose stack, open `http://localhost:8082/dashboard/`, choose
**Connect API**, use `http://localhost:8082`, and enter the development key
`fxg_local_local-dev-secret`. Start the stack using [Getting started](getting-started.md).
The page and API share an origin, so no CORS configuration is needed.

For a deployed query service, open its `/dashboard/` route and connect to that
service's HTTPS origin. The dashboard's public shell contains no tenant data;
every read and live stream still requires the tenant's API key.

To use the Vercel-hosted dashboard with your own HTTPS query API, set this on the
query service before deploying it:

```sh
QUERY_ALLOWED_ORIGINS=https://fluxgate-docs.vercel.app
```

For Terraform-managed services, use
`query_allowed_origins = ["https://fluxgate-docs.vercel.app"]` in your reviewed
inputs. Terraform injects the setting into the query service only.

This comma-separated allowlist accepts at most 16 exact origins. Wildcards,
credentials, paths, and non-HTTPS origins on staging/prod are rejected at boot.
CORS is disabled by default. It enables only browser GET requests to the four
read API routes, including authenticated SSE. It grants no tenant access by
itself, enables no cookies, and does not expose the ingest service to browsers.
For custom domains, list the actual origin hosting the dashboard.

The hosted HTTPS page cannot be used to bypass browser restrictions on local
HTTP services. Open the local query service's dashboard for local development.
There is no Vercel proxy forwarding credentials to arbitrary destinations.

## Explore metrics

- **Overview** shows up to four metric cards and a catalog of retained metrics.
  Each card reports the latest value of its first returned series over the last
  hour; it is not a sum or percentile across series. Cards use sum for counters,
  last for gauges, and p95 for histograms. The query API has no metric-unit field;
  the `_ms` naming convention is displayed as milliseconds.
- **Metric explorer** queries a metric, one of ten aggregations, a rolling range
  (15m, 1h, 6h, or 24h), or an explicit date range. Custom dates use your browser's
  local timezone; charts, tooltips, and data tables display UTC.
- **Label filters** are exact matches combined with AND. Suggestions come from
  the bounded label-discovery API; you can enter an exact value manually.
- **Series and data tables** let you inspect all returned observations with
  25-row pages. Chart up to 12 series at once. A chart simplifies large series
  while retaining extrema; table data and CSV exports retain every returned point.
- **Find a metric** searches the discovered catalog with Ctrl/Cmd K. Catalog
  discovery is limited to 1,000 metric/kind entries and has no server pagination.
- **Saved views** store query settings in this browser, including label filters,
  but never results or credentials. Demo and connected-API views are distinguished.
  A copied query link includes filters in its URL fragment, but no API origin or
  key. Only share filters that are appropriate for the recipient.
- **Export CSV** includes all returned series, window timestamps, values, and
  the API truncation flag. It is not restricted to the chart selection or table
  page. Spreadsheet formula prefixes in text cells are escaped.

The dashboard does not zero-fill missing observations, add percentiles together,
infer raw-event rates, or reinterpret rollup totals as immutable final windows.
It displays server truncation and percentile warnings. Narrow a truncated query
before using it as a complete result. See [query semantics](api.md#querying-rollups).

## Live activity

Live mode opens one authenticated SSE connection for the selected metric.
The feed retains at most 50 recent events. Events contain replacement totals,
not deltas, and may include late corrections. They do not contain histogram
buckets, so the dashboard re-queries REST instead of fabricating live percentiles.
Refreshes are coalesced to at most one query every five seconds.

After reconnects, REST reconciliation fills the current chart range. SSE itself
has no replay cursor or historical backlog. Idle connections time out and retry
with backoff; server retry advice is honored. Hidden browser tabs release their
stream and reconcile when visible again. Custom historical ranges disable live
mode. Busy query responses pause automatic refresh so the dashboard does not
amplify overload.

## Credentials and browser storage

API keys remain in JavaScript memory only. They are never written to localStorage,
sessionStorage, URL parameters, saved views, or exported files. Closing the
connection form clears its password field. Disconnecting or leaving the page
clears the in-memory key. A rejected key stops live activity and clears the
displayed result. Requests omit cookies, reject redirects, and send keys only to
the origin explicitly selected in the connection form.

Saved query settings and the color theme persist locally. Anyone with access to
that browser profile can read saved metric names and filters. This dashboard does
not add a user-management system or read-only key scopes: Fluxgate's existing
tenant key can both read and ingest through the corresponding service APIs.

## Development and checks

Dashboard assets live in `internal/dashboard/web/`. Go embeds those files; the
documentation build copies the same files into `/dashboard/` on Vercel. Changes
therefore reach both distributions without maintaining separate implementations.
The Docker context explicitly allows these application assets.

Run Go router/configuration tests and the documentation test suite when changing
the dashboard. Browser checks cover demo exploration and a deterministic
authenticated API fixture, including filters, failed credentials, stale request
cancellation, live updates, saved views, exports, mobile layout, and accessibility.
Real GCP deployment and workload qualification remain separate release gates.
