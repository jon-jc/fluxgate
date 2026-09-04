# 1. Asynchronous pipeline rather than synchronous writes

**Status:** Accepted

## Context

The ingest endpoint could write rollups to Postgres directly. That would remove
the broker, the consumer, the delivery ledger and most of the complexity in this
codebase.

The reason not to is what happens under load and under failure. Telemetry
arrives in bursts that correlate exactly with the incidents it exists to
describe: when a service starts failing, it emits more, not less. A synchronous
path couples the edge's availability to the database's, so the moment the
database is slow, the ingest endpoint is slow, and the telemetry needed to
diagnose the problem stops arriving precisely when it is wanted.

## Decision

The edge validates and publishes. A separate consumer aggregates and writes.

## Consequences

**What this buys.** The edge stays available when the database is not — messages
queue in Pub/Sub, which is built for exactly that. The two halves scale
independently: the edge with request rate, the consumer with series cardinality.
Aggregating before writing turns a write per point into a write per series per
window, which is the difference between a database that keeps up and one that
does not.

**What it costs.** Rollups are not readable the instant a point is accepted;
there is a window plus a flush interval of lag, and the API says so rather than
pretending otherwise. At-least-once delivery has to be handled explicitly — see
[ADR 2](0002-exactly-once-accumulation.md), which exists entirely because of this
decision. Debugging spans three processes, which is why distributed tracing was
not optional.

**When to revisit.** If the deployment only ever has one small tenant, the
broker is overhead. The synchronous version is genuinely simpler and would be
the right answer at that scale.
