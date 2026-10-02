# 2. Exactly-once accumulation via a per-window delivery ledger

**Status:** Accepted

## Context

Pub/Sub delivers at least once. A redelivered batch counted twice corrupts every
aggregate it touches, and nothing downstream can detect it: a sum that is 3%
too high looks exactly like a sum that is correct.

The usual answers are all inadequate here:

- **Ignore it.** Duplicates are rare in practice. But they are systematically
  more likely during an incident — nacks, ack-deadline expiries, restarts — so
  the data is least trustworthy exactly when someone is relying on it.
- **Deduplicate in memory.** Handles a redelivery within one process lifetime.
  Handles nothing after a restart, which is when redeliveries actually cluster.
- **Make the write idempotent.** Aggregation is additive. `sum = sum + delta` is
  not idempotent, and rewriting it to be would mean storing per-batch
  contributions forever.

## Decision

Three mechanisms together, none of which is sufficient alone:

1. **Acknowledge only when the data is durable.** The subscriber runs in manual
   mode; the runner settles a message once every window it fed has committed.
2. **Commit rollups and a delivery ledger in one transaction.** No interval
   exists where the data is stored but the batch is not recorded, or the
   reverse.
3. **Key the ledger on `(tenant_id, batch_id, window_start)`.**

The third is the one that is easy to get wrong. A batch straddling a window
boundary feeds two windows that flush at different times. Keyed on `batch_id`
alone, a batch whose first window committed and whose second failed would be
recorded as fully processed — and the retry that should have rebuilt the second
window would be skipped, losing it silently.

## Consequences

Both crash windows are correct:

| Crash point | Ledger | Acknowledged | On redelivery |
| --- | --- | --- | --- |
| Before commit | absent | no | Re-accumulated ✓ |
| After commit, before ack | present | no | Skipped ✓ |

**What it costs.** A ledger table that grows with batch volume, pruned on a
retention that must outlive the longest possible redelivery — configuration
validation enforces the lower bound. A database round trip per message to check
the ledger. Messages held unacknowledged until a durable checkpoint, which
means the acknowledgement lease has to survive storage delays. And a third state,
"flushing", for a redelivery that arrives mid-write: the outcome is not knowable
yet, so the message is handed back rather than guessed at.

**Concurrent deliveries.** The broker can redeliver while the original consumer
is still processing. Each tenant checkpoint inserts all delivery claims in its
transaction before updating totals. An existing claim aborts that tenant's
transaction. The consumer nacks its deliveries, whose retries rebuild only the
uncommitted contributions. Other tenants commit and settle independently through
a bounded worker pool; one transaction never needs another tenant's revision
lock. All queued tenant transactions share the checkpoint's storage deadline.
A duplicate waiting in memory waits for durability; it cannot acknowledge the
original broker message early. Admission and collection share a lock so a flush
cannot detach a point from its delivery bookkeeping.

**Late data.** A local watermark cannot establish completeness across replicas.
Accepted late points and failed writes therefore remain eligible for additive
corrections. Cardinality pressure rejects the entire batch for redelivery.

**Receive flow control.** Waiting only for event-time closure can fill the
subscriber's receive budget before the newer messages needed to close a window
can arrive. The worker therefore checkpoints all buffered windows on the flush
timer, at half the configured outstanding-message or encoded-byte limit, and
after an engine-capacity rejection. Pressure signals coalesce into one queued
checkpoint. Partial windows merge additively on later checkpoints, using the
same transactional claims and revision ordering. Acknowledgments and receive
credit release still follow commit; failed writes nack for safe reconstruction.
The timer handles large messages that cannot fit even below a pressure threshold.

Queries and SSE may expose partial windows sooner. Event-time boundaries and
last-value ordering are unchanged; a visible window has never been proof that
no later accepted correction can arrive. Decoded point payloads are discarded
after admission instead of being retained beside their aggregates. Pending
message and encoded-byte gauges include transactions still in progress.

**Upgrade.** Migration 0003 adds the tenant to the ledger key and the metric kind
to the rollup key. Drain old aggregators before applying it, then start only the
new version. Older binaries use conflict targets that no longer exist. Restore
from a tested backup for a schema rollback; do not roll back only the image.

**When to revisit.** Frequent overlapping flushes waste work. Measure conflict
rates and database throughput before considering per-batch durable staging.
