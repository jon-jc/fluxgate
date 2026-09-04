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
3. **Key the ledger on `(batch_id, window_start)`.**

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
the ledger. Messages held unacknowledged for up to a window's duration, which
means the acknowledgement lease has to be sized generously. And a third state,
"flushing", for a redelivery that arrives mid-write: the outcome is not knowable
yet, so the message is handed back rather than guessed at.

**What it does not cover.** Two aggregator instances processing the same batch
concurrently would both pass the ledger check before either commits. Pub/Sub
does not deliver one message to two subscribers on the same subscription, so
this does not arise — but it is a property of the broker, not of this design,
and it is the assumption to re-examine if the transport ever changes.

**When to revisit.** If duplicate suppression ever needs to survive a change of
broker, the check and the write would have to become one atomic operation —
which in practice means moving the accumulator itself into the database, at a
large cost in throughput.
