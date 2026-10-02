# Recovery runbooks

[Documentation index](README.md) · [Operations](operations.md) · [Deployment](deployment.md)

These procedures preserve Fluxgate's two identity systems: HTTP retry reservations
and broker delivery claims. A database restore does not rewind broker
acknowledgments. Restoring one side without coordinating the other can lose data
or count it twice. Rehearse on the actual GCP topology and record measured RPO
(recoverable data loss interval) and RTO (time to restored service).

## Database or broker outage

1. Identify the failing dependency using readiness, logs, backlog age, and platform
   metrics. Preserve the current image/configuration and failure timeline.
2. Keep clients on bounded retries with their original bytes and keys. Ingest may
   return 503 when publishing or retry storage cannot confirm an outcome.
3. Repair connectivity, IAM, database availability, or resource exhaustion. Avoid
   repeated worker restarts: they reintroduce uncommitted work and do not fix the
   shared dependency.
4. Watch source retention and DLQ growth throughout the outage. Recovering the
   dependency after messages expire cannot recover expired payloads from rollups.
5. Once healthy, confirm backlog age declines, admitted deliveries settle, query
   visibility recovers, and no tenant remains stuck on repeated transaction errors.
6. Inspect/replay dead letters only after fixing their cause and validating ledger
   coverage. Reconcile representative tenant/window totals before closing the
   incident.

Expected behavior: uncommitted deliveries are retried; committed claims suppress
redelivery after a lost acknowledgment. Successful transactions for other tenants
can settle while one tenant fails, within the bounded flush worker pool. This
does not guarantee unlimited outage tolerance: broker retention, retry TTL,
ledger retention, and storage capacity still bound recovery.

## Dead-letter replay

There is no turnkey production replay CLI in this repository. Use an operator
tool built around the deployed Pub/Sub APIs and test it on a small sample before
bulk replay. Do not send dead-letter payloads through `/v1/ingest`, which assigns
new batch identities.

1. Identify the dead-letter inspection subscription from Terraform output
   `dead_letter_subscription`. Pull a bounded sample **without auto-acknowledging**.
   Preserve a secured copy for diagnosis and track original message IDs.
2. Determine whether the cause is malformed/unsupported envelopes, permissions,
   database failure, capacity, or an implementation defect. Temporary dependency
   failures can exhaust attempts too; a dead letter does not imply bad user data.
3. Confirm the relevant `processed_batches` claims remain available and that
   restored data and source payloads cover the intended replay range. If the
   horizon is outside retained claims, stop and design reconciliation/rebuild
   before publishing anything.
4. Unwrap Pub/Sub's dead-letter wrapper to recover the original Fluxgate envelope.
   Validate its schema and routing metadata. Preserve tenant ID, batch ID,
   `received_at`, point timestamps, metric/kind/labels, and values. Do not invent
   a fresh identity to work around a rejection.
5. Republish the original envelope and appropriate attributes to the raw topic
   using a bounded rate. Wait for publish confirmation, then acknowledge the
   inspected DLQ message. If that ack is uncertain, repeating the same original
   identity remains safe within the ledger horizon.
6. Monitor replay backlog, new dead letters, durable claims, and query totals.
   Reconcile expected contributions before increasing the replay rate or declaring
   the queue recovered.

If payload correction is necessary, classify whether any original windows were
already committed. Mutating the content under an existing batch ID can cause
corrected contributions to be skipped; changing its identity can double-count
already committed data. Such repairs require an explicit data-correction plan,
not blind replay. There is no general public API for subtracting or replacing a
stored rollup.

## Consistent backup and restore

The recovery unit is the complete application database:

- `rollups`
- `processed_batches`
- `ingest_requests`
- `tenant_revisions`
- `schema_migrations`

Keep them at one consistent recovery point. Preserve or reprovision the expected
owner/runtime roles and grants. Restoring rollups without the matching ledger
can double-count replay; restoring only the ledger can suppress missing rollups.
Missing retry reservations can turn an old client retry into a new identity.

### Rehearsal on an isolated target

1. Record source image digests, schema version, configuration, window/histogram
   policy, key version, database recovery point, and source/DLQ replay horizons.
2. Restore into an isolated database/instance. Keep production consumers and
   producers from accidentally connecting to it. Select an application version
   compatible with that schema.
3. Apply reviewed migrations if needed and provision runtime roles with the
   migration identity. Check cross-service permission denials as well as allowed
   operations.
4. Compare expected table contents/identities and selected rollup statistics.
   Verify a known HTTP retry returns the original batch, a changed body conflicts,
   and a replayed committed batch leaves totals unchanged.
5. Validate a fresh write through restricted ingest/aggregator/query credentials.
   Verify revisions and stream behavior after reconnecting.
6. Record elapsed restore/validation time, unresolved gaps, and required replay.
   A successful restore command alone is not a recovery acceptance result.

`python scripts/verify_pipeline.py` implements a local, quiesced logical
backup/restore rehearsal into a separate disposable database, with exact table
fingerprints, restricted roles, retry identity, duplicate suppression, and a new
write. It does not validate Cloud SQL PITR, restore of lost accepted writes, or
production RPO/RTO.

### Production cutover

1. Close or redirect ingestion in a controlled way and stop all consumers before
   selecting/restoring the target recovery point. Preserve pending client retries
   and incident evidence.
2. Ensure retained topic messages or a suitable snapshot/archive cover everything
   acknowledged after the database recovery point. Subscription state does not
   roll back with SQL. Consult the operator procedure in the
   [Terraform recovery guide](../deploy/terraform/README.md#release-checks-requiring-a-real-staging-project)
   and Google's [Pub/Sub replay guide](https://docs.cloud.google.com/pubsub/docs/replay-overview)
   for retention requirements and eventual seek consistency.
3. Restore all five tables consistently and provision runtime grants. Reconcile
   the restored ledger horizon with the chosen replay interval before starting
   consumers. Publication time used for seek is different from metric event time.
4. Recover missing HTTP retry reservations or coordinate affected clients so
   pre-restore retries cannot be submitted as fresh identities. **Broker replay
   alone cannot reconstruct the original HTTP retry response/reservation.** Keep
   ingestion closed until this is resolved.
5. Replay original identities at a controlled rate. Compare expected accepted
   data with restored/replayed series/window statistics, and record irrecoverable
   gaps rather than declaring them recovered.
6. Reopen reads and reconnect stream clients with REST reconciliation. Reopen
   writes after retry continuity is established. Monitor backlog, duplicate
   suppression, query freshness, and credential versions during cutover.
7. Retain the incident record with achieved RPO/RTO and all exceptions. Do not
   retire the old recovery material until the target is accepted.

If no payload source covers a lost interval, Fluxgate cannot reconstruct raw
observations from its aggregates. Report that limitation and coordinate a
tenant-specific recovery or explicit data-loss decision.

## Rollback and incompatible changes

For an application-only compatible release, restore the previous immutable
service digests and an appropriate credential version using a reviewed Terraform
plan. Verify readiness and functional writes/reads before considering rollback
complete. Preserve pending deliveries; the ledger handles duplicate delivery
across compatible revisions.

Changing an image does not reverse a migration. Migrations 0003 and 0005 require
old aggregators to be stopped during application. Migration 0007 builds the
correct retention index and can block writes; plan a maintenance window. There
are no automatic down migrations. Use a tested forward repair or the consistent
restore procedure for incompatible schema changes.

Changing the window width, histogram layout, tenant identity mapping, or deleting
delivery claims changes data semantics. Treat these as explicit migrations with
an acceptance/replay strategy. Do not roll mixed window policies against the
same retained data and call the result a routine scaling change.

## Recovery completion evidence

Record the incident/rehearsal date, operator, affected tenants/time ranges,
source and target builds, database recovery point, message retention coverage,
retry-reservation continuity, migrations/grants, replay rate and counts, exact
reconciliation results, backlog recovery, credential cutover, achieved RPO/RTO,
and any known lost or uncertain observations. Keep sensitive payloads and
credentials out of public reports.
