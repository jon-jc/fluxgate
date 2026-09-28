# Deploying Fluxgate

This directory prepares private Cloud SQL, Pub/Sub, four service identities,
Secret Manager, Artifact Registry, alerts, a migration job and three Cloud Run
services. It defaults to infrastructure only. Local validation never deploys.

## Validate without a GCP project

```sh
terraform init -backend=false
terraform fmt -check -recursive
terraform validate
terraform test
```

The six mocked plan tests cover bootstrap, immutable images, incomplete service
configuration, connection limits and production gates. They do not prove live
IAM, networking, organization policy, or capacity. The provider lock file covers
Linux and Windows. Review dependency upgrades and commit the updated lock.

## First deployment (operator procedure)

1. Choose an isolated project and region, enable billing, and create a restricted
   GCS state bucket with versioning and audit logging. State contains database
   passwords. Grant the deployment principal the required resource management
   permissions and permission to act as the four service accounts. Enable the
   Service Usage API if the project is new; Terraform enables its service APIs.
2. Copy `dev.tfvars.example` or `prod.tfvars.example`, fill in the project, retain
   `deploy_services = false`, and initialize the GCS backend:

   ```sh
   terraform init -backend-config="bucket=YOUR_STATE_BUCKET" -backend-config="prefix=fluxgate/prod"
   terraform plan -var-file=prod.tfvars -out=bootstrap.plan
   terraform apply bootstrap.plan
   ```

3. Build and publish `ingest-api`, `aggregator`, `query-api` and `migrate` from the
   same reviewed commit using `build/docker/Dockerfile` and `--build-arg SERVICE`.
   Pass `--build-arg COMMIT=COMMIT_SHA --build-arg VERSION=RELEASE_VERSION` so the
   version endpoint identifies the running artifact.
   Use the `image_repository` output. Record the registry digest for each image
   in `images = { ingest = "...@sha256:...", aggregator = "...@sha256:...",
   query = "...@sha256:...", migrate = "...@sha256:..." }`. Tags alone are rejected.
4. Populate the `api_keys_secret` output with a valid nonempty API key JSON
   document (see the root README). Store only secret hashes there. Send plaintext
   keys through your credential channel. Run `gcloud secrets versions add
   SECRET_ID --data-file=keys.json --project=PROJECT`, and set `api_keys_version`
   to that numeric version. Remove the local credential document securely.
5. Apply the image configuration with services still disabled. Run the migration
   job and wait for success:

   ```sh
   gcloud run jobs execute fluxgate-prod-migrate --region=REGION --project=PROJECT --wait
   ```

   The job applies schema migrations and removes the default Cloud SQL
   administrative grants from all three runtime database users. The owner
   credential is readable only by the migration identity. Services refuse to
   start on staging/prod with an owner or unprovisioned runtime credential.
   Terraform creates the job; it does not execute or verify its success.
6. Set `deploy_services = true`, review a saved plan, then apply. Production
   requires regional SQL, warm ingest instances and notification channels.
   The capacity gate reserves connections for two complete revisions at maximum
   scale plus administration. Verify the selected SQL tier can afford the
   configured `max_connections`; the arithmetic gate is not a memory benchmark.
7. Exercise the release checks below before sending production traffic.

Cloud SQL connections use the Go connector over private VPC IPs, with IAM
connection authorization and certificate-verified encryption. The DSN uses
`sslmode=disable` because the connector itself establishes TLS. Never copy that
DSN into a client without the connector. No service-account keys are needed.

Ingest and query are public at the Cloud Run layer; the application API key is
required for data routes. The aggregator has internal ingress and no public
invoker binding. Organization policies may forbid `allUsers`; in that case add
your approved authenticated gateway and adapt the invocation policy before apply.
Public APIs disable the unauthenticated Prometheus endpoint. Native Cloud Run,
SQL and Pub/Sub metrics back the supplied alerts. Optional traces need an actual
TLS OTLP collector via `otlp_endpoint`; there is no implicit Cloud Run collector.

## Runtime access

| Identity | Database permissions |
| --- | --- |
| ingest | Read, insert and update retry reservations |
| aggregator | Read/write rollups, claim delivery ledger entries, prune retained data |
| query | Read rollups and committed tenant revisions |
| migrate | Own schema and provision runtime grants |

Tenant isolation remains enforced by API authorization and SQL filters; these
roles isolate services, not individual tenants. The provisioning command assumes
an isolated Fluxgate database, fixed role names and the `public` schema. It
rejects unexpected role memberships or privileged attributes. Do not grant extra
memberships or table ownership to runtime users. After adding tables, explicitly
update the grants and integration tests. Stop services before replacing database
users, rerun provisioning, and verify permissions before enabling them again.

## Upgrades and rollback

Take a verified backup before migrations. Run the new migration image using the
same owner. **Migrations 0003 and 0005 require stopping every old aggregator before they
run**: older writers do not understand the new delivery identity constraints or stream revisions.
Migration 0007 corrects an index-name collision in 0006 and builds the retention
index on `window_end`; plan a maintenance window because index
creation can block writes on large existing tables. Rerun the migration job's
runtime-role provisioning with this release to grant the row-lock privileges used
by cleanup. Verify that expired rows drain before resuming normal traffic.
Existing deployments must move/import the renamed Cloud Run resource addresses
into `google_cloud_run_v2_service.service["ingest"|"aggregator"|"query"]` and review
all state changes. Do not apply the bootstrap defaults to an existing live stack.

For compatible schema changes, pin the previous service image digests and key
version and apply the reviewed plan. A destructive or incompatible migration
needs a tested forward repair or database restore; changing an image is not a
schema rollback. Rotate one secret version and revision at a time and verify
readiness before retiring old versions.

## Retention and recovery

The ledger covers raw retention plus seven days in the DLQ, the 24-hour ingest
retry horizon and a safety day. Keep those horizons aligned when changing limits.
Topic snapshots, exported archives or replay beyond that horizon require retaining
or rebuilding the ledger first. Otherwise replay can count previously processed
observations again. Keep original tenant, batch ID and timestamps during replay.
Pub/Sub wraps dead-letter messages: unwrap the original envelope before republishing.
Only acknowledge inspected DLQ messages after successful republish. Diagnose and
fix the cause first; temporary database or IAM failures also dead-letter messages.

The default delivery-attempt limit is 20. Cleanup runs every five minutes in
10,000-row transactions, with a 30-second time budget per table. Monitor cleanup
errors and table growth, and tune the interval against measured traffic. Completed
ingest retry records discard the original points; pending publishes retain them
until confirmation or expiry. Point and stream limits apply per service instance,
so autoscaling changes the total allowance. Hard tenant-wide quotas need a shared
gateway or another coordinated admission mechanism.

## Release checks requiring a real staging project

A database restore does not rewind Pub/Sub acknowledgments. Keep consumers stopped
while selecting a recovery point covered by retained topic messages or a matching
snapshot, then deliberately replay and reconcile before reopening traffic. Topic
retention permits this even though the subscription's `retain_acked_messages` is
false; a routine redeploy does not trigger replay. Seek uses publication time and
is eventually consistent. Follow the [Pub/Sub replay guide](https://cloud.google.com/pubsub/docs/replay-overview)
and measure the procedure in staging.

Restore `rollups`, `processed_batches`, `ingest_requests`, `tenant_revisions` and
`schema_migrations` as one consistent database. Reapply runtime-role provisioning
before starting services. If retry reservations were lost after the recovery point,
keep ingestion closed until those records are recovered or affected clients have
completed a coordinated cutover and stopped pre-restore retries. Broker replay
alone cannot rebuild HTTP retry outcomes. Keep original message identities during
replay and keep the recovery range inside the restored ledger's coverage.

`scripts/verify_pipeline.py` also exercises a quiesced local logical backup and
restore into a separate database. It compares all five tables, starts the restored
services with restricted users, checks HTTP retry identity/conflicts, republishes
an old batch to verify ledger suppression, and verifies a new write. This checks
application recovery; it does not certify Cloud SQL PITR, an RPO/RTO, or recovery
from a backup that lost accepted writes.

- Confirm private SQL connectivity and denial of cross-service table access.
- Publish, retry across replicas/restarts, query totals and compare to sent data.
- Terminate aggregators mid-window, interrupt database connectivity, and verify
  recovery, duplicate suppression, DLQ handling and alert delivery.
- Sustain expected peak traffic plus headroom while measuring latency, memory,
  SQL connections, backlog age and per-instance quotas; adjust capacity limits.
- Restore a backup into a separate instance, measure recovery time, and reconcile
  the replay/ledger horizon before enabling writes.
- Verify key rotation, rollback, on-call ownership, regional requirements and
  agreed retention/RPO/RTO. Record results with the release.

These cloud exercises are deliberately separate from preparation and local CI.
Do not describe an untested deployment as production proven.
