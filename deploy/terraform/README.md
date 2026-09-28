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
| query | Read rollups only |
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
same owner. **Migration 0003 requires stopping every old aggregator before it
runs**: older writers do not understand the new delivery identity constraints.
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

## Release checks requiring a real staging project

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
