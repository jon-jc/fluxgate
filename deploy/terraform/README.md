# Deploying Fluxgate

Terraform for the whole platform: Pub/Sub topology, Cloud SQL, three Cloud Run
services, per-service identities, secrets and alert policies.

## What gets created

| | |
| --- | --- |
| **Pub/Sub** | Raw topic, dead-letter topic, working subscription with a retry and dead-letter policy, and an inspection subscription on the DLQ |
| **Cloud SQL** | Postgres 17, private IP only, automated backups, point-in-time recovery on production tiers |
| **Cloud Run** | `ingest-api` (public), `aggregator` (internal), `query-api` (public) |
| **IAM** | One service account per service, each holding only what that service does |
| **Secret Manager** | API key document and the database URL, mounted rather than passed as plain environment variables |
| **Monitoring** | Four alert policies, each documented with what to check and in what order |

## Prerequisites

The state bucket has to exist before the first `init` — Terraform cannot create
the bucket that holds its own state:

```bash
gsutil mb -l us-central1 gs://fluxgate-tfstate-$PROJECT
gsutil versioning set on gs://fluxgate-tfstate-$PROJECT
```

Versioning is not optional. It is what makes a corrupted state file recoverable.

Enable the APIs:

```bash
gcloud services enable \
  run.googleapis.com pubsub.googleapis.com sqladmin.googleapis.com \
  secretmanager.googleapis.com servicenetworking.googleapis.com \
  vpcaccess.googleapis.com artifactregistry.googleapis.com \
  monitoring.googleapis.com cloudtrace.googleapis.com
```

## Applying

```bash
cp dev.tfvars.example dev.tfvars   # then fill it in

terraform init \
  -backend-config="bucket=fluxgate-tfstate-$PROJECT" \
  -backend-config="prefix=fluxgate/dev"

terraform plan  -var-file=dev.tfvars
terraform apply -var-file=dev.tfvars
```

`image_tag` has no default and rejects `latest`: a mutable tag makes a rollback
ambiguous and makes it impossible to tell from state which build is running. Use
the commit SHA that CI built.

## After the first apply

The API key document is created empty, because putting it in a variable would
put the plaintext in state — which lives in a bucket readable by anyone who can
run a plan. Populate it out of band:

```bash
SECRET=$(openssl rand -hex 32)
DIGEST=$(printf %s "$SECRET" | sha256sum | cut -d' ' -f1)

cat > keys.json <<JSON
[{"key_id":"acme01","tenant_id":"acme","secret_sha256":"$DIGEST",
  "rate_limit_per_second":50000,"burst":100000}]
JSON

gcloud secrets versions add "$(terraform output -raw api_keys_secret)" \
  --data-file=keys.json
rm keys.json

echo "give the client: fxg_acme01_$SECRET"
```

The service stores only the digest, so this is the one moment the plaintext
exists. Losing it means issuing a new key, not recovering the old one.

## Things worth knowing before you apply

**Dead lettering silently does nothing without two IAM bindings.** Pub/Sub's own
service agent — not the consumer — performs the dead-letter publish, and it
needs `pubsub.publisher` on the dead-letter topic and `pubsub.subscriber` on the
source subscription. Get this wrong and there is no error at apply time and none
at run time: messages simply keep being redelivered forever. Both bindings are
in `pubsub.tf`, and the subscription depends on them.

**The aggregator is deliberately not autoscaled.** Each instance holds open
windows in memory and writes them only when they close, so scaling in mid-window
hands the survivors a redelivery of everything the departing instance had not
yet flushed. Correct, thanks to the delivery ledger, but pure rework. Change the
instance count deliberately, ideally when traffic is low.

**`cpu_idle` is false on the aggregator and the query API.** Both do work
between requests — flushing on a timer, polling for a live tail. With idle CPU
throttling, the flush timer would be throttled to a stop and windows would only
be written when a message happened to arrive.

**Deletion protection is on for the database, on every tier.** A destroyed
Cloud SQL instance is not recoverable from Terraform state.

## Rolling back

Cloud Run keeps revisions. The fastest rollback does not involve Terraform:

```bash
gcloud run services update-traffic fluxgate-prod-ingest-api \
  --to-revisions=PREVIOUS=100 --region=us-central1
```

Then re-apply with the previous `image_tag` so state matches reality. Doing it in
that order means the outage ends before the paperwork starts.

## What is not here

- **CI/CD.** The workflow builds and tests; it does not deploy. Wiring apply to
  a merge needs Workload Identity Federation and an approval gate, which is a
  decision about who may deploy rather than a Terraform question.
- **A custom domain and TLS.** Cloud Run's generated URL is used directly.
- **Multi-region.** One region, one database. Cross-region would need a
  replication story for the rollups that does not exist yet.
