# Deployment and release guide

[Documentation index](README.md) · [Terraform operator procedure](../deploy/terraform/README.md)

The supplied deployment targets Google Cloud Run, Pub/Sub, private Cloud SQL,
Secret Manager, Artifact Registry, and Cloud Monitoring. Preparation can be
validated without a GCP project. Creating infrastructure, publishing images,
running cloud migrations, and sending production traffic are separate operator
actions.

## Validate without deploying

From the repository root:

```sh
cd deploy/terraform
terraform init -backend=false -input=false
terraform fmt -check -recursive
terraform validate
terraform test
```

Terraform 1.9 or newer is required. These checks initialize provider schemas and
run six mocked plan tests. They do not create cloud resources, execute a migration,
or prove IAM/network access. Use a fresh checkout for isolated validation instead
of repurposing an initialized production state directory.

For application preparation, run the checks in [development](development.md),
the packaged recovery verifier, and the [capacity procedure](capacity.md). Review
container scan findings in [security scanning](security-scanning.md). CI builds
and tests images but does not publish a release or deploy services automatically.

## Target topology

| Component | Supplied deployment behavior |
| --- | --- |
| Ingest | Public Cloud Run invocation; application bearer key; 1 CPU / 512 MiB; request-driven CPU |
| Aggregator | Internal ingress; fixed instance count; 1 CPU / 1 GiB; CPU always allocated |
| Query | Public Cloud Run invocation; application bearer key; 1 CPU / 512 MiB; CPU always allocated for live streams |
| Migrate | Separate Cloud Run job/identity with owner credential; operator executes it |
| PostgreSQL | Private Cloud SQL, per-service login secrets, backups/PITR configuration, runtime permission checks |
| Pub/Sub | Raw topic, worker subscription, dead-letter topic and inspection subscription |
| Observability | Native infrastructure alerts; optional external OTLP collector; private application scraping needs separate setup |

The Cloud Run request concurrency settings (ingest 80, query 40) are platform
admission settings. The application's default four concurrent ordinary data
handlers remains a separate bound. More platform concurrency does not override
that memory protection. Query request timeout is one hour at the platform layer;
individual streams default to 30 minutes in the application.

## First release sequence

Follow the exact commands in the [Terraform procedure](../deploy/terraform/README.md#first-deployment-operator-procedure).
The required order is:

1. Choose the project/region, state bucket, deployment principal, notification
   channels, database tier, and workload/recovery objectives. Restrict remote state
   because it contains database passwords.
2. Review and apply infrastructure with `deploy_services=false`. This creates
   resource identities and secrets without starting data services.
3. Build all four binaries from the same reviewed commit. Scan them, publish to
   the `image_repository` output, and record immutable digest references. The
   module rejects mutable tags for service inputs.
4. Populate the API key secret out of band; record its numeric version in
   `api_keys_version`. Keep plaintext bearer tokens outside Terraform state.
5. Apply the four image references with services still disabled. Execute the
   migration job from output `migration_job`, and wait for successful completion.
   It also provisions the restricted runtime database roles.
6. Set `deploy_services=true`, review the saved plan, then apply. The module can
   validate image/key inputs but cannot prove the migration job actually ran.
7. Verify the functional and operational acceptance record below before admitting
   production traffic. Keep the previous known-good digests/key version available.

Build example in Bash, without publishing:

```sh
COMMIT=$(git rev-parse HEAD)
for service in ingest-api aggregator query-api migrate; do
  docker build -f build/docker/Dockerfile \
    --build-arg SERVICE="$service" --build-arg COMMIT="$COMMIT" \
    --build-arg VERSION="$COMMIT" \
    --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -t "fluxgate/$service:review" .
done
```

Use the recorded registry digest after an approved push, not a local image ID,
as the Terraform image reference. Development builds without build arguments
can report `dev`/`unknown`; do not use ambiguous provenance for releases.

## Upgrades

Review schema, envelope, window, histogram, permission, and retry/retention
compatibility before mixing revisions. Take a verified backup and retain an
appropriate broker replay source. Run new migrations with the owner identity
and rerun runtime-role provisioning whenever grants change.

| Migration | Operational consideration |
| --- | --- |
| 0001 | Initial rollups, histogram merge function, delivery ledger |
| 0002 | Query indexes |
| 0003 | Tenant-scoped delivery keys and kind-aware rollup keys; stop old aggregators first |
| 0004 | Durable HTTP retry reservations |
| 0005 | Tenant commit revisions; stop old aggregators first |
| 0006 | Historical retention-index change; superseded by 0007 correction |
| 0007 | Correct `window_end` retention index; can block writes, so schedule maintenance |

Existing Terraform deployments may need state moves/imports for the service
resource addresses; see the [upgrade procedure](../deploy/terraform/README.md#upgrades-and-rollback).
Do not apply bootstrap defaults to a live stack: `deploy_services=false` can plan
removal of existing runtime services.

Schema-compatible rollbacks restore previous image digests through a reviewed
plan. Incompatible schema changes need forward repair or coordinated restore;
there is no automatic schema rollback. See [recovery](recovery.md#rollback-and-incompatible-changes).

## Scaling a release

Measure point rate, active series, tenant distribution, payload size, metric kind
mix, window turnover, historical backfill, query ranges, concurrent streams, and
retention growth separately. The existing gauge-heavy emulator profiles are a
baseline, not a substitute for that workload.

The database connection gate requires:

```text
2 * (ingest_max_instances + query_max_instances + aggregator_instances)
  * database_pool_connections + 20 <= database_max_connections
```

This reserves overlap for old/new revisions and administration, but it does not
prove the selected SQL tier can sustain those connections, writes, or queries.
Changing `AGGREGATOR_FLUSH_CONCURRENCY` still uses the same pool and cannot remove
the per-tenant revision lock. Increasing replicas also increases effective local
tenant quotas and the number of database pools.

Select an operating rate below observed saturation, with an explicit durability
visibility budget and headroom after losing a replica. Run long enough to observe
retention, storage growth, vacuum, and backlog recovery. Save raw evidence and
qualification policy beside the release record.

## Release acceptance record

Complete and retain this record for a real staging qualification. An unchecked
item is an outstanding gate, not implied by a successful Terraform plan.

| Evidence | Record |
| --- | --- |
| Build identity | Commit, four image digests, build time, CI run, image scan/SBOM references |
| Configuration | Project/region, reviewed plan, runtime limits, numeric key version, schema versions |
| Workload | Offered/accepted rate, active series, tenant skew, payload/kind mix, query/stream mix, run duration |
| Performance | Visibility p95/max, query p95, missed/rejected requests, backlog trend, drain time, memory/CPU/SQL headroom |
| Storage lifecycle | Retention turnover, ledger coverage, table/index growth, vacuum, cleanup duration/errors |
| Permissions | Private SQL connectivity; allowed and denied actions for each runtime identity |
| Durability | Cross-replica retries, restart/crash around commit, database interruption, broker redelivery, exact totals |
| Operations | Alert delivery, on-call owner, DLQ inspection/replay rehearsal, dependency-failure response |
| Recovery | Backup/PITR restore, retry-reservation continuity, replay coverage, cutover, measured RPO/RTO |
| Release safety | Key rotation/revocation, compatible rollback, maintenance plan for incompatible changes |
| Decision | Owner/date, accepted workload envelope, unresolved limitations, next review trigger |

The repository does not assert that these cloud gates have passed. Its published
[capacity results](capacity-results.md) distinguish local/hosted emulator evidence
from production qualification.
