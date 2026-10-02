# Terraform reference

[Documentation index](README.md) · [Deployment guide](deployment.md) ·
[Operator procedure](../deploy/terraform/README.md)

Inputs come from [variables.tf](../deploy/terraform/variables.tf). All resource
names use the `fluxgate-<environment>` prefix where applicable. The module uses
a GCS backend configured at initialization; its bucket must already exist.
See the checked-in [provider requirements](../deploy/terraform/versions.tf) and
lock file for reproducible initialization. Example values are in
[dev.tfvars.example](../deploy/terraform/dev.tfvars.example) and
[prod.tfvars.example](../deploy/terraform/prod.tfvars.example).

## Inputs

| Input | Type | Default | Meaning |
| --- | --- | --- | --- |
| `project_id` | string | Required | Project owning the resources |
| `region` | string | `us-central1` | Cloud Run, SQL, connector, and registry region |
| `environment` | string | Required | `dev`, `staging`, or `prod` |
| `images` | object | Four empty strings | Keys `ingest`, `aggregator`, `query`, `migrate`; nonempty values must end in `@sha256:<64 lowercase hex digits>` |
| `deploy_services` | bool | `false` | Create runtime services only after secrets and successful migration |
| `api_keys_version` | string | Empty | Numeric nonzero Secret Manager version; `latest` rejected |
| `database_pool_connections` | number | `5` | Per-runtime-instance pool ceiling |
| `database_max_connections` | number | `400` | SQL connection ceiling; tier must support it with memory headroom |
| `otlp_endpoint` | string | Empty | External TLS OTLP gRPC collector host:port; empty disables traces |
| `artifact_registry_repository` | string | `fluxgate` | Docker image repository name |
| `ingest_min_instances` | number | `1` | Warm ingest minimum |
| `ingest_max_instances` | number | `20` | Ingest scale ceiling |
| `aggregator_instances` | number | `2` | Fixed consumer count; minimum and maximum are equal |
| `query_min_instances` | number | `0` | Query minimum; zero permits cold starts |
| `query_max_instances` | number | `10` | Query scale ceiling |
| `database_tier` | string | `db-custom-2-7680` | Cloud SQL machine type |
| `database_disk_gb` | number | `50` | Initial data disk size; autoresize ceiling is four times this value |
| `database_availability_type` | string | `ZONAL` | `ZONAL` or `REGIONAL`; prod runtime requires regional |
| `message_retention` | string | `86400s` | Source retention; whole seconds from `600s` through `2678400s` |
| `max_delivery_attempts` | number | `20` | Dead-letter attempt setting, 5–100; not an exact application retry counter |
| `ack_deadline_seconds` | number | `60` | Integer 10–600; client extends leases while processing |
| `alert_notification_channels` | list(string) | `[]` | Existing monitoring channel IDs; prod runtime requires at least one |
| `labels` | map(string) | `{}` | Extra resource labels merged with application/environment/managed-by labels |

Empty image references support infrastructure bootstrap. Supplying the migration
image creates the migration job even while `deploy_services=false`. It does not
execute the job. Supplying all images and a key version is necessary but cannot
prove the secret contents or migration success.

## Deployment gates

The [deployment gate](../deploy/terraform/bootstrap.tf) enforces:

- All four images and a pinned API key version before runtime services are enabled.
- `REGIONAL` SQL, at least one warm ingest instance, and a notification channel
  for `prod` runtime services. Staging uses the runtime security safeguards but
  does not inherit every prod sizing gate.
- Whole-number nonnegative replica/connection inputs, positive maxima/pool size,
  at least one aggregator, and minima no greater than maxima.
- Two full revisions at maximum scale plus 20 administrative SQL connections:
  `2 * (ingest_max_instances + query_max_instances + aggregator_instances) * database_pool_connections + 20 <= database_max_connections`.
- Valid source-retention and acknowledgment-deadline ranges.

These checks do not validate project quotas, regional capacity, billing,
organization policies, notification delivery, database sizing, or runtime access.
The [six mocked tests](../deploy/terraform/tests/deployment.tftest.hcl) exercise
configuration gates without making those cloud claims.

## Outputs

| Output | Use |
| --- | --- |
| `ingest_url` | Public ingest URL, null before services enabled |
| `query_url` | Public query URL, null before services enabled |
| `raw_topic` | Accepted batch topic |
| `dead_letter_topic` | Dead-letter topic |
| `dead_letter_subscription` | Inspection subscription for bounded diagnosis/replay |
| `database_instance` | Cloud SQL instance name |
| `database_private_ip` | Private address, not directly reachable from an ordinary public client |
| `service_accounts` | Map of ingest, aggregator, and query service-account emails |
| `api_keys_secret` | Secret ID to populate out of band |
| `image_repository` | Registry prefix for all four images |
| `migration_job` | Job name, null until migration image configured |

Outputs do not include plaintext API tokens. See [outputs.tf](../deploy/terraform/outputs.tf)
for exact expressions. Do not paste sensitive state or plan contents into public
issues when diagnosing a deployment.

## Runtime overrides and customization

Some deployment settings are fixed in the module rather than exposed as inputs:

| Setting | Current configuration |
| --- | --- |
| Database engine | PostgreSQL 17 |
| Database deletion protection | Enabled on every tier |
| SQL disk | SSD; autoresize enabled up to `database_disk_gb * 4`; size changes after growth are ignored by lifecycle configuration |
| Backups | Enabled, scheduled start `03:00`; 30 retained backups on staging/prod, 7 on dev |
| PITR | Enabled on staging/prod; transaction-log retention configured as 7 days there, 1 day on dev |
| SQL maintenance | Sunday, hour 04, stable update track; [API maintenance times are UTC](https://docs.cloud.google.com/sql/docs/postgres/set-maintenance-window#rest-v1) |
| SQL diagnostics | Query Insights enabled, client address recording off; queries over 1,000ms logged |
| VPC subnet / connector range | `10.20.0.0/24` / `10.21.0.0/28`; check for conflicts when adapting networking |
| VPC connector size | Minimum 2, maximum 3 instances |
| Worker subscription retry | Minimum 1s, maximum 60s backoff; no subscription expiry |
| DLQ inspection | Seven-day retention, 60s ack deadline, no subscription expiry |
| Acknowledged messages | Worker subscription does not retain them itself; raw topic retention supports deliberate replay |

These are module declarations, not evidence that a backup can be restored or
that the regional service accepted the settings. Verify actual resource state
and recovery behavior in staging. Autoresize has a finite ceiling and does not
shrink the disk; retention and disk monitoring remain necessary.

The module sets a subset of runtime environment values in
[cloudrun.tf](../deploy/terraform/cloudrun.tf). The
[configuration comparison](configuration.md#compose-and-terraform-overrides)
lists those overrides. There is no arbitrary `extra_env` input: changing worker
window size, flush policy, retention, or query limits in this module requires a
reviewed configuration change and compatibility assessment.

Dead-letter retention is currently seven days. Runtime ledger retention is source
retention plus seven days plus two days for HTTP retries and safety. Keep this
expression aligned with any changed retry TTL or extended replay/archive policy.

The module's Cloud Run invocation policy, private networking, and service-account
separation are an initial deployment design. If your organization disallows
public invokers, adapt it to an approved gateway before applying. Review saved
plans for replacements/deletions, especially on existing stacks and bootstrap
flag changes. Never use a production state directory as a disposable test fixture.
