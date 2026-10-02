# Fluxgate documentation

**[Read the documentation website](https://fluxgate-docs.vercel.app)** ·
**[Browse the API reference](https://fluxgate-docs.vercel.app/api-reference/)**

Fluxgate accepts metric observations over HTTP, transports them through Google
Cloud Pub/Sub, stores event-time aggregates in PostgreSQL, and serves them through
REST and server-sent events. Ingest, aggregation, and query run independently.

Start with the guide for your task:

| Task | Guide |
| --- | --- |
| Run the full pipeline and send your first points | [Getting started](getting-started.md) |
| Integrate a producer, query client, or live dashboard | [API and client integration](api.md) · [OpenAPI](../api/openapi.yaml) |
| Understand windows, retries, duplicates, and storage | [Architecture and data guarantees](architecture.md) |
| Set environment variables and resource bounds | [Configuration reference](configuration.md) |
| Provision tenants and rotate credentials | [Security and tenant access](security.md) |
| Monitor the pipeline and diagnose failures | [Operations and troubleshooting](operations.md) |
| Recover from an outage, restore, or replay | [Recovery runbooks](recovery.md) |
| Prepare a GCP release or upgrade | [Deployment guide](deployment.md) · [Terraform procedure](../deploy/terraform/README.md) |
| Set Terraform inputs or inspect outputs | [Terraform reference](terraform-reference.md) |
| Measure capacity and qualify a workload | [Capacity guide](capacity.md) · [Recorded results](capacity-results.md) |
| Build, test, or change the implementation | [Development guide](development.md) |
| Understand design tradeoffs | [Architecture decision records](adr/README.md) |
| Review image scan findings | [Security scanning assessment](security-scanning.md) |
| Build or publish the documentation website | [Documentation site](../site/README.md) |

## Scope and readiness

The repository includes bounded request/worker memory, durable retry identities,
transactional duplicate suppression, isolated runtime database roles, retention,
recovery tests, image audits, and measured emulator capacity profiles. The
[recorded results](capacity-results.md) identify the workload, resources, and
limitations of each measurement. They are evidence for those runs, not a GCP
capacity guarantee.

Production acceptance still requires a real staging environment with the intended
Cloud SQL tier, Pub/Sub, IAM, private networking, query mix, tenant skew, retention
volume, alert delivery, and tested recovery objectives. Use the
[release acceptance record](deployment.md#release-acceptance-record) to retain
that evidence. Terraform preparation and CI do not deploy a production system.

Fluxgate stores aggregates, not a raw telemetry archive. It has no PromQL/SQL
public endpoint, automatic cross-series aggregation, user management console,
per-key read/write scopes, or tenant alert evaluation and notification engine.
See [ADR 7](adr/0007-deferred-alerting.md) for the latter's deferred scope.

## Reading conventions

- Shell examples run from the repository root unless a directory change is
  shown. Bash and PowerShell examples are labeled where syntax differs.
- The local Compose key and database password are public development fixtures.
  Production examples use placeholders or secret references.
- Defaults in [configuration](configuration.md) are binary defaults. Compose and
  Terraform deliberately override some of them.
- Source links point to the implementation of each contract. If changing a
  contract, update its guide, OpenAPI where applicable, and the relevant tests
  in the same pull request.

The [root README](../README.md) remains the project overview and short quickstart.
