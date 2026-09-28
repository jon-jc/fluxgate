# 6. Separate read and write services

**Status:** Accepted

## Context

The query endpoints could have been routes on the ingest API. They share the
error envelope, the authentication, the middleware and the credentials, and one
binary is one thing to deploy.

## Decision

Three processes: ingest, aggregator, query.

## Consequences

**Why separate.** Reads and writes fail differently and scale differently. An
expensive dashboard query holding a database connection should not be able to
slow telemetry ingestion, and an ingest spike should not make dashboards
unreadable — which is exactly what shared instances and a shared connection pool
would produce.

The isolation is also structural. The ingest service has database grants limited
to retry reservations so retries retain their identity across replicas and
restarts. It cannot read rollups. The query service has read-only access to
rollups and stream revisions and holds no Pub/Sub permissions. IAM and database
grants enforce these boundaries.

A separate migration job owns the schema and provisions runtime grants before
deployed services start. Runtime services cannot migrate on staging or production.
Local development can apply migrations automatically with an owner credential.

**What it costs.** Three deployments instead of one. Shared code has to live in
internal packages behind real interfaces rather than being reached for directly
— a net benefit for testability, but a cost. Configuration grew a
`Requirements` concept so each binary declares what it needs, after two separate
occasions where a service failed to boot over a setting it never reads.

**When to revisit.** If the operational overhead of three services ever exceeds
the isolation benefit — realistically, only at a scale small enough that the
isolation is not needed either.
