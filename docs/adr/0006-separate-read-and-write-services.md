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

The isolation is also structural rather than merely intended. The ingest service
holds no database credentials at all, and the query service holds no Pub/Sub
permissions. A compromise of one does not reach what the other can touch, and
that is enforced by IAM rather than by discipline.

Schema ownership follows the same line: the aggregator runs migrations, the
query API explicitly does not. A read replica rolling out first cannot apply a
change the writer is not yet running.

**What it costs.** Three deployments instead of one. Shared code has to live in
internal packages behind real interfaces rather than being reached for directly
— a net benefit for testability, but a cost. Configuration grew a
`Requirements` concept so each binary declares what it needs, after two separate
occasions where a service failed to boot over a setting it never reads.

**When to revisit.** If the operational overhead of three services ever exceeds
the isolation benefit — realistically, only at a scale small enough that the
isolation is not needed either.
