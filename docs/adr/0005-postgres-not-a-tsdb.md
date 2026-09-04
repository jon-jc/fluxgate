# 5. Postgres rather than a purpose-built time-series database

**Status:** Accepted

## Context

This system stores time series. There are databases built for exactly that —
Prometheus, InfluxDB, TimescaleDB, BigQuery — and using a general-purpose
relational database instead is a choice that needs defending.

## Decision

Plain Postgres, one row per series per window.

## Consequences

**What Postgres gives that a time-series database does not.** The correctness
argument in [ADR 2](0002-exactly-once-accumulation.md) rests on committing
rollups and a delivery ledger *in one transaction*. Most time-series databases
do not offer multi-statement transactions at all, and the ones that do are not
designed around them. Without that, exactly-once accumulation would need a
second system and a two-phase protocol between them.

Postgres also supplies JSONB containment with a GIN index for label filtering,
an upsert for the additive merge, and an immutable SQL function for merging
histogram buckets — all of which turned out to be load-bearing rather than
conveniences.

**What it costs.** No automatic downsampling, no built-in retention, no columnar
compression. Retention is a scheduled delete, written explicitly here. Storage
is larger than a purpose-built engine would use. Queries spanning very wide
ranges are slower than a time-series database would serve them.

**The honest limit.** This design is appropriate up to the point where one
Postgres instance can hold the rollups. Because the data is already aggregated —
one row per series per window, not per point — that ceiling is much further away
than it first appears, but it is a real ceiling.

**When to revisit.** When rollup volume outgrows a single instance. The
migration is not a rewrite: the aggregation engine has no knowledge of storage,
and the runner depends on a `Store` interface rather than on Postgres.
TimescaleDB would be the smallest step, since it keeps the transactional
guarantee the whole correctness argument rests on.
