# 7. Alerting deferred

**Status:** Accepted

## Context

The original plan included an alerting engine: a fourth service consuming
aggregate events, evaluating threshold rules, and publishing alert events. The
milestone table promised it.

## Decision

Not built. That milestone was spent on observability instead, and the plan was
corrected rather than quietly left in place.

## Consequences

**Why.** Alerting on rollups is a well-understood problem with good existing
answers. Prometheus alerting rules read exactly the metrics this system already
exposes, and Cloud Monitoring policies are already defined in Terraform for the
pipeline's own health. Building a fourth service to re-solve it would have
demonstrated less than making the pipeline properly observable did, and it would
have been a fourth thing to operate.

Distributed tracing across a message broker was the better use of the same
effort: it is what makes an asynchronous pipeline debuggable at all, and nothing
off the shelf provides it for a bespoke topology.

**What is missing.** There is no per-tenant rule evaluation and no alert
delivery. A tenant who wants to be paged on their own metrics cannot be, today.

**What exists instead.** Operational alerting on the pipeline itself —
dead-letter arrivals, subscription backlog, ingest server errors, database disk
— defined in `deploy/terraform/monitoring.tf`, each with documentation saying
what to check and in what order.

**When to revisit.** When a tenant needs alerts on their own data rather than an
operator needing alerts on ours. The natural shape is a consumer on a second
subscription of the aggregate topic, which the topology already supports: the
transport fans out, and adding one requires changing nothing that exists.
