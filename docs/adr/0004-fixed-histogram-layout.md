# 4. Fixed-layout histograms rather than adaptive sketches

**Status:** Accepted

## Context

Latency percentiles have to be computed from data that is aggregated and then
discarded. Retaining raw observations would make memory proportional to
throughput, which is precisely what a streaming aggregator exists to avoid.

The more accurate answer is an adaptive sketch — t-digest, DDSketch — which
places its bucket boundaries based on the data it has seen.

## Decision

A fixed exponential layout: 150 buckets, growth ratio 1.15, from 1e-3 upward,
with negative, zero and overflow counts tracked separately.

## Consequences

**Why fixed.** Two accumulators with the same layout merge by adding their
bucket counts. That single property is what makes everything else work: partial
results combine across a restart, across two replicas writing the same window,
and — critically — *inside SQL*, via a small immutable function, which keeps the
storage upsert a single statement instead of a read-modify-write race between
instances.

An adaptive sketch would be more accurate per series and would make all of that
impossible without re-deriving boundaries from data that has already been
discarded.

**What it costs.** Relative error bounded at roughly the growth ratio, about 7%.
That is far finer than the decisions a latency percentile is actually used to
make — the question is whether p99 is 2ms or 20ms, never whether it is 10.001s
or 10.002s. Each histogram series costs 1.2KB of memory and a bigint array in
storage, which is why buckets are only allocated for histogram-kind series: a
counter percentile is not a meaningful number.

**A deliberate bias.** The estimate is the *upper* boundary of the bucket the
rank falls in, so it never under-reports. An SLO evaluated against it cannot
quietly pass on rounding.

**When to revisit.** If a tenant needs tighter percentiles than 7%, or if the
value range genuinely outgrows the fixed span. The layout constants live in one
file, and the stored vector length is validated on read — a rollup written under
a different layout is rejected rather than misinterpreted as data.
