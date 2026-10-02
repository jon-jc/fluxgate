# 4. Fixed-layout histograms rather than adaptive sketches

**Status:** Accepted

## Context

Latency percentiles have to be computed from data that is aggregated and then
discarded. Retaining raw observations would make memory proportional to
throughput, which is precisely what a streaming aggregator exists to avoid.

Alternative sketches can offer different accuracy/range tradeoffs. They would
need their own stored representation and merge implementation rather than the
fixed bucket-array addition used here.

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

Other mergeable sketches would require a different storage/SQL merge design.
The current choice keeps the on-disk representation and additive merge simple.

**What it costs.** Quantiles use bucket upper boundaries without interpolation;
the layout does not provide a universal 7% error guarantee. Positive interior
buckets have a 1.15 boundary ratio, while underflow, negative values, and overflow
have different error behavior. Each histogram stores 153 bigint counters
(negative, zero, 150 positive buckets, overflow), about 1.2KB before surrounding
allocation overhead. Buckets are allocated only for histogram-kind series.

**A deliberate bias, within range.** The estimate is the upper boundary of the
bucket containing the rank. Negative quantiles report zero; overflow clamps to
the highest representable boundary and can therefore under-report larger values.
Choose units/ranges accordingly; this is not an exact percentile or unconditional
SLO guarantee. See the [current layout](../architecture.md#histograms).

**When to revisit.** If measured percentile accuracy or range is insufficient.
The vector length is validated on read, but a same-length change to boundaries
would not be detected. Any layout change needs an explicit versioned migration;
do not reinterpret existing vectors under new constants.
