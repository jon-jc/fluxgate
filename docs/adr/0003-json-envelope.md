# 3. Versioned JSON on the wire rather than protobuf

**Status:** Accepted

## Context

Messages between the edge and the aggregator need an encoding. Protobuf is the
default choice for a Go service on GCP, and it is smaller and faster than JSON.

## Decision

A JSON envelope carrying an explicit `schema_version`, duplicated as a message
attribute so a consumer can route on it without deserialising anything.

## Consequences

**Why not protobuf.** At this message size the bandwidth difference is
immaterial next to what it costs at 3am. A dead-letter queue holds exactly the
payloads nobody understands; being able to pull one and simply *read* it is
worth more than the bytes. Protobuf would also add a code-generation step and a
schema file that has to stay in sync across three services.

**What it costs.** Larger messages and slower encoding. Both are measured and
neither is close to a bottleneck: publish latency is dominated by the broker
round trip, and transport compression is enabled above 4KB, which recovers most
of the size difference on the repetitive JSON a telemetry envelope actually
contains.

**What the version buys.** Consumers reject a schema version they were not
written for, as a *permanent* failure, rather than guessing at fields they do
not recognise. That makes the encoding a decision that can be revisited: moving
to protobuf later is a version bump and a dual-read period, not a rewrite.

**When to revisit.** If message volume grows to where bandwidth is a real cost,
or if a consumer appears in a language where hand-written JSON structs are a
liability. Neither is true today.
