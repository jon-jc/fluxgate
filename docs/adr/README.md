# Architecture decision records

Each record captures one decision that was genuinely contested — where a
competent engineer could reasonably have chosen otherwise — along with what it
cost and what would make us revisit it.

Decisions that were obvious are not recorded here. A document explaining why the
service uses HTTP would waste the reader's attention, and the value of this
directory depends entirely on every entry being worth reading.

| # | Decision | Status |
| --- | --- | --- |
| [0001](0001-event-driven-pipeline.md) | Asynchronous pipeline rather than synchronous writes | Accepted |
| [0002](0002-exactly-once-accumulation.md) | Exactly-once accumulation via a per-window delivery ledger | Accepted |
| [0003](0003-json-envelope.md) | Versioned JSON on the wire rather than protobuf | Accepted |
| [0004](0004-fixed-histogram-layout.md) | Fixed-layout histograms rather than adaptive sketches | Accepted |
| [0005](0005-postgres-not-a-tsdb.md) | Postgres rather than a purpose-built time-series database | Accepted |
| [0006](0006-separate-read-and-write-services.md) | Separate read and write services | Accepted |
| [0007](0007-deferred-alerting.md) | Alerting deferred | Accepted |
