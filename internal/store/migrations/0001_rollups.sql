-- Windowed rollups and the delivery ledger that makes writing them
-- exactly-once.

-- fluxgate_array_add adds two histogram bucket vectors element-wise.
--
-- Merging histograms in SQL keeps the upsert a single statement: the
-- alternative is reading the existing row, merging in Go and writing it back,
-- which turns every flush into a read-modify-write race between replicas.
--
-- Mismatched lengths keep the existing value rather than producing a
-- half-merged vector. That only happens across a bucket-layout change, where
-- the two vectors describe different boundaries and adding them would be
-- meaningless.
CREATE OR REPLACE FUNCTION fluxgate_array_add(a BIGINT[], b BIGINT[])
RETURNS BIGINT[]
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN a IS NULL THEN b
        WHEN b IS NULL THEN a
        WHEN array_length(a, 1) IS DISTINCT FROM array_length(b, 1) THEN a
        ELSE ARRAY(SELECT a[i] + b[i] FROM generate_subscripts(a, 1) AS i)
    END
$$;

CREATE TABLE IF NOT EXISTS rollups (
    tenant_id     TEXT             NOT NULL,
    metric        TEXT             NOT NULL,
    kind          TEXT             NOT NULL,
    -- label_hash rather than the labels themselves: a JSONB column cannot be a
    -- primary key component without a functional index, and equal label sets
    -- must map to one row regardless of key order.
    label_hash    TEXT             NOT NULL,
    window_start  TIMESTAMPTZ      NOT NULL,
    window_end    TIMESTAMPTZ      NOT NULL,

    -- The labels are stored alongside the hash so a reader never has to
    -- reverse it to find out what the series is.
    labels        JSONB            NOT NULL DEFAULT '{}'::jsonb,

    count         BIGINT           NOT NULL,
    sum           DOUBLE PRECISION NOT NULL,
    min           DOUBLE PRECISION NOT NULL,
    max           DOUBLE PRECISION NOT NULL,
    last          DOUBLE PRECISION NOT NULL,
    -- last_event_at orders `last` by event time, so a redelivered or
    -- out-of-order message cannot change an answer that should be stable.
    last_event_at TIMESTAMPTZ      NOT NULL,

    -- Histogram buckets, NULL for kinds where quantiles are meaningless.
    buckets       BIGINT[],

    updated_at    TIMESTAMPTZ      NOT NULL DEFAULT now(),

    -- One row per series per window. Window start is last so the index is also
    -- usable for a range scan over a known series.
    PRIMARY KEY (tenant_id, metric, label_hash, window_start)
);

-- The shape of every query the API serves: one tenant, one metric, a time
-- range, newest first.
CREATE INDEX IF NOT EXISTS rollups_time_range_idx
    ON rollups (tenant_id, metric, window_start DESC);

-- Retention deletes by age across all tenants, which the primary key cannot
-- serve.
CREATE INDEX IF NOT EXISTS rollups_retention_idx
    ON rollups (window_start);

-- processed_batches is the delivery ledger.
--
-- Entries are written in the same transaction as the rollups they contributed
-- to, which is what upgrades at-least-once delivery to exactly-once
-- accumulation:
--
--   * A crash before the flush commits leaves no ledger entry and no
--     acknowledgement, so redelivery re-accumulates and the result is correct.
--   * A crash after the flush commits but before the acknowledgement leaves a
--     ledger entry, so redelivery finds it and skips.
--
-- The key is (batch_id, window_start), not batch_id alone, because a batch can
-- straddle a window boundary and its windows flush at different times. Keyed on
-- the batch alone, a batch whose first window committed and whose second failed
-- would be recorded as fully processed -- and the retry that should have
-- rebuilt the second window would be skipped, losing it silently. Per window,
-- the retry skips exactly the part that committed and redoes exactly the part
-- that did not.
CREATE TABLE IF NOT EXISTS processed_batches (
    batch_id     TEXT        NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    tenant_id    TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (batch_id, window_start)
);

-- The ledger only needs to outlive the longest possible redelivery, so it is
-- reclaimed by age.
CREATE INDEX IF NOT EXISTS processed_batches_gc_idx
    ON processed_batches (processed_at);
