-- Indexes for the read path.
--
-- Milestone 4 indexed what the writer needs. These serve the two shapes the
-- query API actually issues, which the write-side indexes cannot: filtering by
-- label, and tailing recent changes.

-- Label filters are expressed as JSONB containment (`labels @> '{"k":"v"}'`),
-- which a B-tree cannot serve at all -- it can only match a whole column value.
-- A GIN index over jsonb_path_ops is roughly a third the size of the default
-- opclass and supports exactly the containment operator this API uses, at the
-- cost of the key-existence operators it does not.
CREATE INDEX IF NOT EXISTS rollups_labels_idx
    ON rollups USING GIN (labels jsonb_path_ops);

-- The live tail asks "what has changed since I last looked", which is an
-- ordering on write time rather than on event time. Without this it degrades to
-- a sequential scan on every poll, from every connected client.
CREATE INDEX IF NOT EXISTS rollups_updated_idx
    ON rollups (tenant_id, updated_at);

-- Listing a tenant's metrics is a DISTINCT over a prefix of the primary key,
-- which Postgres can answer from this index alone rather than by scanning every
-- window ever written for that tenant.
CREATE INDEX IF NOT EXISTS rollups_metric_names_idx
    ON rollups (tenant_id, metric);
