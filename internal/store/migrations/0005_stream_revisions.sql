-- Per-tenant row locks order revisions by commit. Sequences and timestamps do
-- not: a slow transaction can commit an older cursor value after a later one.
CREATE TABLE IF NOT EXISTS tenant_revisions (
    tenant_id TEXT PRIMARY KEY,
    revision BIGINT NOT NULL CHECK (revision > 0)
);
ALTER TABLE rollups ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS rollups_revision_idx
    ON rollups (tenant_id, revision, metric, kind, label_hash, window_start);
-- Quiesce old aggregators during this migration; they do not assign revisions.
