-- Retention selects bounded chunks by window end, not window start.
CREATE INDEX IF NOT EXISTS rollups_retention_idx ON rollups (window_end);
