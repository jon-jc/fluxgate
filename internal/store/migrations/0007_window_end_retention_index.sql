-- 0001 already created rollups_retention_idx on window_start, so 0006's
-- IF NOT EXISTS did not replace it. Use a distinct name for the actual predicate.
CREATE INDEX IF NOT EXISTS rollups_window_end_idx ON rollups (window_end);
DROP INDEX IF EXISTS rollups_retention_idx;
