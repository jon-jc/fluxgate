-- Namespace delivery identities by tenant, and keep different metric kinds
-- distinct rather than merging a gauge into a histogram with the same labels.
ALTER TABLE processed_batches DROP CONSTRAINT processed_batches_pkey;
ALTER TABLE processed_batches ADD PRIMARY KEY (tenant_id, batch_id, window_start);
ALTER TABLE rollups DROP CONSTRAINT rollups_pkey;
ALTER TABLE rollups ADD PRIMARY KEY (tenant_id, metric, kind, label_hash, window_start);
