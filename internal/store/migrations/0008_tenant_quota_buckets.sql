-- One token bucket per tenant, shared by every ingest replica. It is the only
-- source of a tenant's rate-limit tokens: replicas lease small batches from it
-- and admit requests locally, so scaling replicas no longer multiplies the
-- tenant's allowance. Refill uses the database clock alone, so replicas never
-- disagree about elapsed time. Additive: existing services ignore the table.
CREATE TABLE IF NOT EXISTS tenant_quota_buckets (
    tenant_id TEXT PRIMARY KEY,
    tokens DOUBLE PRECISION NOT NULL,
    refilled_at TIMESTAMPTZ NOT NULL
);
