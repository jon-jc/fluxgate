-- Reserve batch identity before publication. Only confirmed publishes replay
-- as successful; ambiguous timeouts safely publish the identical batch again.
CREATE TABLE ingest_requests (
    tenant_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    status INTEGER NOT NULL CHECK (status = 202),
    response BYTEA NOT NULL,
    batch JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, idempotency_key)
);
CREATE INDEX ingest_requests_expiry_idx ON ingest_requests (expires_at);
