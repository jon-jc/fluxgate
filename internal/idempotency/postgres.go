package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres shares retry reservations across replicas and process restarts.
type Postgres struct {
	pool *pgxpool.Pool
	ttl  time.Duration
}

// NewPostgres uses an already connected pool. Migrations are run separately.
func NewPostgres(pool *pgxpool.Pool, ttl time.Duration) *Postgres { return &Postgres{pool, ttl} }

// Name identifies the retry store in readiness responses.
func (p *Postgres) Name() string { return "idempotency" }

// Check verifies connectivity, schema presence, and read permission.
func (p *Postgres) Check(ctx context.Context) error {
	_, err := p.pool.Exec(ctx, "SELECT 1 FROM ingest_requests LIMIT 1")
	return err
}

const recordColumns = "fingerprint, status, response, batch, published, created_at"

func scanRecord(row pgx.Row) (Record, error) {
	var rec Record
	var batch []byte
	err := row.Scan(&rec.Fingerprint, &rec.Status, &rec.Body, &batch, &rec.Published, &rec.StoredAt)
	if err != nil {
		return Record{}, err
	}
	if err = json.Unmarshal(batch, &rec.Batch); err != nil {
		return Record{}, fmt.Errorf("decode reserved batch: %w", err)
	}
	return rec, nil
}

// Get returns a pending or confirmed reservation, rejecting changed payloads.
func (p *Postgres) Get(ctx context.Context, tenantID, key, fingerprint string) (Record, bool, error) {
	rec, err := scanRecord(p.pool.QueryRow(ctx, "SELECT "+recordColumns+`
	 FROM ingest_requests WHERE tenant_id=$1 AND idempotency_key=$2 AND expires_at > now()`, tenantID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, fmt.Errorf("read retry reservation: %w", err)
	}
	if rec.Fingerprint != fingerprint {
		return Record{}, false, ErrPayloadMismatch
	}
	return rec, true, nil
}

// Reserve commits the candidate before any broker request. On conflict the
// winner's batch and response are returned unchanged, even after a restart.
func (p *Postgres) Reserve(ctx context.Context, tenantID, key string, candidate Record) (Record, error) {
	batch, err := json.Marshal(candidate.Batch)
	if err != nil {
		return Record{}, err
	}
	rec, err := scanRecord(p.pool.QueryRow(ctx, `
	 INSERT INTO ingest_requests (tenant_id,idempotency_key,fingerprint,status,response,batch,expires_at)
	 VALUES ($1,$2,$3,$4,$5,$6,now()+$7*interval '1 second')
	 ON CONFLICT (tenant_id,idempotency_key) DO UPDATE SET
	 fingerprint=EXCLUDED.fingerprint,status=EXCLUDED.status,response=EXCLUDED.response,
	 batch=EXCLUDED.batch,published=false,created_at=now(),expires_at=EXCLUDED.expires_at
	 WHERE ingest_requests.expires_at <= now()
	 RETURNING `+recordColumns, tenantID, key, candidate.Fingerprint, candidate.Status, candidate.Body, batch, p.ttl.Seconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		var found bool
		rec, found, err = p.Get(ctx, tenantID, key, candidate.Fingerprint)
		if err == nil && !found {
			return Record{}, ErrExpired
		}
	}
	if err != nil {
		return Record{}, fmt.Errorf("reserve retry: %w", err)
	}
	return rec, nil
}

// Complete confirms publication of this exact reservation. A timeout can leave
// it pending even after broker acceptance; retrying republishes the same batch.
func (p *Postgres) Complete(ctx context.Context, tenantID, key, batchID string) error {
	// Confirmed outcomes need only identity/fingerprint/response. Keeping every
	// telemetry payload for the full retry TTL would dominate database storage.
	tag, err := p.pool.Exec(ctx, `UPDATE ingest_requests SET published=true, batch=batch-'Points'
	 WHERE tenant_id=$1 AND idempotency_key=$2 AND batch->>'ID'=$3 AND expires_at > now()`, tenantID, key, batchID)
	if err != nil {
		return fmt.Errorf("complete retry reservation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrExpired
	}
	return nil
}
