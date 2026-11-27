package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// QuotaGrant is the outcome of one lease against a tenant's shared bucket.
type QuotaGrant struct {
	// Granted is how many whole tokens were moved to the caller.
	Granted int
	// Remaining is what the bucket still holds after the grant, which may be
	// fractional and is below one when the bucket is effectively empty.
	Remaining float64
}

// LeaseQuota takes up to want tokens from tenantID's shared bucket, which
// refills at rate tokens per second up to burst.
//
// The statement is the whole algorithm, so it is worth reading closely:
//
//   - The bucket CTE locks the tenant's row. A concurrent lease waits, and on
//     waking re-reads the row its predecessor committed, so leases against one
//     tenant are serialised and none can spend tokens another already took.
//   - The clock is read after that lock, in a materialised CTE (it calls a
//     volatile function, so PostgreSQL evaluates it once). One database clock
//     refills every replica's view of the bucket, so replicas never disagree
//     about elapsed time -- and GREATEST keeps refilled_at from moving
//     backwards if the server clock is ever stepped, which would otherwise
//     credit the same interval twice.
//   - Refill is capped at burst before the grant, so an idle tenant does not
//     bank an unbounded allowance; the grant is whole tokens only, so the
//     fractional remainder keeps accruing rather than being rounded away.
//
// A tenant's first lease creates its bucket full, matching a fresh in-memory
// bucket.
func (db *DB) LeaseQuota(
	ctx context.Context, tenantID string, want int, rate float64, burst int,
) (QuotaGrant, error) {
	if want < 0 || rate <= 0 || burst <= 0 {
		return QuotaGrant{}, fmt.Errorf("lease quota: invalid request want=%d rate=%g burst=%d", want, rate, burst)
	}
	for attempt := 0; attempt < 2; attempt++ {
		var grant QuotaGrant
		err := db.pool.QueryRow(ctx, `
			WITH bucket AS (
			    SELECT tenant_id, tokens, refilled_at
			      FROM tenant_quota_buckets
			     WHERE tenant_id = $1
			       FOR UPDATE
			), clocked AS (
			    SELECT tenant_id, tokens, refilled_at,
			           GREATEST(refilled_at, clock_timestamp()) AS at
			      FROM bucket
			), offer AS (
			    SELECT tenant_id, at,
			           LEAST($3::float8,
			                 tokens + $2::float8 * EXTRACT(EPOCH FROM at - refilled_at)) AS available
			      FROM clocked
			), granted AS (
			    SELECT tenant_id, at, available,
			           LEAST($4::float8, GREATEST(floor(available), 0)) AS amount
			      FROM offer
			)
			UPDATE tenant_quota_buckets b
			   SET tokens = g.available - g.amount,
			       refilled_at = g.at
			  FROM granted g
			 WHERE b.tenant_id = g.tenant_id
			RETURNING g.amount::bigint, b.tokens`,
			tenantID, rate, burst, want,
		).Scan(&grant.Granted, &grant.Remaining)
		if err == nil {
			return grant, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return QuotaGrant{}, fmt.Errorf("lease quota: %w", err)
		}
		// No bucket yet. Create it full; a concurrent first lease may win the
		// race, which is equally fine, and the retry then leases from it.
		if _, err := db.pool.Exec(ctx, `
			INSERT INTO tenant_quota_buckets (tenant_id, tokens, refilled_at)
			VALUES ($1, $2, clock_timestamp())
			ON CONFLICT (tenant_id) DO NOTHING`, tenantID, float64(burst)); err != nil {
			return QuotaGrant{}, fmt.Errorf("create quota bucket: %w", err)
		}
	}
	return QuotaGrant{}, errors.New("lease quota: bucket vanished between creation and lease")
}
