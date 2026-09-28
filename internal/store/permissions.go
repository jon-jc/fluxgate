package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ProvisionRuntimeRoles removes Cloud SQL's default administrative membership
// and grants only the tables used by each service. Run as the migration owner
// after Terraform creates the users, before starting any runtime service.
// These fixed roles and public schema belong to a dedicated Fluxgate database.
func (db *DB) ProvisionRuntimeRoles(ctx context.Context) error {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1179407705)`); err != nil {
		return err
	}
	var cloudSQL bool
	if existsErr := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'cloudsqlsuperuser')`).Scan(&cloudSQL); existsErr != nil {
		return existsErr
	}
	for _, role := range []string{"fluxgate_ingest", "fluxgate_aggregator", "fluxgate_query"} {
		if cloudSQL {
			if _, err = tx.Exec(ctx, "REVOKE cloudsqlsuperuser FROM "+role); err != nil {
				return fmt.Errorf("revoke %s: %w", role, err)
			}
		}
		if _, err = tx.Exec(ctx, "ALTER ROLE "+role+" NOCREATEDB NOCREATEROLE NOINHERIT"); err != nil {
			return fmt.Errorf("restrict %s: %w", role, err)
		}
		var unsafe bool
		if privilegeErr := tx.QueryRow(ctx, `SELECT rolsuper OR rolbypassrls OR EXISTS (
			SELECT 1 FROM pg_auth_members WHERE member = r.oid)
			FROM pg_roles r WHERE rolname = $1`, role).Scan(&unsafe); privilegeErr != nil {
			return privilegeErr
		}
		if unsafe {
			return fmt.Errorf("role %s still has administrative attributes or memberships; remove them before provisioning", role)
		}
	}
	_, err = tx.Exec(ctx, `
		REVOKE CREATE ON SCHEMA public FROM PUBLIC;
		REVOKE ALL ON ALL TABLES IN SCHEMA public FROM fluxgate_ingest, fluxgate_aggregator, fluxgate_query;
		GRANT USAGE ON SCHEMA public TO fluxgate_ingest, fluxgate_aggregator, fluxgate_query;
		GRANT SELECT, INSERT, UPDATE ON ingest_requests TO fluxgate_ingest;
		GRANT SELECT, INSERT, UPDATE, DELETE ON rollups TO fluxgate_aggregator;
		GRANT SELECT, INSERT, DELETE ON processed_batches TO fluxgate_aggregator;
		GRANT SELECT, INSERT, UPDATE ON tenant_revisions TO fluxgate_aggregator;
		GRANT SELECT, DELETE ON ingest_requests TO fluxgate_aggregator;
		-- SELECT FOR UPDATE used by retention requires an UPDATE privilege.
		GRANT UPDATE (expires_at) ON ingest_requests TO fluxgate_aggregator;
		GRANT UPDATE (processed_at) ON processed_batches TO fluxgate_aggregator;
		GRANT SELECT ON rollups TO fluxgate_query;
		GRANT SELECT ON tenant_revisions TO fluxgate_query;
	`)
	if err != nil {
		return fmt.Errorf("grant runtime permissions: %w", err)
	}
	return tx.Commit(ctx)
}

// RequireRuntimeRole prevents a deployed service from starting with an owner
// credential or a Cloud SQL user whose bootstrap grants have not been removed.
func (db *DB) RequireRuntimeRole(ctx context.Context, service string) error {
	roles := map[string]string{"fluxgate-ingest-api": "fluxgate_ingest", "fluxgate-aggregator": "fluxgate_aggregator", "fluxgate-query-api": "fluxgate_query"}
	role, ok := roles[service]
	if !ok {
		return errors.New("unknown runtime service")
	}
	var restricted bool
	err := db.pool.QueryRow(ctx, `SELECT current_user = $1 AND NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolbypassrls)
		AND NOT EXISTS (SELECT 1 FROM pg_auth_members WHERE member = r.oid)
		FROM pg_roles r WHERE rolname = current_user`, role).Scan(&restricted)
	if err != nil {
		return fmt.Errorf("check runtime identity: %w", err)
	}
	if !restricted {
		return fmt.Errorf("service %s requires provisioned least-privilege database role %s", service, role)
	}
	return nil
}
