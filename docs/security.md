# Security and tenant access

[Documentation index](README.md) · [Image scanning assessment](security-scanning.md)

## Access boundaries

Fluxgate authenticates machines using bearer API keys. Each enabled key resolves
to one tenant. Ingest assigns that tenant to the batch; query routes derive the
tenant from authentication and bind it into SQL. There is no caller-controlled
tenant selector. Metric kind and labels are validated before publication, and
broker envelopes are validated again on consumption.

Ingest and query share the credential format and key document. A key authorizes
both writes and reads for its tenant when configured in both services. There are
no per-key read/write scopes, end-user sessions, role hierarchy, or key-management
HTTP endpoints. If a product needs those controls, implement them at an
authenticated gateway/backend and keep the Fluxgate credential server-side.

Terraform exposes ingest/query at the Cloud Run invocation layer and relies on
the application key for data routes. Aggregator ingress is internal and lacks a
public invoker binding. Probes and build metadata need no application key, so
network/platform policy still matters. The provided module does not configure
a custom domain, WAF, browser authentication, or a public administrative UI.

## Credential document

Example shape; replace the digest before use:

```json
[
  {
    "key_id": "acme-2026-10",
    "tenant_id": "acme",
    "secret_sha256": "<64 hex characters: SHA-256 of the secret only>",
    "rate_limit_per_second": 5000,
    "burst": 10000,
    "disabled": false
  }
]
```

| Field | Requirement |
| --- | --- |
| `key_id` | Unique, 1–128 ASCII letters, digits, hyphens, or dots; no underscores |
| `tenant_id` | Nonempty valid UTF-8, at most 255 bytes, no leading/trailing whitespace or control characters |
| `secret_sha256` | Hex encoding of the 32-byte SHA-256 digest of the secret, not the full token |
| `disabled` | Optional, defaults false; disables authentication for that record |
| `rate_limit_per_second` | Optional nonnegative number; zero uses service default |
| `burst` | Optional nonnegative integer; zero uses service default |

The whole document must be one nonempty JSON array, valid UTF-8, at most 4 MiB,
with no unknown fields or duplicate key IDs. Tokens are
`fxg_<key_id>_<secret>`. Secrets must contain 1–1,024 printable non-space ASCII
characters; use cryptographically random high-entropy secrets, not human passwords.
Hash comparisons use constant-time digest comparison; all authentication failures
return the same `401` response shape.

This local Python example generates a random secret and its matching document.
Run it in a private terminal; its output contains a working plaintext token.
Store the document in your approved secret system and deliver the token through
your credential channel, not a commit, ticket, screenshot, or shared log.

```python
import hashlib
import json
import secrets

key_id = "acme-2026-10"
secret = secrets.token_urlsafe(32)
print(f"Bearer token: fxg_{key_id}_{secret}")
print(json.dumps([{
    "key_id": key_id,
    "tenant_id": "acme",
    "secret_sha256": hashlib.sha256(secret.encode("ascii")).hexdigest(),
    "rate_limit_per_second": 5000,
    "burst": 10000,
}], indent=2))
```

Use `API_KEYS` or `API_KEYS_FILE`. The store is immutable after startup, so changing
a mounted file does not rotate running processes. Keep a tenant's quota settings
consistent across keys used during rotation: the limiter is keyed by tenant but
takes the presented key's settings.

## Rotation and revocation

1. Generate a new key ID and secret for the same tenant. Add the new hash record
   alongside the old record, retaining a consistent quota policy.
2. Publish a new numeric Secret Manager version and roll both ingest and query
   to that version. Verify readiness, new-key reads/writes, and old-key continuity.
3. Move clients to the new token. Preserve their pending batch bodies and retry
   keys; reservations are tenant-scoped, so rotating within the same tenant does
   not require new batch identities.
4. After the migration window, remove or disable the old record, publish another
   version, and roll all relevant revisions. Verify the old token returns `401`.
5. Retain the version IDs, release identity, and verification record. Verify that
   no old revision continues serving with the revoked document.

Revocation takes effect when a process authenticates against the new document.
An already authenticated SSE connection is not rechecked on each poll; drain old
connections/revisions when immediate revocation is required. With ordinary
connection expiry it can remain open until its stream duration ends. An image
rollback must also account for the credential version, not accidentally restore
a revoked key.

## Quotas and access scope

Point quotas are per tenant **per ingest instance**. Multiple keys for the same
tenant share that instance's bucket. Replicas multiply the effective allowance,
and restarts lose limiter state. Stream quotas likewise apply per query instance.
Hard global tenant limits require coordinated admission outside this process.

The configured default burst must hold a maximum-sized batch. A per-key burst
override can be smaller; clients receiving `429` with no `Retry-After` must split
such a batch. API concurrency admission separately protects instance memory and
can return `503` even when a tenant has tokens available.

## Database and cloud identities

| Identity | Boundary |
| --- | --- |
| Ingest runtime | Publish raw batches; read/insert/update HTTP retry reservations |
| Aggregator runtime | Consume subscription; claim deliveries, write rollups/revisions, prune retained data |
| Query runtime | Read rollups and tenant revisions |
| Migration identity | Own schema, apply migrations, provision runtime database grants |

Each cloud service has a separate service account and database URL secret. The
owner URL is reserved for migrations. On staging/prod, services check for the
expected restricted database role and reject an owner/admin credential. Provision
roles with the migration job's `-grant-runtime-roles`; do not grant runtime DDL,
extra role membership, or table ownership to bypass a startup failure.

These database roles separate services, **not tenants**. Application filtering
is the tenant boundary; administrators with direct SQL access can see tenant
data. The provisioning routine assumes the isolated Fluxgate database, fixed
runtime role names, and `public` schema. Review new tables and permissions together.

The Cloud SQL connector uses private IP with IAM connection authorization and
certificate-verified TLS. Database login still uses the per-service password.
Terraform state contains sensitive database credentials: restrict state access,
enable bucket versioning/audit logging, and keep plans/state out of Git. The API
key document is populated out of band and pinned by numeric secret version.

## Network, logs, and packaging

- Terminate public TLS at the platform/gateway. Direct native HTTP listeners do
  not provide TLS themselves.
- Enable proxy-address and trace-parent trust only when a trusted ingress
  overwrites/authenticates the relevant headers. Public callers should not be
  able to force trace sampling through parent context.
- Public staging/prod APIs refuse to expose unauthenticated Prometheus metrics.
  Scrape the internal aggregator only from an authorized private path.
- Avoid personal or secret values in metric names and labels; they persist in
  the database, broker, backups, and diagnostic material. Bound label cardinality.
- Service images use a pinned distroless non-root runtime. Packaged verification
  runs read-only containers with dropped capabilities and no-new-privileges.
  Compose is a development fixture, not proof that a target deployment enforces
  the same isolation options.

CI runs reachable Go vulnerability analysis and scans all four service images.
Scanner success is not a statement that no vulnerabilities exist. Review the
[security scanning assessment](security-scanning.md), preserve scan/SBOM artifacts,
and reassess base images and dependencies at release time.

For a suspected key leak, rotate/revoke it and inspect correlated access records.
For a suspected isolation or implementation flaw, use the repository's private
security reporting channel if available; do not post credentials or tenant data
in a public issue. Preserve a minimal redacted reproducer and affected versions.

Source: [credential validation](../internal/auth/apikey.go),
[authorization middleware](../internal/auth/middleware.go),
[runtime grants](../internal/store/permissions.go), and [IAM resources](../deploy/terraform/iam.tf).
