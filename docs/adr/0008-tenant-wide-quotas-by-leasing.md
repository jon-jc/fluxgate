# 8. Tenant-wide rate limits by leasing from a shared bucket

**Status:** Accepted

## Context

Point-rate limits were a token bucket per tenant **per process**. Every ingest
replica held its own bucket, so a tenant configured for 10,000 points a second
could send 40,000 through four replicas, and scaling the service for capacity
silently scaled every tenant's allowance with it. The README said so, and said
that a hard tenant-wide quota needed "a shared gateway or another coordinated
mechanism". This is that mechanism.

Three designs were credible:

- **Check a shared counter on every request.** Exact, but it puts a database
  round trip and a contended row lock on every ingest request, and it fails
  closed or open on every request when the database is unhealthy.
- **Divide each allowance by the replica count.** No coordination per request.
  But traffic is rarely spread evenly: a tenant whose requests all land on one
  of four replicas would be throttled to a quarter of what it pays for.
- **Lease from a shared bucket.** One bucket per tenant in PostgreSQL is the
  only source of tokens. Replicas lease small batches and admit requests from
  their local pool, so the database is consulted when a pool runs dry rather
  than per request, and the allowance follows the traffic wherever it lands.

## Decision

Leasing, implemented in [`ratelimit.Leased`](../../internal/ratelimit/leased.go)
over [`store.LeaseQuota`](../../internal/store/quota.go).

- **One statement is the whole algorithm.** It locks the tenant's row, reads the
  database clock after the lock, refills to at most the burst, grants whole
  tokens, and returns the grant. Leases against one tenant are serialised by the
  row lock, and one clock refills every replica's view, so replicas never
  disagree about elapsed time. The clock is never allowed to move
  `refilled_at` backwards, which would credit the same interval twice.
- **A lease is one horizon of rate** (`RATE_LIMIT_LEASE`, 250 ms by default),
  never less than the request in hand nor more than the burst.
- **Rejections are local.** When the shared bucket cannot cover a request, the
  replica remembers how long the shortfall takes to accrue at the tenant's rate
  and denies locally until then. Without this, every rejected request leased
  again: a tenant over its limit -- exactly when the limiter matters -- cost a
  database round trip per request, which made exceeding a quota a way to load
  the database.
- **A failed lease is a retry; a failed database is a degradation.** A lease
  that errors or times out answers its request with a retryable `503`, which
  clients already handle by retrying with the same idempotency key. Only once
  consecutive failures open the circuit breaker -- the database is persistently
  unavailable -- are requests metered by the in-memory limiter: the guarantee
  the service had before, never unlimited and never refusing everything.
  Leases run on a context detached from the request, so client disconnects
  cannot fail them and trip the breaker.

  The first version fell back to per-instance limits on *every* failed lease.
  That is subtly wrong: the in-memory limiter is never used while leasing
  works, so it always holds a full burst, and each passing timeout handed out
  an unearned one -- in quantities no breaker notices, since blips are not
  consecutive. One packaged verification run admitted 100,000 points in a
  window whose ceiling was about 95,600; its logs were not retained, so the
  cause is inferred rather than confirmed, but modelling the mechanism settles
  it: with one lease in three failing, the first version admitted 49,800 points
  against a 13,000 bound, and the current one stays within it.

## Consequences

**The bound.** Tokens are only created by the shared bucket, so over any
interval *D*, a tenant's admissions across all replicas are at most

    burst + rate·D + R·lease

where *R* is the number of replicas holding unspent tokens. Per-instance
limiting allowed *R*·(burst + rate·D). The replica count moved from a
multiplier to a small additive term.

**Measured.** Against PostgreSQL, 32 concurrent leasers obtained 651 tokens from
a bucket able to emit 656.8 -- and with the row lock removed, 5,087. Four leasing
replicas with sixteen concurrent workers admitted 3,450 points against a 3,502
allowance, where per-instance limits would have admitted 14,007. A modelled
tenant offered ten times more traffic caused no more leases (759 at 20,000
requests, 507 at 200,000).

**What it costs.** One table and one statement; a database round trip a few
times a second per busy tenant per replica; a little under-admission when traffic
moves between replicas, since tokens leased by one replica wait there to be
spent; and a dependency on the database that the fallback, rather than the
request path, absorbs.

**What it does not cover.** Concurrent-stream quotas on the query service remain
per instance. They are a semaphore, not a rate, and leasing fits them poorly: a
held slot has no refill, so a crashed replica's slots would need expiry and
heartbeats to reclaim. During a sustained database outage the fallback
reintroduces the old per-instance multiplication for as long as the outage
lasts. `fluxgate_ratelimit_leases_total` separates `error` (a lease failed and
its request was retried) from `fallback` (the breaker is open and limits are
per instance), and a rate-limited warning accompanies both.

**When to revisit.** If a tenant's lease traffic becomes a measurable share of
database load -- many replicas, very small horizons -- a dedicated store such
as Redis would carry the same algorithm with less contention. If tenants need
quotas that stay exact through a database outage, the fallback would have to
fail closed, which is a product decision rather than an engineering one.
