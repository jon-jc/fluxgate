package store_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
	"github.com/jon-jc/fluxgate/internal/telemetry"
)

// These tests run against a real Postgres. They are skipped when none is
// configured, so `go test ./...` stays green on a machine without Docker, while
// CI covers the SQL itself -- the additive upsert, the histogram merge function
// and the ledger constraint are exactly the parts a mock would get wrong in
// agreement with its author.
//
//	docker compose -f deploy/docker-compose.yml up -d postgres
//	TEST_DATABASE_URL=postgres://fluxgate:fluxgate@localhost:5442/fluxgate?sslmode=disable \
//	  go test ./internal/store/...
const dsnEnvVar = "TEST_DATABASE_URL"

var base = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// openDB connects, migrates, and gives the test a tenant nobody else uses.
//
// Isolating by tenant rather than by database keeps the suite fast: every test
// shares one migrated schema and still cannot see another test's rows.
func openDB(t *testing.T) (db *store.DB, tenant string) {
	t.Helper()

	dsn := os.Getenv(dsnEnvVar)
	if dsn == "" {
		t.Skipf("set %s to run the Postgres integration tests", dsnEnvVar)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := store.Open(ctx, store.Config{DSN: dsn, MaxConns: 8}, discardLogger())
	if err != nil {
		t.Fatalf("open %s: %v", dsnEnvVar, err)
	}
	t.Cleanup(db.Close)

	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	return db, "t-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// rollup builds one series' aggregate over the window starting at base+offset.
func rollup(tenant, metric string, offset time.Duration, kind telemetry.Kind, values ...float64) aggregate.Rollup {
	start := base.Add(offset)
	acc := aggregate.NewAccumulator(kind)
	for i, v := range values {
		acc.Observe(v, start.Add(time.Duration(i)*time.Second).UnixNano())
	}

	labels := map[string]string{"service": "checkout"}
	return aggregate.Rollup{
		Window: aggregate.Window{Start: start, End: start.Add(time.Minute)},
		Key: aggregate.SeriesKey{
			TenantID:  tenant,
			Metric:    metric,
			Kind:      kind,
			LabelHash: aggregate.HashLabels(labels),
		},
		Labels: labels,
		Acc:    acc,
	}
}

func contribution(tenant, batchID string, offset time.Duration) store.Contribution {
	return store.Contribution{
		BatchID:     batchID,
		TenantID:    tenant,
		WindowStart: base.Add(offset),
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	db, _ := openDB(t)

	// Several replicas rolling out at once all run this; the advisory lock
	// serialises them and re-runs must be no-ops.
	for i := range 3 {
		if err := db.Migrate(context.Background()); err != nil {
			t.Fatalf("migrate run %d: %v", i+2, err)
		}
	}
}

func TestFlushAndQuery(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	r := rollup(tenant, "cpu.util", 0, telemetry.KindGauge, 10, 20, 30)
	if err := db.Flush(ctx, []aggregate.Rollup{r},
		[]store.Contribution{contribution(tenant, "b1", 0)}); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := db.QueryRollups(ctx, tenant, "cpu.util", base, base.Add(time.Hour), 100)
	if err != nil {
		t.Fatalf("QueryRollups: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("read %d rollups, want 1", len(got))
	}

	stored := got[0]
	switch {
	case stored.Count != 3:
		t.Errorf("count = %d, want 3", stored.Count)
	case stored.Sum != 60:
		t.Errorf("sum = %v, want 60", stored.Sum)
	case stored.Min != 10:
		t.Errorf("min = %v, want 10", stored.Min)
	case stored.Max != 30:
		t.Errorf("max = %v, want 30", stored.Max)
	case stored.Last != 30:
		t.Errorf("last = %v, want 30", stored.Last)
	case stored.Mean() != 20:
		t.Errorf("mean = %v, want 20", stored.Mean())
	}
	// The labels are stored alongside the hash so a reader never has to
	// reverse it.
	if stored.Labels["service"] != "checkout" {
		t.Errorf("labels = %v, want service=checkout", stored.Labels)
	}
}

// TestUpsertMergesAdditively is the property that lets one window be written in
// pieces -- by two replicas, or by one across a restart -- and still total
// correctly.
func TestUpsertMergesAdditively(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	first := rollup(tenant, "http.requests", 0, telemetry.KindCounter, 1, 2, 3)
	if err := db.Flush(ctx, []aggregate.Rollup{first},
		[]store.Contribution{contribution(tenant, "b1", 0)}); err != nil {
		t.Fatalf("first flush: %v", err)
	}

	second := rollup(tenant, "http.requests", 0, telemetry.KindCounter, 10, 20)
	if err := db.Flush(ctx, []aggregate.Rollup{second},
		[]store.Contribution{contribution(tenant, "b2", 0)}); err != nil {
		t.Fatalf("second flush: %v", err)
	}

	got, err := db.QueryRollups(ctx, tenant, "http.requests", base, base.Add(time.Hour), 100)
	if err != nil {
		t.Fatalf("QueryRollups: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("read %d rollups, want the two writes merged into 1", len(got))
	}

	stored := got[0]
	switch {
	case stored.Count != 5:
		t.Errorf("count = %d, want 5", stored.Count)
	case stored.Sum != 36:
		t.Errorf("sum = %v, want 36", stored.Sum)
	case stored.Min != 1:
		t.Errorf("min = %v, want 1 (LEAST across both writes)", stored.Min)
	case stored.Max != 20:
		t.Errorf("max = %v, want 20 (GREATEST across both writes)", stored.Max)
	}
}

// TestLastIsResolvedByEventTime keeps a straggler from overwriting a newer
// value simply because its write arrived second.
func TestLastIsResolvedByEventTime(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	// The later observation is written first.
	late := rollup(tenant, "gauge.x", 0, telemetry.KindGauge)
	late.Acc.Observe(99, base.Add(50*time.Second).UnixNano())
	if err := db.Flush(ctx, []aggregate.Rollup{late}, nil); err != nil {
		t.Fatalf("first flush: %v", err)
	}

	early := rollup(tenant, "gauge.x", 0, telemetry.KindGauge)
	early.Acc.Observe(11, base.Add(10*time.Second).UnixNano())
	if err := db.Flush(ctx, []aggregate.Rollup{early}, nil); err != nil {
		t.Fatalf("second flush: %v", err)
	}

	got, err := db.QueryRollups(ctx, tenant, "gauge.x", base, base.Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("QueryRollups: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("read %d rollups, want 1", len(got))
	}
	if got[0].Last != 99 {
		t.Errorf("last = %v, want 99; the older event overwrote the newer one", got[0].Last)
	}
}

// TestHistogramBucketsMergeInSQL exercises fluxgate_array_add, which is why the
// upsert can stay a single statement instead of a read-modify-write race.
func TestHistogramBucketsMergeInSQL(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	first := rollup(tenant, "latency.ms", 0, telemetry.KindHistogram, 5, 5, 5)
	second := rollup(tenant, "latency.ms", 0, telemetry.KindHistogram, 5, 5)

	for i, r := range []aggregate.Rollup{first, second} {
		if err := db.Flush(ctx, []aggregate.Rollup{r}, nil); err != nil {
			t.Fatalf("flush %d: %v", i+1, err)
		}
	}

	got, err := db.QueryRollups(ctx, tenant, "latency.ms", base, base.Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("QueryRollups: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("read %d rollups, want 1", len(got))
	}

	var total int64
	for _, c := range got[0].Buckets {
		total += c
	}
	if total != 5 {
		t.Errorf("bucket total = %d, want 5; the buckets were not merged element-wise", total)
	}
	if got[0].Count != 5 {
		t.Errorf("count = %d, want 5", got[0].Count)
	}
}

func TestNonHistogramKindsStoreNoBuckets(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	r := rollup(tenant, "counter.x", 0, telemetry.KindCounter, 1, 2)
	if err := db.Flush(ctx, []aggregate.Rollup{r}, nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := db.QueryRollups(ctx, tenant, "counter.x", base, base.Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("QueryRollups: %v", err)
	}
	// Buckets for a counter would be pure storage overhead for a number nobody
	// can interpret.
	if len(got[0].Buckets) != 0 {
		t.Errorf("buckets = %v, want none for a counter", got[0].Buckets)
	}
}

// TestLedgerCommitsWithTheRollups is the correctness argument for the whole
// pipeline: there must be no interval where one exists without the other.
func TestLedgerCommitsWithTheRollups(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	r := rollup(tenant, "cpu.util", 0, telemetry.KindGauge, 1)
	c := contribution(tenant, "batch-ledger", 0)

	if err := db.Flush(ctx, []aggregate.Rollup{r}, []store.Contribution{c}); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	seen, err := db.SeenContributions(ctx, "batch-ledger", []time.Time{base})
	if err != nil {
		t.Fatalf("SeenContributions: %v", err)
	}
	if !seen[c.Key()] {
		t.Error("the contribution was not recorded alongside the rollup")
	}
}

// TestLedgerIsKeyedPerWindow is why a batch straddling a boundary can have one
// window committed and the other retried.
func TestLedgerIsKeyedPerWindow(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	// Only the first window of a two-window batch is committed.
	if err := db.Flush(ctx,
		[]aggregate.Rollup{rollup(tenant, "cpu.util", 0, telemetry.KindGauge, 1)},
		[]store.Contribution{contribution(tenant, "straddle", 0)}); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	seen, err := db.SeenContributions(ctx, "straddle",
		[]time.Time{base, base.Add(time.Minute)})
	if err != nil {
		t.Fatalf("SeenContributions: %v", err)
	}

	if !seen[contribution(tenant, "straddle", 0).Key()] {
		t.Error("the committed window is not in the ledger")
	}
	// The uncommitted window must not be recorded, or the retry that should
	// rebuild it would be skipped and the data lost.
	if seen[contribution(tenant, "straddle", time.Minute).Key()] {
		t.Error("an uncommitted window was recorded in the ledger")
	}
}

func TestLedgerInsertIsIdempotent(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	c := contribution(tenant, "repeat", 0)

	// A duplicate must not fail the transaction that carries everything else.
	for i := range 3 {
		if err := db.Flush(ctx, nil, []store.Contribution{c}); err != nil {
			t.Fatalf("flush %d: %v", i+1, err)
		}
	}
}

func TestSeenContributionsWithNoInput(t *testing.T) {
	db, _ := openDB(t)
	ctx := context.Background()

	for name, run := range map[string]func() (map[string]bool, error){
		"no batch":   func() (map[string]bool, error) { return db.SeenContributions(ctx, "", []time.Time{base}) },
		"no windows": func() (map[string]bool, error) { return db.SeenContributions(ctx, "b", nil) },
	} {
		t.Run(name, func(t *testing.T) {
			seen, err := run()
			if err != nil {
				t.Fatalf("SeenContributions: %v", err)
			}
			if len(seen) != 0 {
				t.Errorf("seen = %v, want empty", seen)
			}
		})
	}
}

func TestFlushWithNothingToDoIsANoOp(t *testing.T) {
	db, _ := openDB(t)

	if err := db.Flush(context.Background(), nil, nil); err != nil {
		t.Errorf("Flush with no work: %v", err)
	}
}

func TestTenantsAreIsolatedInStorage(t *testing.T) {
	db, tenantA := openDB(t)
	_, tenantB := openDB(t)
	ctx := context.Background()

	if err := db.Flush(ctx,
		[]aggregate.Rollup{rollup(tenantA, "shared.metric", 0, telemetry.KindGauge, 100)},
		nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// One tenant reading another's series would be a data breach, not a bug.
	got, err := db.QueryRollups(ctx, tenantB, "shared.metric", base, base.Add(time.Hour), 100)
	if err != nil {
		t.Fatalf("QueryRollups: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("tenant %s read %d rows belonging to %s", tenantB, len(got), tenantA)
	}
}

func TestQueryRangeIsHalfOpen(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	for _, offset := range []time.Duration{0, time.Minute, 2 * time.Minute} {
		if err := db.Flush(ctx,
			[]aggregate.Rollup{rollup(tenant, "cpu.util", offset, telemetry.KindGauge, 1)},
			nil); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}

	// Consecutive queries must tile without double-counting the shared
	// boundary or leaving a gap at it.
	first, err := db.QueryRollups(ctx, tenant, "cpu.util", base, base.Add(time.Minute), 100)
	if err != nil {
		t.Fatalf("QueryRollups: %v", err)
	}
	second, err := db.QueryRollups(ctx, tenant, "cpu.util",
		base.Add(time.Minute), base.Add(2*time.Minute), 100)
	if err != nil {
		t.Fatalf("QueryRollups: %v", err)
	}

	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("adjacent ranges returned %d and %d rows, want 1 each", len(first), len(second))
	}
	if first[0].WindowStart.Equal(second[0].WindowStart) {
		t.Error("adjacent ranges returned the same window")
	}
}

func TestListMetrics(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	for _, metric := range []string{"b.metric", "a.metric", "c.metric"} {
		if err := db.Flush(ctx,
			[]aggregate.Rollup{rollup(tenant, metric, 0, telemetry.KindGauge, 1)},
			nil); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}

	metrics, err := db.ListMetrics(ctx, tenant, 100)
	if err != nil {
		t.Fatalf("ListMetrics: %v", err)
	}
	if len(metrics) != 3 {
		t.Fatalf("got %v, want 3 metrics", metrics)
	}
	for i := 1; i < len(metrics); i++ {
		if metrics[i] < metrics[i-1] {
			t.Errorf("metrics are not sorted: %v", metrics)
		}
	}
}

func TestPruning(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	old := base.Add(-90 * 24 * time.Hour)
	r := rollup(tenant, "ancient.metric", 0, telemetry.KindGauge, 1)
	r.Window = aggregate.Window{Start: old, End: old.Add(time.Minute)}

	if err := db.Flush(ctx, []aggregate.Rollup{r}, nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	deleted, err := db.PruneRollups(ctx, 30*24*time.Hour)
	if err != nil {
		t.Fatalf("PruneRollups: %v", err)
	}
	if deleted == 0 {
		t.Error("retention deleted nothing despite a rollup well past the horizon")
	}

	got, err := db.QueryRollups(ctx, tenant, "ancient.metric", old, old.Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("QueryRollups: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("read %d rows after pruning, want 0", len(got))
	}
}

func TestPruneProcessedBatches(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	if err := db.Flush(ctx, nil,
		[]store.Contribution{contribution(tenant, "fresh-entry", 0)}); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// A retention window far longer than the entry's age must keep it: pruning
	// the ledger too eagerly would let a redelivery be counted twice.
	if _, err := db.PruneProcessedBatches(ctx, 24*time.Hour); err != nil {
		t.Fatalf("PruneProcessedBatches: %v", err)
	}

	seen, err := db.SeenContributions(ctx, "fresh-entry", []time.Time{base})
	if err != nil {
		t.Fatalf("SeenContributions: %v", err)
	}
	if len(seen) == 0 {
		t.Error("a fresh ledger entry was pruned")
	}
}

// TestConcurrentFlushesMerge is the real-world shape: several replicas writing
// the same window at the same time must total correctly, not deadlock or lose
// writes.
func TestConcurrentFlushesMerge(t *testing.T) {
	db, tenant := openDB(t)
	ctx := context.Background()

	const writers = 8

	var wg sync.WaitGroup
	errs := make(chan error, writers)

	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := rollup(tenant, "concurrent.metric", 0, telemetry.KindGauge, 1)
			c := contribution(tenant, fmt.Sprintf("batch-%d", i), 0)
			if err := db.Flush(ctx, []aggregate.Rollup{r}, []store.Contribution{c}); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent flush: %v", err)
	}

	got, err := db.QueryRollups(ctx, tenant, "concurrent.metric", base, base.Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("QueryRollups: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("read %d rows, want the writers merged into 1", len(got))
	}
	if got[0].Count != writers {
		t.Errorf("count = %d, want %d; a concurrent write was lost", got[0].Count, writers)
	}
}

func TestHealthCheck(t *testing.T) {
	db, _ := openDB(t)

	if err := db.Check(context.Background()); err != nil {
		t.Errorf("Check on a live database: %v", err)
	}
	if db.Name() == "" {
		t.Error("Name() is empty; the readiness report would have no label")
	}
}

func TestOpenRejectsAnEmptyDSN(t *testing.T) {
	if _, err := store.Open(context.Background(), store.Config{}, discardLogger()); err == nil {
		t.Error("Open succeeded with no DSN")
	}
}

func TestOpenFailsFastOnABadDSN(t *testing.T) {
	// pgxpool connects lazily, so without an explicit ping a bad DSN would
	// surface on the first query, long after the process reported itself
	// started.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := store.Open(ctx, store.Config{
		DSN:            "postgres://nobody:nobody@127.0.0.1:1/none?sslmode=disable",
		ConnectTimeout: 2 * time.Second,
	}, discardLogger())
	if err == nil {
		t.Error("Open succeeded against an unreachable database")
	}
}

// TestOpenRetriesAStartingDatabase covers the ordinary race between a service
// and its database starting together: Postgres reports itself ready during
// initialisation, before the server a client will actually reach is listening.
// Failing immediately turns that into a crash loop.
func TestOpenRetriesAStartingDatabase(t *testing.T) {
	// Nothing is listening on this port, so every attempt fails and the
	// elapsed time reveals whether retries happened at all.
	started := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := store.Open(ctx, store.Config{
		DSN:            "postgres://nobody:nobody@127.0.0.1:1/none?sslmode=disable",
		ConnectTimeout: 200 * time.Millisecond,
		ConnectRetries: 3,
	}, discardLogger())

	if err == nil {
		t.Fatal("Open succeeded against a port with nothing listening")
	}
	// Two waits between three attempts: roughly one second plus two.
	if elapsed := time.Since(started); elapsed < 2*time.Second {
		t.Errorf("gave up after %v; the retries did not happen", elapsed)
	}
	if !strings.Contains(err.Error(), "3 attempts") {
		t.Errorf("error = %v, want it to report how many attempts were made", err)
	}
}

// TestOpenStopsRetryingWhenCancelled keeps a shutdown from waiting out the full
// retry budget.
func TestOpenStopsRetryingWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	started := time.Now()
	_, err := store.Open(ctx, store.Config{
		DSN:            "postgres://nobody:nobody@127.0.0.1:1/none?sslmode=disable",
		ConnectTimeout: 100 * time.Millisecond,
		ConnectRetries: 20,
	}, discardLogger())

	if err == nil {
		t.Fatal("Open succeeded against a port with nothing listening")
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Errorf("took %v; cancellation did not stop the retries", elapsed)
	}
}
