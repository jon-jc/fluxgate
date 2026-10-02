package aggregator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
)

type tenantCheckpoint struct {
	tenant        string
	rollups       []aggregate.Rollup
	contributions []store.Contribution
	messages      []*pendingMessage
	windows       map[int64]aggregate.Window
}

// A delivery belongs to one authenticated tenant, so its claims and rollups can
// commit independently of other tenants. Bounded workers avoid holding several
// tenant revision locks in one transaction and isolate conflict/timeout retries.
// All workers share the checkpoint deadline, including time spent in the queue.
func (r *Runner) flushTenants(ctx context.Context, rollups []aggregate.Rollup, contributions []store.Contribution, messages []*pendingMessage) error {
	groups := make(map[string]*tenantCheckpoint)
	group := func(tenant string) *tenantCheckpoint {
		if groups[tenant] == nil {
			groups[tenant] = &tenantCheckpoint{tenant: tenant, windows: make(map[int64]aggregate.Window)}
		}
		return groups[tenant]
	}
	// Collection transfers ownership to us. Partition that array in place,
	// avoiding a second copy of every rollup while checkpoints are outstanding.
	sort.Slice(rollups, func(i, j int) bool { return rollups[i].Key.TenantID < rollups[j].Key.TenantID })
	for start := 0; start < len(rollups); {
		g := group(rollups[start].Key.TenantID)
		end := start
		for end < len(rollups) && rollups[end].Key.TenantID == g.tenant {
			w := rollups[end].Window
			g.windows[w.Start.UnixNano()] = w
			end++
		}
		g.rollups = rollups[start:end]
		start = end
	}
	for _, c := range contributions {
		g := group(c.TenantID)
		g.contributions = append(g.contributions, c)
		g.windows[c.WindowStart.UnixNano()] = aggregate.Window{Start: c.WindowStart, End: c.WindowStart.Add(r.engine.WindowSize())}
	}
	for _, m := range messages {
		g := group(m.tenantID)
		g.messages = append(g.messages, m)
	}
	tenants := make([]string, 0, len(groups))
	for tenant := range groups {
		tenants = append(tenants, tenant)
	}
	sort.Strings(tenants)

	// Only the bounded worker set creates goroutines, even at high tenant count.
	jobs := make(chan int)
	errs := make([]error, len(tenants))
	var workers sync.WaitGroup
	for range min(r.flushConcurrency, len(tenants)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				errs[i] = r.flushTenant(ctx, groups[tenants[i]])
			}
		}()
	}
	for i := range tenants {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	return errors.Join(errs...)
}

func (r *Runner) flushTenant(ctx context.Context, g *tenantCheckpoint) error {
	defer r.releaseFlushing(g.contributions)
	started := time.Now()
	// Do not begin queued work after the collection's shared deadline expires.
	err := ctx.Err()
	if err == nil {
		err = r.store.Flush(ctx, g.rollups, g.contributions)
	}
	if err != nil {
		// Only this tenant's messages need reconstruction. Other tenants may
		// already have committed and been acknowledged by another worker.
		r.nackAll(g.messages)
		r.mu.Lock()
		r.stats.FlushesFailed++
		r.mu.Unlock()
		return fmt.Errorf("flush tenant %s: %w", g.tenant, err)
	}
	windows := make([]aggregate.Window, 0, len(g.windows))
	for _, w := range g.windows {
		windows = append(windows, w)
	}
	r.engine.MarkFlushed(windows)
	r.metrics.ObserveFlush(len(windows), len(g.rollups), time.Since(started))
	r.ackAll(g.messages)
	r.mu.Lock()
	r.stats.FlushesSucceeded++
	r.stats.RollupsWritten += int64(len(g.rollups))
	r.mu.Unlock()
	r.log.Info("flushed tenant checkpoint", slog.String("tenant_id", g.tenant),
		slog.Int("windows", len(windows)), slog.Int("rollups", len(g.rollups)), slog.Int("messages", len(g.messages)))
	return nil
}
