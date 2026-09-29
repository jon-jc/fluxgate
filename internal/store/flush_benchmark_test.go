package store_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jon-jc/fluxgate/internal/aggregate"
	"github.com/jon-jc/fluxgate/internal/store"
)

// Real PostgreSQL, transaction and indexes. Use a disposable database: rows
// remain so successive iterations exercise additive updates as well as inserts.
func BenchmarkFlush(b *testing.B) {
	for _, size := range []int{1000, 10000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			db, tenant := openDB(b)
			rows := make([]aggregate.Rollup, size)
			for i := range rows {
				rows[i] = labelledRollup(tenant, "bench.flush", 0,
					map[string]string{"host": fmt.Sprint(i), "region": "test"}, 1)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				claims := make([]store.Contribution, (size+499)/500)
				for j := range claims {
					claims[j] = contribution(tenant, fmt.Sprintf("batch-%d-%d", i, j), 0)
				}
				b.StartTimer()
				if err := db.Flush(context.Background(), rows, claims); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(size), "rollups/op")
		})
	}
}
