package store

import "errors"

// ErrReadBudget asks a caller to narrow a read instead of allocating a result
// proportional to an entire tenant's retained data.
var ErrReadBudget = errors.New("query exceeds the materialized read byte budget")

const (
	maxQueryBytes  = 8 << 20
	maxChangeBytes = 256 << 10
)

// Account conservatively for decoded maps, string storage, slice capacity and
// row metadata. This is an admission budget, not a measurement of process RSS.
// Database rows must also obey the bounded ingestion schema.
func readRowBytes(r StoredRollup, rawLabels []byte) int {
	return 1024 + 4*len(rawLabels) + 16*len(r.Buckets) +
		len(r.TenantID) + len(r.Metric) + len(r.Kind) + len(r.LabelHash)
}
