package ingest

import (
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// columnStats holds both the HLL sketch and the reservoir sample for
// one column as ingest collects them in one pass.
type columnStats struct {
	hll     *catalog.HLL
	sampler *catalog.ReservoirSampler
}

// computeColumnStats builds HLL sketches AND reservoir samples for
// every supported column in one pass over the rows. Two outputs because
// the catalog needs both — HLL for NDV, samples for histograms.
//
// Uses catalog.AddValueToHLL for hashes and ReservoirSampler.Add for
// samples; both helpers are shared with the ANALYZE path so produced
// stats are byte-compatible across collection sites.
func computeColumnStats(rows []map[string]any, schema parquet.Schema) map[string]*columnStats {
	if len(rows) == 0 {
		return nil
	}
	out := make(map[string]*columnStats, len(schema.Columns))
	for _, col := range schema.Columns {
		if !catalog.IsHLLSupportedType(col.Type) {
			continue
		}
		cs := &columnStats{hll: &catalog.HLL{}, sampler: catalog.NewReservoirSampler(catalog.SampleDefaultSize)}
		any := false
		for _, row := range rows {
			v, ok := row[col.Name]
			if !ok || v == nil {
				continue
			}
			any = true
			catalog.AddValueToHLL(cs.hll, v, col.Type)
			cs.sampler.Add(v)
		}
		if any {
			out[col.Name] = cs
		}
	}
	return out
}
