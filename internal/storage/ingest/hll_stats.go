package ingest

import (
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// computeColumnHLLs builds a HyperLogLog++ sketch for every column in
// the schema, scanning the row map per column. Returns a map keyed by
// column name; columns whose values are all nil are omitted.
//
// Called from flushBuffer alongside extractColumnStats so the catalog
// gets table-level NDV estimates the planner can consult for cardinality
// and dynamic-filter decisions.
//
// Uses catalog.AddValueToHLL for the value→hash conversion so HLLs
// produced here are byte-compatible with those produced by the ANALYZE
// path (catalog.AnalyzeTable). Both can be merged via HLL.Merge.
func computeColumnHLLs(rows []map[string]any, schema parquet.Schema) map[string]*catalog.HLL {
	if len(rows) == 0 {
		return nil
	}
	out := make(map[string]*catalog.HLL, len(schema.Columns))
	for _, col := range schema.Columns {
		if !catalog.IsHLLSupportedType(col.Type) {
			continue
		}
		var h catalog.HLL
		any := false
		for _, row := range rows {
			v, ok := row[col.Name]
			if !ok || v == nil {
				continue
			}
			any = true
			catalog.AddValueToHLL(&h, v, col.Type)
		}
		if any {
			out[col.Name] = &h
		}
	}
	return out
}
