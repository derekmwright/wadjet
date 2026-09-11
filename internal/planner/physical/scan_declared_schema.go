package physical

import (
	"context"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// annotateScanSchemas carries catalog declarations to workers through
// Stage.ScanSchema → OpSpec.ColumnTypes → scan source. Parquet leaves alone
// cannot declare IPv4, IPv6, MAC, UUID, BYTES, PORT, PROTOCOL, DURATION or CIDR.
// DeclaredSchemaKey makes newer files self-describing (#396), but older
// files need catalog types to avoid exposing raw storage representations (#423).
// Matching declarations are a no-op; on disagreement catalog wins only if the
// bytes can carry its type, otherwise SchemaAs/retypeFromCatalog fails the task.
func (p *Planner) annotateScanSchemas(ctx context.Context, stages []Stage) {
	if p.catalog == nil {
		return
	}
	// One lookup per distinct table, not per stage: a self-join plans two
	// scan stages over one table, and a wide plan many.
	cache := make(map[string][]parquet.Column)
	for i := range stages {
		name := stages[i].TableName
		if name == "" || len(stages[i].ScanSchema) > 0 {
			continue
		}
		cols, ok := cache[name]
		if !ok {
			table, err := p.catalog.GetTable(ctx, name)
			if err == nil && table != nil {
				cols = table.Schema.Columns
			}
			// A miss is cached too — a table function or a name the catalog
			// does not know must not be re-asked once per stage.
			cache[name] = cols
		}
		if len(cols) == 0 {
			continue
		}
		stages[i].ScanSchema = cols
	}
}
