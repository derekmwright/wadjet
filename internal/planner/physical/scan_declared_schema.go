package physical

import (
	"context"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The catalog's schema, and the nine types a parquet file cannot say.
//
// buildLeafSchemaElement has no logical annotation to write for IPv4, IPv6,
// MAC, UUID, BYTES, PORT, PROTOCOL or DURATION — they are not parquet
// concepts — and it writes CIDR as plain UTF8. A reader that recovers types
// from the file therefore sees the INT64 / BYTE_ARRAY leaves those nine are
// STORED in, which is why the stage DAG answered an IPv4 column as
// 167772165 and a UUID as sixteen raw bytes while the single-process engine,
// which reads the catalog, answered 10.0.0.5 (#396).
//
// #396 was closed for files written from v0.18.0 on by stamping the declared
// schema into the footer (parquet.DeclaredSchemaKey) and overlaying it on
// read: those files are self-describing and need nothing from the plan. A
// file written by an OLDER build carries no such key, and on those the DAG
// kept answering raw storage form — the same defect, unfixed, on exactly the
// data a migration cannot rewrite (#423).
//
// annotateScanSchemas is the other half: the catalog's declared columns ride
// the plan to the worker (Stage.ScanSchema → OpSpec.ColumnTypes → the scan
// source), so the DAG types a column the way the catalog declares it whether
// or not the file can say so itself. Where the file DOES carry the key the
// two agree and the substitution is a no-op; where they disagree the
// catalog wins if the file's bytes can carry its type and the task FAILS if
// they cannot (parquet.Reader.SchemaAs → retypeFromCatalog).
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
