// This file holds scan annotation for the physical planner, governed by ADR-0034.
package physical

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// AnnotateScanColumns walks the logical plan tree and populates ScanColumns
// on Scan nodes from the catalog. This enables the logical optimizer to resolve
// unqualified column references for filter pushdown through joins.
func (p *Planner) AnnotateScanColumns(ctx context.Context, node *logical.Node) {
	p.annotateScanColumns(ctx, node)
	// With the catalog's real column lists now on the Scan nodes, move any of
	// the planner's own hidden slots that a STORED column already occupies.
	// It has to run here: before annotation there is no schema to collide
	// with, and a stored `__win_0` beside a window is a #694 collision with
	// the planner on the other side of it (slot_collision.go).
	renameCollidingSlots(node)
}

func (p *Planner) annotateScanColumns(ctx context.Context, node *logical.Node) {
	if node == nil {
		return
	}
	if node.Type == logical.NodeScan && node.TableName != "" && !node.IsTableFunc {
		// CANONICALIZE first, once, in place: everything below this pass keys
		// off Node.TableName — the manifest lookup, the pruner, the worker's
		// scan — so conceding at each door would be a different name at each
		// door. Resolving here means a reference that named the table in
		// another case becomes the catalog's own spelling for the whole plan.
		if p.catalog != nil {
			node.TableName = p.catalog.ResolveTableName(node.TableName)
		}
		table, err := p.catalog.GetTable(ctx, node.TableName)
		if err == nil {
			cols := make([]string, len(table.Schema.Columns))
			intCols := make(map[string]bool, len(table.Schema.Columns))
			colTypes := make(map[string]parquet.TypeID, len(table.Schema.Columns))
			colDecimal := make(map[string]logical.DecimalMeta)
			var colFields map[string][]parquet.Column
			for i, c := range table.Schema.Columns {
				cols[i] = c.Name
				colTypes[strings.ToLower(c.Name)] = c.Type
				if c.Type == parquet.TypeDecimal {
					colDecimal[strings.ToLower(c.Name)] = logical.DecimalMeta{Precision: c.Precision, Scale: c.Scale}
				}
				if c.Type == parquet.TypeRow && len(c.Fields) > 0 {
					// A ROW's fields are the only declaration a field path
					// has; they live nowhere in a map keyed by column name
					// (#568). Kept as their full parquet.Column so a
					// DECIMAL field keeps its (p,s) and a nested container
					// field keeps its own shape.
					if colFields == nil {
						colFields = make(map[string][]parquet.Column)
					}
					colFields[strings.ToLower(c.Name)] = c.Fields
				}
				switch c.Type {
				case parquet.TypeInt64, parquet.TypeInt32, parquet.TypeTimestamp,
					parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration,
					parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
					intCols[strings.ToLower(c.Name)] = true
				}
			}
			strictInt := make(map[string]bool, len(table.Schema.Columns))
			for _, c := range table.Schema.Columns {
				switch c.Type {
				case parquet.TypeInt64, parquet.TypeInt32:
					strictInt[strings.ToLower(c.Name)] = true
				}
			}
			node.ScanColumns = cols
			node.ScanIntCols = intCols
			node.ScanStrictIntCols = strictInt
			node.ScanColTypes = colTypes
			node.ScanColDecimal = colDecimal
			node.ScanColFields = colFields
		}
		// Estimate row count from manifest for join reordering
		if manifest, err := p.getManifest(ctx, node.TableName); err == nil {
			var total int64
			for _, part := range manifest.Partitions {
				for _, f := range part.Files {
					total += f.NumRows
				}
			}
			node.ScanRowEstimate = total

			// Aggregate per-column stats for CBO selectivity estimation
			if colStats, err := p.getAggregateColumnStats(ctx, node.TableName); err == nil && colStats != nil {
				scanStats := make(map[string]logical.ScanColumnStats, len(colStats))
				for col, cs := range colStats {
					var hist any
					if cs.Histogram != nil {
						hist = cs.Histogram
					}
					scanStats[col] = logical.ScanColumnStats{
						MinValue:  cs.MinValue,
						MaxValue:  cs.MaxValue,
						NullCount: cs.NullCount,
						TotalRows: cs.TotalRows,
						NDV:       cs.NDV,
						Histogram: hist,
					}
				}
				node.ScanColStats = scanStats
			}
		}
	}
	for _, child := range node.Children {
		p.annotateScanColumns(ctx, child)
	}
}
