// SPDX-License-Identifier: MIT

// This file holds scan annotation for the physical planner, governed by ADR-0034.
package physical

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// AnnotateScanColumns walks the logical plan tree and populates ScanColumns on
// Scan nodes: from the catalog for a base table, and from the CALL itself for a
// table function whose signature declares its columns (tableFuncDeclaredSchema).
// This enables the logical optimizer to resolve unqualified column references
// for filter pushdown through joins.
func (p *Planner) AnnotateScanColumns(ctx context.Context, node *logical.Node) {
	p.annotateScanColumns(ctx, node)
	// With the catalog's real column lists now on the Scan nodes, move any of
	// the planner's own hidden slots that a STORED column already occupies.
	// It has to run here: before annotation there is no schema to collide
	// with, and a stored `__win_0` beside a window is a #694 collision with
	// the planner on the other side of it (slot_collision.go).
	renameCollidingSlots(node)
	// A SCALAR SUBQUERY's declared output is a CATALOG fact the declaration
	// walks cannot ask for — they hold no Planner — so it is stamped on the
	// Project that publishes it, here, once per plan (subquery_decl_annotation.go).
	p.annotateSubqueryColumnDecls(node)
}

func (p *Planner) annotateScanColumns(ctx context.Context, node *logical.Node) {
	if node == nil {
		return
	}
	// A TABLE FUNCTION whose signature declares its columns is annotated the
	// way a base table is, from that declaration rather than from the catalog:
	// the aggregate and arithmetic result-type rules then see an integer as an
	// integer (`SUM(x) FROM generate_series(1,3) gs(x)` is bigint, not float8
	// — #1211, ADR-0024 §2a), a qualified star has a column list to expand,
	// and a call that produces NO batch still publishes its column. The
	// FROM item's column-alias list is applied first, because the names the
	// plan above uses are the RENAMED ones (#1184).
	if node.Type == logical.NodeScan && node.IsTableFunc {
		cols, known := tableFuncDeclaredSchema(node.FuncName, node.FuncArgs, node.WithOrdinality)
		if !known {
			// A FILE READER's columns are its INPUT's, and the door has
			// authorized the capability before this pass runs (ADR-0034,
			// ADR-0039 §3 as this arc rewrote it), so they can be read here
			// — bounded, and only under a context that carries the
			// authorization (reader_schema.go). A context without one, or an
			// input this resolver does not read, leaves the relation exactly
			// as it was: no annotation, and the first-batch refusal.
			cols, known = readerPlanTimeSchema(ctx, node.FuncName, node.FuncArgs, node.FuncNamedArgs)
			// A reader with no columns at all is an EMPTY input, and it is
			// refused by name where the pipeline is built. Annotating a
			// zero-column relation here would put an empty list where "not
			// known" is meant and close a scope over nothing.
			known = known && len(cols) > 0
		}
		if known {
			if renamed, err := applyFuncColumnAliases(cols, node.FuncColAliases, node.TableAlias); err == nil {
				stampScanSchema(node, renamed)
			}
		}
	}
	if node.Type == logical.NodeScan && node.TableName != "" && !node.IsTableFunc {
		// CANONICALIZE first, once, in place: everything below this pass keys
		// off Node.TableName — the manifest lookup, the pruner, the worker's
		// scan — so conceding at each door would be a different name at each
		// door. Resolving here means a reference that named the table in
		// another case becomes the catalog's own spelling for the whole plan.
		if p.Catalog != nil {
			node.TableName = p.Catalog.ResolveTableName(node.TableName)
		}
		table, err := p.Catalog.GetTable(ctx, node.TableName)
		if err == nil {
			stampScanSchema(node, table.Schema.Columns)
		}
		// Estimate row count from manifest for join reordering
		if manifest, err := p.GetManifest(ctx, node.TableName); err == nil {
			var total int64
			for _, part := range manifest.Partitions {
				for _, f := range part.Files {
					total += f.NumRows
				}
			}
			node.ScanRowEstimate = total

			// Aggregate per-column stats for CBO selectivity estimation
			if colStats, err := p.GetAggregateColumnStats(ctx, node.TableName); err == nil && colStats != nil {
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

// stampScanSchema records one relation's column list on a Scan node: the names
// the plan resolves against, the integer sets the arithmetic rules read, the
// declared types the aggregate result-type rules read, and the DECIMAL and ROW
// metadata a declaration cannot be rebuilt without.
//
// One body for the two relations a Scan can be — a catalog table and a table
// function that declares its own columns — so a column of a given type is
// annotated identically whichever it came from, which is what makes "a table
// function in FROM is a relation" true of the TYPE rules and not only of the
// names.
func stampScanSchema(node *logical.Node, columns []parquet.Column) {
	cols := make([]string, len(columns))
	intCols := make(map[string]bool, len(columns))
	colTypes := make(map[string]parquet.TypeID, len(columns))
	colDecimal := make(map[string]logical.DecimalMeta)
	strictInt := make(map[string]bool, len(columns))
	var colFields map[string][]parquet.Column
	for i, c := range columns {
		cols[i] = c.Name
		colTypes[strings.ToLower(c.Name)] = c.Type
		if c.Type == parquet.TypeDecimal {
			colDecimal[strings.ToLower(c.Name)] = logical.DecimalMeta{Precision: c.Precision, Scale: c.Scale}
		}
		if c.Type == parquet.TypeRow && len(c.Fields) > 0 {
			// A ROW's fields are the only declaration a field path has; they
			// live nowhere in a map keyed by column name (#568). Kept as
			// their full parquet.Column so a DECIMAL field keeps its (p,s)
			// and a nested container field keeps its own shape.
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
		// The set expr.operandIsInt's ColRef arm accepts, PORT and PROTOCOL
		// included since their arithmetic moved to the int4 kernels (#1000) —
		// see physical.intArithColumnType.
		if intArithColumnType(c.Type) {
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
