// SPDX-License-Identifier: MIT

// This file holds pipeline plan for the physical planner, governed by ADR-0026 and ADR-0027.
package physical

import (
	"context"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func (p *Planner) buildPipeline(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	// If this subtree is a materialized CTE, serve from cache instead of
	// re-executing the full sub-plan. The columnar form replays from the
	// collector (disk-backed past budget) without consuming it, so every
	// reference — main pipeline, subqueries, recursive steps — streams the
	// same data; the boxed form (recursive work table) keeps SliceSource.
	if node.CTEName != "" {
		if mat, ok := p.cteCache[node.CTEName]; ok {
			// The reference's declared types are the materialization's, and
			// every declaration walk above reads them off this node after it
			// is built (scan_annotation.go's stampRecursiveReference).
			p.stampRecursiveReference(node)
			if mat.coll != nil {
				return mat.coll.NewReplaySource(), nil, &exec.CollectSink{}, nil
			}
			source := exec.NewSliceSource(mat.schema, mat.rows)
			return source, nil, &exec.CollectSink{}, nil
		}
		// A RECURSIVE reference the cache does not hold: materialize it HERE,
		// where this block is planned. materializeCTEs fills the cache from
		// `root.CTEs` alone, so a recursive CTE declared in a derived table,
		// in another CTE's body or in a LATERAL was materialized by nobody and
		// this tagged scan fell through to a scan of a relation that does not
		// exist — zero rows where PostgreSQL 17.11 answers rows (#1047).
		//
		// A reference that still cannot be served is a REFUSAL and never that
		// scan: the relation is the CTE and there is no other one by the name
		// (rule 8, and the door #1041 names).
		if source, ops, sink, err, handled := p.buildNestedRecursiveCTE(ctx, node); handled {
			return source, ops, sink, err
		}
	}

	switch node.Type {
	case logical.NodeLimit:
		return p.buildLimit(ctx, node)
	case logical.NodeSort:
		return p.buildSort(ctx, node)
	case logical.NodeProject:
		return p.buildProject(ctx, node)
	case logical.NodeAggregate:
		return p.buildAggregate(ctx, node)
	case logical.NodeFilter:
		return p.buildFilter(ctx, node)
	case logical.NodeScan:
		return p.buildScan(ctx, node)
	case logical.NodeJoin:
		return p.buildJoin(ctx, node)
	case logical.NodeDistinct:
		return p.buildDistinct(ctx, node)
	case logical.NodeWindow:
		return p.buildWindow(ctx, node)
	case logical.NodeUnion:
		return p.buildSetOp(ctx, node, "union")
	case logical.NodeIntersect:
		return p.buildSetOp(ctx, node, "intersect")
	case logical.NodeExcept:
		return p.buildSetOp(ctx, node, "except")
	case logical.NodeDual:
		return &exec.DualSource{}, nil, &exec.CollectSink{}, nil
	default:
		return nil, nil, nil, fmt.Errorf("unsupported plan node: %s", node.Type)
	}
}

func (p *Planner) buildScan(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	// Track scan alias for both MaterializedInputs and ScanFileFilter.
	// Alias scheme matches dagplan's stage emission: "table" for first, "table:N" for duplicates.
	if p.scanCounter == nil {
		p.scanCounter = make(map[string]int)
	}
	n := p.scanCounter[node.TableName]
	p.scanCounter[node.TableName] = n + 1

	scanAlias := node.TableName
	if n > 0 {
		scanAlias = fmt.Sprintf("%s:%d", node.TableName, n)
	}

	// Scan-split pipeline mode: use streaming or materialized pre-scanned data.
	// StreamingSources is preferred — yields batches lazily without upfront
	// memory allocation. Falls back to MaterializedInputs for compatibility.
	if p.StreamingSources != nil {
		if src, ok := p.StreamingSources[scanAlias]; ok {
			return src, nil, &exec.CollectSink{}, nil
		}
	}
	if p.MaterializedInputs != nil {
		if batches, ok := p.MaterializedInputs[scanAlias]; ok && len(batches) > 0 {
			return exec.NewBatchSource(batches), nil, &exec.CollectSink{}, nil
		}
	}

	// Table functions (read_json, read_csv, etc.) bypass the catalog scan
	if node.IsTableFunc {
		// ...and so they bypassed every access check, which is why they are
		// authorized HERE, where the query's table-function source is built
		// (readerPlanTimeSchema builds one too, and asks the same guard first).
		// A subquery and a CTE body are planned as SEPARATE plans inside this
		// planner, so an authorization pass over the statement's plan alone
		// cannot see the `read_csv` inside `(SELECT COUNT(*) FROM read_csv(…))`
		// — the same bypass #859's column policies had. The decision itself
		// lives in `internal/auth`, which imports this package, so it arrives
		// as a guard on the context (#943).
		if guard := logical.TableFuncGuardFromContext(ctx); guard != nil {
			if err := guard(node.FuncName, node.FuncArgs, node.FuncNamedArgs); err != nil {
				return nil, nil, nil, err
			}
		}
		var source exec.Source
		var readerSchema []parquet.Column
		if node.FuncName == "unnest" {
			us, err := newUnnestSource(node.FuncArgs, node.WithOrdinality)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("unnest: %w", err)
			}
			source = us
		} else {
			ts, err := buildTableFunctionSource(node.FuncName, node.FuncArgs, node.FuncNamedArgs)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("table function %s: %w", node.FuncName, err)
			}
			source = ts
			// What the PLAN read of this reader's input, and the backstop
			// that holds the batches to it. The same cached answer the
			// binder and the annotation pass bound against, so nothing is
			// read twice and nothing can disagree with them here.
			cols, known, err := readerPlanTimeSchema(ctx, node.FuncName, node.FuncArgs, node.FuncNamedArgs)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("%s: %w", node.FuncName, err)
			}
			if known {
				relName := node.TableAlias
				if relName == "" {
					relName = node.FuncName
				}
				if len(cols) == 0 {
					return nil, nil, nil, emptyReaderRefusal(node.FuncName, node.FuncArgs)
				}
				source = withPlanTimeSchema(source, cols, relName)
				readerSchema = cols
			}
		}
		// The FROM item's column-alias list, applied at the one layer that
		// knows the function's width (#1184), and — for a function whose
		// signature declares its columns — the declared schema, so a call
		// that produces no rows still publishes them.
		source = withColumnAliases(source, node.FuncColAliases, node.TableAlias)
		// The columns a consumer ABOVE a join asks of THIS arm, stamped by
		// stampTableFuncRequiredColumns before this pipeline was built. A
		// reader that is a join arm is reached only here: the join's output
		// is what the consumer sees, so the wrapper cannot go on top of it.
		if len(node.FuncRequiredColumns) > 0 {
			relName := node.TableAlias
			if relName == "" {
				relName = node.FuncName
			}
			source = withRequiredColumns(source, node.FuncRequiredColumns, relName)
		}
		// A relation with no rows still publishes its columns — for a
		// function whose SIGNATURE declares them, and now for a READER whose
		// input declares them too (a Parquet footer, a CSV header row).
		// Without it a reader that produced no batch reached the door as
		// `XX000 the result has no columns at all` (#1230).
		declared, known := tableFuncDeclaredSchema(node.FuncName, node.FuncArgs, node.WithOrdinality)
		if !known && len(readerSchema) > 0 {
			declared, known = readerSchema, true
		}
		if known {
			renamed, err := applyFuncColumnAliases(declared, node.FuncColAliases, node.TableAlias)
			if err != nil {
				return nil, nil, nil, err
			}
			source = withDeclaredSchema(source, renamed)
		}
		return source, nil, &exec.CollectSink{}, nil
	}
	scanner := p.newScanner(ctx, node.TableName, node.PartitionFilter, node.RequiredColumns, node.ScanPredicates)

	// Lengths-only decode for columns the logical analysis proved are
	// consumed for their SHAPE only (logical/shape_only_columns.go). Skipped
	// for a scan feeding the multi-consumer scan cache: a replay consumer
	// was not part of the analyzed plan, exactly as scan-filter pushdown
	// excludes it.
	if len(node.ShapeOnlyColumns) > 0 {
		if cs, ok := scanner.(*catalogScanSource); ok && cs.cache == nil {
			cs.shapeOnlyCols = make(map[string]bool, len(node.ShapeOnlyColumns))
			for _, c := range node.ShapeOnlyColumns {
				cs.shapeOnlyCols[strings.ToLower(c)] = true
			}
			ShapeOnlyColumnsPlanned.Add(int64(len(node.ShapeOnlyColumns)))
		}
	}

	// Probe-split pipeline mode: restrict this scan to only allowed files.
	if p.ScanFileFilter != nil {
		if files, ok := p.ScanFileFilter[scanAlias]; ok {
			if cs, ok := scanner.(*catalogScanSource); ok {
				cs.allowedFiles = files
			}
		}
	}

	var ops []exec.UnaryOperator

	if node.SampleMethod != "" && node.SamplePercent > 0 {
		ops = append(ops, newSampleOperator(node.SampleMethod, node.SamplePercent))
	}
	return scanner, ops, &exec.CollectSink{}, nil
}
