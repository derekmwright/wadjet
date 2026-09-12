// This file holds pipeline plan for the physical planner, governed by ADR-0026 and ADR-0027.
package physical

import (
	"context"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

func (p *Planner) buildPipeline(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	// If this subtree is a materialized CTE, serve from cache instead of
	// re-executing the full sub-plan. The columnar form replays from the
	// collector (disk-backed past budget) without consuming it, so every
	// reference — main pipeline, subqueries, recursive steps — streams the
	// same data; the boxed form (recursive work table) keeps SliceSource.
	if node.CTEName != "" {
		if mat, ok := p.cteCache[node.CTEName]; ok {
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
	// Alias scheme matches walkStages: "table" for first, "table:N" for duplicates.
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
		// authorized HERE, at the one place a table-function source is built.
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
		if node.FuncName == "unnest" {
			source, err := newUnnestSource(node.FuncArgs, node.WithOrdinality, node.FuncColAliases)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("unnest: %w", err)
			}
			return source, nil, &exec.CollectSink{}, nil
		}
		source, err := buildTableFunctionSource(node.FuncName, node.FuncArgs, node.FuncNamedArgs)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("table function %s: %w", node.FuncName, err)
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
