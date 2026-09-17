// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"regexp"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// planSubqueryRe conservatively detects a residual (non-decorrelated)
// subquery inside a node's raw expressions. Estimation cannot see inside
// such subqueries, so a match makes the plan unestimable. False positives
// only cost the local fast path (the query runs distributed).
var planSubqueryRe = regexp.MustCompile(`(?i)\bselect\b`)

// EstimatePlanScanBytes walks a logical plan and sums the catalog bytes
// (compressed parquet, after partition-filter pruning) of every scan.
// Returns ok=false when the plan's input size cannot be resolved from the
// catalog — unknown table (table functions, virtual sources) or a residual
// subquery expression whose scans are not visible in the tree. Callers use
// this to route small queries onto the coordinator-local fast path; the
// only safe failure mode is over-estimation, so every unknown is "too big".
func (p *Planner) EstimatePlanScanBytes(ctx context.Context, n *logical.Node) (int64, bool) {
	if n == nil {
		return 0, true
	}
	var total int64
	if n.Type == logical.NodeScan {
		// p.GetManifest, not p.catalog.GetManifest: this is the FIRST
		// catalog read the default route makes (tryLocalFastPath calls it
		// before anything else), and it visits every scan node, so an
		// unpinned call here costs one manifest read per scan node on every
		// statement (#502).
		meta, err := p.GetManifest(ctx, n.TableName)
		if err != nil {
			return 0, false
		}
		for _, part := range meta.Partitions {
			if len(n.PartitionFilter) > 0 && len(part.Values) > 0 &&
				!matchesPartitionFilter(part.Values, n.PartitionFilter) {
				continue
			}
			for _, f := range part.Files {
				total += f.SizeBytes
			}
		}
	}
	for _, pred := range n.Predicates {
		if pred.Raw != "" && planSubqueryRe.MatchString(pred.Raw) {
			return 0, false
		}
	}
	for _, proj := range n.Projections {
		if proj.Expr != "" && planSubqueryRe.MatchString(proj.Expr) {
			return 0, false
		}
	}
	for _, child := range n.Children {
		b, ok := p.EstimatePlanScanBytes(ctx, child)
		if !ok {
			return 0, false
		}
		total += b
	}
	return total, true
}

// EstimatePlanScanCost is the query-limit cost of a logical plan: the catalog
// bytes, rows and file count every scan in the tree reads after
// partition-filter pruning, plus whether the query carries a filter and a
// LIMIT.
//
// It is the LOCAL, stage-free source of the cost guard (#803, ADR-0029). The
// guard used to read the distributed stage list — `EstimateCost(plan.Stages,
// node)` — which made the embedded engine emit a stage DAG on every query
// just to decide whether to refuse it. The numbers are the same because the
// rule is the same one stage emission applies when it fills a scan stage in:
// for every partition the filter admits, sum SizeBytes and NumRows and count
// the files of that partition. TestTheLogicalScanCostMatchesTheStageCost
// holds them equal over the TPC-H corpus.
//
// A table whose manifest cannot be read contributes nothing, exactly as a
// scan stage whose getManifest failed contributes nothing: the cost guard
// refuses what it can see, and an unreadable table fails later, for its own
// reason.
func (p *Planner) EstimatePlanScanCost(ctx context.Context, n *logical.Node) QueryCost {
	var cost QueryCost
	p.accumulateScanCost(ctx, n, &cost)
	cost.HasFilter = hasFilterOrPartition(n)
	cost.HasLimit = hasLimit(n)
	return cost
}

func (p *Planner) accumulateScanCost(ctx context.Context, n *logical.Node, cost *QueryCost) {
	if n == nil {
		return
	}
	if n.Type == logical.NodeScan {
		// p.GetManifest, not p.catalog.GetManifest: one catalog read per
		// table per statement (#502).
		if meta, err := p.GetManifest(ctx, n.TableName); err == nil {
			for _, part := range meta.Partitions {
				if len(n.PartitionFilter) > 0 && len(part.Values) > 0 &&
					!matchesPartitionFilter(part.Values, n.PartitionFilter) {
					continue
				}
				for _, f := range part.Files {
					cost.TotalBytes += f.SizeBytes
					cost.TotalRows += f.NumRows
					cost.TotalFiles++
				}
			}
		}
	}
	for _, child := range n.Children {
		p.accumulateScanCost(ctx, child, cost)
	}
}
