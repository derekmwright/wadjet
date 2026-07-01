package physical

import (
	"context"
	"regexp"

	"github.com/citc-tech/wadjet/internal/planner/logical"
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
		meta, err := p.catalog.GetManifest(ctx, n.TableName)
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
