package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// absorbComputedSubqueryProjection materializes a subquery's COMPUTED
// projection columns into the scan stage that produces the subtree's rows
// (#383).
//
// walkStages treats an ordinary Project as a passthrough — it emits no stage
// — so a subquery's computed column never exists anywhere on the DAG. For a
// RENAME the resolve-through helpers compensate per consumer
// (resolveShuffleKey, resolveAggInputName, resolveSortKeyColumn, the gather's
// OutputRenames), but a computed value has no source column to resolve TO:
// `SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region` under a
// join dispatched a scan reading [r_regionkey, rk2], the parquet reader
// dropped the phantom rk2 (worse: its all-or-nothing projection guard fell
// back to full width), and everything downstream that read rk2 — an outer
// join's ON residual (#358), a projected output, a sort key — saw NULL or a
// missing column, silently.
//
// The aggregate consumer already materializes derived inputs on its own
// (#355: resolveAggInputName hands the worker an InputExpr to project before
// aggregating), which is the resolve-through shape. This helper is the
// materialize-at-source shape for the consumers that have no such hook: the
// computed column is projected INTO the producing scan fragment
// (Stage.ProjectExprs → OpProject, the #169 machinery), so the build/probe
// files a join reads — and the rows a sort keys over — really carry it.
//
// Deliberately additive: bare and renamed columns pass through under their
// SOURCE names (the DAG's naming convention, which every resolver
// compensates for), and only computed aliases are appended. Nothing is
// renamed and nothing existing is dropped, so plans without a computed
// subquery projection are byte-identical — and the #355 aggregate path keeps
// finding the source columns its InputExpr references.
//
// Scope: the subquery must be a Project over a scan-rooted chain
// (Project → Filter* → Scan) whose subtree emitted exactly one scan stage.
// Anything else — aggregates, nested joins, set operations, CTE-deduped
// aliases, nested Projects — bails and keeps today's behavior.
// requireEnclosing restricts the pass to a computing Project that sits UNDER
// at least one other Project — a genuine subquery. The sort hook passes true:
// a sort's child Project can be the query's OUTPUT projection
// (`SELECT NULLIF(x, 1) AS k FROM t ORDER BY k`), and that shape belongs to
// attachScanSelectProjections, which projects exactly the SELECT list under
// its final names for the gather. A join input is never the output
// projection, so the join hook passes false.
func absorbComputedSubqueryProjection(child *logical.Node, childStages []Stage, requireEnclosing bool) {
	// Find the COMPUTING Project: descend through stage-less Filters and
	// rename-only Projects (an outer `SELECT rk2 FROM (…) t` wraps the
	// computing subquery in a bare Project — both are passthroughs on the
	// DAG). The first Project that computes is the candidate; anything
	// else ends the walk.
	var proj *logical.Node
	enclosing := 0
	for n := child; n != nil; {
		if n.Type == logical.NodeFilter && len(n.Children) == 1 {
			n = n.Children[0]
			continue
		}
		if n.Type != logical.NodeProject || n.SecurityBarrier || len(n.Children) != 1 {
			return
		}
		computes := false
		for _, pr := range n.Projections {
			if pr.IsAgg {
				return
			}
			if pr.Column == "" && pr.Alias != "" && pr.ASTExpr != nil && !isSimpleColRefForRename(pr.ASTExpr) {
				computes = true
			}
		}
		if computes {
			if requireEnclosing && enclosing == 0 {
				return
			}
			proj = n
			break
		}
		enclosing++
		n = n.Children[0]
	}
	if proj == nil {
		return
	}
	// Below it: a scan-rooted chain only. A further Project bails — its
	// aliases are what the computing expressions reference, and this pass
	// does not chase rename chains.
	below := proj.Children[0]
	for below != nil && below.Type == logical.NodeFilter && len(below.Children) == 1 {
		below = below.Children[0]
	}
	if below == nil || below.Type != logical.NodeScan {
		return
	}

	// Collect the COMPUTED projections. Renames and bare passthroughs are
	// none of this pass's business; an aggregate projection means the
	// SELECT list is not a row-wise computation at all.
	var computed []ProjectExprSpec
	needCols := map[string]bool{}
	var colTypes map[string]parquet.TypeID
	var strictInt map[string]bool
	haveTypes := false
	for _, pr := range proj.Projections {
		if pr.IsAgg {
			return
		}
		if pr.Column != "" || pr.Alias == "" || pr.ASTExpr == nil || isSimpleColRefForRename(pr.ASTExpr) {
			continue
		}
		if referencesSyntheticAgg(pr.ASTExpr) {
			return
		}
		if !haveTypes {
			colTypes = inputColTypes(proj.Children[0])
			// Same integer-preserving-arithmetic hint as
			// attachScanSelectProjections (#297, #445): without it, `id + 1`
			// over a strict-int column declares (and computes) FLOAT64 here.
			strictInt = strictIntArithCols(proj.Children[0])
			haveTypes = true
		}
		expr := pr.Expr
		if expr == "" {
			expr = pr.ASTExpr.String()
		}
		computed = append(computed, ProjectExprSpec{
			Expr: expr,
			Name: strings.ToLower(pr.Alias),
			// The computed column exists nowhere in the catalog, so its
			// declared type IS its runtime type — the worker builds the
			// output vector from it (#333).
			Type:      inferProjectionTypeCols(pr.ASTExpr, parquet.TypeString, strictInt, colTypes),
			TypeKnown: true,
		})
		collectASTCols(pr.ASTExpr, needCols)
	}
	if len(computed) == 0 {
		return
	}

	// Target: the single scan stage this subtree emitted. Scalar-producer
	// stages a Filter deferred may follow it; a second scan means the
	// subtree is not the shape this pass handles.
	var target *Stage
	for i := range childStages {
		s := &childStages[i]
		if s.Type != StageScan {
			continue
		}
		if target != nil {
			return
		}
		target = s
	}
	if target == nil || !strings.EqualFold(target.TableName, below.TableName) {
		return
	}
	if len(target.ProjectExprs) > 0 || len(target.FusedAggSpecs) > 0 || len(target.FusedAggGroupBy) > 0 {
		return
	}

	alias := make(map[string]bool, len(computed))
	for _, c := range computed {
		alias[c.Name] = true
	}
	// The read set carries the computed alias as a phantom column (needed-
	// column propagation lists it): strip it — the parquet projection
	// guard reverts to full width on any unknown name — and add the real
	// columns the expressions read, which downstream pruning may have
	// dropped when only the alias was referenced.
	cols := make([]string, 0, len(target.Columns)+len(needCols))
	seen := map[string]bool{}
	for _, c := range target.Columns {
		lc := strings.ToLower(c)
		if alias[lc] || seen[lc] {
			continue
		}
		seen[lc] = true
		cols = append(cols, c)
	}
	for c := range needCols {
		if !seen[c] && !alias[c] {
			seen[c] = true
			cols = append(cols, c)
		}
	}
	target.Columns = cols

	// Passthrough of the (post-strip) read set plus the computed aliases:
	// OpProject narrows the fragment's output to exactly its projections,
	// so every column a consumer resolves by source name must be listed.
	specs := make([]ProjectExprSpec, 0, len(cols)+len(computed))
	for _, c := range cols {
		specs = append(specs, ProjectExprSpec{Expr: c, Name: c})
	}
	specs = append(specs, computed...)
	target.ProjectExprs = specs
}
