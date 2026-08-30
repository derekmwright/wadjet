package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// resolveOutputRenameSource maps an OutputRename SOURCE that names a nested
// subquery's alias back to the column the DAG's streams actually carry (#385).
//
// walkStages treats an ordinary Project as a passthrough — it emits no stage
// — so a subquery's rename never happens anywhere on the DAG: every stream
// carries SOURCE column names, and each consumer compensates by resolving
// aliases back through the plan (resolveShuffleKey for join keys,
// resolveAggInputName for aggregate inputs, resolveSortKeyColumn for ORDER BY
// terms). The GATHER is the consumer this helper compensates for: when the
// outer SELECT merely forwards a subquery's alias (`SELECT k FROM (SELECT
// r_regionkey AS k FROM region) t`), extractOutputRenames reads the outermost
// Project and produces {From: k, To: k} — but no stage ever emitted a column
// named k, so applyOutputRenames could not resolve the source, degraded to
// its rename-only fallback, and the client saw the full upstream width under
// source names.
//
// The walk starts at the child of the outermost Project (whose list the
// renames came from) and substitutes at most once per Project — a projection
// list is simultaneous, so `b AS a, a AS b` must not chase itself — while
// descending through order/cardinality-preserving wrappers. Chained renames
// across NESTED Projects (`SELECT a FROM (SELECT b AS a FROM (SELECT c AS b
// ...))`) do resolve level by level.
//
// Three stop conditions mirror the sibling resolvers:
//   - a COMPUTED alias (Projection.Column == "") stops the walk: the value
//     has no source column to resolve to, and the #383/#169 machinery
//     materializes it into the producing fragment under the alias itself;
//   - an Aggregate stops the walk: its outputs are its own GroupBy /
//     OutputCol names, and descending past it would resolve against the
//     wrong schema (#355's aggStageRenames already handles group keys the
//     aggregate itself had to resolve);
//   - a Join recurses into both output-visible children (probe side only for
//     semi/anti), first substitution wins.
//
// See resolveOutputRenameSource / resolveOutputRenameSourceForGather below for
// the one place the two callers disagree.

// aggregateGroupKeyName returns the name an aggregate stage emits for a
// computed SELECT-list item that IS one of its GROUP BY keys — the key's own
// expression text, which is what the worker's pre-aggregate projection
// computes it under. ok=false for anything else, including an aggregate
// output (whose AggSpec.OutputCol already IS the alias).
func aggregateGroupKeyName(proj *logical.Projection, projectNode *logical.Node) (string, bool) {
	if proj.IsAgg || proj.Expr == "" {
		return "", false
	}
	agg := logical.AggregateBelowProject(projectNode)
	if agg == nil {
		return "", false
	}
	want := strings.ToLower(strings.TrimSpace(proj.Expr))
	for _, k := range agg.GroupBy {
		if strings.ToLower(strings.TrimSpace(k)) == want {
			return strings.ToLower(k), true
		}
	}
	return "", false
}

// resolveOutputRenameSource is the EXPRESSION-rewriting form: it resolves only
// the renames a stage really performs.
func resolveOutputRenameSource(name string, child *logical.Node) string {
	return resolveRenameSource(name, child, false)
}

// resolveOutputRenameSourceForGather additionally resolves a computed alias
// over an AGGREGATE to the group key's expression TEXT, which is the name the
// aggregate stage emits when nothing renamed it.
func resolveOutputRenameSourceForGather(name string, child *logical.Node) string {
	return resolveRenameSource(name, child, true)
}

func resolveRenameSource(name string, child *logical.Node, forGather bool) string {
	resolved := name
	if child == nil || name == "" {
		return resolved
	}
	for n := child; n != nil; {
		switch {
		case n.Type == logical.NodeProject:
			// A source spelled through the derived table's own alias
			// (`SELECT x.k FROM (SELECT s_suppkey AS k ...) x`) is looked up
			// bare inside that table's scope — see derivedScopeBareName.
			// Without it the gather could not resolve the source, degraded
			// to its rename-only fallback, and the client saw the join's
			// full upstream width under source names instead of `k` (#467).
			bare := derivedScopeBareName(resolved, n)
			if proj := projectionForName(n.Projections, resolved, bare); proj != nil {
				if proj.IsAgg || proj.Column == "" {
					// A computed alias over an AGGREGATE is the exception,
					// and only for the GATHER's rename: the aggregate stage
					// emits a computed GROUP BY key under the exact TEXT of
					// its expression, not under the alias, so the gather
					// looked for `gk`, found `g + 1`, and the client got the
					// expression text as the column name (#656 shape f with
					// no WHERE above it).
					//
					// forGather gates it because the other callers rewrite
					// EXPRESSIONS. absorbAggregateOutputProjection may put
					// that rename on the aggregate stage, and then the stream
					// really does carry `gk` — an expression re-spelled to
					// `"g + 1" * 10` would name the column that projection
					// renamed away, and answered NULL on every row.
					if src, ok := aggregateGroupKeyName(proj, n); ok && forGather {
						return src
					}
					// Otherwise: the stage that evaluates it emits it under
					// this very name.
					return resolved
				}
				// Plain rename: prefer the qualifier-preserving Expr
				// spelling, mirroring extractOutputRenames — the gather's
				// resolveRenameSource applies the qualified↔bare fallback
				// either way.
				next := proj.Column
				if proj.Expr != "" {
					next = strings.ToLower(proj.Expr)
				}
				if strings.EqualFold(next, resolved) {
					return resolved // self-rename, nothing to chase
				}
				resolved = next
			}
		case n.Type == logical.NodeAggregate:
			return resolved
		case n.Type == logical.NodeJoin && len(n.Children) == 2:
			if r := resolveRenameSource(resolved, n.Children[0], forGather); !strings.EqualFold(r, resolved) {
				return r
			}
			jt := strings.ToLower(n.JoinType)
			if jt == "semi" || jt == "anti" {
				return resolved
			}
			return resolveRenameSource(resolved, n.Children[1], forGather)
		}
		if len(n.Children) == 1 {
			n = n.Children[0]
			continue
		}
		break
	}
	return resolved
}

// substituteNestedRenameRefs returns expr with every column reference that
// names a NESTED subquery rename replaced by a reference to its source
// column, resolved with the #385 walk (#387). attachScanSelectProjections
// writes the outer SELECT list against the subquery's OUTPUT schema, but the
// scan fragment it attaches to carries SOURCE names — walkStages drops the
// rename-only Project as a passthrough — so `k + 1` over `r_regionkey AS k`
// compiled against a schema with no `k` and the task hard-failed. Rewriting
// the reference to `r_regionkey + 1` lets the fragment compute the value the
// query means.
//
// Copy-on-write, mirroring the #384 predicate rewriter
// (logical.substituteColRefs): shared unchanged subtrees are reused, and the
// returned node is the input itself when nothing referenced a rename.
// ok=false declines the whole rewrite — returned for subquery-bearing nodes
// (their SQL re-parses in its own scope), window functions (evaluated by
// their own stage), and any node kind this walk does not recognize. The
// caller then leaves the spec untouched, which keeps today's LOUD failure
// (the fragment errors on the unknown column) rather than inventing a
// silently different expression.
//
// A rewritten reference drops its table qualifier: the qualifier named the
// subquery alias, and the source column lives in the scan's own schema.
func substituteNestedRenameRefs(expr plansql.Node, child *logical.Node) (plansql.Node, bool) {
	if expr == nil || child == nil {
		return expr, true
	}
	switch e := expr.(type) {
	case *plansql.ColRef:
		src := resolveOutputRenameSource(strings.ToLower(e.Column), child)
		if strings.EqualFold(src, e.Column) {
			return expr, true
		}
		return &plansql.ColRef{Column: src}, true
	case *plansql.Lit, *plansql.IntervalLit, *plansql.LiteralPlaceholder, *plansql.StarNode:
		return expr, true
	case *plansql.CmpExpr:
		l, lok := substituteNestedRenameRefs(e.Left, child)
		r, rok := substituteNestedRenameRefs(e.Right, child)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return expr, true
		}
		return &plansql.CmpExpr{Left: l, Op: e.Op, Right: r}, true
	case *plansql.AndNode:
		l, lok := substituteNestedRenameRefs(e.Left, child)
		r, rok := substituteNestedRenameRefs(e.Right, child)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return expr, true
		}
		return &plansql.AndNode{Left: l, Right: r}, true
	case *plansql.OrNode:
		l, lok := substituteNestedRenameRefs(e.Left, child)
		r, rok := substituteNestedRenameRefs(e.Right, child)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return expr, true
		}
		return &plansql.OrNode{Left: l, Right: r}, true
	case *plansql.BinaryOp:
		l, lok := substituteNestedRenameRefs(e.Left, child)
		r, rok := substituteNestedRenameRefs(e.Right, child)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return expr, true
		}
		return &plansql.BinaryOp{Left: l, Op: e.Op, Right: r}, true
	case *plansql.UnaryOp:
		in, ok := substituteNestedRenameRefs(e.Inner, child)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return expr, true
		}
		return &plansql.UnaryOp{Op: e.Op, Inner: in}, true
	case *plansql.NotNode:
		in, ok := substituteNestedRenameRefs(e.Inner, child)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return expr, true
		}
		return &plansql.NotNode{Inner: in}, true
	case *plansql.ParenNode:
		in, ok := substituteNestedRenameRefs(e.Inner, child)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return expr, true
		}
		return &plansql.ParenNode{Inner: in}, true
	case *plansql.CastNode:
		in, ok := substituteNestedRenameRefs(e.Inner, child)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return expr, true
		}
		return &plansql.CastNode{Inner: in, TypeName: e.TypeName}, true
	case *plansql.FuncCallNode:
		newArgs, changed, ok := substituteNestedRenameList(e.Args, child)
		if !ok {
			return nil, false
		}
		if !changed {
			return expr, true
		}
		return &plansql.FuncCallNode{Name: e.Name, Args: newArgs, Distinct: e.Distinct, Star: e.Star}, true
	case *plansql.InExpr:
		l, lok := substituteNestedRenameRefs(e.Left, child)
		newVals, changed, ok := substituteNestedRenameList(e.Values, child)
		if !lok || !ok {
			return nil, false
		}
		if l == e.Left && !changed {
			return expr, true
		}
		return &plansql.InExpr{Left: l, Not: e.Not, Values: newVals}, true
	case *plansql.BetweenExpr:
		l, lok := substituteNestedRenameRefs(e.Left, child)
		lo, look := substituteNestedRenameRefs(e.Low, child)
		hi, hok := substituteNestedRenameRefs(e.High, child)
		if !lok || !look || !hok {
			return nil, false
		}
		if l == e.Left && lo == e.Low && hi == e.High {
			return expr, true
		}
		return &plansql.BetweenExpr{Left: l, Not: e.Not, Low: lo, High: hi}, true
	case *plansql.LikeExpr:
		l, lok := substituteNestedRenameRefs(e.Left, child)
		p, pok := substituteNestedRenameRefs(e.Pattern, child)
		if !lok || !pok {
			return nil, false
		}
		if l == e.Left && p == e.Pattern {
			return expr, true
		}
		return &plansql.LikeExpr{Left: l, Not: e.Not, Pattern: p}, true
	case *plansql.IsExpr:
		l, ok := substituteNestedRenameRefs(e.Left, child)
		if !ok {
			return nil, false
		}
		if l == e.Left {
			return expr, true
		}
		return &plansql.IsExpr{Left: l, Not: e.Not, Check: e.Check}, true
	case *plansql.CaseNode:
		changed := false
		var subj plansql.Node
		if e.Subject != nil {
			s, ok := substituteNestedRenameRefs(e.Subject, child)
			if !ok {
				return nil, false
			}
			subj = s
			changed = changed || s != e.Subject
		}
		whens := make([]plansql.WhenClause, len(e.Whens))
		for i, w := range e.Whens {
			c, cok := substituteNestedRenameRefs(w.Cond, child)
			r, rok := substituteNestedRenameRefs(w.Result, child)
			if !cok || !rok {
				return nil, false
			}
			whens[i] = plansql.WhenClause{Cond: c, Result: r}
			changed = changed || c != w.Cond || r != w.Result
		}
		var els plansql.Node
		if e.Else != nil {
			el, ok := substituteNestedRenameRefs(e.Else, child)
			if !ok {
				return nil, false
			}
			els = el
			changed = changed || el != e.Else
		}
		if !changed {
			return expr, true
		}
		return &plansql.CaseNode{Subject: subj, Whens: whens, Else: els}, true
	case *plansql.TupleNode:
		els, changed, ok := substituteNestedRenameList(e.Elements, child)
		if !ok {
			return nil, false
		}
		if !changed {
			return expr, true
		}
		return &plansql.TupleNode{Elements: els}, true
	case *plansql.ArrayLitNode:
		els, changed, ok := substituteNestedRenameList(e.Elements, child)
		if !ok {
			return nil, false
		}
		if !changed {
			return expr, true
		}
		return &plansql.ArrayLitNode{Elements: els}, true
	default:
		// SubqueryNode / ExistsNode / AnyAllExpr (nested SQL re-parses in
		// its own scope), WindowFuncNode, and anything newer than this walk.
		return nil, false
	}
}

// substituteNestedRenameList applies substituteNestedRenameRefs to each
// element; changed reports whether any element was rewritten.
func substituteNestedRenameList(nodes []plansql.Node, child *logical.Node) ([]plansql.Node, bool, bool) {
	out := make([]plansql.Node, len(nodes))
	changed := false
	for i, n := range nodes {
		nn, ok := substituteNestedRenameRefs(n, child)
		if !ok {
			return nil, false, false
		}
		out[i] = nn
		if nn != n {
			changed = true
		}
	}
	return out, changed, true
}

// resolveJoinNeededColumns maps each entry of a join node's NeededColumns
// that names a subquery's rename back to its source column (#385). The join
// stage's Columns become the worker's OutputColumns filter
// (probe.OutputFilter), and the streams the join reads carry SOURCE names —
// an alias entry matches nothing, so the column the user asked for was
// silently dropped from the join output and the gather had nothing to rename
// (`SELECT n_name, k FROM nation JOIN (SELECT r_regionkey AS k FROM region) t
// ON n_regionkey = k` came back as [n_name n_regionkey]).
//
// Resolution reuses resolveShuffleKey — the join-key resolver for exactly
// this passthrough — which rewrites only plain renames (computed aliases are
// materialized under their own name by the #383 pass and stay). The original
// slice is returned untouched when nothing resolves, keeping unaffected
// plans byte-identical; when something does, duplicates introduced by the
// mapping (alias and its source both needed) collapse.
func resolveJoinNeededColumns(node *logical.Node) []string {
	if len(node.NeededColumns) == 0 {
		return node.NeededColumns
	}
	changed := false
	resolved := make([]string, len(node.NeededColumns))
	for i, c := range node.NeededColumns {
		resolved[i] = resolveShuffleKey(c, node)
		if resolved[i] != c {
			changed = true
		}
	}
	if !changed {
		return node.NeededColumns
	}
	out := make([]string, 0, len(resolved))
	seen := make(map[string]bool, len(resolved))
	for _, c := range resolved {
		lc := strings.ToLower(c)
		if seen[lc] {
			continue
		}
		seen[lc] = true
		out = append(out, c)
	}
	return out
}
