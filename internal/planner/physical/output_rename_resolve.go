// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// ResolveOutputRenameSource maps nested aliases to DAG stream SOURCE names
// for gather OutputRenames (#385). Start below the outermost Project;
// substitute at most once per simultaneous projection list, resolving nested
// Projects level by level through order/cardinality-preserving wrappers.
// Stop at computed aliases, materialized by #383/#169, and at Aggregates,
// whose GroupBy/OutputCol outputs have their own schema (#355's aggStageRenames, now in dagplan).
// Joins recurse into output-visible children (probe only for semi/anti), first
// substitution wins. See resolveOutputRenameSourceForGather for caller differences.
// See docs/internals/gather-output-rename-resolution.md for the design.

// AggregateGroupKeyName returns the name an aggregate stage emits for a
// computed SELECT-list item that IS one of its GROUP BY keys — the key's own
// expression text, which is what the worker's pre-aggregate projection
// computes it under. ok=false for anything else, including an aggregate
// output (whose AggSpec.OutputCol already IS the alias).
func AggregateGroupKeyName(proj *logical.Projection, projectNode *logical.Node) (string, bool) {
	if proj.IsAgg || proj.Expr == "" {
		return "", false
	}
	agg := logical.AggregateBelowProject(projectNode)
	if agg == nil {
		return "", false
	}
	// By identity: the SELECT item and the GROUP BY term are two spellings
	// of one expression and the rendering does not have to match, which is
	// what made `SELECT (g + 1) AS gk … GROUP BY g + 1` miss here (#723).
	want := strings.ToLower(strings.TrimSpace(proj.Expr))
	if proj.ASTExpr != nil {
		want = plansql.ExprIdentity(proj.ASTExpr)
	}
	for _, k := range groupKeyOutputs(agg) {
		if k.Identity == want {
			return strings.ToLower(k.Name), true
		}
	}
	return "", false
}

// ResolveOutputRenameSource is the EXPRESSION-rewriting form: it resolves only
// the renames a stage really performs.
func ResolveOutputRenameSource(name string, child *logical.Node) string {
	return ResolveRenameSource(name, child, false)
}

func ResolveRenameSource(name string, child *logical.Node, forGather bool) string {
	resolved := name
	if child == nil || name == "" {
		return resolved
	}
	for n := child; n != nil; {
		switch {
		case n.Type == logical.NodeProject:
			// A source spelled through the derived table's own alias
			// (`SELECT x.k FROM (SELECT s_suppkey AS k ...) x`) is looked up
			// bare inside that table's scope — see DerivedScopeBareName.
			// Without it the gather could not resolve the source, degraded
			// to its rename-only fallback, and the client saw the join's
			// full upstream width under source names instead of `k` (#467).
			bare := DerivedScopeBareName(resolved, n)
			if proj := ProjectionForName(n.Projections, resolved, bare); proj != nil {
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
					if src, ok := AggregateGroupKeyName(proj, n); ok && forGather {
						return src
					}
					// Otherwise: the stage that evaluates it emits it under
					// this very name.
					return resolved
				}
				// Plain rename: prefer the qualifier-preserving Expr
				// spelling, mirroring extractOutputRenames — the gather's
				// ResolveRenameSource applies the qualified↔bare fallback
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
			jt := strings.ToLower(n.JoinType)
			// A QUALIFIED reference names ONE relation, so it resolves
			// through the arm that owns the qualifier and nowhere else. The
			// first-substitution-wins walk below is right for a BARE name and
			// is #742 for a qualified one: two derived tables publishing `w`
			// from different expressions, and `q.w` resolved through p's
			// Project to p's window slot — the client got p's value under
			// BOTH output columns, silently, on every execution path.
			//
			// Only when exactly one arm answers to the qualifier. Neither
			// (an alias this walk cannot see) or both (two spellings of one
			// name) keep the old behaviour, which is the conservative side:
			// the walk then resolves nothing new.
			if own := OwnedJoinArm(n, resolved); own != nil {
				if (jt == "semi" || jt == "anti") && own == n.Children[1] {
					return resolved // the build side is not output-visible
				}
				src := ResolveRenameSource(resolved, own, forGather)
				if forGather && own == n.Children[1] {
					src = buildArmQualified(own, src)
				}
				return src
			}
			if r := ResolveRenameSource(resolved, n.Children[0], forGather); !strings.EqualFold(r, resolved) {
				return r
			}
			if jt == "semi" || jt == "anti" {
				return resolved
			}
			return ResolveRenameSource(resolved, n.Children[1], forGather)
		}
		if len(n.Children) == 1 {
			n = n.Children[0]
			continue
		}
		break
	}
	return resolved
}

// OwnedJoinArm returns the join arm whose subtree answers to a QUALIFIED
// name's qualifier, or nil when the qualifier is not exactly one arm's.
//
// `subtreeNamesRelation` is the same scope test DerivedScopeBareName uses: a
// derived table's alias is stamped on every scan below it and a CTE's name
// sits on the subtree root, so both spellings of a named scope are covered.
func OwnedJoinArm(n *logical.Node, name string) *logical.Node {
	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 || dot == len(name)-1 || len(n.Children) != 2 {
		return nil
	}
	qual := name[:dot]
	left := subtreeNamesRelation(n.Children[0], qual)
	right := subtreeNamesRelation(n.Children[1], qual)
	if left == right {
		return nil
	}
	if left {
		return n.Children[0]
	}
	return n.Children[1]
}

// windowArgKeepsItsQualifier retains a window argument's qualifier exactly
// when more than one input arm publishes its bare name and the qualifier names
// an input relation. Otherwise keep the usual bare-name resolution.
// Duplicate aliases are different values; dropping their qualifier can bind the
// other arm, and local/DAG join streams can give opposite arms the bare name.
// See docs/internals/window-argument-qualification.md for the design.
func windowArgKeepsItsQualifier(arg string, child *logical.Node) bool {
	dot := strings.LastIndexByte(arg, '.')
	if dot <= 0 || dot == len(arg)-1 || child == nil {
		return false
	}
	qual, bare := arg[:dot], arg[dot+1:]
	if RelationScopeSubtree(child, qual) == nil {
		return false // the qualifier names no relation of this input
	}
	return armsPublishingBareName(child, bare) > 1
}

// armsPublishingBareName counts the join arms below n whose subtree publishes
// bare — a scan column of theirs, or a name one of their Projects mints.
func armsPublishingBareName(n *logical.Node, bare string) int {
	for n != nil && ScopePreservingWrapper(n) {
		n = n.Children[0]
	}
	if n == nil {
		return 0
	}
	if n.Type == logical.NodeJoin && len(n.Children) == 2 {
		return armsPublishingBareName(n.Children[0], bare) +
			armsPublishingBareName(n.Children[1], bare)
	}
	if SubtreeNamingOf(n).ownsBareName(strings.ToLower(bare)) {
		return 1
	}
	return 0
}

// RelationScopeSubtree descends through two-arm JOINs to the named relation
// and through ScopePreservingWrapper: Filter, Sort, Limit, Distinct and Window.
// These narrow rows or append columns without renaming existing ones; Filter
// and Window must not hide a join and let a sibling capture a reference (#742).
// Stop at Project (the scope's SELECT list), Aggregate (its own outputs), and
// set operations (multiple arms with output names rooted in the first arm).
// Unlike ResolveRenameSource, this walk does not consume the scope's Project.
// See docs/internals/relation-scope-through-wrappers.md for the design.
func RelationScopeSubtree(n *logical.Node, name string) *logical.Node {
	if n == nil || name == "" || !subtreeNamesRelation(n, name) {
		return nil
	}
	for {
		if n.Type == logical.NodeJoin && len(n.Children) == 2 {
			left := subtreeNamesRelation(n.Children[0], name)
			right := subtreeNamesRelation(n.Children[1], name)
			if left == right {
				return n
			}
			if left {
				n = n.Children[0]
			} else {
				n = n.Children[1]
			}
			continue
		}
		if ScopePreservingWrapper(n) {
			n = n.Children[0]
			continue
		}
		return n
	}
}

// ScopePreservingWrapper reports whether n is a single-child node that leaves
// the relations below it addressable by the same names and renames none of
// their columns, so a scope walk may descend through it.
//
// It is `ResolveRenameSource`'s own descent rule written as a list rather than
// as a default, because this walk's default must be to STOP: returning a node
// too HIGH hands the caller a whole join subtree and a bare lookup inside it
// takes the first arm that answers, which is the silent capture. Keeping the
// two in step is therefore a standing obligation and not a one-time argument —
// `TestScopePreservingWrapperMatchesTheRenameWalk` asserts it over every
// logical node type, so a new node kind fails there rather than resolving one
// way in one walk and the other way in the other.
//
// Window is in the list because it APPENDS its output columns to its child's
// schema: it renames nothing, and every relation below it keeps the name the
// enclosing query calls it. Aggregate is deliberately NOT, and that is not a
// gap — its output schema is its own GROUP BY keys and aggregate output names,
// so the child's columns are no longer addressable and resolving a bare name
// below it would answer from a schema the stream does not carry.
func ScopePreservingWrapper(n *logical.Node) bool {
	if len(n.Children) != 1 {
		return false
	}
	switch n.Type {
	case logical.NodeFilter, logical.NodeSort, logical.NodeLimit,
		logical.NodeDistinct, logical.NodeWindow:
		return true
	}
	return false
}

// SubstituteNestedRenameRefs resolves nested subquery aliases to scan SOURCE
// columns using the #385 walk (#387), dropping the subquery's table qualifier.
// Copy-on-write like logical.substituteColRefs (#384): reuse unchanged subtrees
// and return the input itself if no reference changes. Decline the whole rewrite
// (ok=false) for subquery-bearing nodes, window functions or unknown node kinds.
// Callers leave declined specs untouched, preserving loud unknown-column errors
// rather than inventing an expression across an unsupported scope.
func SubstituteNestedRenameRefs(expr plansql.Node, child *logical.Node) (plansql.Node, bool) {
	if expr == nil || child == nil {
		return expr, true
	}
	switch e := expr.(type) {
	case *plansql.ColRef:
		if EmittedColDecls(child).isFieldPath(e) {
			if _, def, _, renamed := ResolveAggInputName(QualifiedColumn(e), child); renamed && def != nil {
				return def, true
			}
		}

		src := ResolveOutputRenameSource(strings.ToLower(e.Column), child)
		if strings.EqualFold(src, e.Column) {
			return expr, true
		}
		return &plansql.ColRef{Column: src}, true
	case *plansql.Lit, *plansql.IntervalLit, *plansql.LiteralPlaceholder, *plansql.StarNode:
		return expr, true
	case *plansql.CmpExpr:
		l, lok := SubstituteNestedRenameRefs(e.Left, child)
		r, rok := SubstituteNestedRenameRefs(e.Right, child)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return expr, true
		}
		return &plansql.CmpExpr{Left: l, Op: e.Op, Right: r}, true
	case *plansql.AndNode:
		l, lok := SubstituteNestedRenameRefs(e.Left, child)
		r, rok := SubstituteNestedRenameRefs(e.Right, child)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return expr, true
		}
		return &plansql.AndNode{Left: l, Right: r}, true
	case *plansql.OrNode:
		l, lok := SubstituteNestedRenameRefs(e.Left, child)
		r, rok := SubstituteNestedRenameRefs(e.Right, child)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return expr, true
		}
		return &plansql.OrNode{Left: l, Right: r}, true
	case *plansql.BinaryOp:
		l, lok := SubstituteNestedRenameRefs(e.Left, child)
		r, rok := SubstituteNestedRenameRefs(e.Right, child)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return expr, true
		}
		return &plansql.BinaryOp{Left: l, Op: e.Op, Right: r}, true
	case *plansql.UnaryOp:
		in, ok := SubstituteNestedRenameRefs(e.Inner, child)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return expr, true
		}
		return &plansql.UnaryOp{Op: e.Op, Inner: in}, true
	case *plansql.NotNode:
		in, ok := SubstituteNestedRenameRefs(e.Inner, child)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return expr, true
		}
		return &plansql.NotNode{Inner: in}, true
	case *plansql.ParenNode:
		in, ok := SubstituteNestedRenameRefs(e.Inner, child)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return expr, true
		}
		return &plansql.ParenNode{Inner: in}, true
	case *plansql.CastNode:
		in, ok := SubstituteNestedRenameRefs(e.Inner, child)
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
		l, lok := SubstituteNestedRenameRefs(e.Left, child)
		newVals, changed, ok := substituteNestedRenameList(e.Values, child)
		if !lok || !ok {
			return nil, false
		}
		if l == e.Left && !changed {
			return expr, true
		}
		return &plansql.InExpr{Left: l, Not: e.Not, Values: newVals}, true
	case *plansql.BetweenExpr:
		l, lok := SubstituteNestedRenameRefs(e.Left, child)
		lo, look := SubstituteNestedRenameRefs(e.Low, child)
		hi, hok := SubstituteNestedRenameRefs(e.High, child)
		if !lok || !look || !hok {
			return nil, false
		}
		if l == e.Left && lo == e.Low && hi == e.High {
			return expr, true
		}
		return &plansql.BetweenExpr{Left: l, Not: e.Not, Low: lo, High: hi}, true
	case *plansql.LikeExpr:
		l, lok := SubstituteNestedRenameRefs(e.Left, child)
		p, pok := SubstituteNestedRenameRefs(e.Pattern, child)
		if !lok || !pok {
			return nil, false
		}
		if l == e.Left && p == e.Pattern {
			return expr, true
		}
		return &plansql.LikeExpr{Left: l, Not: e.Not, Pattern: p}, true
	case *plansql.IsExpr:
		l, ok := SubstituteNestedRenameRefs(e.Left, child)
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
			s, ok := SubstituteNestedRenameRefs(e.Subject, child)
			if !ok {
				return nil, false
			}
			subj = s
			changed = changed || s != e.Subject
		}
		whens := make([]plansql.WhenClause, len(e.Whens))
		for i, w := range e.Whens {
			c, cok := SubstituteNestedRenameRefs(w.Cond, child)
			r, rok := SubstituteNestedRenameRefs(w.Result, child)
			if !cok || !rok {
				return nil, false
			}
			whens[i] = plansql.WhenClause{Cond: c, Result: r}
			changed = changed || c != w.Cond || r != w.Result
		}
		var els plansql.Node
		if e.Else != nil {
			el, ok := SubstituteNestedRenameRefs(e.Else, child)
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

// substituteNestedRenameList applies SubstituteNestedRenameRefs to each
// element; changed reports whether any element was rewritten.
func substituteNestedRenameList(nodes []plansql.Node, child *logical.Node) ([]plansql.Node, bool, bool) {
	out := make([]plansql.Node, len(nodes))
	changed := false
	for i, n := range nodes {
		nn, ok := SubstituteNestedRenameRefs(n, child)
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

// buildArmQualified puts the BUILD arm's own name back on a source column the
// walk resolved to a bare one, for the GATHER's rename alone.
//
// The walk enters an arm through its qualifier and comes back out with the
// column the block's projection reads — and where the block wrote a bare
// source name (`SELECT order_id AS k`), the arm identity is gone with it. Two
// copies of one block then resolve to one name: `SELECT a.k, a.p, b.k, b.p
// FROM (…) a JOIN (…) b ON b.k = a.k` bound BOTH items to the probe's column
// and paired every row with itself on the three DAG arms, where PostgreSQL
// pairs each row of one arm with each matching row of the other.
//
// The join qualifies its BUILD's duplicate columns by that arm's name, so the
// spelling this puts back is the one the stream really carries — and where the
// column is not a duplicate, the stream carries it bare and
// `exec.ColumnIndexFallback` strips the qualifier on the miss, which is the
// same column. The PROBE arm needs nothing: its columns keep their bare names,
// which is what a resolution that lost the qualifier already bound.
//
// It applies only where the arm holds exactly ONE relation and computes no
// relation of its own — no aggregate, no set operation — because that is
// exactly the case where the stream is that relation's columns and the name
// the join qualifies them with describes all of them. A block over two
// relations names a raw inner column after the arm, which is #773's wrong
// value from the other side; an aggregate or a set operation publishes a
// relation of its OWN, whose identity is the arm's name and not the scan's,
// and re-qualifying there would spell a column after a relation it did not
// come from.
//
// The spelling is `BuildStreamAlias`, the DAG's answer, because this is
// the GATHER's rename and the gather reads what the stage DAG emitted
// (`JoinArmAlias`' comment: the two engines hand the join two different
// streams, and a name describes a stream).
func buildArmQualified(arm *logical.Node, name string) string {
	if arm == nil || strings.TrimSpace(name) == "" || strings.IndexByte(name, '.') >= 0 {
		return name
	}
	if !armIsOneRelationsColumns(arm) {
		return name
	}
	alias := BuildStreamAlias(arm)
	if alias == "" {
		return name
	}
	return strings.ToLower(alias + "." + name)
}

// armIsOneRelationsColumns reports whether every column this arm emits is ONE
// relation's, read and renamed but never recomputed into a relation of the
// arm's own.
func armIsOneRelationsColumns(arm *logical.Node) bool {
	if len(SubtreeNamingOf(arm).AliasCols) != 1 {
		return false
	}
	computes := false
	var walk func(n *logical.Node)
	walk = func(n *logical.Node) {
		if n == nil || computes {
			return
		}
		switch n.Type {
		case logical.NodeAggregate, logical.NodeUnion, logical.NodeIntersect,
			logical.NodeExcept, logical.NodeWindow:
			computes = true
			return
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(arm)
	return !computes
}
