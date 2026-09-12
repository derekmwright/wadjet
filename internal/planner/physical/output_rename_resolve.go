package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// resolveOutputRenameSource maps nested aliases to DAG stream SOURCE names
// for gather OutputRenames (#385). Start below the outermost Project;
// substitute at most once per simultaneous projection list, resolving nested
// Projects level by level through order/cardinality-preserving wrappers.
// Stop at computed aliases, materialized by #383/#169, and at Aggregates,
// whose GroupBy/OutputCol outputs have their own schema (#355's aggStageRenames).
// Joins recurse into output-visible children (probe only for semi/anti), first
// substitution wins. See resolveOutputRenameSourceForGather for caller differences.
// See docs/internals/gather-output-rename-resolution.md for the design.

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

// renameIsAggregateOutput reports whether the SELECT item named `name` is,
// after every rename between here and the operator that computes it, an
// AGGREGATE OUTPUT rather than a group-key reference.
//
// `physical.OutputRename.IsAgg` is a property of the item the OUTER block
// wrote, and the gather's duplicate-name pairing needs the class of what that
// item REFERS to. One derived table is enough to separate them:
//
//	SELECT u.g, u.x FROM (SELECT COUNT(*) AS g, g AS x FROM t
//	                      GROUP BY g HAVING COUNT(*) > 0) u ORDER BY u.x
//
// `u.g` is a plain column reference — IsAgg false — while the value it names
// is the COUNT, and the aggregate publishes its KEY under that same name. The
// key branch of `classScopedMatch` then took the first column of the name,
// which is the key, and both DAG arms answered 0,0 | 1,1 | 2,2 for
// PostgreSQL's 80,0 | 80,1 | 80,2 (#785 round 2).
//
// It walks the same way resolveRenameSource does and stops where that stops,
// so the two answers are about the same projection.
func renameIsAggregateOutput(name string, child *logical.Node) bool {
	resolved := name
	for n, hops := child, 0; n != nil && hops < 64; hops++ {
		switch {
		case n.Type == logical.NodeProject:
			bare := derivedScopeBareName(resolved, n)
			proj := projectionPublishingName(n.Projections, resolved, bare)
			if proj == nil {
				return false
			}
			if proj.IsAgg {
				return true
			}
			if proj.Column == "" {
				return false // a computed item is neither
			}
			// A Project whose INPUT is the aggregate's own output is where the
			// two classes are separated, so the answer is this projection's
			// own class and the walk stops. Descending past it re-asks by NAME
			// at the aggregate — where the key and the output answer to the
			// same name, which is the very collision being resolved — and
			// `g AS x` over `COUNT(*) AS g` came back "aggregate" because the
			// aggregate publishes a `g`.
			if logical.AggregateOverGroupRows(n) != nil {
				return false
			}
			next := proj.Column
			if proj.Expr != "" {
				next = strings.ToLower(proj.Expr)
			}
			if strings.EqualFold(next, resolved) {
				return false
			}
			resolved = next
		case n.Type == logical.NodeAggregate:
			// The AGGREGATE itself answers nothing. Its output schema is
			// exactly where a key and an output can share a name, so asking
			// it by name is the collision, not its resolution — and asking it
			// re-classified the block's OWN key reference as an aggregate
			// (`SELECT COUNT(*) AS g, g AS x … GROUP BY g`: `x`'s source is
			// `g`, which the aggregate does publish as an output). The class
			// of an item in THIS block is the item's own `IsAgg`; this walk
			// exists only to carry that class across a WRAPPER, and where
			// there is no wrapper there is nothing to carry.
			return false
		case n.Type == logical.NodeJoin && len(n.Children) == 2:
			if own := ownedJoinArm(n, resolved); own != nil {
				return renameIsAggregateOutput(resolved, own)
			}
			return renameIsAggregateOutput(resolved, n.Children[0]) ||
				renameIsAggregateOutput(resolved, n.Children[1])
		}
		if len(n.Children) == 1 {
			n = n.Children[0]
			continue
		}
		return false
	}
	return false
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
			if own := ownedJoinArm(n, resolved); own != nil {
				if (jt == "semi" || jt == "anti") && own == n.Children[1] {
					return resolved // the build side is not output-visible
				}
				return resolveRenameSource(resolved, own, forGather)
			}
			if r := resolveRenameSource(resolved, n.Children[0], forGather); !strings.EqualFold(r, resolved) {
				return r
			}
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

// ownedJoinArm returns the join arm whose subtree answers to a QUALIFIED
// name's qualifier, or nil when the qualifier is not exactly one arm's.
//
// `subtreeNamesRelation` is the same scope test derivedScopeBareName uses: a
// derived table's alias is stamped on every scan below it and a CTE's name
// sits on the subtree root, so both spellings of a named scope are covered.
func ownedJoinArm(n *logical.Node, name string) *logical.Node {
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

// resolveRenameSourceInScope resolves the BARE part of a qualified name
// inside the subtree its qualifier names, and reports whether that subtree
// was found at all.
//
// attachScanSelectProjections' fallback for a qualified spec — the nested
// Project's alias is bare, so `q.w` has to be looked up as `w` — dropped the
// qualifier and ran the ordinary walk, which takes the first arm that
// answers. With two arms publishing `w` that is the other arm's column
// (#742). Scoping the lookup asks the same question of the right relation.
func resolveRenameSourceInScope(name string, child *logical.Node) (string, bool) {
	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 || dot == len(name)-1 || child == nil {
		return "", false
	}
	qual, bare := name[:dot], name[dot+1:]
	scope := relationScopeSubtree(child, qual)
	if scope == nil {
		return "", false
	}
	src := resolveOutputRenameSource(bare, scope)
	if strings.EqualFold(src, bare) {
		return "", true // the scope exists and renames nothing: not a miss
	}
	return src, true
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
	if relationScopeSubtree(child, qual) == nil {
		return false // the qualifier names no relation of this input
	}
	return armsPublishingBareName(child, bare) > 1
}

// armsPublishingBareName counts the join arms below n whose subtree publishes
// bare — a scan column of theirs, or a name one of their Projects mints.
func armsPublishingBareName(n *logical.Node, bare string) int {
	for n != nil && scopePreservingWrapper(n) {
		n = n.Children[0]
	}
	if n == nil {
		return 0
	}
	if n.Type == logical.NodeJoin && len(n.Children) == 2 {
		return armsPublishingBareName(n.Children[0], bare) +
			armsPublishingBareName(n.Children[1], bare)
	}
	if subtreeNamingOf(n).ownsBareName(strings.ToLower(bare)) {
		return 1
	}
	return 0
}

// windowArgSourceInScope resolves a QUALIFIED window argument to the source
// column the DAG's streams carry, inside the arm its qualifier names.
//
// `derivedAliasSourceColumn` stops at a Join — it has no way to choose an arm
// — so asked of a join it answers nothing and the argument reached the worker
// under the derived ALIAS, which on the DAG no stream carries. Scoping it
// first is the same composition `resolveRenameSourceInScope` performs for a
// projection. ok=false means the qualifier names no relation here and the
// caller keeps the unscoped walk.
func windowArgSourceInScope(name string, child *logical.Node) (string, bool) {
	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 || dot == len(name)-1 || child == nil {
		return "", false
	}
	qual, bare := name[:dot], name[dot+1:]
	scope := relationScopeSubtree(child, qual)
	if scope == nil {
		return "", false
	}
	return derivedAliasSourceColumn(bare, scope), true
}

// relationScopeSubtree descends through two-arm JOINs to the named relation
// and through scopePreservingWrapper: Filter, Sort, Limit, Distinct and Window.
// These narrow rows or append columns without renaming existing ones; Filter
// and Window must not hide a join and let a sibling capture a reference (#742).
// Stop at Project (the scope's SELECT list), Aggregate (its own outputs), and
// set operations (multiple arms with output names rooted in the first arm).
// Unlike resolveRenameSource, this walk does not consume the scope's Project.
// See docs/internals/relation-scope-through-wrappers.md for the design.
func relationScopeSubtree(n *logical.Node, name string) *logical.Node {
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
		if scopePreservingWrapper(n) {
			n = n.Children[0]
			continue
		}
		return n
	}
}

// scopePreservingWrapper reports whether n is a single-child node that leaves
// the relations below it addressable by the same names and renames none of
// their columns, so a scope walk may descend through it.
//
// It is `resolveRenameSource`'s own descent rule written as a list rather than
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
func scopePreservingWrapper(n *logical.Node) bool {
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

// substituteNestedRenameRefs resolves nested subquery aliases to scan SOURCE
// columns using the #385 walk (#387), dropping the subquery's table qualifier.
// Copy-on-write like logical.substituteColRefs (#384): reuse unchanged subtrees
// and return the input itself if no reference changes. Decline the whole rewrite
// (ok=false) for subquery-bearing nodes, window functions or unknown node kinds.
// Callers leave declined specs untouched, preserving loud unknown-column errors
// rather than inventing an expression across an unsupported scope.
func substituteNestedRenameRefs(expr plansql.Node, child *logical.Node) (plansql.Node, bool) {
	if expr == nil || child == nil {
		return expr, true
	}
	switch e := expr.(type) {
	case *plansql.ColRef:
		if emittedColDecls(child).isFieldPath(e) {
			if _, def, _, renamed := resolveAggInputName(qualifiedColumn(e), child); renamed && def != nil {
				return def, true
			}
		}

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
func resolveJoinNeededColumns(node *logical.Node, published map[*logical.Node]bool) []string {
	if len(node.NeededColumns) == 0 {
		return node.NeededColumns
	}
	changed := false
	resolved := make([]string, len(node.NeededColumns))
	for i, c := range node.NeededColumns {
		resolved[i] = resolveShuffleKey(c, node, published)
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
