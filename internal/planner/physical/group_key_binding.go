// This file holds group key binding for the physical planner, governed by ADR-0026.
package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// resolveShuffleKey resolves a join key name through any Project alias nodes
// in the child subtree. For example, a CTE with `l_suppkey AS supplier_no`
// creates a Project that renames the column — the shuffle key `supplier_no`
// must be mapped back to `l_suppkey` so the executor can find it in the data
// (distributed walkStages treats ordinary Projects as passthrough, so the
// physical columns keep their original names).
//
// Join nodes recurse into their output-visible children: both sides for
// inner/outer joins, probe side only for semi/anti. First resolution wins.
//
// A key qualified by the derived table's own alias (`ON x.k = n_nationkey`)
// resolves through derivedScopeBareName, which drops the qualifier only
// inside the scope that owns it — without that the key reached the worker as
// `x.k`, a broadcast join's probe matched nothing and the query returned 0
// rows where the single-process path returned 24, and a hash join's shuffle
// failed loud with `partitioned shuffle: key "x.a" not in schema` (#467,
// #480).
//
// Renames CHAIN: `SELECT k AS j FROM (SELECT s_nationkey AS k FROM supplier)`
// has to walk j → k → s_nationkey, mirroring resolveAggInputName and
// resolveOutputRenameSource. Each Project substitutes at most once (a
// projection list is simultaneous, so `b AS a, a AS b` must not chase
// itself) and the walk only ever descends, so it terminates.
func resolveShuffleKey(key string, child *logical.Node, published map[*logical.Node]bool) string {
	if child == nil {
		return key
	}
	resolved := key
	for n := child; n != nil; {
		if n.Type == logical.NodeProject {
			bare := derivedScopeBareName(resolved, n)
			proj := projectionForName(n.Projections, resolved, bare)
			if proj != nil && bare != "" && (published[n] || projectsAMintedGroupKey(n, bare)) {
				// The aggregate below PUBLISHES this name (a hidden
				// correlation slot, ADR-0026 3a): the stage emits it under
				// exactly this name and nothing below carries it, so the walk
				// stops here. Chasing proj.Column would hand the shuffle the
				// key's SOURCE column, which the aggregate stage does not
				// emit -- measured as `partitioned shuffle: key "g" not in
				// schema` and, where the join still built, zero matched rows.
				return bare
			}
			switch {
			case proj != nil && proj.Column != "" && !strings.EqualFold(proj.Column, resolved):
				resolved = proj.Column
			case proj != nil && proj.Column == "" && bare != "" && !strings.EqualFold(bare, resolved):
				// A COMPUTED output (`COUNT(*) + 1 AS k`) has no source
				// column to chase. It exists on the DAG only under its own
				// alias — absorbAggregateOutputProjection materializes it on
				// the producing stage — so the key is the BARE alias, and
				// the walk stops: nothing below this Project carries it.
				// Before, the qualified spelling travelled to the worker and
				// the shuffle refused it (`key "b.k" not in schema`, #681).
				return bare
			}
		}
		if n.Type == logical.NodeJoin && len(n.Children) == 2 {
			if r := resolveShuffleKey(resolved, n.Children[0], published); r != resolved {
				return r
			}
			jt := strings.ToLower(n.JoinType)
			if jt != "semi" && jt != "anti" {
				return resolveShuffleKey(resolved, n.Children[1], published)
			}
			return resolved
		}
		// Continue down to single-child nodes (Filter, Sort, Limit, Project, Aggregate)
		if len(n.Children) == 1 {
			n = n.Children[0]
		} else {
			break
		}
	}
	return resolved
}

// projectsAMintedGroupKey reports whether the aggregate below a Project
// publishes `name` as one of its group keys' MINTED slots -- the reverse of an
// ordinary key, whose published name is a column of the aggregate's input.
//
// A minted key exists on the DAG only under that slot, exactly as a computed
// output exists only under its own alias, so a name walk that reaches one has
// arrived rather than having something further to chase.
func projectsAMintedGroupKey(project *logical.Node, name string) bool {
	if project == nil || len(project.Children) == 0 {
		return false
	}
	agg := findAggregateAncestor(project.Children[0])
	if agg == nil {
		return false
	}
	for i := range agg.GroupBy {
		if i < len(agg.GroupByPublish) && agg.GroupByPublish[i] != "" &&
			strings.EqualFold(agg.GroupByPublish[i], name) {
			return true
		}
	}
	return false
}

// resolveAggInputName maps a name an aggregate stage READS — an aggregate
// argument, or a GROUP BY key — back to what the stage below it actually
// emits, following the SELECT-list renames of any Project in between.
//
// walkStages treats an ordinary Project as a passthrough: it emits no stage,
// so a subquery's rename never happens on the DAG. `SELECT MAX(n) FROM
// (SELECT o_custkey AS n FROM orders)` therefore dispatched a scan reading
// o_custkey and an aggregate asking for `n`, and exec.HashAggregate answers a
// column it cannot resolve with NULL — 1499 on the single-process path and on
// DuckDB, NULL on the DAG (#355). A renamed GROUP BY key is the louder half of
// the same defect: an unresolvable key serializes as a NULL key, so every row
// collapses into one NULL group.
//
// This is the aggregate's version of what resolveShuffleKey does for join keys
// and resolveSortKeyColumn for ORDER BY terms — the same root cause, patched
// per consumer because the passthrough is what all three share.
//
// Three outcomes:
//
//	name unchanged, alias false — not a rename; the name is whatever the
//	  child already emits, which is the overwhelmingly common case.
//	name rewritten, alias true — the Project renamed a plain column; the
//	  aggregate reads the source column instead.
//	expr non-nil, alias true — the Project computed an EXPRESSION under this
//	  name (`SELECT o_custkey * 2 AS n`). There is no column to read; the
//	  caller attaches it as the aggregate's derived InputExpr, which the
//	  worker projects before aggregating. exprInput is then the node that
//	  Project reads, which is what the expression's column references are
//	  written against — the caller types the expression there, because the
//	  Project's OWN output does not carry them and a polymorphic declaration
//	  (COALESCE, NULLIF, GREATEST, LEAST) falls back to Float64 without them
//	  and drops every string (#333).
//
// It stops at an Aggregate: that node's outputs are its own GroupBy and
// OutputCol names, which the parent reads directly, and descending past it
// would resolve a name against the wrong schema.
func resolveAggInputName(name string, child *logical.Node) (resolved string, expr plansql.Node, exprInput *logical.Node, alias bool) {
	resolved = name
	if child == nil || name == "" {
		return resolved, nil, nil, false
	}
	// A projection list is simultaneous, so `b AS a, a AS b` must not chase
	// itself: each Project substitutes at most once, and the walk is finite
	// because it only ever descends.
	for n := child; n != nil; {
		switch {
		case n.Type == logical.NodeProject:
			// A name qualified by the derived table's own alias (`GROUP BY
			// u.k`) is looked up bare inside that table's scope — see
			// derivedScopeBareName. Without it the key reached the worker
			// as `u.k` and the task failed loud: `hash aggregate: GROUP BY
			// key "u.k" is not a column of its input` (#467).
			bare := derivedScopeBareName(resolved, n)
			if proj := projectionForName(n.Projections, resolved, bare); proj != nil {
				switch {
				case proj.Column != "" && !strings.EqualFold(projSourceName(proj), resolved):
					// The QUALIFIER-PRESERVING spelling, for the reason
					// resolveSortKeyColumn and annotateDerivedAliasSortKey
					// both prefer it: a self-joined table gives both arms the
					// same bare column name, so `n2.n_name AS b` carries
					// Column "n_name" — which names n1's column just as well
					// as n2's, and the aggregate bound the wrong one. GROUP BY
					// over that derived table answered 25 groups where
					// PostgreSQL 17 answers 5 (#489). Only "n2.n_name" says
					// which arm, and the worker's own lookup applies the
					// qualified↔bare fallback where the stream spells it the
					// other way.
					resolved, alias = projSourceName(proj), true
				case proj.Column == "" && !proj.IsAgg && proj.ASTExpr != nil:
					var below *logical.Node
					if len(n.Children) == 1 {
						below = n.Children[0]
					}
					return resolved, proj.ASTExpr, below, true
				}
			}
		case n.Type == logical.NodeAggregate:
			return resolved, nil, nil, alias
		case n.Type == logical.NodeJoin && len(n.Children) == 2:
			// Mirror resolveShuffleKey: a rename can sit under either arm.
			left, lexpr, lin, lok := resolveAggInputName(resolved, n.Children[0])
			if lok {
				return left, lexpr, lin, true
			}
			jt := strings.ToLower(n.JoinType)
			if jt == "semi" || jt == "anti" {
				return resolved, nil, nil, alias
			}
			right, rexpr, rin, rok := resolveAggInputName(resolved, n.Children[1])
			if rok {
				return right, rexpr, rin, true
			}
			return resolved, nil, nil, alias
		}
		if len(n.Children) == 1 {
			n = n.Children[0]
			continue
		}
		break
	}
	return resolved, expr, exprInput, alias
}

// aggStageGroupKey reports the name the aggregate STAGE will emit for a
// logical GROUP BY key, and whether that differs from the key as written.
//
// A key naming a subquery's rename is dispatched under the source column; a
// key naming a subquery's computed alias is dispatched under the EXPRESSION
// TEXT, which is the spelling the worker's pre-aggregate projection already
// compiles and emits (buildAggInputProjection treats a group-by entry that
// does not parse as a bare column reference as derived). Both walkStages and
// the sort's aggregateOutputName have to agree on it, so both call this.
func aggStageGroupKey(key string, e plansql.Node, child *logical.Node) (string, bool) {
	// Only a term that IS a column reference may be resolved as one. The
	// recorded key text cannot say which: `GROUP BY "g + 1"` names a column
	// and `GROUP BY g + 1` is arithmetic, and both are recorded as `g + 1`
	// because a delimited identifier's quotes are not part of its name
	// (#725). Asked of the TEXT, the arithmetic key found a delimited column
	// spelled the same way and bound to it — PostgreSQL answered five groups
	// of the sum and both DAG arms answered nine groups of the column,
	// silently. PostgreSQL's rule is that unquoted `g + 1` is arithmetic,
	// full stop.
	if e != nil {
		if _, bare := e.(*plansql.ColRef); !bare {
			return key, false
		}
	}
	resolved, expr, _, renamed := resolveAggInputName(key, child)
	if !renamed {
		return key, false
	}
	if expr != nil {
		// The alias names an EXPRESSION. There are two answers to "what is this
		// value called on the DAG" — the expression, or the name the producing
		// fragment materialized it under — and which one is right is a question
		// about a STAGE, not about the logical plan. It cannot be answered here:
		// walkStages runs BEFORE attachScanSelectProjections and
		// absorbWindowArmProjection, the passes that decide whether any fragment
		// materializes the alias at all.
		//
		// Round 1 of this arc inferred the answer from NODE KINDS and got it
		// wrong three times in three different directions (#777's history is in
		// ADR-0026 §4a). A key whose value no stage can name is REFUSED and
		// routed instead — refuseUnstageableGroupKey condition (3).
		return expr.String(), true
	}
	return resolved, true
}

// aggStageDispatchKey is aggStageGroupKey for the DISPATCHED spelling: the
// text the worker parses and computes the key from.
//
// It answers for a DERIVED key too, which aggStageGroupKey deliberately does
// not. `a_b + 1` is not a name, so resolveAggInputName declines it — but its
// LEAVES are names, and a rename Project between the aggregate and its scan
// emits no stage of its own, so the key reached the worker spelled over `a_b`,
// which the scan does not emit: the key computed NULL for every row and the
// whole table collapsed into ONE group.
//
// aggregateOutputName deliberately does NOT take this path. It answers what a
// SORT KEY should name, and a sort key is resolved on both engines — the
// single-process aggregate publishes the key under its own canonical text and
// knows nothing of the DAG's source re-spelling.
func aggStageDispatchKey(key string, e plansql.Node, child *logical.Node) (string, bool) {
	if resolved, renamed := aggStageGroupKey(key, e, child); renamed {
		return resolved, true
	}
	return aggStageDerivedKey(key, child)
}

// derivedGroupKeyDecl is the declared type of a computed GROUP BY key.
//
// A key's leaves may be bound by a rename Project, and inputColDecls STOPS at
// one — a rename can bind a name to a different value, so the walk refuses to
// look through it. For a PLAIN rename that caution costs the declaration:
// `GROUP BY g + 1` over `(SELECT a AS g …)` with `a` DECIMAL(9,2) could type
// neither `g` nor the sum, fell to the float rule, and the pre-aggregate
// projection then handed a DECIMAL's rendered text to a FLOAT64 vector — a
// hard failure on a query PostgreSQL answers (five groups).
//
// The repair is #387's own: an expression re-spelled into SOURCE columns is
// typed against the decls BELOW the rename chain, because a plain rename
// rebinds names and not values. aggStageDerivedKey performs exactly that
// re-spelling, and the key the DAG dispatches has already had it done, so the
// second arm catches that case too.
func derivedGroupKeyDecl(key string, node plansql.Node, child *logical.Node) expr.DeclType {
	// The form the WORKER computes. A key whose leaves a rename Project binds
	// is dispatched re-spelled into source columns (aggStageDerivedKey), and
	// typing the spelling the query wrote instead types an expression nothing
	// evaluates.
	typed := node
	if respelled, changed := aggStageDerivedKey(key, child); changed {
		if n, err := plansql.ParseExpression(respelled); err == nil {
			typed = n
		}
	}
	// The scope that can NAME the key's columns is the one that types it.
	if decls, scope, ok := namingScopeDecls(typed, child); ok {
		return inferProjectionDeclType(typed, parquet.TypeString,
			strictIntArithCols(scope), decls)
	}
	// No level of the chain names them: keep the answer this had before, which
	// is the float rule over the aggregate's input decls.
	return inferProjectionDeclType(node, parquet.TypeString,
		strictIntArithCols(child), inputColDecls(child))
}

// namingScopeDecls answers WHICH declaration scope types a dispatched
// expression, by descending the chain below the aggregate until it finds the
// level whose emitted columns can name every column the expression references.
//
// Two consumers, one question. A GROUP BY key and an aggregate's ARGUMENT are
// both re-spelled for dispatch — the key into its defining expression, the
// argument into the column a rename Project binds — and ADR-0026 §2c's rule
// applies to both: a name so re-spelled is TYPED where it was re-spelled TO,
// not where the query wrote it. Asking the question in one place is what keeps
// the two from answering it differently (ADR-0023 item 5).
//
// A Project emits no stage of its own on the DAG, so a key spelled against a
// Project's OUTPUT and a key spelled against its INPUT are both evaluated in
// the same fragment and only one of them resolves — and which one depends on
// whether the key was re-spelled. Both scopes were consulted before, in fixed
// order and each with its own gate, and neither gate asked the one question
// that decides it:
//
//   - the emitted scope was accepted whenever `nodeDeclaredType` answered
//     Decided, which arithmetic always does. The FLOAT rule is a rule, not an
//     observation, so `GROUP BY k` over `(SELECT c_dec + 1 AS k FROM typemx) s`
//     — dispatched as `c_dec + 1` into a scope carrying `k` and no `c_dec` —
//     was answered FLOAT64 with confidence, and died at the #361 store guard:
//     `cannot store string into FLOAT64 vector`, on a query the same SQL over
//     the base table answers (#792).
//   - the source scope (`sourceColDeclsThroughRenames`) stops at a COMPUTED
//     projection item and returns NOTHING, because a rename may rebind a name
//     to a different value. True of a name; not true of the DEFINING
//     EXPRESSION that Project item was hoisted out of, which is spelled in the
//     Project's own input scope. So `a * 3` over `(SELECT id, a * 3 AS w FROM
//     decpair) x` had no scope at all and fell to the same float rule (#781's
//     loud cell, #786).
//
// Descending is gated on coverage in both directions, which is what makes it
// safe: a key the Project's OUTPUT can name stops at the OUTPUT, so a rebound
// name is never read past its rebinding; a key it cannot name is looked for
// one level down, where it either resolves or the walk gives up.
func namingScopeDecls(node plansql.Node, child *logical.Node) (colDecls, *logical.Node, bool) {
	for n := child; n != nil; {
		d := emittedColDecls(n)
		if declsCoverEveryColRef(node, d) {
			return d, n, true
		}
		if !groupKeyScopeDescends(n.Type) || len(n.Children) != 1 {
			return colDecls{}, nil, false
		}
		n = n.Children[0]
	}
	return colDecls{}, nil, false
}

// groupKeyScopeDescends lists the nodes namingScopeDecls looks THROUGH.
//
// The question is narrower than logical.AggScopePreservingWrapper's and is
// deliberately its own list rather than a copy of one, for the reason
// ADR-0026 §4 gives about the fifth reader: "are this node's input columns
// still values of the same rows" is not "do this node's rows carry one row per
// group", and a shared list that answers both is a list answering neither.
//
// Here: a node belongs when a name it does not emit may still be a name its
// INPUT emits, for the same rows. A Project renames and computes, and the
// coverage test above is what keeps a rebound name from being read past its
// rebinding; Filter/Sort/Limit/Distinct forward every column untouched; a
// Window only ADDS columns, so a name it does not carry is its input's.
//
// NodeAggregate is excluded, and that is the boundary: an aggregate REPLACES
// its input's scope with keys and aggregate outputs, so a key naming one of
// its outputs must be typed there and a key naming a column it consumed is
// not a key this plan can evaluate at all.
func groupKeyScopeDescends(t logical.NodeType) bool {
	switch t {
	case logical.NodeProject, logical.NodeFilter, logical.NodeSort,
		logical.NodeLimit, logical.NodeDistinct, logical.NodeWindow:
		return true
	}
	return false
}

// declsCoverEveryColRef reports whether decls can name every column the
// expression references.
//
// It is the gate on reading a decl set's answer at all. `nodeDeclaredType`
// answers Decided for arithmetic whose operands it cannot type — the float
// rule is a rule, not an observation — so "Decided" says nothing about whether
// the set was looking at this expression's columns. A set that cannot name a
// leaf has not typed the expression; it has guessed at it.
//
// A term with no column references at all (a literal, `now()`) is covered
// vacuously, which is correct: there is nothing for a decl set to know.
func declsCoverEveryColRef(node plansql.Node, decls colDecls) bool {
	for _, ref := range collectColRefs(node) {
		if _, ok := decls.colDecl(ref); !ok {
			return false
		}
	}
	return true
}

// aggStageDerivedKey re-spells a computed GROUP BY key's column references
// into the columns the aggregate's input really emits, the way
// aggStageGroupKey does for a bare one. ok=false leaves the key exactly as it
// was — which is every key whose leaves are already source columns, and every
// shape this walk does not recognize.
func aggStageDerivedKey(key string, child *logical.Node) (string, bool) {
	node, err := plansql.ParseExpression(key)
	if err != nil {
		return key, false
	}
	if _, bare := node.(*plansql.ColRef); bare {
		return key, false // aggStageGroupKey's own case, already answered
	}
	changed := false
	out := plansql.RewriteExpr(node, func(n plansql.Node) (plansql.Node, bool) {
		ref, isRef := n.(*plansql.ColRef)
		if !isRef {
			return nil, false
		}
		resolved, expr, _, renamed := resolveAggInputName(qualifiedColumn(ref), child)
		if !renamed {
			return nil, false
		}
		changed = true
		if expr != nil {
			// The rename's own DEFINITION is an expression, so the key is
			// that expression substituted in: `k + 1` over `SELECT a * 2 AS
			// k` is `(a * 2) + 1`, parenthesised so the substitution cannot
			// change what binds to what.
			return &plansql.ParenNode{Inner: expr}, true
		}
		return &plansql.ColRef{Column: resolved}, true
	})
	if !changed {
		return key, false
	}
	return out.String(), true
}

// qualifiedColumn renders a column reference the way resolveAggInputName
// expects to receive it.
func qualifiedColumn(ref *plansql.ColRef) string {
	if ref.Table != "" {
		return ref.Table + "." + ref.Column
	}
	return ref.Column
}

// derivedGroupKeyTypes types every DERIVED (non-bare) GROUP BY key for the
// wire (Stage.GroupByTypes): the same inferProjectionTypeCols call, over the
// same input column types, that the single-process pre-aggregate projection
// uses for its synthetic key columns — so both engines store one key
// expression in one vector type. Keys that parse as bare column references
// (or not at all) are omitted; the worker passes those through untyped.
//
// The map is keyed by the exact dispatched key text (post-aggStageGroupKey),
// because that text is what the worker parses and looks up (#379).
func derivedGroupKeyTypes(groupBy []string, child *logical.Node) (map[string]parquet.TypeID, map[string]logical.DecimalMeta) {
	var out map[string]parquet.TypeID
	var dec map[string]logical.DecimalMeta
	var colTypes colDecls
	var strictInt map[string]bool
	resolved := false
	for _, key := range groupBy {
		if key == "" {
			continue
		}
		node, err := plansql.ParseExpression(key)
		if err != nil {
			continue
		}
		if !resolved {
			colTypes = inputColDecls(child)
			strictInt = strictIntArithCols(child)
			resolved = true
		}
		// A bare column reference needs no derived key — except a ROW FIELD
		// PATH, which parses as one and is not one: no stage emits a column
		// by that name, so the worker has to COMPUTE it, and an entry here is
		// how it learns that (buildAggInputProjection reads this map as the
		// planner's "this key is derived, and this is its type"). Without it
		// the key reached HashAggregate as a name and failed to resolve
		// (#568).
		if _, bare := node.(*plansql.ColRef); bare && !astIsFieldPath(node, colTypes) {
			continue
		}
		if out == nil {
			out = make(map[string]parquet.TypeID)
		}
		d := derivedGroupKeyDecl(key, node, child)
		_, _ = strictInt, colTypes
		out[key] = d.ID
		if d.ID == parquet.TypeDecimal && d.DecKnown {
			// The (p,s) beside the TypeID: the worker builds the key vector
			// from this declaration, and a DECIMAL one with no scale
			// truncates every value into it (ADR-0024 item 2).
			if dec == nil {
				dec = make(map[string]logical.DecimalMeta)
			}
			dec[key] = logical.DecimalMeta{Precision: d.Precision, Scale: d.Scale}
		}
	}
	return out, dec
}

// resolveSortKeyColumn maps an ORDER BY key that names a SELECT-list alias
// back to the name the aggregate stage below it actually emits.
//
// The logical builder resolves ORDER BY to the SELECT list's OUTPUT name
// (logical.resolveOrderByColumn), which is exactly what the single-process
// pipeline needs — there the Project really does run below the Sort, so the
// alias exists by the time the sort reads a row. Distributed walkStages
// instead treats an ordinary Project as a passthrough (the same reason
// resolveShuffleKey exists for join keys), so an aggregate's output keeps the
// GroupBy spelling. A sort keyed on `p` from `o_orderpriority AS p` then
// matches no column, the sort is a no-op, and the ORDER BY is silently lost —
// while the same query without the rename sorts correctly (#313, and TPC-H
// Q09 via `n_name AS nation` / `SUBSTR(o_orderdate,1,4) AS o_year`).
//
// Scope is deliberately the aggregate: a final_aggregate names its output
// from GroupByCols and AggSpec.OutputCol, which walkStages copies verbatim
// from this node, so the mapping is exact and decidable here. Sorts over a
// bare scan or join are left alone — attachScanSelectProjections may attach
// an alias-naming OpProject to the producing fragment later in
// PlanDistributed, so the correct spelling for those is not yet known at
// this point (it declines every plan carrying an aggregate, so the two
// never overlap).
//
// Each Project substitutes at most once: a projection list is simultaneous,
// so `b AS a, a AS b` must not chase itself.
func resolveSortKeyColumn(key string, child *logical.Node) string {
	// resolved is the preferred candidate, alt a second one to try when the
	// first names no output of the aggregate below.
	resolved, alt := key, ""
	for n := child; n != nil; {
		switch n.Type {
		case logical.NodeProject:
			for _, proj := range n.Projections {
				if !strings.EqualFold(proj.Alias, resolved) &&
					(alt == "" || !strings.EqualFold(proj.Alias, alt)) {
					continue
				}
				// Expression text first, because it keeps the table
				// qualifier: a self-joined table gives both aliases the same
				// bare column name, so `n1.n_name AS supp_nation` carries
				// column "n_name" — which matches neither group key and
				// cannot, since n2 shares it. Only "n1.n_name" identifies
				// which alias, and a GROUP BY that spells its key bare is
				// covered by the Column fallback below (#314/#313).
				resolved, alt = proj.Expr, proj.Column
				if resolved == "" {
					resolved, alt = proj.Column, ""
				}
				if alt == resolved {
					alt = ""
				}
				break
			}
		case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
			// Order-preserving passthroughs: keep descending.
		case logical.NodeAggregate:
			if out, ok := aggregateOutputName(n, resolved); ok {
				return out
			}
			if alt != "" {
				if out, ok := aggregateOutputName(n, alt); ok {
					return out
				}
			}
			return key
		default:
			return key
		}
		if len(n.Children) != 1 {
			return key
		}
		n = n.Children[0]
	}
	return key
}

// aggregateOutputName reports the name an Aggregate node emits for col — its
// own spelling of the matching group key or aggregate output — so a sort key
// resolved through a rename lands on a column the stage really produces.
//
// A group key is reported the way the aggregate's fragment will EMIT it —
// `stageEmittedKeyNames` over the key's PUBLISHED name — which is one answer
// for both engines. It used to be the DISPATCH re-spelling
// (`aggStageGroupKey`), because a stage published its keys under the spelling
// the worker computed them from; now the two names are separate fields and the
// published one is what any consumer above the aggregate reads (ADR-0026 §2b,
// #355 and #794).
func aggregateOutputName(n *logical.Node, col string) (string, bool) {
	var child *logical.Node
	if len(n.Children) == 1 {
		child = n.Children[0]
	}
	published, resolve := stageGroupKeyNames(n, child)
	names := stageEmittedKeyNames(published, resolve)
	emit := func(i int, g string) (string, bool) {
		if i >= 0 && i < len(names) {
			return names[i], true
		}
		return g, true
	}
	for i, g := range n.GroupBy {
		if strings.EqualFold(g, col) {
			return emit(i, g)
		}
	}
	// The two spellings of one key: `GROUP BY u.k` names the same output as
	// the `k` the SELECT list and the ORDER BY use, and either side may be
	// the qualified one. Both are dropped to their bare form only inside the
	// derived scope that owns the qualifier — see derivedScopeBareName
	// (#467). A bare spelling that matches TWO group keys is a self-join's
	// `n1.n_name`/`n2.n_name`: naming one of them would order by an
	// arbitrary side, so the key is left for the caller to give up on, the
	// same call lookupEmittedColumn makes on the same ambiguity.
	bare := func(name string) string {
		if b := derivedScopeBareName(name, child); b != "" {
			return b
		}
		return name
	}
	cb := bare(col)
	match, matchIdx, count := "", -1, 0
	for i, g := range n.GroupBy {
		if strings.EqualFold(bare(g), cb) {
			match, matchIdx, count = g, i, count+1
		}
	}
	if count == 1 {
		return emit(matchIdx, match)
	}
	for _, a := range n.AggExprs {
		if strings.EqualFold(a.OutputCol, col) {
			return a.OutputCol, true
		}
	}
	return "", false
}
