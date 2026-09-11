// This file holds group key binding for the physical planner, governed by ADR-0026.
package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// resolveShuffleKey follows Project aliases to the source columns ordinary
// DAG Projects leave unchanged. Recurse into output-visible join children:
// both sides for inner/outer, probe only for semi/anti; first resolution wins.
// Use derivedScopeBareName to drop qualifiers only in their owning scope
// (#467, #480). Follow chained renames, substituting at most once per Project:
// a projection list is simultaneous (b AS a, a AS b must not chase itself).
// The walk only descends, so it terminates.
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

// resolveAggInputName resolves aggregate arguments/group keys through Project
// renames to the columns the stage below emits (#355). Unchanged/alias=false
// means no rename; rewritten/alias=true reads a renamed plain column.
// Non-nil expr/alias=true means a computed alias: attach derived InputExpr
// for worker pre-projection, typing it at exprInput, the Project's INPUT,
// where its references resolve (#333). Stop at an Aggregate: its own GroupBy
// and OutputCol names define the schema the parent reads.
// See docs/internals/aggregate-input-name-resolution.md for the design.
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

// namingScopeDecls finds the first emitted scope covering EVERY reference in
// a dispatched expression. Re-spelled GROUP BY keys and aggregate arguments
// must be typed where they were re-spelled TO (ADR-0026 §2c; ADR-0023 item 5).
// Coverage, not nodeDeclaredType confidence, gates descent: arithmetic can
// decide FLOAT64 even with missing references (#361, #792). Computed Project
// definitions need their input scope (#781, #786). Stop where OUTPUT covers
// the expression; never read a rebound name past its rebinding.
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

// resolveSortKeyColumn maps ORDER BY aliases to an aggregate's emitted
// GroupByCols/AggSpec.OutputCol names (#313). Ordinary DAG Projects do not
// perform the rename. Scope this to aggregates: scan/join producers may get
// an alias-naming OpProject later via attachScanSelectProjections, which
// declines aggregate plans. Each Project substitutes at most once because
// its projection list is simultaneous.
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
