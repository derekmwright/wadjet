// SPDX-License-Identifier: MIT

// This file holds group key binding for the physical planner, governed by ADR-0026.
package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ResolveAggInputName resolves aggregate arguments/group keys through Project
// renames to the columns the stage below emits (#355). Unchanged/alias=false
// means no rename; rewritten/alias=true reads a renamed plain column.
// Non-nil expr/alias=true means a computed alias: attach derived InputExpr
// for worker pre-projection, typing it at exprInput, the Project's INPUT,
// where its references resolve (#333). Stop at an Aggregate: its own GroupBy
// and OutputCol names define the schema the parent reads.
// See docs/internals/aggregate-input-name-resolution.md for the design.
func ResolveAggInputName(name string, child *logical.Node) (resolved string, expr plansql.Node, exprInput *logical.Node, alias bool) {
	resolved = name
	if ref, err := plansql.ParseExpression(name); err == nil {
		if field, ok := ref.(*plansql.ColRef); ok && EmittedColDecls(child).isFieldPath(field) {
			// A join publishes its own container identities. Resolve only an
			// alias owned by this unary scope, never a rename inside another
			// join arm (where the source may collide with the other arm).
			ownsParent := false
			for scope := child; scope != nil; {
				if scope.Type == logical.NodeProject && ProjectionForName(scope.Projections, field.Table, DerivedScopeBareName(field.Table, scope)) != nil {
					ownsParent = true
					break
				}
				if len(scope.Children) != 1 {
					break
				}
				scope = scope.Children[0]
			}
			if !ownsParent {
				return name, nil, nil, false
			}

			parent, def, scope, renamed := ResolveAggInputName(field.Table, child)
			if renamed {
				if def == nil {
					def = &plansql.ColRef{Column: parent}
					_, scope, _ = namingScopeDecls(def, child)
				}
				return name, &plansql.FuncCallNode{Name: "row_field", Args: []plansql.Node{def, &plansql.Lit{Kind: plansql.LitString, Value: field.Column}}}, scope, true
			}
		}
	}
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
			// DerivedScopeBareName. Without it the key reached the worker
			// as `u.k` and the task failed loud: `hash aggregate: GROUP BY
			// key "u.k" is not a column of its input` (#467).
			bare := DerivedScopeBareName(resolved, n)
			if proj := ProjectionForName(n.Projections, resolved, bare); proj != nil {
				switch {
				case proj.Column != "" && !strings.EqualFold(ProjSourceName(proj), resolved):
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
					resolved, alias = ProjSourceName(proj), true
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
			left, lexpr, lin, lok := ResolveAggInputName(resolved, n.Children[0])
			if lok {
				return left, lexpr, lin, true
			}
			jt := strings.ToLower(n.JoinType)
			if jt == "semi" || jt == "anti" {
				return resolved, nil, nil, alias
			}
			right, rexpr, rin, rok := ResolveAggInputName(resolved, n.Children[1])
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

// DerivedGroupKeyDecl is the declared type of a computed GROUP BY key.
//
// A key's leaves may be bound by a rename Project, and InputColDecls STOPS at
// one — a rename can bind a name to a different value, so the walk refuses to
// look through it. For a PLAIN rename that caution costs the declaration:
// `GROUP BY g + 1` over `(SELECT a AS g …)` with `a` DECIMAL(9,2) could type
// neither `g` nor the sum, fell to the float rule, and the pre-aggregate
// projection then handed a DECIMAL's rendered text to a FLOAT64 vector — a
// hard failure on a query PostgreSQL answers (five groups).
//
// The repair is #387's own: an expression re-spelled into SOURCE columns is
// typed against the decls BELOW the rename chain, because a plain rename
// rebinds names and not values. AggStageDerivedKey performs exactly that
// re-spelling, and the key the DAG dispatches has already had it done, so the
// second arm catches that case too.
func DerivedGroupKeyDecl(key string, node plansql.Node, child *logical.Node) expr.DeclType {
	// The form the WORKER computes. A key whose leaves a rename Project binds
	// is dispatched re-spelled into source columns (AggStageDerivedKey), and
	// typing the spelling the query wrote instead types an expression nothing
	// evaluates.
	typed := node
	if respelled, changed := AggStageDerivedKey(key, child); changed {
		if n, err := plansql.ParseExpression(respelled); err == nil {
			typed = n
		}
	}
	// The scope that can NAME the key's columns is the one that types it.
	if decls, scope, ok := namingScopeDecls(typed, child); ok {
		return InferProjectionDeclType(typed, parquet.TypeString,
			StrictIntArithCols(scope), decls)
	}
	// No level of the chain names them: keep the answer this had before, which
	// is the float rule over the aggregate's input decls.
	return InferProjectionDeclType(node, parquet.TypeString,
		StrictIntArithCols(child), InputColDecls(child))
}

// namingScopeDecls finds the first emitted scope covering EVERY reference in
// a dispatched expression. Re-spelled GROUP BY keys and aggregate arguments
// must be typed where they were re-spelled TO (ADR-0026 §2c; ADR-0023 item 5).
// Coverage, not NodeDeclaredType confidence, gates descent: arithmetic can
// decide FLOAT64 even with missing references (#361, #792). Computed Project
// definitions need their input scope (#781, #786). Stop where OUTPUT covers
// the expression; never read a rebound name past its rebinding.
func namingScopeDecls(node plansql.Node, child *logical.Node) (ColDecls, *logical.Node, bool) {
	for n := child; n != nil; {
		d := EmittedColDecls(n)
		if declsCoverEveryColRef(node, d) {
			return d, n, true
		}
		if !groupKeyScopeDescends(n.Type) || len(n.Children) != 1 {
			return ColDecls{}, nil, false
		}
		n = n.Children[0]
	}
	return ColDecls{}, nil, false
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
// It is the gate on reading a decl set's answer at all. `NodeDeclaredType`
// answers Decided for arithmetic whose operands it cannot type — the float
// rule is a rule, not an observation — so "Decided" says nothing about whether
// the set was looking at this expression's columns. A set that cannot name a
// leaf has not typed the expression; it has guessed at it.
//
// A term with no column references at all (a literal, `now()`) is covered
// vacuously, which is correct: there is nothing for a decl set to know.
func declsCoverEveryColRef(node plansql.Node, decls ColDecls) bool {
	for _, ref := range CollectColRefs(node) {
		if _, ok := decls.colDecl(ref); !ok {
			return false
		}
	}
	return true
}

// AggStageDerivedKey re-spells a computed GROUP BY key's column references
// into the columns the aggregate's input really emits, the way
// aggStageGroupKey does for a bare one. ok=false leaves the key exactly as it
// was — which is every key whose leaves are already source columns, and every
// shape this walk does not recognize.
func AggStageDerivedKey(key string, child *logical.Node) (string, bool) {
	node, err := plansql.ParseExpression(key)
	if err != nil {
		return key, false
	}
	if ref, bare := node.(*plansql.ColRef); bare && !EmittedColDecls(child).isFieldPath(ref) {
		return key, false
	}
	changed := false
	out := plansql.RewriteExpr(node, func(n plansql.Node) (plansql.Node, bool) {
		ref, isRef := n.(*plansql.ColRef)
		if !isRef {
			return nil, false
		}

		if EmittedColDecls(child).isFieldPath(ref) {
			_, def, _, renamed := ResolveAggInputName(QualifiedColumn(ref), child)
			if !renamed || def == nil {
				return nil, false
			}
			changed = true
			return def, true
		}
		resolved, expr, _, renamed := ResolveAggInputName(QualifiedColumn(ref), child)
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

// QualifiedColumn renders a column reference the way ResolveAggInputName
// expects to receive it.
func QualifiedColumn(ref *plansql.ColRef) string {
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
	var colTypes ColDecls
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
			colTypes = InputColDecls(child)
			strictInt = StrictIntArithCols(child)
			resolved = true
		}
		// A bare column reference needs no derived key — except a ROW FIELD
		// PATH, which parses as one and is not one: no stage emits a column
		// by that name, so the worker has to COMPUTE it, and an entry here is
		// how it learns that (buildAggInputProjection reads this map as the
		// planner's "this key is derived, and this is its type"). Without it
		// the key reached HashAggregate as a name and failed to resolve
		// (#568).
		if _, bare := node.(*plansql.ColRef); bare && !AstIsFieldPath(node, colTypes) {
			continue
		}
		if out == nil {
			out = make(map[string]parquet.TypeID)
		}
		d := DerivedGroupKeyDecl(key, node, child)
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
