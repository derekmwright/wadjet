package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// respellWindowKeyExprs rewrites each materialized window-key EXPRESSION so
// its column references name what the window stage's input really carries.
//
// resolveWindowKeys is shared by both paths, so a key expression written over
// a derived table's or CTE's SELECT-list alias (`SUM(v * 2) OVER ()` above
// `SELECT c_i64 AS v`) is correct for the single-process pipeline, where the
// Project below the window is a real operator. On the DAG that Project emits
// no stage, so `v` names nothing: the fragment's pre-window projection
// evaluated it to NULL and the window wrote NULL in every row — #672's other
// half, and the ARGUMENT sibling of #658's PARTITION BY key.
//
// Only a reference the alias walk RESOLVES is rewritten; a spec whose
// expression cannot be re-parsed, or that names nothing derived, comes back
// exactly as it was.
func respellWindowKeyExprs(specs []ProjectExprSpec, child *logical.Node) []ProjectExprSpec {
	if len(specs) == 0 || child == nil {
		return specs
	}
	for i := range specs {
		ast, err := plansql.ParseExpression(specs[i].Expr)
		if err != nil {
			continue
		}
		if rewritten, changed := respellDerivedAliasRefs(ast, child); changed {
			specs[i].Expr = rewritten.String()
		}
	}
	return specs
}

// respellDerivedAliasRefs replaces every column reference naming a derived
// table's or CTE's SELECT-list RENAME with the source column the DAG's streams
// carry. Copy-on-write; a reference the alias walk does not resolve comes back
// exactly as it was.
//
// It resolves a rename only. A COMPUTED alias has no source column to point at
// — `derivedAliasSourceColumn` answers "" for one — and the expression that
// defines it is what respellAggInputExpr substitutes instead.
func respellDerivedAliasRefs(n plansql.Node, child *logical.Node) (plansql.Node, bool) {
	out, changed, complete := rewriteColRefs(n, func(ref *plansql.ColRef) (plansql.Node, bool) {
		src := derivedAliasSourceColumn(ref.String(), child)
		if src == "" && ref.Table == "" {
			src = derivedAliasSourceColumn(ref.Column, child)
		}
		if src == "" {
			return nil, false
		}
		return &plansql.ColRef{Column: cleanExpr(src)}, true
	})
	if !complete {
		// The walk met a node it does not rewrite — a subquery, an EXISTS, a
		// window call, a kind added since — so some references in this
		// expression were NOT considered. A PARTIAL respell is the worst of
		// the three outcomes: it looks resolved and is not. Decline the whole
		// rewrite and leave the expression exactly as written.
		return n, false
	}
	return out, changed
}

// respellAggInputExpr rewrites aggregate argument references to the columns
// emitted by the stage below, using resolveAggInputName per reference (#702).
// A rename becomes its source column; a computed alias becomes its defining
// expression, parenthesized to preserve association inside the larger AST.
// DAG-only: rewrite stage-spec text, never the logical node executed by the
// local pipeline, where the derived Project is a real operator (ADR-0025).
// See docs/internals/aggregate-argument-alias-substitution.md for the design.
func respellAggInputExpr(n plansql.Node, child *logical.Node) (plansql.Node, bool) {
	return respellAggInputExprAt(n, child, 0)
}

// aggRespellDepth bounds the recursion below. Derived-table chains are one or
// two deep in practice; the bound is there so a malformed plan cannot spin.
const aggRespellDepth = 8

func respellAggInputExprAt(n plansql.Node, child *logical.Node, depth int) (plansql.Node, bool) {
	if n == nil || child == nil || depth >= aggRespellDepth {
		return n, false
	}
	if !aggInputRespellable(child) {
		return n, false
	}
	out, changed, complete := rewriteColRefs(n, func(ref *plansql.ColRef) (plansql.Node, bool) {
		if resolved, expr, below, renamed := resolveAggInputName(ref.String(), child); renamed {
			if expr != nil {
				// The definition may name aliases of its OWN input:
				// `SELECT twice * 3 AS t FROM (SELECT id * 2 AS twice …)`
				// substitutes `twice * 3`, which still names `twice`.
				// resolveAggInputName returns at the first computed alias it
				// meets and cannot continue past it, so the substituted
				// subtree is respelled against the node that Project reads.
				inner := expr
				if r, ok := respellAggInputExprAt(expr, below, depth+1); ok {
					inner = r
				}
				return &plansql.ParenNode{Inner: inner}, true
			}
			if !strings.EqualFold(resolved, ref.String()) {
				return &plansql.ColRef{Column: cleanExpr(resolved)}, true
			}
			return nil, false
		}
		// A ROW FIELD PATH whose CONTAINER is the rename: `rw.b` over
		// `SELECT c_row AS rw`. The whole spelling names no column, and the
		// field `b` is not a column either — only the QUALIFIER is a name to
		// resolve, and resolving it gives the path the stage's stream really
		// carries. Without this the reference reached the worker as `rw.b`,
		// which nothing there can look up.
		//
		// It runs only AFTER the whole spelling has failed, which is
		// ADR-0022 §1's order: a derived table that emits a column named
		// `rw.b`, or whose own alias is `rw`, is resolved above and never
		// reaches here. A qualifier that is a FROM alias rather than a
		// rename resolves to nothing and is left exactly as written.
		if ref.Table == "" {
			return nil, false
		}
		qual, qexpr, _, qrenamed := resolveAggInputName(ref.Table, child)
		if !qrenamed || qexpr != nil || strings.EqualFold(qual, ref.Table) {
			return nil, false
		}
		return &plansql.ColRef{Table: cleanExpr(qual), Column: ref.Column}, true
	})
	if !complete {
		// Same rule as above, and here it has teeth: an argument carrying a
		// subquery or an EXISTS has references this walk never saw, so a
		// partial rewrite would ship a spelling that resolves for some of them
		// and not others. Declining leaves the original text, which
		// assertAggregateInputsResolve then judges — and refuses, with the
		// sentinel, if it names something no stage emits.
		return n, false
	}
	return out, changed
}

// aggInputRespellable permits source respelling only when a Scan is reached
// through Project and Filter alone (#702): no intervening stage materializes
// the derived names. Join/Distinct materialize aliases; Aggregate/SetOp/Window
// publish their own column sets. Sort/Limit may receive an OpProject in a
// later pass, so materialization cannot be decided here (ADR-0025).
// All other shapes retain their behavior; assertAggregateInputsResolve makes
// unresolved residual inputs loud rather than silently reading missing names.
// See docs/internals/aggregate-input-respelling-boundary.md for the design.
func aggInputRespellable(n *logical.Node) bool {
	for depth := 0; n != nil && depth < aggRespellDepth; depth++ {
		switch n.Type {
		case logical.NodeScan:
			return true
		case logical.NodeProject, logical.NodeFilter:
			if len(n.Children) != 1 {
				return false
			}
			n = n.Children[0]
		default:
			return false
		}
	}
	return false
}

// aggInputAliasIsAggregateGroupKey matches a derived alias's defining expression
// against the aggregate's GROUP BY list, where it is emitted under expression
// text and can be read as a bare name. The mere presence of an aggregate is
// insufficient: arithmetic over its output must be computed, not read by name.
// DISTINCT groups by the whole projected expression and therefore qualifies;
// join/ordering materialization uses the alias instead of expression text.
// See docs/internals/aggregate-input-group-key-materialization.md for the design.
func aggInputAliasIsAggregateGroupKey(n *logical.Node, exprText string) (string, bool) {
	exprText = strings.TrimSpace(exprText)
	if exprText == "" {
		return "", false
	}
	for depth := 0; n != nil && depth < aggRespellDepth; depth++ {
		switch n.Type {
		case logical.NodeAggregate:
			for _, g := range n.GroupBy {
				// The match is case-INSENSITIVE, because column resolution is,
				// but the name returned is the PRODUCER'S — the group key as
				// the aggregate emits it, not the SELECT item's case-preserved
				// text. `SELECT SUM(v) FROM (SELECT A * 2 AS v … GROUP BY
				// a * 2)` matched on `A * 2` and shipped that spelling, and the
				// operator looks its input up by name: `aggregate input
				// "A * 2" is not a column of its input (input has: v, a * 2)`.
				if strings.EqualFold(strings.TrimSpace(g), exprText) {
					return strings.TrimSpace(g), true
				}
			}
			return "", false
		case logical.NodeProject, logical.NodeFilter:
			if len(n.Children) != 1 {
				return "", false
			}
			n = n.Children[0]
		default:
			return "", false
		}
	}
	return "", false
}

// aggInputAliasIsMaterializedUnderItsName reports whether the producer between
// the aggregate and its source materializes the derived alias under the ALIAS
// — which is what attachScanSelectProjections' alias-naming OpProject does on a
// join arm's fragment, and on a sort, a LIMIT or a union.
//
// An AGGREGATE is deliberately not in the list. It may publish the alias
// (absorbAggregateOutputProjection puts the SELECT list on a collapsing
// producer) or it may not, and where it does not the alias names nothing;
// computing the expression works in both cases, because the aggregate's own
// outputs are what the expression reads. So an aggregate below falls through to
// the compute answer, which is what ff7c3f19 did for every one of these shapes.
//
// A WINDOW was in the list and does not belong there, which is the other half
// of #877/#878. The alias-naming OpProject a window fragment can carry comes
// from attachScanSelectProjections, and that pass attaches the OUTERMOST SELECT
// list and returns at its first aggregate item — so whenever an AGGREGATE is
// the consumer asking this question, the answer is provably no: nothing put the
// alias on the window's fragment, and reading it gave NULL on every row. It
// stops the walk rather than being deleted from the case list, because a
// producer below a window is not the aggregate's producer either.
func aggInputAliasIsMaterializedUnderItsName(n *logical.Node) bool {
	for depth := 0; n != nil && depth < aggRespellDepth; depth++ {
		switch n.Type {
		case logical.NodeWindow:
			return false
		case logical.NodeJoin, logical.NodeSort, logical.NodeLimit,
			logical.NodeDistinct:
			return true
		case logical.NodeProject, logical.NodeFilter:
			if len(n.Children) != 1 {
				return false
			}
			n = n.Children[0]
		default:
			return false
		}
	}
	return false
}

// respellWindowSlotAliasRefs rewrites derived/CTE SELECT aliases resolving to
// window output slots to those slots (#877, #878; ADR-0025).
// Only the reserved window-output family qualifies: users cannot store or
// alias a column there (plansql.RefuseReservedSlotName). Other resolutions
// belong to the preceding alias rules.
// DAG-only: rewrite stage-spec text, never the local engine's logical node.
// See docs/internals/window-output-slot-alias-resolution.md for the design.
func respellWindowSlotAliasRefs(n plansql.Node, child *logical.Node) (plansql.Node, bool) {
	if n == nil || child == nil {
		return n, false
	}
	out, changed, complete := rewriteColRefs(n, func(ref *plansql.ColRef) (plansql.Node, bool) {
		resolved, expr, _, renamed := resolveAggInputName(ref.String(), child)
		if !renamed || expr != nil {
			return nil, false
		}
		if plansql.ReservedSlotFamily(resolved) != string(plansql.SlotWindowOutput) {
			return nil, false
		}
		return &plansql.ColRef{Column: cleanExpr(resolved)}, true
	})
	if !complete {
		// Same rule as respellDerivedAliasRefs: a walk that met a node kind it
		// does not rewrite has NOT considered every reference, and a partial
		// respell looks resolved without being it.
		return n, false
	}
	return out, changed
}
