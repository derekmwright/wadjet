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

// respellAggInputExpr rewrites an aggregate's ARGUMENT expression so that every
// column reference in it names what the stage BELOW the aggregate really
// emits.
//
// walkStages emits no stage for an ordinary Project, so a derived table's
// SELECT list never happens on the DAG (ADR-0025). The aggregate's argument is
// shipped to the worker as TEXT and compiled there against the batch the stage
// hands it — which carries the SCAN's columns, not the derived table's names.
// `SUM(CASE WHEN s = 'x' THEN twice ELSE 0 END)` over
// `(SELECT s, id * 2 AS twice FROM t)` therefore read `twice` off a batch that
// has no such column, `expr.ColRef.Eval` answered nil for every row, and the
// SUM came back as the total of the CASE's ELSE branch — 0 where PostgreSQL
// answers 2. It is TPC-H Q08's exact shape and it is type-independent: a plain
// rename triggers it too, and a rename that SHADOWS a base column answered a
// different wrong number rather than a zero (#702).
//
// resolveAggInputName already does this for an argument that IS a name; this
// is the same resolution applied one level down, to each reference inside an
// argument that is an EXPRESSION. Both outcomes it can report are used:
//
//	a RENAME       — the reference becomes the source column;
//	a COMPUTED     — the reference becomes the expression that defines it,
//	  alias          PARENTHESIZED, because the definition is substituted into
//	                 a larger expression and `id * 2` spliced bare into `x * 3`
//	                 would re-associate.
//
// The single-process pipeline runs that Project as a real operator, so it is
// already right and this rewrite is DAG-only: it is applied to the stage spec's
// text, never to the logical node the local engine executes.
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

// aggInputRespellable reports whether the derived names between the aggregate
// and its producer are ones NO stage materializes — the only condition under
// which respelling them to their sources is right.
//
// The question is never "does this name exist below" but "is this name
// MATERIALIZED HERE", and it has a different answer per producer:
//
//   - A JOIN materializes it. attachScanSelectProjections puts an alias-naming
//     OpProject on the arm's fragment, so `x.v` really IS a column of the
//     join's output and the source spelling is the one that is not.
//     Respelling took a CORRECT 25.50 to 0.00 on `SUM(CASE WHEN x.s = '1.50'
//     THEN x.v ELSE 0 END)` over `(SELECT s, a * 2 AS v FROM t) x JOIN t y`,
//     because a self-join qualifies both sides' `a`.
//   - A DISTINCT materializes it. rewriteDistinctAsGroupBy lowers it to an
//     aggregate whose OUTPUT is the projection's names, so `v` is emitted and
//     `a` is gone: respelling turned a LOUD failure into a silent 0 on
//     `SUM(CASE WHEN v > 0 THEN v ELSE 0 END)` over
//     `(SELECT DISTINCT a * 2 AS v FROM t)`, where PostgreSQL answers 29.50.
//   - An AGGREGATE, a SET OPERATION and a WINDOW each emit a new column set of
//     their own, for the same reason.
//   - A SORT and a LIMIT are pass-throughs, but ADR-0025 gave both an
//     OpProject slot, so whether the alias is materialized on them is decided
//     by a LATER pass and is not knowable here.
//
// Enumerating the materializing kinds was the first attempt and it was wrong
// twice — once per kind nobody had thought of. The rule is stated POSITIVELY
// instead: respell only where the walk reaches a SCAN through Project and
// Filter alone, which is exactly the shapes #702 names and exactly the ones
// where walkStages provably emits no stage for the Project. Everything else
// keeps today's behaviour, and assertAggregateInputsResolve is what makes a
// residual there loud rather than silent.
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
