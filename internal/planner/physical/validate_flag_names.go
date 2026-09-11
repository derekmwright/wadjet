package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// refuseUnknownFlagNames is the BINDER's half of the TCP flag family's
// constant-name refusal, and it is the half that decides WHETHER THE QUERY IS
// REFUSED AT ALL.
//
// WHY IT CANNOT LIVE AT COMPILATION ALONE. A flag NAME is a mask operand whose
// spelling is a property of the QUERY; PostgreSQL raises for `'x'::int` under
// `WHERE false` because the coercion happens at parse analysis and does not
// wait for data. Folding the name in expr.compileFuncCallNamed gives that on
// the single-process path, where Plan compiles the whole expression tree while
// it builds the physical plan — and NOT on the stage DAG, where a stage's
// fragment compiles its own expressions WHEN A TASK RUNS. A position whose
// stage receives no rows is then never folded at all, so `… WHERE id < 0 GROUP
// BY id HAVING TCP_FLAGS_HAS_ALL(MIN(f),'BOGUS')` raised 22023 in one process
// and answered zero rows on three DAG arms (#1018 round 6, B1). Four positions
// behaved that way — HAVING, an ORDER BY key, a set-operation arm and a
// projection above a GROUP BY — while a SELECT-list projection and a WHERE
// predicate, which the coordinator folds on both paths, did not. Whether a typo
// was an error depended on the data AND on the plan shape.
//
// This runs from the binder, which BOTH Plan and PlanDistributed reach through
// auth.ValidateStatementColumns before any stage exists, so the refusal is one
// answer for every arm.
//
// IT ASKS NOTHING OF THE SCHEMA, so unlike checkLiteralTypes it is not gated on
// a closed scope: `TCP_FLAG_MASK('BOGUS')` names no flag whatever the FROM list
// turns out to be, and an open scope is a statement about COLUMNS.
//
// WHAT IS STILL FOLDED AT COMPILE TIME. expr.compileFuncCallNamed keeps the
// same call as the BACKSTOP for the doors this walk does not see: the DML
// predicate, which is not planned at all (ADR-0031) and reaches the engine as a
// compiled expression; a recursive CTE's body, which the binder registers open
// and does not validate; a window function's raw OVER terms where they do not
// parse; an expression the binder cannot re-parse (an ORDER BY item, a subquery
// body); a policy row filter; and any entry point with no catalog, where
// ValidateColumnsUnderPolicy declines. ONE function answers for both
// (expr.RefuseUnknownTCPFlagNameLiterals over expr.TCPFlagMask), so the two
// layers cannot disagree about which names exist.
func refuseUnknownFlagNames(node plansql.Node) error {
	if node == nil {
		return nil
	}
	// A WINDOW call is its own node kind and walkExpr stops at it (its OVER
	// terms are a different namespace, which is why the binder's name
	// resolution skips them). Its ARGUMENTS and its frame terms are still
	// expressions this statement wrote, and a misspelling in one is a
	// misspelling: `SUM(TCP_FLAG_MASK('BOGUS')) OVER ()` is the same typo as
	// the same call without the OVER.
	if w, ok := node.(*plansql.WindowFuncNode); ok {
		if err := refuseUnknownFlagNames(w.Func); err != nil {
			return err
		}
		for _, p := range w.PartitionBy {
			if err := refuseUnknownFlagNames(p); err != nil {
				return err
			}
		}
		for _, o := range w.OrderBy {
			if err := refuseUnknownFlagNames(o.Expr); err != nil {
				return err
			}
		}
		return nil
	}
	var calls []*plansql.FuncCallNode
	walkExpr(node, nil, nil, &calls)
	for _, fc := range calls {
		if err := expr.RefuseUnknownTCPFlagNameLiterals(fc); err != nil {
			return err
		}
	}
	return nil
}
