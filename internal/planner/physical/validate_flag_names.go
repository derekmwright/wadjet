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
// compiled expression. Recursive CTEs are registered open, then their bodies
// are validated by the existing block walk, including both UNION arms.
// Compilation is only a backstop WHEN REACHED: a policy row filter on an empty
// DAG may never compile; catalog-less table-less plans route locally; syntax
// the binder cannot re-parse may be refused by the logical builder instead.
// TestTCPFlagValidationDoors pins these distinct doors. ONE function answers for both
// (expr.RefuseUnknownTCPFlagNameLiterals over expr.TCPFlagMask), so the two
// layers cannot disagree about which names exist.
func refuseUnknownFlagNames(node plansql.Node) error {
	if node == nil {
		return nil
	}
	// ONE walk. A window call used to be handled by a copy of the descent
	// written here, and that copy was TOP-LEVEL ONLY: it saw
	// `SUM(TCP_FLAG_MASK('BOGUS')) OVER ()` and missed
	// `1 + SUM(TCP_FLAG_MASK('BOGUS')) OVER ()`, because the wrapping
	// arithmetic put the window node one level down where walkExpr stopped.
	// walkExpr descends through a WindowFuncNode itself now, so the two can no
	// longer disagree about which positions exist (#1018 round 7, B1).
	var calls []*plansql.FuncCallNode
	walkExpr(node, nil, nil, &calls)
	for _, fc := range calls {
		if err := expr.RefuseUnknownTCPFlagNameLiterals(fc); err != nil {
			return err
		}
	}
	return nil
}
