package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// refuseInvalidSemverRanges is the BINDER's half of `semver_satisfies`'s
// constant-range refusal, and it is the half that decides WHETHER THE QUERY IS
// REFUSED AT ALL.
//
// It is refuseUnknownFlagNames's twin and the reasoning is that function's,
// measured there: a fold that lives at COMPILATION alone is one seam on the
// single-process path — where `Plan` compiles the whole expression tree while
// it builds the physical plan — and is NOT one on the stage DAG, where a
// stage's fragment compiles its own expressions when a TASK RUNS. A position
// whose stage receives no rows would then never fold at all, so the same typo
// would be an error in one process and zero rows on three DAG arms.
//
// It is a SEPARATE function rather than a second call inside its twin because
// the two refusals are independent families with independent messages, and the
// walk is not duplicated logic: both ask the SAME `walkExpr`, which is what
// #1018 round 7 established after a hand-rolled second descent disagreed with
// it about where a window function's arguments live. Two collections of one
// walk cannot see different positions; two DIFFERENT descents can.
//
// IT ASKS NOTHING OF THE SCHEMA, so like its twin it is not gated on a closed
// scope: `SEMVER_SATISFIES(v,'^^1.0')` names no range whatever the FROM list
// turns out to be, and an open scope is a statement about COLUMNS.
//
// The compile-time call (`expr.compileFuncCallNamed`) remains the BACKSTOP for
// the doors this walk does not see — ADR-0031's DML predicate, which is not
// planned at all; a policy row filter; an entry point with no catalog. The
// coverage residuals are the ones `TestTCPFlagValidationDoors` already names,
// because the two refusals ride the same two layers.
func refuseInvalidSemverRanges(node plansql.Node) error {
	if node == nil {
		return nil
	}
	var calls []*plansql.FuncCallNode
	walkExpr(node, nil, nil, &calls)
	for _, fc := range calls {
		if err := expr.RefuseInvalidSemverRangeLiterals(fc); err != nil {
			return err
		}
	}
	return nil
}
