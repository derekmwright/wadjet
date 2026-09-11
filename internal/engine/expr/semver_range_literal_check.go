// This file holds the PLAN-TIME half of `semver_satisfies`'s range refusal: a
// range written as a CONSTANT is read from the DECLARATION, before any row.
// Governed by ADR-0012 item 1 (#967).
package expr

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// RefuseInvalidSemverRangeLiterals refuses a `semver_satisfies` call whose
// RANGE argument is a constant this grammar does not know — before any row
// exists, and whatever the rows would have been.
//
// WHY THIS IS NOT A PER-ROW QUESTION. A range is not data. It is the second
// operand of a predicate and is almost always a literal the query's author
// typed, so its spelling is a property of the STATEMENT. PostgreSQL settles
// the analogy for every other malformed literal: `SELECT 'x'::int FROM t WHERE
// false` raises 22P02 there, because the coercion happens at parse analysis
// and does not wait for data. Folded per row instead,
// `SELECT semver_satisfies(v,'^^1.0') FROM pkgs WHERE id < 0` would answer
// zero rows and no error while the same typo over a reached row raised —
// whether a typo is an error would depend on the data. That is the defect
// #1018 B2 fixed for a TCP flag name, one family earlier.
//
// WHAT IS STILL PER ROW. Only a STRING LITERAL is folded. A range supplied by
// a COLUMN or by an expression is not knowable here and keeps the evaluator's
// per-row refusal — one refusal, two layers, never two rules, because both
// layers call ParseSemverRange. A NULL literal is a NULL operand rather than a
// misspelling and is left alone, exactly as a NULL flag NAME is.
//
// WHERE IT IS ASKED. At PLAN time by the BINDER
// (`physical.refuseInvalidSemverRanges`), which `Plan` AND `PlanDistributed`
// both reach through `auth.ValidateStatementColumns` before any stage exists,
// and again at COMPILE time (`compileFuncCallNamed`) as the BACKSTOP for the
// doors the binder does not see. Both layers and the reasoning behind the
// split are `RefuseUnknownTCPFlagNameLiterals`'s, whose doc comment carries
// the measurement: compilation is not one seam, because a DAG stage compiles
// its fragment only when a TASK RUNS, so a fold living there alone refuses in
// one process and answers zero rows on the DAG for every position whose stage
// receives no rows.
func RefuseInvalidSemverRangeLiterals(fc *plansql.FuncCallNode) error {
	if fc == nil {
		return nil
	}
	fn := strings.ToLower(strings.TrimSpace(fc.Name))
	if fn != "semver_satisfies" || len(fc.Args) != 2 {
		return nil
	}
	lit, ok := semverRangeLiteral(fc.Args[1])
	if !ok {
		return nil
	}
	_, err := ParseSemverRange(fn, lit)
	return err
}

// semverRangeLiteral unwraps a constant range. Anything else — a column, a
// call, a NULL, a number — answers false and leaves the call to the evaluator.
func semverRangeLiteral(n plansql.Node) (string, bool) {
	for {
		p, ok := n.(*plansql.ParenNode)
		if !ok || p.Inner == nil {
			break
		}
		n = p.Inner
	}
	lit, ok := n.(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitString {
		return "", false
	}
	return lit.Value, true
}
