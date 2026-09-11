// This file holds the PLAN-TIME half of the TCP flag family's name refusal:
// a constant flag-name list is folded from the DECLARATION, before any row.
// Governed by ADR-0012 item 1 (#1018 round 5, B2).
package expr

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// RefuseUnknownTCPFlagNameLiterals refuses a call in the TCP flag family whose
// flag-NAME arguments are CONSTANTS that name no flag — before any row exists,
// and whatever the rows would have been.
//
// WHY THIS IS NOT A PER-ROW QUESTION. A flag name is a MASK OPERAND, and its
// spelling is a property of the QUERY, not of the data. PostgreSQL settles the
// analogy for the arithmetic this family is named for:
//
//	SELECT NULL::bigint & 'x'::bigint                      ERROR 22P02
//	SELECT 'x'::int FROM (VALUES (1)) t WHERE false        ERROR 22P02
//
// The coercion happens at parse analysis and does not wait for data, so
// neither a NULL operand nor an empty row set excuses it. Here the fold was
// per row, so `SELECT tcp_flags_has_all(f8,'BOGUS') FROM tcpflow WHERE id < 0`
// returned zero rows and no error on every arm and both wire formats, while
// the same typo over a reached row was 22023 — whether a typo is an error
// depended on the data (#1018 round 5, B2).
//
// WHAT IS STILL PER ROW. Only a STRING LITERAL is folded. A name supplied by a
// column or by an expression is not knowable here, and that call keeps the
// evaluator's per-row refusal exactly as before — one refusal, two layers,
// never two rules. A NULL literal is a NULL mask operand and is not a
// misspelling: `NULL & NULL` is NULL, so it is left alone.
//
// WHERE IT IS ASKED, AND WHICH LAYER DECIDES. At PLAN time by the BINDER —
// physical.refuseUnknownFlagNames, over every expression position of every
// query block, which Plan AND PlanDistributed both reach through
// auth.ValidateStatementColumns before any stage exists. That is the layer that
// decides whether a query is refused at all, because COMPILATION is not one
// seam: the single-process path compiles the whole expression tree while it
// PLANS, and a stage of the DAG compiles its own fragment only when a TASK
// RUNS. A fold that lived at compilation alone therefore refused a misspelling
// in one process and answered zero rows on the DAG for every position whose
// stage received no rows — HAVING, an ORDER BY key, a set-operation arm, a
// projection above a GROUP BY, a subquery body (#1018 round 6, B1).
//
// And again at COMPILE time (compileFuncCallNamed), which is now the BACKSTOP
// for the doors the binder does not see: ADR-0031's DML predicate, which is not
// planned at all; a recursive CTE's body, which the binder registers open and
// does not validate; an expression it cannot re-parse (an ORDER BY item, a
// subquery body); a policy row filter; and an entry point with no catalog,
// where physical.ValidateColumnsUnderPolicy declines. One function, so the two
// layers cannot disagree.
func RefuseUnknownTCPFlagNameLiterals(fc *plansql.FuncCallNode) error {
	if fc == nil {
		return nil
	}
	fn := strings.ToLower(strings.TrimSpace(fc.Name))
	switch fn {
	case "tcp_flags_has_all", "tcp_flags_has_any", "tcp_flags_has_none":
		// flags, then one or more names.
		if len(fc.Args) < 2 {
			return errTCPFlagListEmpty(fn)
		}
		return refuseTCPFlagNameArgs(fn, fc.Args[1:])
	case "has_tcp_flag":
		// flags, then exactly one name.
		if len(fc.Args) < 2 {
			return errTCPFlagListEmpty(fn)
		}
		return refuseTCPFlagNameArgs(fn, fc.Args[1:2])
	case "tcp_flag_mask":
		// names only.
		if len(fc.Args) == 0 {
			return errTCPFlagListEmpty(fn)
		}
		return refuseTCPFlagNameArgs(fn, fc.Args)
	case "tcp_flags_from_string":
		// ONE argument holding a comma-separated list, with its own
		// empty-element rule and its own "the empty string is no names at
		// all" concession — the same reading fnTCPFlagsFromString does, so
		// the two layers cannot disagree about which strings are lists.
		if len(fc.Args) != 1 {
			return nil
		}
		lit, ok := tcpFlagNameLiteral(fc.Args[0])
		if !ok {
			return nil
		}
		if strings.TrimSpace(lit) == "" {
			return nil
		}
		parts := strings.Split(lit, ",")
		for i, part := range parts {
			if strings.TrimSpace(part) == "" {
				return errEmptyTCPFlagNameAt(fn, i+1, lit)
			}
		}
		if _, bad, ok := TCPFlagMask(parts); !ok {
			return errUnknownTCPFlagName(fn, strings.TrimSpace(bad))
		}
		return nil
	}
	return nil
}

// refuseTCPFlagNameArgs folds the name arguments that are constants. One
// argument the query does not spell as a literal takes the whole refusal away:
// the fold would then be over a list this layer cannot see, and a refusal made
// on a guess is the false positive the binder's standing contract forbids.
func refuseTCPFlagNameArgs(fn string, args []plansql.Node) error {
	names := make([]string, 0, len(args))
	for _, a := range args {
		lit, ok := tcpFlagNameLiteral(a)
		if !ok {
			return nil
		}
		names = append(names, lit)
	}
	if _, bad, ok := TCPFlagMask(names); !ok {
		if len(names) == 0 {
			return errTCPFlagListEmpty(fn)
		}
		return errUnknownTCPFlagName(fn, bad)
	}
	return nil
}

// tcpFlagNameLiteral unwraps a constant flag NAME. Anything else — a column, a
// call, a NULL, a number — answers false and leaves the call to the evaluator.
func tcpFlagNameLiteral(n plansql.Node) (string, bool) {
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
