package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// groupingErrSQLState is PostgreSQL's SQLSTATE for a GROUPING call whose
// arguments are not grouping expressions of the query level: 42803,
// grouping_error. Verified against PostgreSQL 17
// (finalize_grouping_exprs_walker, parse_agg.c).
const groupingErrSQLState = "42803"

// bareGroupingCall returns the call when expr IS a GROUPING(...) call
// (parentheses transparent), and nil when it merely contains one. The
// distinction decides the rewrite: a bare call projects its bitmask slot
// directly, while a nested one substitutes the slot into the surrounding
// expression. Projecting the slot for a nested call would DROP the rest of
// the expression, which is how `GROUPING(g) + 1` answered 0 (#804).
func bareGroupingCall(expr plansql.Node) *plansql.FuncCallNode {
	if expr == nil {
		return nil
	}
	fn, ok := plansql.Unparen(expr).(*plansql.FuncCallNode)
	if !ok || !strings.EqualFold(fn.Name, "grouping") {
		return nil
	}
	return fn
}

// groupingOutputName is the column name a GROUPING(...) SELECT item reports.
// PostgreSQL names an unaliased one `grouping` (its FigureColname rule for a
// function call), which the parser already puts in Alias; this only supplies
// the fallback for a call that somehow reached here unnamed.
func groupingOutputName(col plansql.SelectColumn) string {
	if col.Alias != "" {
		return col.Alias
	}
	return "grouping"
}

// checkGroupingArgs rejects a GROUPING call whose arguments are not GROUP BY
// terms of this query level, the way PostgreSQL does:
//
//	ERROR:  arguments to GROUPING must be grouping expressions of the
//	        associated query level                            (SQLSTATE 42803)
//
// Every argument must appear in the GROUP BY term list — which, for GROUPING
// SETS / ROLLUP / CUBE, is the deduped UNION of every set's terms
// (SelectInfo.GroupBy), not the terms of any one set. A column grouped in
// only some sets is a legal argument; that is the whole question GROUPING
// answers.
func checkGroupingArgs(args []string, info *plansql.SelectInfo) error {
	known := make(map[string]bool, len(info.GroupBy))
	for _, gb := range info.GroupBy {
		known[strings.ToLower(cleanExpr(gb))] = true
	}
	for _, a := range args {
		if !known[strings.ToLower(a)] {
			return sqlerr.New(groupingErrSQLState,
				"arguments to GROUPING must be grouping expressions of the associated query level: %q is not a GROUP BY term", a)
		}
	}
	return nil
}
