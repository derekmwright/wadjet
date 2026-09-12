package sql

import "strings"

// A SCALAR SUBQUERY WITH NO FROM CLAUSE IS ITS SELECT EXPRESSION (#1044).
//
// `(SELECT u.x)` produces exactly one row whose one column is `u.x` evaluated
// in the ENCLOSING scope, which is what PostgreSQL answers and what its
// planner builds — a Result node under the SubLink, with the outer reference
// as a parameter. This engine ran the block as a STATEMENT instead: `u` names
// no relation a FROM-less SELECT provides, `expr.ResolveColumnRef` stripped
// the qualifier and found no bare `x` either, and every row read the EMPTY BOX
// under a text declaration. The filing's own shape,
//
//	SELECT SUM(a.v), SUM(b.v)
//	FROM (SELECT (SELECT u.x) AS v FROM (SELECT id AS x FROM users) u) a
//	CROSS JOIN (SELECT (SELECT u.x) AS v FROM (SELECT visits AS x FROM users) u) b
//
// answered "" and "" under OID 701 for PostgreSQL's 18 (bigint) and 1026
// (numeric). The reference does not have to be a derived table's output alias
// to reach it: `SELECT (SELECT u.id) FROM users u` answered the empty box too.
//
// The rewrite is done at the PARSER, at the one site that builds a
// SubqueryNode for a subquery in an expression position, so every reader of
// the tree — the correlation classifier, the binder, the logical builder, the
// declaration walk, the DAG's stage emission and the per-row re-run — sees the
// expression rather than a subquery it would each have to unfold for itself.
// That is also what makes the DECLARATION right: the item is typed like any
// other expression over the outer row, so `(SELECT u.x)` over an INT32 column
// declares int4 and its SUM declares bigint, instead of falling to the string
// fallback and summing on the float rung.
//
// An OPERATOR expression is wrapped in a ParenNode and nothing else is.
// SelectColumn.Expr is the AST's own rendering and BinaryOp.String() does not
// parenthesise, so an unwrapped `(SELECT u.x + 1) * 2` would render as
// `u.x + 1 * 2` and mean something else if that text were ever re-parsed — the
// original spelling had the parentheses too. A COLUMN REFERENCE gets none,
// and that is not cosmetic: `(u.x)` and `u.x` are one expression to the
// compiler but not to the stage emission, which reads a projection's node to
// decide whether a stage passes a column through. Wrapped, the filing's own
// shape routed to the coordinator-local pipeline on three DAG arms and its
// aggregate spelling reached the worker as a schemaless batch (#277); the same
// query with the subquery spelled out — cell 00 of the two-path gate — was
// fine, which is what named the wrapper.

// fromlessScalarExpr answers the expression a scalar subquery IS, for a
// subquery that has no FROM clause and nothing that can suppress, duplicate or
// reorder the single row such a SELECT produces. The second result is false
// for every other subquery, including one this package cannot parse — which
// keeps the caller's existing behaviour, since the same text is parsed again
// downstream and raises there.
//
// The excluded clauses are excluded because each one changes the ANSWER and
// not only the shape, and PostgreSQL was measured on every one of them:
//
//	(SELECT u.id WHERE 1=0)   NULL — an empty result is the scalar NULL
//	(SELECT u.id LIMIT 0)     NULL
//	(SELECT u.id OFFSET 1)    NULL
//	(SELECT u.id, u.visits)   42601, a scalar subquery has ONE column
//
// DISTINCT, ORDER BY, GROUP BY, HAVING and QUALIFY are excluded for the same
// reason in the other direction: over the single row a FROM-less SELECT yields
// they are provably no-ops, but "provably" is a claim about clauses this
// function would have to interpret, and declining them costs only the shapes
// nobody writes.
//
// An AGGREGATE or a WINDOW CALL in the item is excluded for a different reason,
// and the measurement is the reason. PostgreSQL decides which query an
// aggregate belongs to by whether its ARGUMENT names the enclosing one, so
// `SELECT (SELECT MAX(u.id)) FROM users u` is the ENCLOSING query's aggregate
// and answers ONE row, 3, while `SELECT (SELECT MAX(1)) FROM users u` and
// `SELECT (SELECT COUNT(*)) FROM users u` are the BLOCK's own, over the single
// row it produces, and answer 1 for every outer row. Substituting the
// expression would make it the enclosing query's UNCONDITIONALLY, so the
// second pair would turn from three rows into one. That is a rule about
// aggregate LEVELS rather than about one item, and this rewrite does not
// implement it. A window call splits the same way — `(SELECT SUM(u.id) OVER
// ())` is 1,2,3 and `(SELECT COUNT(*) OVER ())` is 1,1,1 — and is where
// #1045's refusal lives.
func fromlessScalarExpr(subSQL string) (Node, bool) {
	parsed, err := Parse(subSQL)
	if err != nil || parsed == nil || parsed.SelectInfo == nil {
		return nil, false
	}
	info := parsed.SelectInfo
	if info.Union != nil || len(info.CTEs) > 0 {
		return nil, false
	}
	if len(info.Tables) > 0 || len(info.Joins) > 0 {
		return nil, false
	}
	if info.WhereExpr != nil || strings.TrimSpace(info.Where) != "" {
		return nil, false
	}
	if len(info.GroupBy) > 0 || len(info.GroupingSets) > 0 {
		return nil, false
	}
	if info.HavingExpr != nil || strings.TrimSpace(info.Having) != "" {
		return nil, false
	}
	if info.QualifyExpr != nil || strings.TrimSpace(info.Qualify) != "" {
		return nil, false
	}
	if len(info.OrderBy) > 0 || info.Limit != "" || info.Offset != "" || info.Distinct {
		return nil, false
	}
	if len(info.Columns) != 1 {
		return nil, false
	}
	col := info.Columns[0]
	if col.Star || col.ASTExpr == nil || col.IsAgg || col.IsWindow {
		return nil, false
	}
	if len(FindAllAggregates(col.ASTExpr)) > 0 || len(FindAllWindowFuncs(col.ASTExpr)) > 0 {
		return nil, false
	}
	if needsParens(col.ASTExpr) {
		return &ParenNode{Inner: col.ASTExpr}, true
	}
	return col.ASTExpr, true
}

// needsParens reports whether an expression's own rendering is ambiguous once
// it is placed inside a larger one — an OPERATOR expression, whose String()
// emits its operands with no brackets of its own. Everything else (a column, a
// literal, a function call, a CAST, a CASE, an array or tuple, a subquery) is
// self-delimiting and takes no wrapper.
func needsParens(n Node) bool {
	switch n.(type) {
	case *BinaryOp, *UnaryOp, *CmpExpr, *AndNode, *OrNode, *NotNode,
		*IsExpr, *LikeExpr, *BetweenExpr, *InExpr, *AnyAllExpr:
		return true
	}
	return false
}
