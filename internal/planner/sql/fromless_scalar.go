package sql

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A SCALAR SUBQUERY WITH NO FROM CLAUSE IS ITS SELECT EXPRESSION, AND THE
// ENCLOSING BLOCK HAS TO BE THE ONE THAT SUPPLIES THE ROW (#1044).
//
// `(SELECT u.x)` produces one row whose one column is `u.x` evaluated in the
// ENCLOSING scope — PostgreSQL plans it as a Result node under the SubLink
// with the outer reference as a parameter. This engine ran the block as a
// STATEMENT, where `u` names no relation a FROM-less SELECT provides:
// `expr.ResolveColumnRef` stripped the qualifier, found no bare `x` either,
// and every row read the EMPTY BOX under a text declaration.
//
// **The rewrite is a SCOPE decision, so it is made where the scope is known.**
// The first form of this pass ran at the PARSER, at the site that builds a
// SubqueryNode, and a parser standing on `(SELECT u.id)` cannot see which
// block will supply `u`. Written there it also fired inside a subquery that
// HAS a FROM clause — and such a block's text is REBUILT by the per-row re-run
// (plansql.RebuildSQL), so a bare `u.id` left in its SELECT list survived into
// the rebuilt statement, where the qualifier strip bound it to the INNER
// relation's own `id`. Nine shapes that had been refused loudly by ADR-0021
// §1c's dangling guard — `(SELECT (SELECT u.id) FROM c2users x WHERE x.id=1)`
// and its IN / HAVING / LATERAL / CTE / nested spellings — started answering
// one constant for every outer row instead (round-2 review, B1).
//
// This pass runs AFTER the block is parsed, so the block's own FROM list is
// there to ask, and it rewrites only where the reference RESOLVES HERE:
//
//   - a QUALIFIED reference whose qualifier is one of this block's own FROM
//     identifiers (`sourceIdentifier`: the alias where there is one, else the
//     name) — `SELECT (SELECT u.x) … FROM (SELECT id AS x FROM t) u`;
//   - an UNQUALIFIED reference, when this block has a FROM item at all: SQL
//     scopes innermost-first and a FROM-less subquery has no scope of its own,
//     so the innermost block that can supply the name is this one;
//   - a subquery with no column reference at all (`(SELECT 1)`).
//
// Everything else keeps its SubqueryNode, and with it the refusal §1c already
// raises. That is also why the rewrite is safe for a DERIVED TABLE's or a
// CTE's body — those bodies are not rebuilt, and their own FROM is what the
// reference names — while it declines inside a correlated subquery's body,
// whose text is.
//
// Running after the parse is what keeps the PUBLISHED NAME right as well.
// `SelectColumn.PublishedName` is stamped on the item AS WRITTEN, and
// PostgreSQL names a scalar subquery's column after the subquery's own target
// list, alias included: `SELECT (SELECT 1 AS zzz) FROM u` publishes `zzz`. The
// parser-time form returned the inner expression and dropped the alias, so
// that name became `?column?` (round-2 review, B2). Here the stamp is already
// on the item and the rewrite leaves it alone.

// unfoldFromlessScalars rewrites every FROM-less scalar subquery in one parsed
// block whose references resolve in THAT block, and recurses through the
// block's set-operation arms. It is called once per block, from parseSelect.
func unfoldFromlessScalars(info *SelectInfo) {
	if info == nil {
		return
	}
	if info.Union != nil {
		unfoldFromlessScalars(info.Union.Left)
		unfoldFromlessScalars(info.Union.Right)
		return
	}
	scope := blockScopeIdents(info)
	hasFrom := len(info.Tables) > 0 || len(info.Joins) > 0
	rw := func(n Node) Node { return unfoldIn(n, scope, hasFrom) }

	for i := range info.Columns {
		col := &info.Columns[i]
		if col.ASTExpr != nil {
			before := col.ASTExpr.String()
			col.ASTExpr = rw(col.ASTExpr)
			if col.ASTExpr.String() != before {
				// The item's own rendering is its text, and an item that is
				// now a plain COLUMN REFERENCE says so — parseSelectColumn
				// stamps ColumnRef/TableRef for an item written that way, and
				// the stage emission reads them to decide whether a stage
				// passes the column through. Without them the filing's own
				// shape reached the worker as a schemaless batch (#277).
				//
				// The published NAME is not touched: it is stamped on the
				// item AS WRITTEN, which is what keeps `(SELECT 1 AS zzz)`
				// publishing `zzz` (round-2 review, B2).
				col.Expr = col.ASTExpr.String()
				if c, ok := col.ASTExpr.(*ColRef); ok && !col.Star {
					col.ColumnRef, col.TableRef = c.Column, c.Table
				}
			}
		}
		// A WINDOW item's arguments are a THIRD place the same expression
		// lives: WindowSpec is built from the node at parse time and is what
		// the window planner reads, so rewriting only ASTExpr left
		// `SUM((SELECT CAST(3 AS INT))) OVER ()` declaring text
		// (pgwire.TestScalarSubqueryAggregateMatrix's /window cells).
		if col.IsWindow && col.WindowSpec != nil {
			if wfn, ok := col.ASTExpr.(*WindowFuncNode); ok {
				col.WindowSpec = windowSpecFromNode(wfn, col.WindowSpec.Alias)
			}
		}
		// An AGGREGATE item's argument is a second tree, and the one the
		// aggregate planner reads. It is rewritten whether or not the item
		// also carries an ASTExpr, because a parsed aggregate carries its
		// argument there and `SUM((SELECT u.x))` is the filing's own
		// aggregated spelling.
		if col.AggArgExpr != nil {
			before := col.AggArgExpr.String()
			col.AggArgExpr = rw(col.AggArgExpr)
			if col.AggArgExpr.String() != before {
				col.AggArg = col.AggArgExpr.String()
				if col.IsAgg && col.AggFunc != "" {
					col.Expr = col.AggFunc + "(" + col.AggArg + ")"
				}
			}
		}
		for j := range col.AggArgs {
			col.AggArgs[j] = rw(col.AggArgs[j])
		}
	}
	if info.WhereExpr != nil {
		before := info.WhereExpr.String()
		info.WhereExpr = rw(info.WhereExpr)
		if info.WhereExpr.String() != before {
			info.Where = info.WhereExpr.String()
		}
	}
	if info.HavingExpr != nil {
		before := info.HavingExpr.String()
		info.HavingExpr = rw(info.HavingExpr)
		if info.HavingExpr.String() != before {
			info.Having = info.HavingExpr.String()
		}
	}
	for i := range info.GroupByExprs {
		if info.GroupByExprs[i] == nil {
			continue
		}
		before := info.GroupByExprs[i].String()
		rewritten := rw(info.GroupByExprs[i])
		// A GROUP BY term reads a bare numeric literal as a select-list
		// POSITION exactly as an ORDER BY term does, and the consequence is
		// worse: `SELECT visits FROM t GROUP BY (SELECT 1)` passed the
		// ungrouped-column validator as "group by item #1" and projected a
		// fabricated NULL row where PostgreSQL 17.11 and main both raise
		// 42803 (round-3 review, B2). The decline is the ORDER BY one, in the
		// clause its own comment always claimed.
		if isBareNumericLit(rewritten) && !isBareNumericLit(info.GroupByExprs[i]) {
			continue
		}
		info.GroupByExprs[i] = rewritten
		if info.GroupByExprs[i].String() != before && i < len(info.GroupBy) {
			info.GroupBy[i] = info.GroupByExprs[i].String()
		}
	}
	for i := range info.OrderBy {
		if info.OrderBy[i].Expr == nil {
			continue
		}
		before := info.OrderBy[i].Expr.String()
		rewritten := rw(info.OrderBy[i].Expr)
		// AN ORDER BY TERM THAT WOULD BECOME A BARE NUMERIC LITERAL KEEPS ITS
		// SUBQUERY. PostgreSQL reads only an integer literal WRITTEN IN THE
		// CLAUSE as a select-list position, never one a subquery evaluates to:
		// `ORDER BY (SELECT 1)` is a constant sort and answers the table's own
		// order. Rewritten to `1` — and this pass runs AFTER
		// resolvePositionalRefs, so nothing resolves it again — the term
		// reaches the logical planner as an ordinal that names no item, and
		// ten shapes main answers exactly as PostgreSQL were refused 42P10
		// (round-2 review, B1). A constant sort is what the subquery is
		// either way, so declining costs the shape nothing.
		if isBareNumericLit(rewritten) && !isBareNumericLit(info.OrderBy[i].Expr) {
			continue
		}
		info.OrderBy[i].Expr = rewritten
		if info.OrderBy[i].Expr.String() != before {
			info.OrderBy[i].Column = info.OrderBy[i].Expr.String()
		}
	}
}

// blockScopeIdents is the set of identifiers this block's FROM clause answers
// to — an alias where the item has one, else its own name, which is the rule
// collectInnerTables states.
func blockScopeIdents(info *SelectInfo) map[string]bool {
	out := make(map[string]bool, len(info.Tables)+len(info.Joins))
	for i := range info.Tables {
		if id := sourceIdentifier(&info.Tables[i]); id != "" {
			out[strings.ToLower(id)] = true
		}
	}
	for i := range info.Joins {
		if r := joinRightSource(&info.Joins[i]); r != nil {
			if id := sourceIdentifier(r); id != "" {
				out[strings.ToLower(id)] = true
			}
		}
	}
	return out
}

// unfoldIn walks one expression and replaces every FROM-less scalar subquery
// the enclosing block can supply. It is an ordinary expression walk, so a
// subquery nested in an aggregate argument, a CASE arm, a function call or an
// operand is reached; a subquery that KEEPS its node is walked no further,
// because its own block is parsed (and unfolded) in its own right.
func unfoldIn(n Node, scope map[string]bool, hasFrom bool) Node {
	if n == nil {
		return nil
	}
	switch e := n.(type) {
	case *SubqueryNode:
		if repl, ok := fromlessScalarExpr(e.SQL, scope, hasFrom); ok {
			// To a FIXED POINT: `(SELECT (SELECT u.x))` is `(SELECT u.x)` is
			// `u.x`, and each unfold hands back a strictly shorter statement,
			// so the recursion ends.
			return unfoldIn(repl, scope, hasFrom)
		}
		return e
	case *ParenNode:
		return &ParenNode{Inner: unfoldIn(e.Inner, scope, hasFrom)}
	case *BinaryOp:
		return &BinaryOp{Left: unfoldIn(e.Left, scope, hasFrom), Op: e.Op,
			Right: unfoldIn(e.Right, scope, hasFrom)}
	case *UnaryOp:
		return &UnaryOp{Op: e.Op, Inner: unfoldIn(e.Inner, scope, hasFrom)}
	case *CmpExpr:
		return &CmpExpr{Left: unfoldIn(e.Left, scope, hasFrom), Op: e.Op,
			Right: unfoldIn(e.Right, scope, hasFrom)}
	case *AndNode:
		return &AndNode{Left: unfoldIn(e.Left, scope, hasFrom), Right: unfoldIn(e.Right, scope, hasFrom)}
	case *OrNode:
		return &OrNode{Left: unfoldIn(e.Left, scope, hasFrom), Right: unfoldIn(e.Right, scope, hasFrom)}
	case *NotNode:
		return &NotNode{Inner: unfoldIn(e.Inner, scope, hasFrom)}
	case *IsExpr:
		return &IsExpr{Left: unfoldIn(e.Left, scope, hasFrom), Not: e.Not, Check: e.Check}
	case *LikeExpr:
		return &LikeExpr{Left: unfoldIn(e.Left, scope, hasFrom), Not: e.Not,
			Pattern: unfoldIn(e.Pattern, scope, hasFrom)}
	case *BetweenExpr:
		return &BetweenExpr{Left: unfoldIn(e.Left, scope, hasFrom), Not: e.Not,
			Low: unfoldIn(e.Low, scope, hasFrom), High: unfoldIn(e.High, scope, hasFrom)}
	case *InExpr:
		out := &InExpr{Left: unfoldIn(e.Left, scope, hasFrom), Not: e.Not,
			Values: make([]Node, len(e.Values))}
		for i, v := range e.Values {
			out.Values[i] = unfoldIn(v, scope, hasFrom)
		}
		return out
	case *AnyAllExpr:
		out := &AnyAllExpr{Left: unfoldIn(e.Left, scope, hasFrom), Op: e.Op,
			Modifier: e.Modifier, Values: make([]Node, len(e.Values))}
		for i, v := range e.Values {
			out.Values[i] = unfoldIn(v, scope, hasFrom)
		}
		return out
	case *FuncCallNode:
		out := *e
		out.Args = make([]Node, len(e.Args))
		for i, a := range e.Args {
			out.Args[i] = unfoldIn(a, scope, hasFrom)
		}
		return &out
	case *CaseNode:
		out := &CaseNode{Subject: unfoldIn(e.Subject, scope, hasFrom),
			Whens: make([]WhenClause, len(e.Whens)), Else: unfoldIn(e.Else, scope, hasFrom)}
		for i, w := range e.Whens {
			out.Whens[i] = WhenClause{Cond: unfoldIn(w.Cond, scope, hasFrom),
				Result: unfoldIn(w.Result, scope, hasFrom)}
		}
		return out
	case *CastNode:
		return &CastNode{Inner: unfoldIn(e.Inner, scope, hasFrom), TypeName: e.TypeName}
	case *ArrayLitNode:
		out := &ArrayLitNode{Elements: make([]Node, len(e.Elements))}
		for i, el := range e.Elements {
			out.Elements[i] = unfoldIn(el, scope, hasFrom)
		}
		return out
	case *TupleNode:
		out := &TupleNode{Elements: make([]Node, len(e.Elements))}
		for i, el := range e.Elements {
			out.Elements[i] = unfoldIn(el, scope, hasFrom)
		}
		return out
	case *WindowFuncNode:
		out := *e
		if e.Func != nil {
			if f, ok := unfoldIn(e.Func, scope, hasFrom).(*FuncCallNode); ok {
				out.Func = f
			}
		}
		out.PartitionBy = make([]Node, len(e.PartitionBy))
		for i, p := range e.PartitionBy {
			out.PartitionBy[i] = unfoldIn(p, scope, hasFrom)
		}
		out.OrderBy = make([]WindowOrderBy, len(e.OrderBy))
		for i, o := range e.OrderBy {
			out.OrderBy[i] = WindowOrderBy{Expr: unfoldIn(o.Expr, scope, hasFrom),
				Desc: o.Desc, NullsFirst: o.NullsFirst}
		}
		return &out
	}
	return n
}

// fromlessScalarExpr answers the expression a scalar subquery IS, for a
// subquery that has no FROM clause, nothing that can suppress or duplicate the
// single row such a SELECT produces, and whose every column reference the
// ENCLOSING block supplies. The second result is false for every other
// subquery, including one this package cannot parse — which keeps the caller's
// existing behaviour, since the same text is parsed again downstream and
// raises there.
//
// The excluded clauses are excluded because each one changes the ANSWER and
// not only the shape, and PostgreSQL 17.11 was measured on every one of them:
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
func fromlessScalarExpr(subSQL string, scope map[string]bool, hasFrom bool) (Node, bool) {
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
	if !resolvesInScope(col.ASTExpr, scope, hasFrom) {
		return nil, false
	}
	if needsParens(col.ASTExpr) {
		return &ParenNode{Inner: col.ASTExpr}, true
	}
	return col.ASTExpr, true
}

// resolvesInScope reports whether every column reference in a FROM-less
// subquery's item is one the ENCLOSING block supplies — the test that keeps
// this rewrite out of a block whose text a per-row re-run will rebuild.
//
// A QUALIFIED reference must name one of the block's own FROM identifiers. An
// UNQUALIFIED one is accepted when the block has a FROM item at all, because
// SQL scopes innermost-first and a FROM-less subquery has no scope of its own,
// so the innermost block that can supply a bare name is this one. Deciding
// that exactly needs the relation's COLUMN LIST and therefore a catalog, which
// this layer does not have; the residual is a bare name this block does not
// carry and an enclosing one does, and it is recorded in ADR-0021 §1l.
func resolvesInScope(n Node, scope map[string]bool, hasFrom bool) bool {
	ok := true
	walkColRefs(n, func(c *ColRef) {
		if c.Table != "" {
			if !scope[strings.ToLower(c.Table)] {
				ok = false
			}
			return
		}
		if !hasFrom {
			ok = false
		}
	})
	return ok
}

// walkColRefs calls f for every column reference under n. A nested subquery is
// NOT descended into: its references are its own block's question, and a
// subquery nested inside a FROM-less one keeps its node until that block is
// unfolded in its own right.
func walkColRefs(n Node, f func(*ColRef)) {
	switch e := n.(type) {
	case nil:
		return
	case *ColRef:
		f(e)
	case *ParenNode:
		walkColRefs(e.Inner, f)
	case *BinaryOp:
		walkColRefs(e.Left, f)
		walkColRefs(e.Right, f)
	case *UnaryOp:
		walkColRefs(e.Inner, f)
	case *CmpExpr:
		walkColRefs(e.Left, f)
		walkColRefs(e.Right, f)
	case *AndNode:
		walkColRefs(e.Left, f)
		walkColRefs(e.Right, f)
	case *OrNode:
		walkColRefs(e.Left, f)
		walkColRefs(e.Right, f)
	case *NotNode:
		walkColRefs(e.Inner, f)
	case *IsExpr:
		walkColRefs(e.Left, f)
	case *LikeExpr:
		walkColRefs(e.Left, f)
		walkColRefs(e.Pattern, f)
	case *BetweenExpr:
		walkColRefs(e.Left, f)
		walkColRefs(e.Low, f)
		walkColRefs(e.High, f)
	case *InExpr:
		walkColRefs(e.Left, f)
		for _, v := range e.Values {
			walkColRefs(v, f)
		}
	case *AnyAllExpr:
		walkColRefs(e.Left, f)
		for _, v := range e.Values {
			walkColRefs(v, f)
		}
	case *FuncCallNode:
		for _, a := range e.Args {
			walkColRefs(a, f)
		}
	case *CaseNode:
		walkColRefs(e.Subject, f)
		for _, w := range e.Whens {
			walkColRefs(w.Cond, f)
			walkColRefs(w.Result, f)
		}
		walkColRefs(e.Else, f)
	case *CastNode:
		walkColRefs(e.Inner, f)
	case *ArrayLitNode:
		for _, el := range e.Elements {
			walkColRefs(el, f)
		}
	case *TupleNode:
		for _, el := range e.Elements {
			walkColRefs(el, f)
		}
	case *SubqueryNode, *ExistsNode:
		// A nested block's references are that block's question.
		return
	}
}

// needsParens reports whether an expression's own rendering is ambiguous once
// it is placed inside a larger one — an OPERATOR expression, whose String()
// emits its operands with no brackets of its own. Everything else (a column, a
// literal, a function call, a CAST, a CASE, an array or tuple, a subquery) is
// self-delimiting and takes no wrapper.
//
// A COLUMN REFERENCE gets none, and that is not cosmetic: `(u.x)` and `u.x`
// are one expression to the compiler but not to the stage emission, which
// reads a projection's node to decide whether a stage passes a column through.
// Wrapped, the filing's own shape routed to the coordinator-local pipeline on
// three DAG arms and its aggregate spelling reached the worker as a schemaless
// batch (#277).
func needsParens(n Node) bool {
	switch n.(type) {
	case *BinaryOp, *UnaryOp, *CmpExpr, *AndNode, *OrNode, *NotNode,
		*IsExpr, *LikeExpr, *BetweenExpr, *InExpr, *AnyAllExpr:
		return true
	}
	return false
}

// refuseStarWithNoRelation answers PostgreSQL's 42601 for a block whose SELECT
// list holds a star and whose FROM clause names nothing.
//
// `SELECT *` is `SELECT * with no tables specified is not valid` on
// PostgreSQL 17.11 and this engine answered NULL for it — at the top level and
// as a scalar subquery (`SELECT id, (SELECT *) FROM c2users u`) alike. A star
// is the only item that cannot be evaluated without a relation, so the test is
// exactly "a star and no FROM"; a set operation is checked through its arms,
// each of which is a block with its own FROM.
func refuseStarWithNoRelation(info *SelectInfo) error {
	if info == nil {
		return nil
	}
	if info.Union != nil {
		if err := refuseStarWithNoRelation(info.Union.Left); err != nil {
			return err
		}
		return refuseStarWithNoRelation(info.Union.Right)
	}
	if len(info.Tables) > 0 || len(info.Joins) > 0 {
		return nil
	}
	for i := range info.Columns {
		if info.Columns[i].Star {
			return sqlerr.New("42601", "SELECT * with no tables specified is not valid")
		}
	}
	return nil
}

// isBareNumericLit reports whether a node renders as a NUMERIC LITERAL and
// nothing else — the one shape an ORDER BY or GROUP BY term must not acquire,
// because both engines read it there as a select-list POSITION. It is asked in
// BOTH clauses, and of a SUBSTITUTED term as well as a rewritten one: the
// per-row re-run renders the outer row's value as a literal, and a literal in
// those two clauses is a position rather than a value. Parentheses are
// transparent to that reading in this engine's own planner
// (logical.unwrapParens), so they are unwrapped here too.
func isBareNumericLit(n Node) bool {
	for {
		p, ok := n.(*ParenNode)
		if !ok {
			break
		}
		n = p.Inner
	}
	lit, ok := n.(*Lit)
	return ok && lit.Kind == LitNumber
}
