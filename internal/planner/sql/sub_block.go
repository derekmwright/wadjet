// SPDX-License-Identifier: MIT

package sql

import (
	"errors"
	"strings"
	"sync"
)

// Parse each derived-table/CTE block ONCE: binder and logical builder must
// share the SAME SelectInfo, including binder rewrites (#739, #851).
// Memoize on each reference so the same rule propagates to any nesting depth.
// Callers must hold reference POINTERS: caching into a struct copy loses the
// shared tree. This preserves schema-aware GROUP BY input precedence rather
// than discarding it when the builder reparses provisional alias substitutions.
// See docs/internals/sql-shared-nested-query-tree.md for the design.

// SubSelect returns the parsed SELECT body of a DERIVED TABLE reference,
// memoized on the reference. It returns (nil, nil) when the reference is not a
// derived table, and the parse error when the body does not parse — callers
// wrap that in their own message, which is why the error is memoized too.
func (t *TableRef) SubSelect() (*SelectInfo, error) {
	if t == nil || !strings.HasPrefix(t.Name, "(") {
		return nil, nil
	}
	if t.subDone {
		return t.sub, t.subErr
	}
	t.subDone = true
	t.sub, t.subErr = parseBlockText(strings.TrimSuffix(strings.TrimPrefix(t.Name, "("), ")"))
	return t.sub, t.subErr
}

// BodySelect returns the parsed SELECT body of a CTE definition, memoized on
// the definition, for the same reason SubSelect memoizes a derived table's.
func (c *CTEDef) BodySelect() (*SelectInfo, error) {
	if c == nil {
		return nil, nil
	}
	if c.bodyDone {
		return c.body, c.bodyErr
	}
	c.bodyDone = true
	c.body, c.bodyErr = parseBlockText(c.SQL)
	return c.body, c.bodyErr
}

// subqueryBody is an EXPRESSION subquery's body parsed once, on the node that
// names it, with the WITH items in scope where the node is written (ADR-0032
// extended to SubqueryNode and ExistsNode, ADR-0047 stage 3). A derived table
// and a CTE memoize their bodies on the reference (SubSelect, BodySelect); an
// expression subquery is a node of its enclosing block's expression tree, and
// the memo rides on that node, so the binder, the planners and the
// declaration passes read one tree — and the binder's bindings on it — rather
// than a private parse of SQL each.
//
// The memo records the text it parsed: a copy of the node whose SQL was
// rewritten (a correlated re-run's substitution builds a NEW node, never a
// copy, but a copy is cheap to guard) parses its own text.
type subqueryBody struct {
	sql string
	// parseMu guards done, info and err: the body is parsed on first use
	// unless the statement's parse seeded it (bodySyntax), and a FROM-less
	// unfold may take the tree away (takeBody).
	parseMu sync.Mutex
	done    bool
	info    *SelectInfo
	err     error
	scope   []CTEDef
	scoped  bool
	// outer is the binder's classification of the body (SetOuterRefs).
	outer    []OuterRef
	outerSet bool
	// plan serializes the planners that build a pipeline from the shared
	// tree at run time (an uncorrelated subquery's one run, reached from any
	// pipeline goroutine): building rewrites the tree's provisional
	// decisions in place, idempotently, but not concurrently.
	plan sync.Mutex
}

// subqueryBodyMu guards the memo's creation and its scope: a node is reached
// from parallel pipeline goroutines at run time. The PARSE is not under it —
// parsing a body checks the subqueries nested in it, whose memos take this
// lock — and runs once per memo (subqueryBody.parseMu).
var subqueryBodyMu sync.Mutex

func memoSubqueryBody(slot **subqueryBody, sql string) *subqueryBody {
	subqueryBodyMu.Lock()
	b := *slot
	if b == nil || b.sql != sql {
		nb := &subqueryBody{sql: sql}
		if b != nil && b.scoped {
			nb.scope, nb.scoped = b.scope, true
		}
		*slot = nb
		b = nb
	}
	subqueryBodyMu.Unlock()
	return b
}

func (b *subqueryBody) parsed() (*SelectInfo, error) {
	b.parseMu.Lock()
	defer b.parseMu.Unlock()
	if !b.done {
		b.info, b.err = parseBlockText(b.sql)
		b.done = true
	}
	return b.info, b.err
}

// seeded is a memo whose body is the tree the statement's parse already built.
func seeded(sql string, info *SelectInfo, err error) *subqueryBody {
	return &subqueryBody{sql: sql, done: true, info: info, err: err}
}

// takeBody hands the memoized tree to a caller that SPLICES it into the
// enclosing statement — a FROM-less body unfolded into its item
// (unfoldFromlessScalars) — and leaves the memo unparsed, so the spliced tree
// is never also the memo's: a requester that still reaches the node parses
// the text again, privately.
func (s *SubqueryNode) takeBody() (*SelectInfo, error) {
	b := memoSubqueryBody(&s.body, s.SQL)
	b.parseMu.Lock()
	defer b.parseMu.Unlock()
	if !b.done {
		return parseBlockText(b.sql)
	}
	info, err := b.info, b.err
	b.info, b.err, b.done = nil, nil, false
	return info, err
}

// Select returns the subquery's parsed body, memoized on the node, and the
// parse error with it (a caller wraps that in its own message).
func (s *SubqueryNode) Select() (*SelectInfo, error) {
	if s == nil {
		return nil, nil
	}
	return memoSubqueryBody(&s.body, s.SQL).parsed()
}

// Select returns the EXISTS body's parsed tree, memoized as SubqueryNode's.
func (e *ExistsNode) Select() (*SelectInfo, error) {
	if e == nil {
		return nil, nil
	}
	return memoSubqueryBody(&e.body, e.SQL).parsed()
}

// CTEScope is the WITH chain in scope where the subquery is written: the
// enclosing blocks' items, outermost first, then each nested block's own, so
// the LAST item of a name is the one a reference binds — a nested
// WITH that reuses a name shadows the enclosing item inside its block, as
// PostgreSQL scopes it. ok is false until a pass that knows the enclosing
// blocks recorded it (StampSubqueryScopes).
func (s *SubqueryNode) CTEScope() ([]CTEDef, bool) {
	if s == nil {
		return nil, false
	}
	return memoScope(&s.body, s.SQL)
}

// CTEScope is SubqueryNode.CTEScope's twin.
func (e *ExistsNode) CTEScope() ([]CTEDef, bool) {
	if e == nil {
		return nil, false
	}
	return memoScope(&e.body, e.SQL)
}

func memoScope(slot **subqueryBody, sql string) ([]CTEDef, bool) {
	b := memoSubqueryBody(slot, sql)
	subqueryBodyMu.Lock()
	defer subqueryBodyMu.Unlock()
	return b.scope, b.scoped
}

// LockPlan serializes the run-time planners that build a pipeline from the
// shared body (see subqueryBody.plan); the returned func unlocks.
func (s *SubqueryNode) LockPlan() func() {
	b := memoSubqueryBody(&s.body, s.SQL)
	b.plan.Lock()
	return b.plan.Unlock
}

// LockPlan is SubqueryNode.LockPlan's twin.
func (e *ExistsNode) LockPlan() func() {
	b := memoSubqueryBody(&e.body, e.SQL)
	b.plan.Lock()
	return b.plan.Unlock
}

// SetOuterRefs records the binder's classification of the body: the
// references whose binding reaches the query the subquery is written in,
// spelled as the per-row re-run substitutes them (ADR-0021 §1k's 2026-10-09
// amendment). The binder records it only where it resolved every reference.
func (s *SubqueryNode) SetOuterRefs(refs []OuterRef) { setMemoOuter(&s.body, s.SQL, refs) }

// SetOuterRefs is SubqueryNode.SetOuterRefs's twin.
func (e *ExistsNode) SetOuterRefs(refs []OuterRef) { setMemoOuter(&e.body, e.SQL, refs) }

// OuterRefs is the classification SetOuterRefs recorded, ok=false when none.
func (s *SubqueryNode) OuterRefs() ([]OuterRef, bool) { return memoOuter(&s.body, s.SQL) }

// OuterRefs is SubqueryNode.OuterRefs's twin.
func (e *ExistsNode) OuterRefs() ([]OuterRef, bool) { return memoOuter(&e.body, e.SQL) }

func setMemoOuter(slot **subqueryBody, sql string, refs []OuterRef) {
	b := memoSubqueryBody(slot, sql)
	subqueryBodyMu.Lock()
	b.outer, b.outerSet = refs, true
	subqueryBodyMu.Unlock()
}

func memoOuter(slot **subqueryBody, sql string) ([]OuterRef, bool) {
	b := memoSubqueryBody(slot, sql)
	subqueryBodyMu.Lock()
	defer subqueryBodyMu.Unlock()
	return b.outer, b.outerSet
}

func setMemoScope(slot **subqueryBody, sql string, chain []CTEDef) {
	b := memoSubqueryBody(slot, sql)
	subqueryBodyMu.Lock()
	b.scope, b.scoped = chain, true
	subqueryBodyMu.Unlock()
}

// ScopeChain is the WITH chain in scope inside a block whose enclosing chain
// is outer and whose own WITH list is own: outer's items, then own's.
func ScopeChain(outer, own []CTEDef) []CTEDef {
	if len(own) == 0 {
		return outer
	}
	out := make([]CTEDef, 0, len(outer)+len(own))
	out = append(out, outer...)
	return append(out, own...)
}

// StampSubqueryScopes records chain — the WITH chain in scope inside info,
// its own items included — on every expression subquery and EXISTS written in
// info's own clauses: the SELECT list (aggregate arguments and window terms
// included), WHERE, GROUP BY, HAVING, QUALIFY, a JOIN's ON, a table
// function's arguments, a TABLESAMPLE argument and an ORDER BY item's parsed
// term (OrderByItem.Expr). A set operation's arms are stamped with the same
// chain. A derived table's, a CTE's and a nested subquery's own clauses are
// stamped by the pass that plans that block, with that block's chain.
func StampSubqueryScopes(info *SelectInfo, chain []CTEDef) {
	if info == nil {
		return
	}
	if info.Union != nil {
		StampSubqueryScopes(info.Union.Left, chain)
		StampSubqueryScopes(info.Union.Right, chain)
		return
	}
	stamp := func(n Node) {
		ForEachSubquery(n, func(sq Node) {
			switch q := sq.(type) {
			case *SubqueryNode:
				setMemoScope(&q.body, q.SQL, chain)
			case *ExistsNode:
				setMemoScope(&q.body, q.SQL, chain)
			}
		})
	}
	for i := range info.Columns {
		stamp(info.Columns[i].ASTExpr)
		stamp(info.Columns[i].AggArgExpr)
	}
	stamp(info.WhereExpr)
	stamp(info.HavingExpr)
	stamp(info.QualifyExpr)
	for _, g := range info.GroupByExprs {
		stamp(g)
	}
	for i := range info.Joins {
		stamp(info.Joins[i].CondExpr)
	}
	for i := range info.OrderBy {
		stamp(info.OrderBy[i].Expr)
	}
	for i := range info.Tables {
		for _, a := range info.Tables[i].FuncArgExprs {
			stamp(a)
		}
		stamp(info.Tables[i].SampleArg)
	}
}

// ForEachSubquery calls f for every SubqueryNode and ExistsNode under n,
// through every operator, call, CASE, cast, ANY/ALL, IN list and window call,
// and never into a subquery's own body.
func ForEachSubquery(n Node, f func(Node)) {
	switch e := n.(type) {
	case nil:
		return
	case *SubqueryNode, *ExistsNode:
		f(e)
	case *WindowFuncNode:
		if e.Func != nil {
			ForEachSubquery(e.Func, f)
		}
		for _, p := range e.PartitionBy {
			ForEachSubquery(p, f)
		}
		for _, o := range e.OrderBy {
			ForEachSubquery(o.Expr, f)
		}
	default:
		walkColRefsWith(n, func(*ColRef) {}, func(c Node, _ func(*ColRef)) { ForEachSubquery(c, f) })
	}
}

// parseBlockText parses one block's SQL text into a SelectInfo.
//
// The block is the text between a pair of parentheses, so a syntax error at
// the END of that text is PostgreSQL's `syntax error at or near ")"` — the
// token that closes it in the statement the client sent.
func parseBlockText(sql string) (*SelectInfo, error) {
	parsed, err := Parse(sql)
	if err != nil {
		var se *syntaxError
		if errors.As(err, &se) && se.atEnd {
			return nil, &syntaxError{code: se.code, msg: `syntax error at or near ")"`, err: se.err}
		}
		return nil, err
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// bodySyntax is a parenthesised subquery body's SYNTAX, checked where the
// statement is read: a body that cannot be parsed is the statement's syntax
// error, worded as PostgreSQL words it for the statement the client sent — a
// failure at the end of the body is at the ")" that closes it. Any other
// failure is left for the planner that asks for the body later, exactly as
// before; only the sentence of a syntax error is decided here (arc PC round
// 3, B6: `x IN (SELECT … WHERE)` said "at end of input").
//
// The parse it makes is THE parse of the body (arc CI3 round 2, B1): the memo
// it returns is seeded with the tree, so the node built from it answers
// Select() without reading the text again, and the subqueries nested in the
// body were seeded by the same parse. A body is therefore parsed once per
// statement however many requesters ask for it.
func bodySyntax(sql string) (*subqueryBody, error) {
	parsed, err := Parse(sql)
	var se *syntaxError
	if errors.As(err, &se) {
		if se.atEnd {
			return nil, &syntaxError{code: se.code, msg: `syntax error at or near ")"`, err: se.err}
		}
		return nil, se
	}
	var info *SelectInfo
	if err == nil {
		info, err = ExtractSelect(parsed)
		if err != nil {
			info = nil
		}
	}
	return seeded(sql, info, err), nil
}

// BlockOutputColumns lists the column names one query block PUBLISHES, and
// reports whether the list is incomplete because the SELECT list holds a star.
//
// It is the block-namespace rule in ONE place: the binder asks it for the
// output aliases a GROUP BY may name, for a derived table's published columns
// and for a CTE's; correlation analysis asks it for the same reason one level
// down — an unqualified name inside a subquery binds the subquery's own FROM
// first, and a CTE reference or a derived table is a relation with a schema
// exactly as a base table is (ADR-0021 §1k).
//
// A set operation publishes its LEFT arm's names, which is PostgreSQL's rule.
// A star is not expanded here: naming what it stands for needs the sources'
// schemas, which this package does not have, so the second result says "ask
// somebody with a catalog".
func BlockOutputColumns(info *SelectInfo) ([]string, bool) {
	if info == nil {
		return nil, true
	}
	if info.Union != nil {
		return BlockOutputColumns(info.Union.Left)
	}
	var names []string
	for i := range info.Columns {
		c := info.Columns[i]
		if c.Star {
			return nil, true
		}
		if name := SelectItemName(c); name != "" {
			names = append(names, strings.ToLower(name))
		}
	}
	return names, false
}

// SelectItemName is the name one SELECT item publishes: its alias, else the
// column it names, else the expression text as written — and for a window call
// the name the logical builder's projection gives it, so the namespace this
// enumerates is the one the query really produces.
func SelectItemName(c SelectColumn) string {
	if c.IsWindow {
		return WindowOutputName(c)
	}
	if c.Alias != "" {
		return c.Alias
	}
	if c.ColumnRef != "" {
		return c.ColumnRef
	}
	return strings.TrimSpace(c.Expr)
}
