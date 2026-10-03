// SPDX-License-Identifier: MIT

package pgwire

// The type of the POSITION an undeclared parameter occupies (#1410).
//
// A client that declares no type for `$n` (OID 0) leaves the choice to the
// server, and PostgreSQL makes it during parse analysis: the parameter takes
// the type the expression around it requires — the other operand's type in a
// comparison, an arithmetic or a membership, the target column's in an INSERT
// or an UPDATE, bigint in LIMIT and OFFSET, the cast's target in a CAST,
// integer in a window function's offset — and text where nothing requires
// one (a bare select-list item). The ParameterDescription carries that type,
// the client encodes its value for it, and Bind splices the value as a literal
// OF it (bindparams.go), so the reading and the encoding cannot disagree.
//
// This used to be lexical: a placeholder standing directly beside an
// identifier in a comparison took that column's type and everything else
// stayed OID 0. `$1 = n + d` read as `$1 = n` and typed the parameter int4,
// so a client encoded 14.5 as an integer and matched nothing; `$1 + 1`,
// `n IN ($1, $2)`, `INSERT … VALUES ($1)`, `LIMIT $1` and `SELECT $1` were
// not typed at all.
//
// Here the statement is PARSED with each placeholder replaced by a sentinel
// column reference, and the parser's tree is walked: at each node holding a
// sentinel, the sibling expression is typed by the planner's own declaration
// walk (physical.DeclaredTypeOfNode) over the columns in scope at that block.
// The scope is read by asking the engine to describe each FROM item —
// `SELECT * FROM <item> LIMIT 0` under this connection's identity — so a base
// table, a derived table, a CTE, a table function and a join arm are all read
// the way the statement itself would read them, and a relation the identity
// may not read contributes nothing (its types never reach the wire).
//
// A position the walk does not decide keeps OID 0 — the report the server
// made before — rather than a guess.

import (
	"context"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// paramSentinel is the column name a placeholder is parsed as. A placeholder
// is not an expression this parser reads, and a column reference is valid
// wherever an expression is, which is every position a parameter may hold
// except the constant-only ones (paramPositions).
const paramSentinel = "__pw_param_"

// inferParamOIDs returns declared with every OID-0 entry (and every entry past
// the declared list, up to the statement's placeholder count) filled with the
// type of the position the placeholder occupies, where the walk decides one.
// A declared entry is never overridden: the client's word wins.
func (c *pgConn) inferParamOIDs(sql string, declared []uint32) []uint32 {
	n := countParamPlaceholders(sql)
	if n == 0 {
		return declared
	}
	oids := make([]uint32, n)
	copy(oids, declared)
	missing := false
	for _, oid := range oids {
		if oid == 0 {
			missing = true
			break
		}
	}
	if !missing {
		return oids
	}

	// One resolution per statement text per connection AND per catalog
	// generation: pgx and pgJDBC re-Parse one text per execution, and the
	// scope probes behind a resolution are queries. The generation advances
	// on every catalog write (catalog.Generation — a DROP / CREATE, an ALTER,
	// a function, a commit's manifest), from this connection or any other, so
	// a type resolved against a table that has since been recreated is never
	// reused (round-2 B1: keyed by the text alone, '2.5' bound for
	// `INSERT … VALUES (1, $1)` was spliced as the numeric the dropped table's
	// column was and stored 3 in the recreated INTEGER, where PostgreSQL
	// raises 22P02). The generation is read BEFORE the walk, so an entry can
	// only be newer than its key, never older; a store without one caches
	// nothing.
	gen, cacheable := c.db.Catalog().Generation()
	key := paramCacheKey(gen, sql, declared)
	if cacheable {
		if cached, ok := c.paramOIDCache[key]; ok {
			return append([]uint32(nil), cached...)
		}
	}

	ctx, cancel := c.inferenceContext()
	defer cancel()
	pt := &paramTyper{c: c, ctx: ctx, oids: oids, fixed: make([]bool, n)}
	for i, oid := range oids {
		pt.fixed[i] = oid != 0
	}
	pt.statement(sql)

	if cacheable {
		// Entries of an older generation can never be read again.
		if c.paramOIDCache == nil || c.paramOIDCacheGen != gen {
			c.paramOIDCache = make(map[string][]uint32)
			c.paramOIDCacheGen = gen
		}
		c.paramOIDCache[key] = append([]uint32(nil), oids...)
	}
	return oids
}

// paramCacheKey keys the cache by the catalog generation, the statement AND
// the declared OIDs: one text prepared twice with different declarations
// types its undeclared positions the same way, but the declared ones differ,
// and a cached slice is returned whole.
func paramCacheKey(gen uint64, sql string, declared []uint32) string {
	var b strings.Builder
	b.WriteString(strconv.FormatUint(gen, 10))
	b.WriteByte(0)
	b.WriteString(sql)
	b.WriteByte(0)
	for _, oid := range declared {
		b.WriteString(strconv.FormatUint(uint64(oid), 10))
		b.WriteByte(',')
	}
	return b.String()
}

type paramTyper struct {
	c     *pgConn
	ctx   context.Context
	oids  []uint32
	fixed []bool // declared by the client: never assigned
}

// assign records oid for parameter n, unless the client declared it or an
// earlier position already decided it (PostgreSQL types a parameter once;
// the first position the walk meets decides).
func (pt *paramTyper) assign(n int, oid uint32) {
	if n < 1 || n > len(pt.oids) || oid == 0 || pt.fixed[n-1] || pt.oids[n-1] != 0 {
		return
	}
	pt.oids[n-1] = oid
}

// statement rewrites the placeholders and walks the parsed statement.
func (pt *paramTyper) statement(sql string) {
	refs := scanParamRefs(sql)
	positions := paramPositions(sql, refs)
	var b strings.Builder
	prev := 0
	for i, r := range refs {
		b.WriteString(sql[prev:r.start])
		switch positions[i] {
		case posCount:
			// LIMIT / OFFSET / FETCH take bigint (PostgreSQL's int8 for
			// all three); a TABLESAMPLE percentage takes real. The count
			// grammar reads a number token, so the parse sees one.
			if tablesamplePosition(sql, r) {
				pt.assign(r.n, oidFloat4)
			} else {
				pt.assign(r.n, oidInt8)
			}
			b.WriteString("1")
		case posWindowInt:
			// A window function's integer argument (LAG / LEAD offset,
			// NTILE's buckets, NTH_VALUE's position) is integer by the
			// function's signature. The parser takes only a constant
			// there, so the parse sees one.
			pt.assign(r.n, oidInt4)
			b.WriteString("1")
		default:
			b.WriteString(paramSentinel + strconv.Itoa(r.n))
		}
		prev = r.end
	}
	b.WriteString(sql[prev:])

	pq, err := plansql.Parse(b.String())
	if err != nil || pq == nil {
		return
	}
	pt.parsed(pq)
}

// parsed walks one parsed statement.
func (pt *paramTyper) parsed(pq *plansql.ParsedQuery) {
	switch pq.Type {
	case plansql.QueryExplain:
		// EXPLAIN's statement is typed as the statement itself is.
		if pq.Explain != nil {
			if inner, err := plansql.Parse(pq.Explain.InnerSQL); err == nil && inner != nil {
				pt.parsed(inner)
			}
		}
	case plansql.QueryCreateTable:
		// CREATE TABLE … AS SELECT: its query, whose outputs become the
		// columns (a bare parameter there is text, as in PostgreSQL).
		if pq.CreateTable != nil && pq.CreateTable.AsSelect != nil {
			pt.parsed(pq.CreateTable.AsSelect)
		}
	case plansql.QuerySelect:
		if pq.SelectInfo != nil {
			pt.block(pq.SelectInfo, nil, nil)
		}
	case plansql.QueryInsert:
		pt.insert(pq.Insert)
	case plansql.QueryUpdate:
		pt.update(pq.Update)
	case plansql.QueryDelete:
		if pq.Delete != nil {
			sc := pt.targetScope(pq.Delete.Table, pq.Delete.Qualifier, pq.Delete.Alias)
			pt.exprText(pq.Delete.WhereSQL, sc)
		}
	case plansql.QueryMerge:
		pt.merge(pq.Merge)
	}
}

// scopeSource is one FROM item's published columns, as the engine describes
// them, under the name a reference qualifies them by.
type scopeSource struct {
	qual string
	cols []wadjet.ColumnMeta
}

// scope is the columns visible to one block: its own FROM items first, then
// the enclosing blocks' (a correlated reference), and the CTEs it may name.
type scope struct {
	sources []scopeSource
	outer   *scope
	ctes    []plansql.CTEDef
}

// lookup finds the column a reference names: the innermost block that
// publishes it wins, as name resolution does.
func (sc *scope) lookup(ref *plansql.ColRef) (wadjet.ColumnMeta, bool) {
	for s := sc; s != nil; s = s.outer {
		for _, src := range s.sources {
			if ref.Table != "" && !strings.EqualFold(ref.Table, src.qual) {
				continue
			}
			for _, m := range src.cols {
				if strings.EqualFold(m.Name, ref.Column) {
					return m, true
				}
			}
		}
	}
	return wadjet.ColumnMeta{}, false
}

// schema is the scope flattened for the declaration walk: every column under
// its qualified name, and under its bare name where no nearer source
// published that name first.
func (sc *scope) schema() []parquet.Column {
	var out []parquet.Column
	seen := map[string]bool{}
	add := func(name string, m wadjet.ColumnMeta) {
		key := strings.ToLower(name)
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, parquet.Column{Name: name, Type: m.TypeID, Precision: m.Precision,
			Scale: m.Scale, Fields: m.Fields, ElementType: m.ElementType, Nullable: true})
	}
	for s := sc; s != nil; s = s.outer {
		for _, src := range s.sources {
			for _, m := range src.cols {
				if src.qual != "" {
					add(src.qual+"."+m.Name, m)
				}
				add(m.Name, m)
			}
		}
	}
	return out
}

func (sc *scope) allCTEs() []plansql.CTEDef {
	var out []plansql.CTEDef
	for s := sc; s != nil; s = s.outer {
		out = append(out, s.ctes...)
	}
	return out
}

// probe describes one FROM item: the columns `SELECT * FROM <item>` publishes,
// with the CTEs in scope declared before it. Every placeholder sentinel in the
// item's text stands as NULL for the probe. A failure — a relation this
// identity may not read, a LATERAL item that names an outer column, anything
// the engine refuses — contributes no columns.
func (pt *paramTyper) probe(t plansql.TableRef, ctes []plansql.CTEDef) []wadjet.ColumnMeta {
	si := &plansql.SelectInfo{
		Tables:  []plansql.TableRef{t},
		Columns: []plansql.SelectColumn{{Star: true}},
		CTEs:    ctes,
	}
	text := sentinelsAsNull(plansql.RebuildSQL(si, nil)) + " LIMIT 0"
	res, err := pt.c.db.Query(pt.ctx, text)
	if err != nil || res == nil {
		return nil
	}
	return res.ColumnMetas
}

// sentinelsAsNull replaces every placeholder sentinel in text with NULL.
func sentinelsAsNull(text string) string {
	for {
		i := strings.Index(text, paramSentinel)
		if i < 0 {
			return text
		}
		j := i + len(paramSentinel)
		for j < len(text) && text[j] >= '0' && text[j] <= '9' {
			j++
		}
		text = text[:i] + "NULL" + text[j:]
	}
}

// source is one FROM item as a scope entry: probed, and qualified by its
// alias, or by its own name when it has none.
func (pt *paramTyper) source(t plansql.TableRef, ctes []plansql.CTEDef) scopeSource {
	qual := t.Alias
	if qual == "" && !strings.HasPrefix(t.Name, "(") {
		qual = t.Name
		if t.IsFunction {
			qual = ""
		}
	}
	return scopeSource{qual: qual, cols: pt.probe(t, ctes)}
}

// targetScope is a DML statement's target table as a scope.
func (pt *paramTyper) targetScope(table, qualifier, alias string) *scope {
	t := plansql.TableRef{Name: table, Qualifier: qualifier, Alias: alias}
	return &scope{sources: []scopeSource{pt.source(t, nil)}}
}

// block walks one SELECT block (and every block nested in it).
func (pt *paramTyper) block(info *plansql.SelectInfo, outer *scope, ctes []plansql.CTEDef) {
	if info == nil {
		return
	}
	if info.Union != nil {
		pt.block(info.Union.Left, outer, ctes)
		pt.block(info.Union.Right, outer, ctes)
	}
	sc := &scope{outer: outer, ctes: append([]plansql.CTEDef(nil), info.CTEs...)}
	all := append(sc.allCTEs(), ctes...)
	for i := range info.CTEs {
		body, err := info.CTEs[i].BodySelect()
		if err == nil {
			pt.block(body, outer, all)
		}
	}
	for i := range info.Tables {
		t := &info.Tables[i]
		if sub, err := t.SubSelect(); err == nil && sub != nil {
			pt.block(sub, outer, all)
		}
		for _, a := range t.FuncArgExprs {
			pt.expr(a, &scope{outer: outer, ctes: all})
		}
		sc.sources = append(sc.sources, pt.source(*t, all))
	}
	for _, j := range info.Joins {
		var t plansql.TableRef
		if j.RightTableRef != nil {
			t = *j.RightTableRef
		} else {
			t = plansql.TableRef{Name: j.RightTable, Alias: j.RightAlias}
		}
		if sub, err := t.SubSelect(); err == nil && sub != nil {
			pt.block(sub, outer, all)
		}
		sc.sources = append(sc.sources, pt.source(t, all))
	}
	for _, j := range info.Joins {
		pt.predicate(j.CondExpr, sc)
	}
	for _, col := range info.Columns {
		if col.Star {
			continue
		}
		node := col.ASTExpr
		if node == nil && col.Expr != "" {
			node, _ = plansql.ParseExpression(col.Expr)
		}
		if n := sentinelNum(node); n > 0 {
			// A bare select-list parameter is required to be nothing:
			// PostgreSQL resolves it as text.
			pt.assign(n, oidText)
			continue
		}
		pt.expr(node, sc)
	}
	pt.predicate(info.WhereExpr, sc)
	pt.predicate(info.HavingExpr, sc)
	pt.predicate(info.QualifyExpr, sc)
	for _, g := range info.GroupByExprs {
		pt.expr(g, sc)
	}
	for _, o := range info.OrderBy {
		pt.expr(o.Expr, sc)
	}
}

// predicate walks a boolean clause; a bare parameter there is a boolean.
func (pt *paramTyper) predicate(n plansql.Node, sc *scope) {
	if k := sentinelNum(n); k > 0 {
		pt.assign(k, oidBool)
		return
	}
	pt.expr(n, sc)
}

// exprText parses and walks a clause the parser kept as text.
func (pt *paramTyper) exprText(text string, sc *scope) {
	if strings.TrimSpace(text) == "" {
		return
	}
	n, err := plansql.ParseExpression(text)
	if err != nil {
		return
	}
	pt.predicate(n, sc)
}

// subquery walks a subquery's block with the current scope as its outer one.
func (pt *paramTyper) subquery(text string, sc *scope) {
	pq, err := plansql.Parse(text)
	if err != nil || pq == nil || pq.SelectInfo == nil {
		return
	}
	pt.block(pq.SelectInfo, sc, sc.allCTEs())
}

// sentinelNum is the parameter number a node stands for, or 0.
func sentinelNum(n plansql.Node) int {
	for {
		p, ok := n.(*plansql.ParenNode)
		if !ok {
			break
		}
		n = p.Inner
	}
	ref, ok := n.(*plansql.ColRef)
	if !ok || ref.Table != "" || !strings.HasPrefix(ref.Column, paramSentinel) {
		return 0
	}
	k, err := strconv.Atoi(ref.Column[len(paramSentinel):])
	if err != nil {
		return 0
	}
	return k
}

// typeOf is the OID the planner declares for n in scope sc, or 0 when the
// declaration walk does not decide one. A column reference answers its
// column's own declaration — the OID a RowDescription would carry for it,
// varchar included.
func (pt *paramTyper) typeOf(n plansql.Node, sc *scope) uint32 {
	if n == nil || sentinelNum(n) > 0 {
		return 0
	}
	inner := n
	for {
		p, ok := inner.(*plansql.ParenNode)
		if !ok {
			break
		}
		inner = p.Inner
	}
	if ref, ok := inner.(*plansql.ColRef); ok {
		if m, ok := sc.lookup(ref); ok {
			return uint32(pgColumnOID(m))
		}
		return 0
	}
	if containsSentinel(inner) {
		return 0
	}
	d, conf := physical.DeclaredTypeOfNode(inner, sc.schema())
	if conf != expr.Decided {
		return 0
	}
	return declOID(d)
}

// declOID maps a declaration onto the OID PostgreSQL names for it. A FLOAT64
// carrier PostgreSQL types numeric (ADR-0024's category) is numeric here: the
// parameter takes PostgreSQL's type, not the carrier's.
func declOID(d expr.DeclType) uint32 {
	switch {
	case d.Untyped:
		return 0
	case d.ID == parquet.TypeDecimal, d.ID == parquet.TypeFloat64 && d.PGNumeric:
		return oidNumeric
	case d.ID == parquet.TypeArray, d.ID == parquet.TypeRow, d.ID == parquet.TypeMap:
		return 0
	}
	return uint32(pgTypeOID(d.ID.String()))
}

// containsSentinel reports whether any placeholder sits inside n — an
// expression over a parameter has no declaration of its own to offer.
func containsSentinel(n plansql.Node) bool {
	found := false
	plansql.RewriteExpr(n, func(x plansql.Node) (plansql.Node, bool) {
		if sentinelNum(x) > 0 {
			found = true
		}
		return nil, false
	})
	return found
}

// peers types every parameter among nodes by the first node that is not one —
// the rule for a comparison's two operands, an IN list and its subject, a
// BETWEEN's three, COALESCE's arguments and a CASE's results: PostgreSQL
// resolves an unknown operand to its known peers' type.
func (pt *paramTyper) peers(sc *scope, nodes ...plansql.Node) {
	var oid uint32
	for _, n := range nodes {
		if sentinelNum(n) == 0 {
			if oid = pt.typeOf(n, sc); oid != 0 {
				break
			}
		}
	}
	allParams := true
	for _, n := range nodes {
		if sentinelNum(n) == 0 {
			allParams = false
		}
	}
	if allParams {
		// Nothing known among them: PostgreSQL resolves unknown = unknown
		// as text.
		oid = oidText
	}
	for _, n := range nodes {
		if k := sentinelNum(n); k > 0 {
			pt.assign(k, oid)
		}
	}
}

// expr walks one expression tree, typing every parameter whose position it
// can decide.
func (pt *paramTyper) expr(n plansql.Node, sc *scope) {
	switch e := n.(type) {
	case nil:
		return
	case *plansql.ParenNode:
		pt.expr(e.Inner, sc)
	case *plansql.CmpExpr:
		pt.peers(sc, e.Left, e.Right)
		pt.expr(e.Left, sc)
		pt.expr(e.Right, sc)
	case *plansql.BinaryOp:
		if e.Op == "||" {
			for _, side := range []plansql.Node{e.Left, e.Right} {
				if k := sentinelNum(side); k > 0 {
					pt.assign(k, oidText)
				}
			}
		} else if sentinelNum(e.Left) == 0 || sentinelNum(e.Right) == 0 {
			pt.peers(sc, e.Left, e.Right)
		}
		pt.expr(e.Left, sc)
		pt.expr(e.Right, sc)
	case *plansql.UnaryOp:
		pt.expr(e.Inner, sc)
	case *plansql.InExpr:
		if len(e.Values) == 1 {
			if sq, ok := e.Values[0].(*plansql.SubqueryNode); ok {
				pt.expr(e.Left, sc)
				pt.subquery(sq.SQL, sc)
				return
			}
		}
		pt.peers(sc, append([]plansql.Node{e.Left}, e.Values...)...)
		pt.expr(e.Left, sc)
		for _, v := range e.Values {
			pt.expr(v, sc)
		}
	case *plansql.AnyAllExpr:
		if len(e.Values) == 1 {
			if sq, ok := e.Values[0].(*plansql.SubqueryNode); ok {
				pt.expr(e.Left, sc)
				pt.subquery(sq.SQL, sc)
				return
			}
		}
		pt.peers(sc, append([]plansql.Node{e.Left}, e.Values...)...)
		pt.expr(e.Left, sc)
		for _, v := range e.Values {
			pt.expr(v, sc)
		}
	case *plansql.BetweenExpr:
		pt.peers(sc, e.Left, e.Low, e.High)
		pt.expr(e.Left, sc)
		pt.expr(e.Low, sc)
		pt.expr(e.High, sc)
	case *plansql.LikeExpr:
		for _, side := range []plansql.Node{e.Left, e.Pattern} {
			if k := sentinelNum(side); k > 0 {
				pt.assign(k, oidText)
			}
		}
		pt.expr(e.Left, sc)
		pt.expr(e.Pattern, sc)
	case *plansql.IsExpr:
		if e.Check == "true" || e.Check == "false" {
			pt.predicate(e.Left, sc)
			return
		}
		pt.expr(e.Left, sc)
	case *plansql.AndNode:
		pt.predicate(e.Left, sc)
		pt.predicate(e.Right, sc)
	case *plansql.OrNode:
		pt.predicate(e.Left, sc)
		pt.predicate(e.Right, sc)
	case *plansql.NotNode:
		pt.predicate(e.Inner, sc)
	case *plansql.CastNode:
		if k := sentinelNum(e.Inner); k > 0 {
			pt.assign(k, castOID(e))
			return
		}
		pt.expr(e.Inner, sc)
	case *plansql.CaseNode:
		if e.Subject != nil {
			subj := []plansql.Node{e.Subject}
			for _, w := range e.Whens {
				subj = append(subj, w.Cond)
			}
			pt.peers(sc, subj...)
			pt.expr(e.Subject, sc)
			for _, w := range e.Whens {
				pt.expr(w.Cond, sc)
			}
		} else {
			for _, w := range e.Whens {
				pt.predicate(w.Cond, sc)
			}
		}
		var results []plansql.Node
		for _, w := range e.Whens {
			results = append(results, w.Result)
		}
		if e.Else != nil {
			results = append(results, e.Else)
		}
		if anyNonParam(results) {
			pt.peers(sc, results...)
		}
		for _, r := range results {
			pt.expr(r, sc)
		}
	case *plansql.FuncCallNode:
		pt.call(e, sc)
	case *plansql.WindowFuncNode:
		if e.Func != nil {
			pt.windowCall(e.Func, sc)
		}
		for _, p := range e.PartitionBy {
			pt.expr(p, sc)
		}
		for _, o := range e.OrderBy {
			pt.expr(o.Expr, sc)
		}
	case *plansql.TupleNode:
		for _, x := range e.Elements {
			pt.expr(x, sc)
		}
	case *plansql.ArrayLitNode:
		if anyNonParam(e.Elements) {
			pt.peers(sc, e.Elements...)
		}
		for _, x := range e.Elements {
			pt.expr(x, sc)
		}
	case *plansql.SubqueryNode:
		pt.subquery(e.SQL, sc)
	case *plansql.ExistsNode:
		pt.subquery(e.SQL, sc)
	}
}

func anyNonParam(nodes []plansql.Node) bool {
	for _, n := range nodes {
		if sentinelNum(n) == 0 {
			return true
		}
	}
	return false
}

// call types the parameters of a function call whose arguments share one
// type (COALESCE, NULLIF, GREATEST, LEAST) by their known peers, and walks
// every argument. Any other function's argument type is its signature's,
// which this walk does not resolve; such a parameter stays undecided.
func (pt *paramTyper) call(f *plansql.FuncCallNode, sc *scope) {
	switch strings.ToLower(f.Name) {
	case "coalesce", "nullif", "greatest", "least":
		if anyNonParam(f.Args) {
			pt.peers(sc, f.Args...)
		}
	case "lag", "lead", "ntile", "nth_value", "first_value", "last_value":
		pt.windowCall(f, sc)
		return
	}
	for _, a := range f.Args {
		pt.expr(a, sc)
	}
}

// windowCall types a window function's arguments by its signature: LAG and
// LEAD take an integer offset and a default of the value's type, NTILE an
// integer bucket count, NTH_VALUE an integer position.
func (pt *paramTyper) windowCall(f *plansql.FuncCallNode, sc *scope) {
	switch strings.ToLower(f.Name) {
	case "lag", "lead":
		if len(f.Args) > 1 {
			if k := sentinelNum(f.Args[1]); k > 0 {
				pt.assign(k, oidInt4)
			}
		}
		if len(f.Args) > 2 {
			pt.peers(sc, f.Args[0], f.Args[2])
		}
	case "ntile":
		if len(f.Args) > 0 {
			if k := sentinelNum(f.Args[0]); k > 0 {
				pt.assign(k, oidInt4)
			}
		}
	case "nth_value":
		if len(f.Args) > 1 {
			if k := sentinelNum(f.Args[1]); k > 0 {
				pt.assign(k, oidInt4)
			}
		}
	}
	for _, a := range f.Args {
		pt.expr(a, sc)
	}
}

// castOID is the OID of a CAST's target type, read by the type grammar a
// column declaration uses — `CAST($1 AS INTEGER)` takes integer, whatever
// width the engine's integer CAST then carries.
func castOID(c *plansql.CastNode) uint32 {
	col, err := parquet.ResolveColumn("p", c.TypeName)
	if err != nil {
		return 0
	}
	switch col.Type {
	case parquet.TypeArray:
		return uint32(pgArrayColumnOID(col.ElementType))
	case parquet.TypeRow, parquet.TypeMap:
		return 0
	}
	m := wadjet.ColumnMeta{TypeID: col.Type, TypeName: col.Type.String()}
	if col.Type == parquet.TypeString && isVarcharName(c.TypeName) {
		m.StringLength = parquet.StringLengthUnconstrainedVarchar
	}
	return uint32(pgColumnOID(m))
}

// isVarcharName reports the character-varying spellings, which PostgreSQL
// types varchar rather than text.
func isVarcharName(name string) bool {
	u := strings.ToUpper(strings.TrimSpace(name))
	return strings.HasPrefix(u, "VARCHAR") || strings.HasPrefix(u, "CHARACTER VARYING")
}

// insert types an INSERT's VALUES positions by their target columns, and an
// INSERT … SELECT's bare select-list parameters the same way.
func (pt *paramTyper) insert(ins *plansql.InsertInfo) {
	if ins == nil {
		return
	}
	targets := pt.probe(plansql.TableRef{Name: ins.Table}, nil)
	target := func(j int) (wadjet.ColumnMeta, bool) {
		if len(ins.Columns) == 0 {
			if j < len(targets) {
				return targets[j], true
			}
			return wadjet.ColumnMeta{}, false
		}
		if j >= len(ins.Columns) {
			return wadjet.ColumnMeta{}, false
		}
		for _, m := range targets {
			if strings.EqualFold(m.Name, ins.Columns[j]) || m.Name == plansql.FoldIdent(ins.Columns[j]) {
				return m, true
			}
		}
		return wadjet.ColumnMeta{}, false
	}
	empty := &scope{}
	for _, row := range ins.Values {
		for j, text := range row {
			n, err := plansql.ParseExpression(text)
			if err != nil {
				continue
			}
			if k := sentinelNum(n); k > 0 {
				if m, ok := target(j); ok {
					pt.assign(k, uint32(pgColumnOID(m)))
				}
				continue
			}
			pt.expr(n, empty)
		}
	}
	if ins.Select != nil && ins.Select.SelectInfo != nil {
		si := ins.Select.SelectInfo
		if si.Union == nil {
			for j, col := range si.Columns {
				if k := sentinelNum(col.ASTExpr); k > 0 {
					if m, ok := target(j); ok {
						pt.assign(k, uint32(pgColumnOID(m)))
					}
				}
			}
		}
		pt.block(si, nil, nil)
	}
}

// update types SET targets by their columns and walks the WHERE clause over
// the target table.
func (pt *paramTyper) update(up *plansql.UpdateInfo) {
	if up == nil {
		return
	}
	sc := pt.targetScope(up.Table, up.Qualifier, up.Alias)
	for _, set := range up.SetClauses {
		pt.setClause(set.Column, set.Value, sc)
	}
	pt.exprText(up.WhereSQL, sc)
}

// setClause types `col = value`: a bare parameter takes the column's type,
// and an expression is walked in scope.
func (pt *paramTyper) setClause(column, value string, sc *scope) {
	n, err := plansql.ParseExpression(value)
	if err != nil {
		return
	}
	if k := sentinelNum(n); k > 0 {
		if m, ok := sc.lookup(&plansql.ColRef{Column: column}); ok {
			pt.assign(k, uint32(pgColumnOID(m)))
		}
		return
	}
	pt.expr(n, sc)
}

// merge types a MERGE's ON condition, its WHEN conditions and its actions
// over the target and the source.
func (pt *paramTyper) merge(m *plansql.MergeInfo) {
	if m == nil {
		return
	}
	target := pt.source(plansql.TableRef{Name: m.Target, Qualifier: m.TargetQualifier, Alias: m.TargetAlias}, nil)
	src := plansql.TableRef{Name: m.Source, Alias: m.SourceAlias}
	if strings.HasPrefix(strings.TrimSpace(m.Source), "(") {
		src.Name = strings.TrimSpace(m.Source)
		if sub, err := src.SubSelect(); err == nil && sub != nil {
			pt.block(sub, nil, nil)
		}
	}
	sc := &scope{sources: []scopeSource{target, pt.source(src, nil)}}
	tsc := &scope{sources: []scopeSource{target}}
	pt.exprText(m.OnCondition, sc)
	for _, w := range m.WhenClauses {
		pt.exprText(w.Condition, sc)
		body := strings.TrimSpace(w.SQL)
		switch strings.ToUpper(w.Action) {
		case "UPDATE":
			body = strings.TrimSpace(trimKeyword(body, "UPDATE"))
			body = strings.TrimSpace(trimKeyword(body, "SET"))
			for _, item := range splitTopLevel(body, ',') {
				eq := strings.IndexByte(item, '=')
				if eq < 0 {
					continue
				}
				col := strings.TrimSpace(item[:eq])
				if dot := strings.LastIndexByte(col, '.'); dot >= 0 {
					col = col[dot+1:]
				}
				col = plansql.FoldIdent(strings.Trim(col, `"`))
				val := item[eq+1:]
				n, err := plansql.ParseExpression(val)
				if err != nil {
					continue
				}
				if k := sentinelNum(n); k > 0 {
					if mm, ok := tsc.lookup(&plansql.ColRef{Column: col}); ok {
						pt.assign(k, uint32(pgColumnOID(mm)))
					}
					continue
				}
				pt.expr(n, sc)
			}
		case "INSERT":
			pt.mergeInsert(trimKeyword(body, "INSERT"), target, sc)
		}
	}
}

// mergeInsert types `[(cols)] VALUES (vals)` by the target's columns.
func (pt *paramTyper) mergeInsert(body string, target scopeSource, sc *scope) {
	body = strings.TrimSpace(body)
	var cols []string
	if strings.HasPrefix(body, "(") {
		end := matchParen(body, 0)
		if end < 0 {
			return
		}
		for _, c := range splitTopLevel(body[1:end], ',') {
			cols = append(cols, plansql.FoldIdent(strings.Trim(strings.TrimSpace(c), `"`)))
		}
		body = strings.TrimSpace(body[end+1:])
	}
	body = strings.TrimSpace(trimKeyword(body, "VALUES"))
	if !strings.HasPrefix(body, "(") {
		return
	}
	end := matchParen(body, 0)
	if end < 0 {
		return
	}
	for j, v := range splitTopLevel(body[1:end], ',') {
		n, err := plansql.ParseExpression(v)
		if err != nil {
			continue
		}
		if k := sentinelNum(n); k > 0 {
			var m wadjet.ColumnMeta
			ok := false
			if cols == nil && j < len(target.cols) {
				m, ok = target.cols[j], true
			} else if j < len(cols) {
				for _, tc := range target.cols {
					if strings.EqualFold(tc.Name, cols[j]) {
						m, ok = tc, true
					}
				}
			}
			if ok {
				pt.assign(k, uint32(pgColumnOID(m)))
			}
			continue
		}
		pt.expr(n, sc)
	}
}

// trimKeyword drops a leading keyword (case-insensitive) from s.
func trimKeyword(s, kw string) string {
	t := strings.TrimSpace(s)
	if len(t) >= len(kw) && strings.EqualFold(t[:len(kw)], kw) &&
		(len(t) == len(kw) || !isWordByte(t[len(kw)])) {
		return t[len(kw):]
	}
	return s
}

// matchParen returns the index of the parenthesis closing the one at open,
// skipping quoted text, or -1.
func matchParen(s string, open int) int {
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '\'', '"':
			q := s[i]
			for i++; i < len(s) && s[i] != q; i++ {
			}
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// splitTopLevel splits s at sep outside parentheses and quotes.
func splitTopLevel(s string, sep byte) []string {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'', '"':
			q := s[i]
			for i++; i < len(s) && s[i] != q; i++ {
			}
		case '(':
			depth++
		case ')':
			depth--
		case sep:
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
}
