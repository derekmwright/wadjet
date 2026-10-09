// SPDX-License-Identifier: MIT

package physical

import (
	"fmt"
	"strings"
	"sync"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// annotateSubqueryColumnDecls stamps the declared output column of every
// SCALAR SUBQUERY in a plan onto the plan's nodes, keyed by the subquery's own
// SQL text — the key `NodeDeclaredType` already resolves a subquery by.
//
// WHY A STAMP AND NOT A RESOLVER. A subquery is a whole second query whose type
// lives in the CATALOG, so it can only be answered by planning it — and the
// declaration walks (EmittedColTypes, emittedColDecimal, emittedColIntWidth)
// are free functions over the logical tree that hold no Planner.
// `ColDecls.subqueryDecl` was therefore nil in every one of them, and the only
// caller that passed a resolver was `DeclaredOutputSchema` at the OUTPUT
// projection. A scalar-subquery column MATERIALIZED one level down — by a
// derived table, a CTE or a set-operation arm — was declared STRING, and every
// reader above it fell to float64: `SELECT SUM(v) FROM (SELECT (SELECT c & 3
// FROM u) AS v FROM t) s` declared OID 701 on all five arms and both wire
// formats where PostgreSQL declares bigint, and the int8, bare-int8-column and
// COUNT(*) forms declared 701 where PostgreSQL declares numeric (#1018
// review, P2). A float64 accumulator over a wide bigint drops digits past 2^53,
// which is the class ADR-0024 exists to prevent.
//
// Threading a resolver through the walks instead would make each of them plan a
// second query AT EVERY LEVEL of a nested derived table, which is exactly the
// cost #1034 records against the declaration walks. This runs ONCE per plan,
// beside the pass that already puts ScanColTypes on a Scan, and adds no walk:
// ONE map is built and the same map is shared by every node, so whichever node
// a walk happens to hold can ask.
func (p *Planner) annotateSubqueryColumnDecls(node *logical.Node) {
	if p == nil || node == nil {
		return
	}
	decls := map[string]logical.SubqueryColumnDecl{}
	respelled := map[string]bool{}
	for _, sq := range collectPlanSubqueries(node) {
		// Each subquery is declared in the WITH chain where it is written
		// (subqueryScopingIn): this pass runs before Plan sets the
		// statement's list, and a subquery in a nested block reads that
		// block's items (#1603, #1602).
		dp := p
		if sp, ok := p.forSubqueryNode(sq.node); ok {
			dp = sp
		}
		declSQL := dp.subqueryDeclSQLOf(sq.node, sq.scope)
		// One text can be met twice: `SUM((SELECT c.i …))` is written in the
		// Project ABOVE the aggregate, whose input has no `c.i`, and again as
		// the aggregate's argument, over the relation that does. The reading
		// that could type the outer references wins.
		if _, done := decls[sq.sql]; done && (respelled[sq.sql] || declSQL == sq.sql) {
			continue
		}
		// A CORRELATED subquery is declared with its outer references typed
		// as the columns of the relation it sits over — the compile site's
		// own reading (subqueryDeclOptionFor), so the vector a projection
		// allocates and the operand the kernels classify are one type
		// (#1422). The stamp stays keyed by the text as written.
		d, ok := dp.scalarSubqueryColumnDecl(declSQL)
		if !ok {
			continue
		}
		// PostgreSQL's numeric CATEGORY is read off the text AS WRITTEN
		// wherever that text can answer it: the rebuild spells a construct
		// by the function it evaluates — EXTRACT(EPOCH FROM …) as epoch(…),
		// a float — and the category is the SQL type the user's spelling
		// has (numeric), which an INTEGER assignment rounds by (#1353). The
		// respelled text answers only what the written one cannot.
		if declSQL != sq.sql {
			if cat := dp.subqueryOutputPGCategory(sq.sql); cat != pgCatUnknown {
				d.PGCategory = cat
			}
		}
		decls[sq.sql] = d
		respelled[sq.sql] = declSQL != sq.sql
	}
	if len(decls) == 0 {
		return
	}
	shareSubqueryColDecls(node, decls)
}

func shareSubqueryColDecls(n *logical.Node, decls map[string]logical.SubqueryColumnDecl) {
	if n == nil {
		return
	}
	n.SubqueryColDecls = decls
	for _, c := range n.Children {
		shareSubqueryColDecls(c, decls)
	}
}

// collectPlanSubqueries is every scalar subquery written anywhere in a plan's
// expressions. A subquery BURIED in arithmetic counts as much as one that is a
// whole SELECT item: the arithmetic rule types itself from its operands, and an
// operand nobody declared is what sends the whole expression to the float rule.
//
// Each comes with its SCOPE — the one input of the node whose expression
// holds it, which is the relation a correlated subquery's outer references
// read — or nil where the node has no single input.
func collectPlanSubqueries(n *logical.Node) []planSubquery {
	var out []planSubquery
	var walk func(*logical.Node)
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		var scope *logical.Node
		if len(n.Children) == 1 {
			scope = n.Children[0]
		}
		var texts []*plansql.SubqueryNode
		for _, proj := range n.Projections {
			collectSubqueryNodes(proj.ASTExpr, &texts)
		}
		for _, agg := range n.AggExprs {
			collectSubqueryNodes(agg.InputExpr, &texts)
		}
		for _, we := range n.WindowExprs {
			collectSubqueryNodes(we.InputExpr, &texts)
			// The PARTITION BY / ORDER BY terms are materialized keys too,
			// compiled with these declarations (windowKeyProjections) from
			// the same parse of their text; a subquery in one that was
			// never stamped was declared FLOAT64 while the kernel computed
			// `(SELECT z.v …) * t.n` as the exact numeric, and #361's guard
			// failed the key's store on every arm.
			for _, term := range we.PartitionBy {
				if ast, err := plansql.ParseExpression(term); err == nil {
					collectSubqueryNodes(ast, &texts)
				}
			}
			for _, ob := range we.OrderBy {
				if ast, err := plansql.ParseExpression(ob.Column); err == nil {
					collectSubqueryNodes(ast, &texts)
				}
			}
		}
		for _, sq := range texts {
			out = append(out, planSubquery{sql: sq.SQL, node: sq, scope: scope})
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(n)
	return out
}

// planSubquery is one scalar subquery — its text and its node, which records
// the WITH chain it is planned in — and the relation it sits over.
type planSubquery struct {
	sql   string
	node  *plansql.SubqueryNode
	scope *logical.Node
}

// collectSubqueryNodes descends an expression for SubqueryNode NODES, whose
// memo records the WITH chain each is declared in. It is the binder's
// walkExpr shape one package over, kept here because this package's node set
// is the one the planner rewrites.
func collectSubqueryNodes(e plansql.Node, out *[]*plansql.SubqueryNode) {
	var walk func(plansql.Node)
	walk = func(e plansql.Node) {
		switch n := e.(type) {
		case nil:
			return
		case *plansql.SubqueryNode:
			*out = append(*out, n)
		case *plansql.ParenNode:
			walk(n.Inner)
		case *plansql.UnaryOp:
			walk(n.Inner)
		case *plansql.CastNode:
			walk(n.Inner)
		case *plansql.BinaryOp:
			walk(n.Left)
			walk(n.Right)
		case *plansql.CmpExpr:
			walk(n.Left)
			walk(n.Right)
		case *plansql.FuncCallNode:
			for _, a := range n.Args {
				walk(a)
			}
		case *plansql.CaseNode:
			walk(n.Subject)
			for _, w := range n.Whens {
				walk(w.Cond)
				walk(w.Result)
			}
			walk(n.Else)
		}
	}
	walk(e)
}

// subqueryDeclsOf turns a node's stamped map into the three resolvers ColDecls
// carries: the declared COLUMN, its PostgreSQL integer WIDTH and its numeric
// CATEGORY. All are nil when nothing was stamped, which is the "this caller
// cannot ask" the SubqueryNode arms already decline on.
func subqueryDeclsOf(n *logical.Node) (func(string) (parquet.Column, bool), func(string) (intWidth, bool), func(string) pgCategory) {
	if n == nil || len(n.SubqueryColDecls) == 0 {
		return nil, nil, nil
	}
	m := n.SubqueryColDecls
	return subqueryResolvers(func(sql string) (logical.SubqueryColumnDecl, bool) {
		d, ok := m[sql]
		return d, ok
	})
}

// subqueryResolvers is the three ColDecls resolvers over one lookup of a
// subquery's declaration: a plan's stamped map (subqueryDeclsOf), or a
// Planner's memoized scalarSubqueryColumnDecl for a caller that evaluates an
// expression outside any plan (DeclaredTypeOfNodeWith).
func subqueryResolvers(lookup func(string) (logical.SubqueryColumnDecl, bool)) (func(string) (parquet.Column, bool), func(string) (intWidth, bool), func(string) pgCategory) {
	return func(sql string) (parquet.Column, bool) {
			d, ok := lookup(sql)
			if !ok {
				return parquet.Column{}, false
			}
			return parquet.Column{Type: d.Type, Precision: d.Precision, Scale: d.Scale,
				ElementType: d.ElementType, Fields: d.Fields}, true
		}, func(sql string) (intWidth, bool) {
			d, ok := lookup(sql)
			if !ok {
				return intWidthUnknown, false
			}
			switch d.IntWidth {
			case 4:
				return intWidth4, true
			case 8:
				return intWidth8, true
			}
			return intWidthUnknown, false
		}, func(sql string) pgCategory {
			d, _ := lookup(sql)
			return d.PGCategory
		}
}

// withSubqueryDecls is decls plus whatever node carries the stamp — the one
// line every walk needs so a subquery's declaration reaches it.
func withSubqueryDecls(decls ColDecls, n *logical.Node) ColDecls {
	d, w, pg := subqueryDeclsOf(n)
	if d == nil {
		return decls
	}
	decls.subqueryDecl = d
	decls.subqueryIntWidth = w
	decls.subqueryPGCategory = pg
	return decls
}

// scalarSubqueryColumnDecl is subqueryOutputColumn plus the facts a bare
// parquet.Column cannot carry: PostgreSQL's INTEGER WIDTH, which decides
// whether SUM over the column is bigint or numeric, and its numeric CATEGORY
// (subqueryOutputPGCategory).
//
// MEMOIZED per Planner, keyed by the subquery's SQL. subqueryOutputColumn
// parses, builds and annotates a whole second plan, and that annotation runs
// this pass over the INNER plan too — so without the memo a query with nested
// subqueries would re-plan the same text once per level. The in-flight sentinel
// stops a self-referencing text from recursing.
func (p *Planner) scalarSubqueryColumnDecl(sql string) (decl logical.SubqueryColumnDecl, ok bool) {
	// The same guard subqueryOutputColumn carries, and for the same reason:
	// resolving a declaration plans a SECOND QUERY, and a declaration nobody
	// can compute is a missing entry — never a crashed statement. The WIDTH
	// half walks a tree the type half does not (emittedColIntWidth over the
	// subquery's own plan), so it needs the guard too.
	defer func() {
		if r := recover(); r != nil {
			decl, ok = logical.SubqueryColumnDecl{}, false
		}
	}()
	if p.subqueryDeclCache == nil {
		// A Planner built without NewPlanner (a test's literal) gets its memo
		// on first use, which is at plan time, on the planning goroutine.
		p.subqueryDeclCache = &subqueryDeclMemo{}
	}
	memo := p.subqueryDeclCache
	// One text declares differently in two WITH chains (a nested item that
	// reuses a name), so the memo is keyed by the chain it was planned in.
	key := sql + cteChainKey(p.Ctes)
	if e, seen := memo.claim(key); seen {
		if e == nil {
			return logical.SubqueryColumnDecl{}, false // in flight
		}
		return e.decl, e.ok
	}
	// ONE plan of the body answers all three questions (arc CI3 round 2,
	// B3): the integer width and PostgreSQL's category are read off the plan
	// as built, and the answer column off the same plan once markScalarAnswer
	// has marked it — the order the three separate plans had, each of which
	// re-declared every subquery nested in the body.
	plan := p.subqueryLogicalPlan(sql)
	var (
		width int
		cat   = pgCatUnknown
	)
	if plan != nil {
		width = p.intWidthOfPlan(plan)
		cat = pgCategoryOfPlan(plan)
		markScalarAnswer(plan)
	}
	col, ok := p.subqueryOutputColumnOfPlan(plan)
	d := logical.SubqueryColumnDecl{}
	if ok {
		if !carriesIntWidth(col.Type) {
			width = 0
		}
		d = logical.SubqueryColumnDecl{
			Type: col.Type, Precision: col.Precision, Scale: col.Scale,
			IntWidth:    width,
			ElementType: col.ElementType, Fields: col.Fields,
			PGCategory: cat,
		}
		// A DECIMAL without its scale is not a declaration: a vector built
		// from it reads every value at the wrong power of ten, which is why
		// NodeDeclaredType declines the same shape (ADR-0024 item 2).
		if d.Type == parquet.TypeDecimal && d.Precision == 0 {
			ok = false
		}
	}
	memo.store(key, &subqueryDeclEntry{decl: d, ok: ok})
	return d, ok
}

// cteChainKey names a WITH chain by its items' identities, for a memo key: two
// chains that bind a name to different items never share an entry.
func cteChainKey(chain []plansql.CTEDef) string {
	if len(chain) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("\x00")
	for i := range chain {
		if id := chain[i].Identity(); id != nil {
			fmt.Fprintf(&b, "%p;", id)
		} else {
			b.WriteString(chain[i].Name + "\x01" + chain[i].SQL + ";")
		}
	}
	return b.String()
}

// subqueryDeclEntry is one memo slot; a nil slot means "in flight".
type subqueryDeclEntry struct {
	decl logical.SubqueryColumnDecl
	ok   bool
}

// subqueryDeclMemo is the per-build declaration memo, locked: it is read and
// written from every pipeline goroutine that re-plans a correlated subquery's
// per-row text (Planner.subqueryDeclCache). The lock is held for the map
// operations only, never across the planning between claim and store, which
// re-enters this memo for the nested texts; a text another goroutine has in
// flight answers "not declared" exactly as a self-reference does, as the
// unlocked map answered it.
type subqueryDeclMemo struct {
	mu sync.Mutex
	m  map[string]*subqueryDeclEntry
}

// claim returns the slot for sql if one exists; otherwise it marks sql in
// flight and reports seen=false, and the caller must store its answer.
func (m *subqueryDeclMemo) claim(sql string) (e *subqueryDeclEntry, seen bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, seen = m.m[sql]; seen {
		return e, true
	}
	if m.m == nil {
		m.m = map[string]*subqueryDeclEntry{}
	}
	m.m[sql] = nil
	return nil, false
}

func (m *subqueryDeclMemo) store(sql string, e *subqueryDeclEntry) {
	m.mu.Lock()
	m.m[sql] = e
	m.mu.Unlock()
}

// subqueryOutputPGCategory is PostgreSQL's numeric CATEGORY of a scalar
// subquery's single output column, read off the subquery's own plan with the
// declaredOutputPGCategory every INSERT … SELECT reads: `(SELECT SQRT(k2.d)
// FROM k k2 WHERE …)` is computed in a double and is numeric in PostgreSQL.
// The bare parquet.Column the type half returns cannot say it, so a subquery
// with a FROM rounded as a float8 into an integer column (#1353). Unknown —
// the carrier's reading — for a plan the walk cannot read.
func (p *Planner) subqueryOutputPGCategory(sql string) pgCategory {
	return pgCategoryOfPlan(p.subqueryLogicalPlan(sql))
}

// pgCategoryOfPlan is subqueryOutputPGCategory over a plan already built.
func pgCategoryOfPlan(plan *logical.Node) pgCategory {
	pg := declaredOutputPGCategory(plan)
	if len(pg) != 1 {
		return pgCatUnknown
	}
	return pg[0]
}

// subqueryOutputIntWidth is the declared INTEGER WIDTH of a scalar subquery's
// single output column, read off the subquery's own plan with the same
// emittedColIntWidth every other consumer reads. 0 — "say nothing" — for a
// non-integer carrier and for a column the walk cannot prove, which leaves the
// reader on the carrier exactly as an absent ColDecls entry does.
func (p *Planner) subqueryOutputIntWidth(sql string, carrier parquet.TypeID) int {
	if !carriesIntWidth(carrier) {
		return 0
	}
	return p.intWidthOfPlan(p.subqueryLogicalPlan(sql))
}

// intWidthOfPlan is subqueryOutputIntWidth over a plan already built, for an
// integer carrier.
func (p *Planner) intWidthOfPlan(plan *logical.Node) int {
	if plan == nil {
		return 0
	}
	schema := declaredOutputSchema(plan, p.SubqueryOutputColumn)
	if len(schema) != 1 {
		return 0
	}
	w, ok := lookupColIntWidth(emittedColIntWidth(plan), schema[0].Name)
	if !ok {
		return 0
	}
	switch w {
	case intWidth4:
		return 4
	case intWidth8:
		return 8
	}
	return 0
}
