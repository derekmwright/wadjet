package physical

import (
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// annotateSubqueryColumnDecls stamps the declared output column of every
// SCALAR SUBQUERY in a plan onto the plan's nodes, keyed by the subquery's own
// SQL text — the key `nodeDeclaredType` already resolves a subquery by.
//
// WHY A STAMP AND NOT A RESOLVER. A subquery is a whole second query whose type
// lives in the CATALOG, so it can only be answered by planning it — and the
// declaration walks (emittedColTypes, emittedColDecimal, emittedColIntWidth)
// are free functions over the logical tree that hold no Planner.
// `colDecls.subqueryDecl` was therefore nil in every one of them, and the only
// caller that passed a resolver was `declaredOutputSchema` at the OUTPUT
// projection. A scalar-subquery column MATERIALIZED one level down — by a
// derived table, a CTE or a set-operation arm — was declared STRING, and every
// reader above it fell to float64: `SELECT SUM(v) FROM (SELECT (SELECT c & 3
// FROM u) AS v FROM t) s` declared OID 701 on all five arms and both wire
// formats where PostgreSQL declares bigint, and the int8, bare-int8-column and
// COUNT(*) forms declared 701 where PostgreSQL declares numeric (#1018 round 5
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
	for _, sql := range collectPlanSubquerySQL(node) {
		if _, done := decls[sql]; done {
			continue
		}
		d, ok := p.scalarSubqueryColumnDecl(sql)
		if !ok {
			continue
		}
		decls[sql] = d
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

// collectPlanSubquerySQL is every scalar subquery written anywhere in a plan's
// expressions. A subquery BURIED in arithmetic counts as much as one that is a
// whole SELECT item: the arithmetic rule types itself from its operands, and an
// operand nobody declared is what sends the whole expression to the float rule.
func collectPlanSubquerySQL(n *logical.Node) []string {
	var out []string
	var walk func(*logical.Node)
	add := func(e plansql.Node) {
		collectSubquerySQL(e, &out)
	}
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		for _, proj := range n.Projections {
			add(proj.ASTExpr)
		}
		for _, agg := range n.AggExprs {
			add(agg.InputExpr)
		}
		for _, we := range n.WindowExprs {
			add(we.InputExpr)
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(n)
	return out
}

// collectSubquerySQL descends an expression for SubqueryNode texts. It is the
// binder's walkExpr shape one package over, kept here because this package's
// node set is the one the planner rewrites.
func collectSubquerySQL(e plansql.Node, out *[]string) {
	switch n := e.(type) {
	case nil:
		return
	case *plansql.SubqueryNode:
		*out = append(*out, n.SQL)
	case *plansql.ParenNode:
		collectSubquerySQL(n.Inner, out)
	case *plansql.UnaryOp:
		collectSubquerySQL(n.Inner, out)
	case *plansql.CastNode:
		collectSubquerySQL(n.Inner, out)
	case *plansql.BinaryOp:
		collectSubquerySQL(n.Left, out)
		collectSubquerySQL(n.Right, out)
	case *plansql.CmpExpr:
		collectSubquerySQL(n.Left, out)
		collectSubquerySQL(n.Right, out)
	case *plansql.FuncCallNode:
		for _, a := range n.Args {
			collectSubquerySQL(a, out)
		}
	case *plansql.CaseNode:
		collectSubquerySQL(n.Subject, out)
		for _, w := range n.Whens {
			collectSubquerySQL(w.Cond, out)
			collectSubquerySQL(w.Result, out)
		}
		collectSubquerySQL(n.Else, out)
	}
}

// subqueryDeclsOf turns a node's stamped map into the two resolvers colDecls
// carries: the declared COLUMN and its PostgreSQL integer WIDTH. Both are nil
// when nothing was stamped, which is the "this caller cannot ask" the
// SubqueryNode arms already decline on.
func subqueryDeclsOf(n *logical.Node) (func(string) (parquet.Column, bool), func(string) (intWidth, bool)) {
	if n == nil || len(n.SubqueryColDecls) == 0 {
		return nil, nil
	}
	m := n.SubqueryColDecls
	return func(sql string) (parquet.Column, bool) {
			d, ok := m[sql]
			if !ok {
				return parquet.Column{}, false
			}
			return parquet.Column{Type: d.Type, Precision: d.Precision, Scale: d.Scale}, true
		}, func(sql string) (intWidth, bool) {
			d, ok := m[sql]
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
		}
}

// withSubqueryDecls is decls plus whatever node carries the stamp — the one
// line every walk needs so a subquery's declaration reaches it.
func withSubqueryDecls(decls colDecls, n *logical.Node) colDecls {
	d, w := subqueryDeclsOf(n)
	if d == nil {
		return decls
	}
	decls.subqueryDecl = d
	decls.subqueryIntWidth = w
	return decls
}

// scalarSubqueryColumnDecl is subqueryOutputColumn plus the fact a bare
// parquet.Column cannot carry: PostgreSQL's INTEGER WIDTH, which decides
// whether SUM over the column is bigint or numeric.
//
// MEMOIZED per Planner, keyed by the subquery's SQL. subqueryOutputColumn
// parses, builds and annotates a whole second plan, and that annotation runs
// this pass over the INNER plan too — so without the memo a query with nested
// subqueries would re-plan the same text once per level. The in-flight sentinel
// stops a self-referencing text from recursing.
func (p *Planner) scalarSubqueryColumnDecl(sql string) (logical.SubqueryColumnDecl, bool) {
	if p.subqueryDeclCache == nil {
		p.subqueryDeclCache = map[string]*subqueryDeclEntry{}
	}
	if e, seen := p.subqueryDeclCache[sql]; seen {
		if e == nil {
			return logical.SubqueryColumnDecl{}, false // in flight
		}
		return e.decl, e.ok
	}
	p.subqueryDeclCache[sql] = nil
	col, ok := p.subqueryOutputColumn(sql)
	d := logical.SubqueryColumnDecl{}
	if ok {
		d = logical.SubqueryColumnDecl{
			Type: col.Type, Precision: col.Precision, Scale: col.Scale,
			IntWidth: p.subqueryOutputIntWidth(sql, col.Type),
		}
		// A DECIMAL without its scale is not a declaration: a vector built
		// from it reads every value at the wrong power of ten, which is why
		// nodeDeclaredType declines the same shape (ADR-0024 item 2).
		if d.Type == parquet.TypeDecimal && d.Precision == 0 {
			ok = false
		}
	}
	p.subqueryDeclCache[sql] = &subqueryDeclEntry{decl: d, ok: ok}
	return d, ok
}

// subqueryDeclEntry is one memo slot; a nil slot means "in flight".
type subqueryDeclEntry struct {
	decl logical.SubqueryColumnDecl
	ok   bool
}

// subqueryOutputIntWidth is the declared INTEGER WIDTH of a scalar subquery's
// single output column, read off the subquery's own plan with the same
// emittedColIntWidth every other consumer reads. 0 — "say nothing" — for a
// non-integer carrier and for a column the walk cannot prove, which leaves the
// reader on the carrier exactly as an absent colDecls entry does.
func (p *Planner) subqueryOutputIntWidth(sql string, carrier parquet.TypeID) int {
	if !carriesIntWidth(carrier) {
		return 0
	}
	plan := p.subqueryLogicalPlan(sql)
	if plan == nil {
		return 0
	}
	schema := declaredOutputSchema(plan, p.subqueryOutputColumn)
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
