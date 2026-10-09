// SPDX-License-Identifier: MIT

// This file holds the scope an expression subquery is planned in, governed by
// ADR-0032 and ADR-0047 (stage 3).
package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// AN EXPRESSION SUBQUERY IS PLANNED IN THE WITH CHAIN WHERE IT IS WRITTEN, FROM
// ITS ONE PARSE (ADR-0047 stage 3).
//
// The planner answered every expression subquery from the STATEMENT's WITH
// list (statementCTEs → Planner.Ctes), whichever block the subquery was
// written in. A subquery inside a derived table, a CTE body or another
// subquery that read a WITH item declared by THAT block planned a scan of a
// relation of the item's name, which no catalog has, and read zero rows in
// silence: `SELECT x.d FROM (WITH t AS (SELECT 7 AS r) SELECT (SELECT r FROM t)
// AS d) x` answered NULL for PostgreSQL 17.11's 7 (#1602), and a nested WITH
// reusing the statement WITH's name answered the statement's item inside its
// block (#1606). The declaration pass (annotateSubqueryColumnDecls) ran before
// Plan set the list at all, so `sum((SELECT g FROM s …))` over a CTE was
// declared from a relation `s` that does not exist and the sum read NULL
// (#1603).
//
// The logical builder records on each SubqueryNode / ExistsNode the chain in
// scope where it is written (plansql.StampSubqueryScopes), and every question
// the compiler asks of one subquery — its runner, its declaration, its column
// count, the relations its body may name — is
// answered by a child planner over THAT chain. A node with no recorded chain
// (an expression built outside a block) keeps the planner's own answers.
func (p *Planner) subqueryScopingIn(scope *logical.Node) expr.SubqueryScoping {
	return func(n plansql.Node) (expr.SubqueryHooks, bool) {
		sp, ok := p.forSubqueryNode(n)
		if !ok {
			return expr.SubqueryHooks{}, false
		}
		return expr.SubqueryHooks{
			Runner: sp.makeSubqueryRunner(),
			Decl:   sp.subqueryOutputColumnOf(n, scope),
			Cols:   sp.SubqueryOutputArity,
			Scope:  sp.SubqueryInnerColumns(),
		}, true
	}
}

// forSubqueryNode is a child planner for one expression subquery node: its
// WITH chain as the planner's list, and the node itself, so a plan of its
// text reads its memoized body (subqueryBodyFor). ok is false for a node
// that records no chain.
func (p *Planner) forSubqueryNode(n plansql.Node) (*Planner, bool) {
	chain, ok := subqueryChain(n)
	if !ok {
		return nil, false
	}
	sp := p.forSubquery()
	sp.Ctes = chain
	sp.memoSub = n
	return sp, true
}

// subqueryChain is the WITH chain a SubqueryNode or ExistsNode records.
func subqueryChain(n plansql.Node) ([]plansql.CTEDef, bool) {
	switch q := n.(type) {
	case *plansql.SubqueryNode:
		return q.CTEScope()
	case *plansql.ExistsNode:
		return q.CTEScope()
	}
	return nil, false
}

// subqueryBodyFor is the tree a plan of sql reads: the memoized body of the
// node this planner was made for when sql is that node's text — or that text
// with the evaluators' read bound appended (plansql.AppendRowLimit), planned
// as the body with the bound set — and a parse of sql otherwise (a correlated
// re-run's substituted text, a declaration's outer-typed text). release ends
// the hold on a shared body: the run-time planners that reach one body from
// several pipeline goroutines build it one at a time.
func (p *Planner) subqueryBodyFor(sql string) (info *plansql.SelectInfo, release func(), err error) {
	if body, unlock, ok := memoBodyFor(p.memoSub, sql); ok {
		return body, unlock, nil
	}
	pq, err := plansql.Parse(sql)
	if err != nil {
		return nil, func() {}, err
	}
	info, err = plansql.ExtractSelect(pq)
	return info, func() {}, err
}

func memoBodyFor(n plansql.Node, sql string) (*plansql.SelectInfo, func(), bool) {
	var text string
	var sel func() (*plansql.SelectInfo, error)
	var lock func() func()
	switch q := n.(type) {
	case *plansql.SubqueryNode:
		text, sel, lock = q.SQL, q.Select, q.LockPlan
	case *plansql.ExistsNode:
		text, sel, lock = q.SQL, q.Select, q.LockPlan
	default:
		return nil, nil, false
	}
	body, err := sel()
	if err != nil || body == nil {
		return nil, nil, false
	}
	if sql == text {
		return body, lock(), true
	}
	for _, k := range []int{1, 2} {
		if bounded := plansql.AppendRowLimit(text, body, k); bounded != text && sql == bounded {
			cp := *body
			cp.Limit = plansql.RowLimitText(k)
			return &cp, lock(), true
		}
	}
	return nil, nil, false
}

// subqueryNodeBody is a SubqueryNode's or ExistsNode's memoized body.
func subqueryNodeBody(n plansql.Node) (*plansql.SelectInfo, error) {
	switch q := n.(type) {
	case *plansql.SubqueryNode:
		return q.Select()
	case *plansql.ExistsNode:
		return q.Select()
	}
	return nil, nil
}

// subqueryOutputColumnOf is subqueryOutputColumnIn for one subquery node: its
// own text is declared with the outer references its re-run substitutes
// (subqueryDeclSQLOf); any other text as subqueryOutputColumnIn declares it.
func (p *Planner) subqueryOutputColumnOf(n plansql.Node, scope *logical.Node) func(string) (parquet.Column, bool) {
	text := subqueryText(n)
	return func(sql string) (parquet.Column, bool) {
		if sql == text {
			return p.SubqueryOutputColumn(p.subqueryDeclSQLOf(n, scope))
		}
		return p.SubqueryOutputColumn(p.subqueryDeclSQLIn(sql, scope))
	}
}

// subqueryDeclSQLOf is subqueryDeclSQLIn for one subquery node: a correlated
// one is declared with the outer references the per-row re-run substitutes
// for IT (expr.OuterTypedSubqueryNodeSQL, which classifies by the bindings
// on its body where the binder bound it).
func (p *Planner) subqueryDeclSQLOf(n plansql.Node, scope *logical.Node) string {
	text := subqueryText(n)
	if scope == nil {
		return text
	}
	if typed, ok := expr.OuterTypedSubqueryNodeSQL(n, collectTableAliases(scope),
		collectOuterColumns(scope), p.SubqueryInnerColumns(), outerDeclsOf(scope)); ok {
		return typed
	}
	return text
}

// subqueryNodeDeclIn declares a scalar subquery NODE over scope: planned by a
// child planner over the WITH chain the node records, its outer references
// typed as scope's columns (subqueryDeclSQLOf), memoized in the build's
// declaration memo under that chain (scalarSubqueryColumnDecl). ok is false
// for a node that records no chain; the walk then asks by text.
func (p *Planner) subqueryNodeDeclIn(scope *logical.Node) func(*plansql.SubqueryNode) (logical.SubqueryColumnDecl, bool) {
	return func(n *plansql.SubqueryNode) (logical.SubqueryColumnDecl, bool) {
		sp, ok := p.forSubqueryNode(n)
		if !ok {
			return logical.SubqueryColumnDecl{}, false
		}
		if p.subqueryDeclCache == nil {
			p.subqueryDeclCache = &subqueryDeclMemo{}
		}
		sp.subqueryDeclCache = p.subqueryDeclCache
		return sp.scalarSubqueryColumnDecl(sp.subqueryDeclSQLOf(n, scope))
	}
}

// subqueryNodeColumn is the node resolver's answer, when the walk has one and
// the node records a chain.
func (d ColDecls) subqueryNodeColumn(n *plansql.SubqueryNode) (logical.SubqueryColumnDecl, bool) {
	if d.subqueryNode == nil || n == nil {
		return logical.SubqueryColumnDecl{}, false
	}
	return d.subqueryNode(n)
}

// subqueryColumn is a scalar subquery's declared column: by its node where
// the walk can ask by node (subqueryNodeColumn), by its text otherwise.
func (d ColDecls) subqueryColumn(n *plansql.SubqueryNode) (parquet.Column, bool) {
	if sd, ok := d.subqueryNodeColumn(n); ok {
		return parquet.Column{Type: sd.Type, Precision: sd.Precision, Scale: sd.Scale,
			ElementType: sd.ElementType, Fields: sd.Fields,
			PGNumeric: sd.Type == parquet.TypeFloat64 && sd.PGCategory == pgCatNumeric}, true
	}
	if d.subqueryDecl == nil {
		return parquet.Column{}, false
	}
	return d.subqueryDecl(n.SQL)
}

// declIntWidth is a declaration's PostgreSQL integer width.
func declIntWidth(sd logical.SubqueryColumnDecl) intWidth {
	switch sd.IntWidth {
	case 4:
		return intWidth4
	case 8:
		return intWidth8
	}
	return intWidthUnknown
}
