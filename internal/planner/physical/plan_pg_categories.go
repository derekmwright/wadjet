// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// PlanPGCategories is PostgreSQL's numeric category of every column NAME a
// plan's nodes emit, for an executor that compiles an expression over a
// column some other part of the plan materialized and has only the column's
// carrier to read (#381): `5 / 2.0 AS x` and `f AS x` are one FLOAT64 to the
// batch, and the rounding rule of `round(x)` is the plan's to say.
//
// The single-process planner reads each operator's own input categories
// (expr.WithInputPGCategories over emittedColPGCategory); this is the same
// walk folded over the whole plan by name, which is all an executor that
// receives an expression's TEXT can key on. A name two nodes emit under two
// different categories — or a base column of a different category under the
// same name — is left out, and a reader then keeps the carrier's reading.
func PlanPGCategories(root *logical.Node) map[string]expr.PGCategory {
	seen := map[string]expr.PGCategory{}
	conflict := map[string]bool{}
	note := func(name string, c expr.PGCategory) {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" || c == expr.PGCatUnknown || conflict[name] {
			return
		}
		if prev, ok := seen[name]; ok && prev != c {
			conflict[name] = true
			delete(seen, name)
			return
		}
		seen[name] = c
	}
	var walk func(n *logical.Node)
	visited := map[*logical.Node]bool{}
	walk = func(n *logical.Node) {
		if n == nil || visited[n] {
			return
		}
		visited[n] = true
		if n.Type == logical.NodeScan {
			for name, t := range n.ScanColTypes {
				note(name, pgCategoryOfDecl(expr.Decl(t), expr.Decided))
			}
		}
		for name, c := range emittedColPGCategory(n) {
			note(name, c)
		}
		for _, ch := range n.Children {
			walk(ch)
		}
	}
	walk(root)
	if len(seen) == 0 {
		return nil
	}
	return seen
}

// AggregatePGCategory is PostgreSQL's category of an aggregate's (or a
// window aggregate's) result — fn over the argument arg, an expression's text
// or a column name — read over the input columns schema and the plan's
// categories cats (PlanPGCategories): max(5 / 2.0 + id * 0) is numeric,
// sum(f) float8. Unknown when the argument does not parse or names no
// category.
func AggregatePGCategory(fn, arg string, schema []parquet.Column, cats map[string]expr.PGCategory) expr.PGCategory {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return expr.PGCatUnknown
	}
	node, err := plansql.ParseExpression(arg)
	if err != nil || node == nil {
		return expr.PGCatUnknown
	}
	decls := schemaColDecls(schema, nil)
	if len(cats) > 0 {
		decls.pgCat = cats
	}
	return pgAggregateCategory(fn, pgCategoryOf(node, decls))
}
