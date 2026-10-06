// SPDX-License-Identifier: MIT

package physical

import (
	"sort"
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
// same name — is left out and returned in conflicts, and a reader then keeps
// the carrier's reading. loss names the first left-out name that reading is
// wrong for (planPGCategoryLoss): a reader of it must not run on this map.
//
// Only the entries that CHANGE a reading are returned: a stored column's
// category is its declared type's (a FLOAT64 is float8, a DECIMAL numeric),
// which the reader takes from the batch without an entry — except a column
// created with the numeric category (parquet.Column.PGNumeric), whose entry
// rides because the batch's FLOAT64 cannot say it — and an integer
// value rounds the same under either rule. A 500-column table read by one
// column ships none of its columns (round-2 review P3).
func PlanPGCategories(root *logical.Node) (cats map[string]expr.PGCategory, conflicts map[string]bool, loss string) {
	seen := map[string]expr.PGCategory{}
	stored := map[string]bool{}
	conflict := map[string]bool{}
	note := func(name string, c expr.PGCategory, fromScan bool) {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" || c == expr.PGCatUnknown || conflict[name] {
			return
		}
		if prev, ok := seen[name]; ok && prev != c {
			conflict[name] = true
			delete(seen, name)
			return
		}
		if _, ok := seen[name]; !ok {
			stored[name] = fromScan
		} else if !fromScan {
			stored[name] = false
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
				if c, ok := n.ScanColPGCategory[name]; ok && c != expr.PGCatUnknown {
					// A column created with a category its carrier does not
					// read (parquet.Column.PGNumeric) changes a reading: it
					// rides, noted below from emittedColPGCategory.
					continue
				}
				note(name, pgCategoryOfDecl(expr.Decl(t), expr.Decided), true)
			}
		}
		for name, c := range emittedColPGCategory(n) {
			note(name, c, false)
		}
		for _, ch := range n.Children {
			walk(ch)
		}
	}
	walk(root)
	for name, c := range seen {
		if c == expr.PGCatInteger || stored[name] {
			delete(seen, name)
		}
	}
	if len(seen) == 0 {
		seen = nil
	}
	if len(conflict) == 0 {
		conflict = nil
	}
	return seen, conflict, planPGCategoryLoss(root, conflict)
}

// planPGCategoryLoss is the first name, in order, of conflicts — the names
// PlanPGCategories leaves out — that some node of the plan emits under a
// category its carrier does not read: a FLOAT64 PostgreSQL types numeric
// (`sqrt(6.25 + id * 0) AS b`, a stored column created from one) or a DECIMAL
// it types float8. A reader of such a name keeps the carrier's reading, which
// for that column is the other rounding rule; "" when every left-out name
// reads its carrier's category wherever it is emitted.
func planPGCategoryLoss(root *logical.Node, conflicts map[string]bool) string {
	if len(conflicts) == 0 {
		return ""
	}
	var hit []string
	visited := map[*logical.Node]bool{}
	var walk func(n *logical.Node)
	walk = func(n *logical.Node) {
		if n == nil || visited[n] {
			return
		}
		visited[n] = true
		cats := emittedColPGCategory(n)
		if len(cats) > 0 {
			types := emittedColTypes(n)
			for name, c := range cats {
				name = strings.ToLower(strings.TrimSpace(name))
				if !conflicts[name] {
					continue
				}
				t, ok := lookupColType(types, name)
				if ok && (t == parquet.TypeFloat64 && c == expr.PGCatNumeric || t == parquet.TypeDecimal && c == expr.PGCatFloat8) {
					hit = append(hit, name)
				}
			}
		}
		for _, ch := range n.Children {
			walk(ch)
		}
	}
	walk(root)
	if len(hit) == 0 {
		return ""
	}
	sort.Strings(hit)
	return hit[0]
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
