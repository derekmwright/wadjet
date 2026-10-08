// SPDX-License-Identifier: MIT

package physical

import (
	"reflect"
	"regexp"
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
// column ships none of its columns.
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
// reads its carrier's category wherever it is emitted, or when nothing in
// the plan rounds (planHasRoundingSite): only a ROUND or an integer cast
// reads a category, so a lost name with no such reader changes no answer.
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
	if len(hit) == 0 || !planHasRoundingSite(root) {
		return ""
	}
	sort.Strings(hit)
	return hit[0]
}

// roundingSiteText matches the spelling of every expression that reads a
// column's category: ROUND and a cast to an integer or an integer array —
// `CAST(… AS …)`, `::`, and the function-style integer casts. It is wider
// than the rule (a cast to text matches too): a false match only keeps a
// plan on the local route it took before, a missed one would let a stage
// round a lost column by its carrier.
var roundingSiteText = regexp.MustCompile(`(?i)\bround\s*\(|\bcast\s*\(|::|\b(int|int2|int4|int8|integer|smallint|bigint)\s*\(`)

// planHasRoundingSite reports whether any expression of the plan rounds by
// its operand's category (roundsHalfEven: ROUND, the integer cast, the
// integer array cast). A plan without one reads no category, so a name its
// map loses changes no answer and the stage DAG runs it (cell FC15: a join or a semi join of a marked table with a float8 column of the
// same name routed local with nothing rounding it).
//
// It reads EVERY string and every expression tree the plan's nodes hold,
// through reflection rather than a list of the fields that carry an
// expression today: a field added later is read too, and a field that is
// not an expression (a table name) can only make the answer true.
func planHasRoundingSite(root *logical.Node) bool {
	seen := map[uintptr]bool{}
	astType := reflect.TypeOf((*plansql.Node)(nil)).Elem()
	var visit func(v reflect.Value) bool
	visit = func(v reflect.Value) bool {
		switch v.Kind() {
		case reflect.String:
			return roundingSiteText.MatchString(v.String())
		case reflect.Interface:
			if v.IsNil() {
				return false
			}
			if v.Type().Implements(astType) && v.CanInterface() {
				if n, ok := v.Interface().(plansql.Node); ok {
					return roundingSiteText.MatchString(n.String())
				}
			}
			return visit(v.Elem())
		case reflect.Pointer:
			if v.IsNil() || seen[v.Pointer()] {
				return false
			}
			seen[v.Pointer()] = true
			if v.Type().Implements(astType) && v.CanInterface() {
				if n, ok := v.Interface().(plansql.Node); ok {
					return roundingSiteText.MatchString(n.String())
				}
			}
			return visit(v.Elem())
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				if visit(v.Field(i)) {
					return true
				}
			}
		case reflect.Slice, reflect.Array:
			for i := 0; i < v.Len(); i++ {
				if visit(v.Index(i)) {
					return true
				}
			}
		case reflect.Map:
			it := v.MapRange()
			for it.Next() {
				if visit(it.Key()) || visit(it.Value()) {
					return true
				}
			}
		}
		return false
	}
	return visit(reflect.ValueOf(root))
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
