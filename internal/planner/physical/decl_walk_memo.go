// SPDX-License-Identifier: MIT

package physical

import (
	"maps"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// declWalk is ONE declaration walk: the emitted-column walks
// (emittedColTypes, emittedColDecimal, emittedColIntWidth), the container
// shape walk (inputColShapes) and everything they call through, with each
// node's answers memoized for the walk's lifetime.
//
// Without the memo the walks are exponential in derived-table depth. A
// Project's declarations are childDecls(child) — inputColShapes, the carrier
// and the (p,s) of the child — and each of those, at the child's own
// Project, builds childDecls of ITS child again: every level multiplied the
// work below it by the number of walks that ask, so `SELECT v FROM (…)`
// nested sixteen deep planned in minutes (b69c2412, which made the
// publishing walks read childDecls too; base was already 2^depth). With the
// memo each node's declarations are computed once per walk, so a walk is
// linear in the plan.
//
// THE SCOPE IS ONE WALK, not the Planner's build. A build mutates the plan
// between walks — buildProject expands a star and re-annotates its child
// (expandStarProjections), annotateSubqueryColumnDecls stamps every node,
// buildJoin swaps a semi join's children — and the distributed planner asks
// these walks through PlanContext between its own rewrites, so a memo that
// outlived one walk would answer for a node as it WAS. Within one walk
// nothing mutates the plan, so a walk-scoped memo answers exactly what the
// unmemoized walk did. Each package-level spelling below starts a fresh
// walk; the methods are the walk's own recursion. A walk is never shared
// between goroutines (every caller builds its own), so the maps need no
// lock — unlike the Planner's subqueryDeclMemo, which a correlated
// re-run reaches from every pipeline goroutine.
//
// A hit returns a COPY of the memoized map: the walks' callers build on
// what they are handed (inputColShapes' Window arm adds to its child's
// map), and a shared map would carry one caller's additions into another's
// answer.
type declWalk struct {
	types  map[*logical.Node]map[string]parquet.TypeID
	dec    map[*logical.Node]map[string]logical.DecimalMeta
	shapes map[*logical.Node]map[string]parquet.Column
	widths map[*logical.Node]map[string]intWidth
	// computed, when a test sets it, is told each uncached computation.
	computed func(kind string, n *logical.Node)
}

func newDeclWalk() *declWalk { return &declWalk{} }

func memoized[V any](w *declWalk, m *map[*logical.Node]map[string]V, n *logical.Node, kind string, compute func(*logical.Node) map[string]V) map[string]V {
	if r, ok := (*m)[n]; ok {
		return maps.Clone(r)
	}
	if w.computed != nil {
		w.computed(kind, n)
	}
	r := compute(n)
	if *m == nil {
		*m = make(map[*logical.Node]map[string]V)
	}
	(*m)[n] = maps.Clone(r)
	return r
}

func (w *declWalk) emittedColTypes(n *logical.Node) map[string]parquet.TypeID {
	return memoized(w, &w.types, n, "types", w.emittedColTypesUncached)
}

func (w *declWalk) emittedColDecimal(n *logical.Node) map[string]logical.DecimalMeta {
	return memoized(w, &w.dec, n, "decimal", w.emittedColDecimalUncached)
}

func (w *declWalk) inputColShapes(n *logical.Node) map[string]parquet.Column {
	return memoized(w, &w.shapes, n, "shapes", w.inputColShapesUncached)
}

func (w *declWalk) emittedColIntWidth(n *logical.Node) map[string]intWidth {
	return memoized(w, &w.widths, n, "widths", w.emittedColIntWidthUncached)
}

// The package-level spellings: each is one walk of its own.

func aggDerivedGroupKey(key string, child *logical.Node) (string, bool) {
	return newDeclWalk().aggDerivedGroupKey(key, child)
}

func aggInputColumnDecimal(node *logical.Node, col string) (logical.DecimalMeta, bool) {
	return newDeclWalk().aggInputColumnDecimal(node, col)
}

func aggOhlcvOutputFields(node *logical.Node, agg logical.AggExpr) ([]parquet.Column, bool) {
	return newDeclWalk().aggOhlcvOutputFields(node, agg)
}

func aggSpecOutputDecimal(node *logical.Node, agg logical.AggExpr) (logical.DecimalMeta, bool) {
	return newDeclWalk().aggSpecOutputDecimal(node, agg)
}

func aggSpecOutputType(node *logical.Node, agg logical.AggExpr) (parquet.TypeID, bool) {
	return newDeclWalk().aggSpecOutputType(node, agg)
}

func blockColumnsOf(p *logical.Node, published map[*logical.Node]bool, subqueryDecl func(string) (parquet.Column, bool), aggFromStream bool) ([]blockColumn, bool) {
	return newDeclWalk().blockColumnsOf(p, published, subqueryDecl, aggFromStream)
}

func childDecls(child *logical.Node) ColDecls { return newDeclWalk().childDecls(child) }

func declaredJoinSchema(n *logical.Node, want []string, published map[*logical.Node]bool, subqueryDecl func(string) (parquet.Column, bool)) []parquet.Column {
	return newDeclWalk().declaredJoinSchema(n, want, published, subqueryDecl)
}

func declaredOutputSchema(root *logical.Node, subqueryDecl func(string) (parquet.Column, bool)) []parquet.Column {
	return newDeclWalk().declaredOutputSchema(root, subqueryDecl)
}

func declaredProjectionInputs(root *logical.Node) (projs []logical.Projection, childTypes ColDecls, strictInt map[string]bool, ok bool) {
	return newDeclWalk().declaredProjectionInputs(root)
}

func declaredWireUnconstrainedDecimal(root *logical.Node) map[string]bool {
	return newDeclWalk().declaredWireUnconstrainedDecimal(root)
}

func derivedGroupKeyDecl(key string, node plansql.Node, child *logical.Node) expr.DeclType {
	return newDeclWalk().derivedGroupKeyDecl(key, node, child)
}

func emittedColDecimal(n *logical.Node) map[string]logical.DecimalMeta {
	return newDeclWalk().emittedColDecimal(n)
}

func emittedColDecls(n *logical.Node) ColDecls { return newDeclWalk().emittedColDecls(n) }

func emittedColIntWidth(n *logical.Node) map[string]intWidth {
	return newDeclWalk().emittedColIntWidth(n)
}

func emittedColTypes(n *logical.Node) map[string]parquet.TypeID {
	return newDeclWalk().emittedColTypes(n)
}

func emittedColumnNames(n *logical.Node) []string { return newDeclWalk().emittedColumnNames(n) }

func emittedComputedCols(n *logical.Node) map[string]bool {
	return newDeclWalk().emittedComputedCols(n)
}

func groupKeyNames(agg *logical.Node, child *logical.Node) (published []string, resolve []GroupKeyResolution) {
	return newDeclWalk().groupKeyNames(agg, child)
}

func groupKeyOutputs(agg *logical.Node) []groupKeyOut { return newDeclWalk().groupKeyOutputs(agg) }

func groupKeysPublishedBelow(n *logical.Node) map[string]string {
	return newDeclWalk().groupKeysPublishedBelow(n)
}

func inputColDecimal(n *logical.Node) map[string]logical.DecimalMeta {
	return newDeclWalk().inputColDecimal(n)
}

func inputColDecls(n *logical.Node) ColDecls { return newDeclWalk().inputColDecls(n) }

func inputColShapes(n *logical.Node) map[string]parquet.Column {
	return newDeclWalk().inputColShapes(n)
}

func inputColTypes(n *logical.Node) map[string]parquet.TypeID { return newDeclWalk().inputColTypes(n) }

func joinHiddenPositions(node *logical.Node) (probe, build map[int]string) {
	return newDeclWalk().joinHiddenPositions(node)
}

func lateralOuterDecls(outer *logical.Node) ColDecls { return newDeclWalk().lateralOuterDecls(outer) }

func resolveAggInputName(name string, child *logical.Node) (resolved string, expr plansql.Node, exprInput *logical.Node, alias bool) {
	return newDeclWalk().resolveAggInputName(name, child)
}

func setOpDeclaredOutputSchema(root *logical.Node) ([]parquet.Column, bool) {
	return newDeclWalk().setOpDeclaredOutputSchema(root)
}

func starOnlyDeclaredOutputSchema(root *logical.Node, subqueryDecl func(string) (parquet.Column, bool)) ([]parquet.Column, bool) {
	return newDeclWalk().starOnlyDeclaredOutputSchema(root, subqueryDecl)
}

func windowSpecOutputType(node *logical.Node, we logical.WindowExpr) expr.DeclType {
	return newDeclWalk().windowSpecOutputType(node, we)
}
