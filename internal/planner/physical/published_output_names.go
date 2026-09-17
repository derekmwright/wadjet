// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// The names a QUERY publishes, as distinct from the names it RESOLVES by.
//
// PostgreSQL names an unaliased SELECT item by `FigureColname`
// (plansql.OutputColumnName, #732): `?column?` for an operator expression or a
// literal, the function's own name for a call, the ARGUMENT's name for a cast.
// wadjet named every one of them after the item's rendered TEXT — `g + 1`,
// `count(*)`, `case when … end` — so a pgwire client keying a column by name
// read a different one from the one PostgreSQL would have given it.
//
// The two names cannot be one string, which is what stopped the first attempt
// at this (arc E3). Inside the planner a name is a HANDLE: an aggregate's
// output column IS `AggExpr.OutputCol`, which GROUP BY, HAVING, ORDER BY and
// the stage's rename source all spell against, and `SELECT COUNT(*), COUNT(g)`
// publishes ONE name for two of them. So the published name is a SECOND name
// (logical.Projection.PublishedName), carried beside the resolution spelling
// and applied at the two places where values leave the engine: the collecting
// sink on the single-process path, and the gather's OutputRename target on the
// stage DAG.

// PublishedOutputNames is the list `Plan` stamps on the collecting sink as
// `CollectSink.OutputNames`, derived from the LOGICAL plan alone.
//
// It is exported for the one door that needs the names a query publishes
// WITHOUT running it: `CREATE TABLE … AS SELECT … WITH NO DATA`, which declares
// a table and does not execute the statement (#1024). `Plan` is not a pure
// derivation — it materializes every CTE body (`materializeCTEs` runs a whole
// pipeline) and builds every hash join's build side — so asking `Plan` for
// these names reads the source table and evaluates part of the query, which is
// the thing that clause exists not to do. This is the same walk `Plan` makes to
// stamp them, and nothing else.
//
// A nil or short answer means "the projection publishes what it always did",
// and so does an empty entry inside it; a caller renames only the positions
// this names.
func PublishedOutputNames(plan *logical.Node) []string {
	return publishedNamesOfProjection(findOutputProjectionNode(plan))
}

// publishedNamesOfProjection is the published name of each visible column of
// the OUTPUT projection, positionally, or nil when the projection publishes
// what it always did.
//
// Exported from the MIT side because both planners stamp these names: the
// local entry from its own output projection, the distributed one from the
// projection its gather publishes. The name says what it computes; it used to
// carry a DAG prefix, which was an artefact of the export pass, not a claim
// about where it belongs (LS review round 2, P2).
//
// Nil rather than a copy of the current names, because the sink applies the
// list only when it is non-empty: a query whose every item is aliased or is a
// bare column costs nothing.
func publishedNamesOfProjection(projNode *logical.Node) []string {
	if projNode == nil || projNode.Type != logical.NodeProject {
		return nil
	}
	visible := logical.VisibleProjections(projNode.Projections)
	if len(visible) == 0 {
		return nil
	}
	names := make([]string, len(visible))
	differs := false
	for i := range visible {
		p := visible[i]
		names[i] = p.PublishedName
		if names[i] == "" {
			continue
		}
		if !strings.EqualFold(names[i], projectionOutputName(p)) {
			differs = true
		}
	}
	if !differs {
		return nil
	}
	return names
}

// republishDeclaredNames re-keys a PLAN-TIME declaration map from the
// resolution spelling to the published name.
//
// `DeclaredWireUnconstrainedDecimal` and `DeclaredStringLengths` answer "what
// modifier does this OUTPUT column declare", keyed by name, and both are looked
// up by the name the CLIENT reads. Once that name is PostgreSQL's rather than
// the expression's text, the old key misses: an unaliased `s_acctbal + 1` over
// a DECIMAL(15,2) column went out with typmod (16,2) where PostgreSQL sends -1,
// because the "this one is unconstrained" entry was filed under
// `s_acctbal + 1` and asked for under `?column?`.
//
// The entry is MOVED, not copied: keeping both would leave a stale key that a
// later column of that name would collide with. Only the output projection's
// own names are re-keyed — a nested block's declaration is not this map.
func republishDeclaredNames[T any](projNode *logical.Node, m map[string]T) map[string]T {
	if projNode == nil || len(m) == 0 {
		return m
	}
	visible := logical.VisibleProjections(projNode.Projections)
	for i := range visible {
		p := visible[i]
		if p.PublishedName == "" {
			continue
		}
		old := declaredProjectionName(p)
		if old == "" || strings.EqualFold(old, p.PublishedName) {
			continue
		}
		v, ok := m[old]
		if !ok {
			continue
		}
		delete(m, old)
		m[p.PublishedName] = v
	}
	return m
}

// publishedOutputDecls is the POSITIONAL form of the two plan-time declaration
// maps — which DECIMAL outputs declare an unconstrained wire typmod, and what
// LENGTH each string output declares.
//
// Both are keyed by output-column NAME, and a name stopped being an address the
// moment `SELECT CAST(s AS VARCHAR(4)), CAST(s AS VARCHAR(9))` became two
// columns called `s` (#732). The map gave both the LAST one's modifier, and the
// same for a DECIMAL aggregate beside a bare DECIMAL column. The lists are read
// off the SAME projections in the SAME order the schema is built from, so two
// items publishing one name each keep their own answer.
//
// Both are nil when nothing in the list has an answer, which is the ordinary
// case and costs nothing.
func publishedOutputDecls(projNode *logical.Node, wire map[string]bool,
	lens map[string]int) ([]bool, []int) {
	if projNode == nil || projNode.Type != logical.NodeProject {
		return nil, nil
	}
	if len(wire) == 0 && len(lens) == 0 {
		return nil, nil
	}
	visible := logical.VisibleProjections(projNode.Projections)
	if len(visible) == 0 {
		return nil, nil
	}
	w := make([]bool, len(visible))
	l := make([]int, len(visible))
	anyW, anyL := false, false
	for i := range visible {
		// The key the maps were BUILT with — declaredProjectionName, before
		// any republishing — because that is what declaredWireUnconstrained-
		// Decimal and DeclaredStringLengths filed each entry under.
		k := declaredProjectionName(visible[i])
		if k == "" {
			continue
		}
		if wire[k] {
			w[i], anyW = true, true
		}
		if n := lens[k]; n > 0 {
			l[i], anyL = n, true
		}
	}
	if !anyW {
		w = nil
	}
	if !anyL {
		l = nil
	}
	return w, l
}
