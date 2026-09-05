package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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

// publishedOutputNames is the published name of each visible column of the
// OUTPUT projection, positionally, or nil when the projection publishes what
// it always did.
//
// Nil rather than a copy of the current names, because the sink applies the
// list only when it is non-empty: a query whose every item is aliased or is a
// bare column costs nothing.
func publishedOutputNames(projNode *logical.Node) []string {
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
// `declaredWireUnconstrainedDecimal` and `DeclaredStringLengths` answer "what
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

// republishDeclaredSchema renames a plan-declared output schema's columns to
// the published names, positionally — the same rename CollectSink.OutputNames
// applies to the sink's own schema, for the copy the GATHER carries.
func republishDeclaredSchema(projNode *logical.Node, cols []parquet.Column) []parquet.Column {
	names := publishedOutputNames(projNode)
	if len(names) == 0 || len(names) != len(cols) {
		return cols
	}
	for i := range cols {
		if names[i] != "" {
			cols[i].Name = names[i]
		}
	}
	return cols
}
