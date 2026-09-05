package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// Looking a DERIVED TABLE's SELECT-list alias up in the logical plan.
//
// walkStages emits no stage for an ordinary Project, so a derived table's
// rename never happens anywhere on the DAG: every stream carries SOURCE
// column names and each consumer compensates by resolving the alias back
// through the plan — resolveShuffleKey for join keys, resolveAggInputName for
// aggregate arguments and GROUP BY keys, resolveSortKeyColumn (plus
// annotateDerivedAliasSortKey) for ORDER BY terms, resolveOutputRenameSource
// for the gather's result schema. The two helpers here are that lookup: which
// SELECT-list item a name refers to, and when a table qualifier on that name
// may be dropped.
//
// derivedScopeBareName is the rule for when a reference qualified by the
// derived table's own alias — `x.k`, `u.k`, `y.j`, the spelling every BI tool
// writes — may drop that qualifier. It is NOT an unconditional strip:
// `SUM(t.c)` over `t JOIN (SELECT d AS c FROM u) v` must keep naming t's own
// column, and a blind strip would resolve it to `d`, a silently different
// answer. The qualifier may only be dropped in a subtree that actually
// contains the relation it names, which is exactly the derived table's own
// scope: BuildFromTable's setSubtreeAlias stamps the derived alias onto every
// Scan below it, so `u` names a relation inside `(SELECT ... FROM nation) u`
// and names nothing inside the sibling arm of a join. Both join-recursing
// resolvers descend one arm at a time, so the scoping is exact.
func derivedScopeBareName(name string, subtree *logical.Node) string {
	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 || dot == len(name)-1 {
		return ""
	}
	if !subtreeNamesRelation(subtree, name[:dot]) {
		return ""
	}
	return name[dot+1:]
}

// subtreeNamesRelation reports whether any scan in the subtree answers to
// name — its alias when it has one, its table name otherwise, or a DERIVED
// TABLE whose scope it sits inside. This is the same alias→scan association
// subtreeNaming.aliasCols builds; the walk is kept separate because this one
// needs no column sets and runs per key.
//
// The derived aliases are consulted separately from TableAlias because a scan
// inside a derived table can have both: `(SELECT … FROM nation n1 JOIN nation
// n2 …) u` leaves each scan named n1 or n2 — which is what tells the join's
// two sides apart (#489) — while both sit in u's scope.
func subtreeNamesRelation(n *logical.Node, name string) bool {
	if n == nil || name == "" {
		return false
	}
	// A CTE reference is a named scope too, and the enclosing query writes
	// `c.col` against its OUTPUT columns exactly as it writes `x.col` for a
	// derived table's alias. The two record that scope in different places:
	// a derived table's alias is stamped onto every scan below it
	// (BuildFromTable's setSubtreeAlias), while a CTE's name sits on the
	// SUBTREE ROOT (Node.CTEName, plus Node.CTERefAlias for the name ONE
	// reference gives it in `FROM c AS x`) — stamping it on the scans would
	// make two relations comma-joined inside the CTE body share one
	// identity for predicate attribution (#281's q18 CTE spelling). So the
	// scope test reads both. Without this a join key, GROUP BY term or sort
	// key naming a RENAMED CTE column resolved to nothing: the broadcast
	// join's probe matched no row and the query answered zero in silence
	// (#653).
	if strings.EqualFold(n.CTEName, name) || strings.EqualFold(n.CTERefAlias, name) {
		return true
	}
	if n.Type == logical.NodeScan {
		alias := n.TableAlias
		if alias == "" {
			alias = n.TableName
		}
		if strings.EqualFold(alias, name) {
			return true
		}
		for _, d := range n.DerivedAliases {
			if strings.EqualFold(d, name) {
				return true
			}
		}
	}
	for _, c := range n.Children {
		if subtreeNamesRelation(c, name) {
			return true
		}
	}
	return false
}

// projSourceName is the spelling of the column a PLAIN rename reads, keeping
// the table qualifier when the projection has one.
//
// Projection.Column is the bare name, which is enough everywhere one relation
// in scope carries it and ambiguous exactly where two do: over a self-join
// both arms answer to `n_name`, and only `n2.n_name` names one of them. Expr
// is the reference as WRITTEN, so it carries the qualifier when the query did;
// where it did not, the two agree and this is Column.
func projSourceName(proj *logical.Projection) string {
	if proj.Expr != "" {
		return proj.Expr
	}
	return proj.Column
}

// projectionForName finds the SELECT-list item of a Project that a consumer's
// name refers to: the alias as written first, then — for a reference
// qualified by the derived table this Project belongs to — its bare form.
//
// Exact-first matters: a projection that aliases the qualified spelling
// itself (`n1.n_name AS "n1.n_name"`) owns the name outright, and the bare
// fallback must not overtake it.
func projectionForName(projs []logical.Projection, name, bare string) *logical.Projection {
	for i := range projs {
		if projs[i].Alias != "" && strings.EqualFold(projs[i].Alias, name) {
			return &projs[i]
		}
	}
	if bare == "" {
		return nil
	}
	for i := range projs {
		if projs[i].Alias != "" && strings.EqualFold(projs[i].Alias, bare) {
			return &projs[i]
		}
	}
	return nil
}

// projectionPublishingName is projectionForName widened to an item the SELECT
// list wrote with NO ALIAS, which publishes under its own bare column name:
// `SELECT u.g, u.x FROM (…) u` publishes `g` and `x`.
//
// Only the CLASS walk asks it, and the split is deliberate rather than a
// second copy of the rule. The other walks resolve a NAME to its SOURCE, and
// for an unaliased qualified item those two are the same string — `u.x`
// resolves to `u.x`, a self-rename that stops the walk one Project short of
// the answer. Measured: widening projectionForName itself made
// `SELECT u.g, u.x FROM (SELECT COUNT(*) AS g, g AS x … ) u ORDER BY u.x`
// refuse its whole plan, because the sort key stopped resolving to the column
// the aggregate emits. The CLASS question has no such fixpoint: it wants the
// item, and an unaliased item is one.
//
// Without it, `renameIsAggregateOutput` answered "not an aggregate output" for
// a name whose value IS one two derived tables above the aggregate, and the
// gather's duplicate-name pairing then took the first column of the name —
// the group KEY (#785, ADR-0026 §3a).
func projectionPublishingName(projs []logical.Projection, name, bare string) *logical.Projection {
	if p := projectionForName(projs, name, bare); p != nil {
		return p
	}
	for _, want := range []string{name, bare} {
		if want == "" {
			continue
		}
		for i := range projs {
			if projs[i].Alias != "" || projs[i].Column == "" {
				continue
			}
			if strings.EqualFold(projs[i].Column, want) {
				return &projs[i]
			}
		}
	}
	return nil
}
