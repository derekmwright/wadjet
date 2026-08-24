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
// name — its alias when it has one, its table name otherwise. This is the
// same alias→scan association subtreeNaming.aliasCols builds; the walk is
// kept separate because this one needs no column sets and runs per key.
func subtreeNamesRelation(n *logical.Node, name string) bool {
	if n == nil || name == "" {
		return false
	}
	if n.Type == logical.NodeScan {
		alias := n.TableAlias
		if alias == "" {
			alias = n.TableName
		}
		if strings.EqualFold(alias, name) {
			return true
		}
	}
	for _, c := range n.Children {
		if subtreeNamesRelation(c, name) {
			return true
		}
	}
	return false
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
