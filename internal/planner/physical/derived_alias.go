// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// Ordinary Projects emit no DAG stage; consumers resolve SELECT-list aliases
// back to source names (shuffle/aggregate/sort keys and gather renames).
// derivedScopeBareName drops a derived-table qualifier ONLY when this subtree
// contains the named relation. BuildFromTable's setSubtreeAlias stamps that
// alias onto its scans; join-recursing resolvers examine one arm at a time.
// Never strip unconditionally: a qualified sibling column must keep its scope.
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
// SubtreeNaming.aliasCols builds; the walk is kept separate because this one
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
