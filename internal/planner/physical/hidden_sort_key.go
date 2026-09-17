// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// Resolve a derived alias to the source column its producer supplies. The
// local sort uses that spelling when it has not materialized the alias itself.
// The distributed caller is dagplan.resolveHiddenSortKeys (ADR-0037 §6): it
// leaves materialized keys alone, renames a plain-column key to its source,
// and projects a computed term under its hidden name (#424; #313/#316/#320).
// That caller owns the projection and gather choices (#383, #169); this walk
// only resolves names, returning no answer for a shape it cannot describe.
// See docs/internals/hidden-sort-key-materialization.md for the design.

// derivedAliasSourceColumn resolves a name that may be a DERIVED TABLE's or
// CTE's SELECT-list alias to the column the DAG's streams actually carry,
// walking the Projects between the consumer and its producer. It returns ""
// when the name is not such an alias, when it names an aggregate output or a
// computed alias (neither has a source column to point at), or when the walk
// reaches a producer it cannot reason about.
//
// Chained renames resolve level by level (`j` → `k` → `s_nationkey`), each
// Project substituting at most once because a projection list is
// simultaneous.
func derivedAliasSourceColumn(name string, child *logical.Node) string {
	if name == "" {
		return ""
	}
	resolved := name
	for n := child; n != nil; {
		switch n.Type {
		case logical.NodeProject:
			bare := derivedScopeBareName(resolved, n)
			proj := projectionForName(n.Projections, resolved, bare)
			if proj == nil {
				break
			}
			if proj.IsAgg || proj.Column == "" {
				// One computed alias DOES have a source spelling: a GROUP
				// BY key, which the aggregate stage emits under the exact
				// text of its expression. Without it a sort above a window
				// above `SELECT g + 1 AS k … GROUP BY g + 1` keyed on `k`,
				// which nothing between the aggregate and the gather emits,
				// and the task failed loud (#656 F2).
				if src, hit := aggregateGroupKeyName(proj, n); hit {
					return src
				}
				return "" // aggregate output or genuinely computed alias
			}
			next := proj.Column
			if proj.Expr != "" {
				// The qualifier-preserving spelling, for the same reason
				// resolveSortKeyColumn prefers it: a self-joined table gives
				// both aliases the same bare column name, and only "n1.n_name"
				// says which. lookupEmittedColumn applies the qualified↔bare
				// fallback when the producer spells it the other way.
				next = proj.Expr
			}
			if strings.EqualFold(next, resolved) {
				return "" // self-rename, nothing to point at
			}
			resolved = next
		case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
			// Order/cardinality-preserving passthroughs: keep descending.
		default:
			// A producer this walk cannot reason about (Aggregate, Join,
			// Scan, Window, a set operation): stop, and keep whatever the
			// Projects above it resolved.
			n = nil
			continue
		}
		if len(n.Children) != 1 {
			break
		}
		n = n.Children[0]
	}
	if strings.EqualFold(resolved, name) {
		return ""
	}
	return resolved
}
