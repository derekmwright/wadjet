// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// A hidden __sortkey_N must be emitted by the sort's producer; ordinary DAG
// Projects emit no stage (#424; #313/#316/#320). resolveHiddenSortKeys runs
// after attachScanSelectProjections and leaves already-materialized keys alone.
// Otherwise rename a plain-column key to the shipped source name, or project
// a computed term under its hidden name via Stage.ProjectExprs/OpProject
// (#383, #169). Gather uses the visible SELECT list; consumers use source names.
// Leave unrecognized shapes unchanged to preserve loud failure, never invent order.
// See docs/internals/hidden-sort-key-materialization.md for the design.

// DerivedAliasSourceColumn resolves a name that may be a DERIVED TABLE's or
// CTE's SELECT-list alias to the column the DAG's streams actually carry,
// walking the Projects between the consumer and its producer. It returns ""
// when the name is not such an alias, when it names an aggregate output or a
// computed alias (neither has a source column to point at), or when the walk
// reaches a producer it cannot reason about.
//
// Chained renames resolve level by level (`j` → `k` → `s_nationkey`), each
// Project substituting at most once because a projection list is
// simultaneous.
func DerivedAliasSourceColumn(name string, child *logical.Node) string {
	if name == "" {
		return ""
	}
	resolved := name
	for n := child; n != nil; {
		switch n.Type {
		case logical.NodeProject:
			bare := DerivedScopeBareName(resolved, n)
			proj := ProjectionForName(n.Projections, resolved, bare)
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
				if src, hit := AggregateGroupKeyName(proj, n); hit {
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
