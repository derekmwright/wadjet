// SPDX-License-Identifier: MIT

package physical

import (
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// respellDerivedAliasRefs replaces every column reference naming a derived
// table's or CTE's SELECT-list RENAME with the source column the DAG's streams
// carry. Copy-on-write; a reference the alias walk does not resolve comes back
// exactly as it was.
//
// It resolves a rename only. A COMPUTED alias has no source column to point at
// — `DerivedAliasSourceColumn` answers "" for one — and the expression that
// defines it is what respellAggInputExpr substitutes instead.
func respellDerivedAliasRefs(n plansql.Node, child *logical.Node) (plansql.Node, bool) {
	out, changed, complete := RewriteColRefs(n, func(ref *plansql.ColRef) (plansql.Node, bool) {
		src := derivedAliasSourceColumn(ref.String(), child)
		if src == "" && ref.Table == "" {
			src = derivedAliasSourceColumn(ref.Column, child)
		}
		if src == "" {
			return nil, false
		}
		return &plansql.ColRef{Column: cleanExpr(src)}, true
	})
	if !complete {
		// The walk met a node it does not rewrite — a subquery, an EXISTS, a
		// window call, a kind added since — so some references in this
		// expression were NOT considered. A PARTIAL respell is the worst of
		// the three outcomes: it looks resolved and is not. Decline the whole
		// rewrite and leave the expression exactly as written.
		return n, false
	}
	return out, changed
}
