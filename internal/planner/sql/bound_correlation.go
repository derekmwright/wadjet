// SPDX-License-Identifier: MIT

package sql

// THE CORRELATION CLASSIFIER ASKS THE BINDING (ADR-0021 §1k, ADR-0047 stage 3).
//
// §1k's rule is SQL's: an unqualified name inside a subquery binds the
// innermost scope that supplies it, and only a name no inner relation carries
// reads the enclosing query. FindCorrelatedRefsWithScope applies it by asking
// a resolver which NAMES each relation of the body publishes — and a name the
// resolver could not answer fell through to the enclosing scope, which is how
// `(SELECT MAX(v) FROM c WHERE id < 4000)` over a CTE was classified
// correlated and answered 4999014997 for 3999011997. The binder resolves the
// same references with the schemas in hand, each to a relation instance at a
// query level, and records on the subquery's node the references whose level
// reaches the query the subquery is written in (SubqueryNode.SetOuterRefs).
// Where it recorded them that list IS the classification: a reference is
// correlated exactly when its binding level is past the body.

// CorrelatedRefsOf classifies one expression subquery node: by the binder's
// recorded classification (OuterRefs) where it holds one and every recorded
// reference is spelled in the caller's outer scope — a qualified one under a
// qualifier of outerTables, a bare one under the table outerCols maps it to —
// and by name otherwise: FindCorrelatedRefsWithScope with innerCols, or
// FindCorrelatedRefs where the caller has no column map.
func CorrelatedRefsOf(n Node, outerTables map[string]bool, outerCols map[string]string, innerCols TableColumns) ([]OuterRef, error) {
	var sql string
	var recorded []OuterRef
	var ok bool
	switch q := n.(type) {
	case *SubqueryNode:
		sql = q.SQL
		recorded, ok = q.OuterRefs()
	case *ExistsNode:
		sql = q.SQL
		recorded, ok = q.OuterRefs()
	}
	if ok && len(outerCols) > 0 {
		if refs, spelled := spellOuterRefs(recorded, outerTables, outerCols); spelled {
			return refs, nil
		}
	}
	if len(outerCols) > 0 {
		return FindCorrelatedRefsWithScope(sql, outerTables, outerCols, innerCols)
	}
	return FindCorrelatedRefs(sql, outerTables)
}

// spellOuterRefs spells the binder's outer references in the caller's outer
// scope; spelled is false when one of them is not in it.
func spellOuterRefs(recorded []OuterRef, outerTables map[string]bool, outerCols map[string]string) ([]OuterRef, bool) {
	out := make([]OuterRef, 0, len(recorded))
	for _, r := range recorded {
		if r.Bare {
			tbl, known := outerCols[r.Column]
			if !known {
				return nil, false
			}
			out = append(out, OuterRef{Table: tbl, Column: r.Column, Bare: true})
			continue
		}
		if !outerTables[r.Table] {
			return nil, false
		}
		out = append(out, r)
	}
	return dedup(out), true
}
