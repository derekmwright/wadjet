// SPDX-License-Identifier: MIT

package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A DECORRELATED SUBQUERY KEEPS EVERY OUTER REFERENCE, WHEREVER IN ITS BODY
// IT IS WRITTEN.
//
// The three decorrelations (IN/NOT IN, EXISTS/NOT EXISTS, and the scalar
// comparison) all classify the body's WHERE clause and nothing else. A body is
// larger than its WHERE: a JOIN's ON, the HAVING, the GROUP BY, the SELECT
// list and the ORDER BY can each name the enclosing row, and PostgreSQL
// evaluates the whole body once per outer row, so every one of those
// references decides the answer.
//
// The rule this file states is:
//
//   - an INNER (or cross) join's ON conjunct IS a WHERE conjunct, so a
//     conjunct of one that names the enclosing query is LIFTED into the
//     classification the caller already runs over the WHERE — it becomes a
//     correlation key, a residual, or an outer-side filter exactly as the same
//     text written in the WHERE would;
//   - an outer reference anywhere the rewrite cannot carry it — an OUTER
//     join's ON, where the padding makes a conjunct mean something a WHERE
//     conjunct does not; the HAVING, which is read after the grouping the
//     build side performs; the GROUP BY; the SELECT list; the ORDER BY; the
//     QUALIFY — BLOCKS the rewrite, and the subquery stays an executable
//     predicate re-run per outer row, which is right by construction;
//   - a condition that PROVABLY names only the enclosing query is neither a
//     key nor an inner filter. PostgreSQL applies it per outer row, so it
//     gates WHICH outer rows can match at all. See outerOnlyDisposition.
//
// Before this, the ON was invisible: `o.id IN (SELECT b.order_id FROM lat_item
// b JOIN lat_item c ON c.id = b.id AND o.total > b.amount)` built the body's
// join with `o.total` still in its condition, where the enclosing relation is
// not in scope, and answered ZERO rows for PostgreSQL 17.11's two (#1232).

// namesEnclosingQuery reports whether a clause of a subquery body contains a
// column reference QUALIFIED by a relation the ENCLOSING query reads and the
// body does not.
//
// It is deliberately strict in both directions:
//
//   - a qualifier is read only when it names an outer relation and NO inner
//     one. `dc_out o` outside and `dc_out z` inside makes `o.total` outer and
//     `z.id` inner; `dc_out.total` names both and is not decided here.
//   - plansql.ColumnRefs REFUSES a node it does not know and the three nodes
//     that carry raw SQL rather than a parsed subtree (a subquery, an EXISTS,
//     a window call). An error therefore means "this clause may hold a
//     reference I cannot see", and the answer is true — the rewrite declines
//     rather than proceeding over a clause it has not read.
func namesEnclosingQuery(node plansql.Node, outerTables, innerTables map[string]bool) bool {
	if node == nil {
		return false
	}
	refs, err := plansql.ColumnRefs(node)
	if err != nil {
		return true
	}
	for _, r := range refs {
		if r.Table == "" {
			continue
		}
		t := strings.ToLower(r.Table)
		if outerTables[t] && !innerTables[t] {
			return true
		}
	}
	return false
}

// provablyOuterOnly reports whether EVERY column reference in node is
// qualified by an enclosing-only relation — the one spelling this pass can
// prove names the outer row and nothing else.
//
// The qualifier is what makes it provable. An UNQUALIFIED name is decided by
// nodeTableRefs from the enclosing query's column map, which has no catalog
// for the body's own relations and therefore cannot tell the outer column
// `total` from an inner one of the same name: TPC-H Q2's official spelling
// writes `p_partkey = ps_partkey` inside the subquery with no qualifier at
// all, and BOTH names are in the enclosing map because the enclosing query
// reads those relations too. Hoisting such a conjunct onto the outer side
// would change Q2's row set. So the unqualified spelling keeps the
// disposition it had — see the boundary recorded in ADR-0021 §1r.
func provablyOuterOnly(node plansql.Node, outerTables, innerTables map[string]bool) bool {
	if node == nil {
		return false
	}
	refs, err := plansql.ColumnRefs(node)
	if err != nil || len(refs) == 0 {
		return false
	}
	for _, r := range refs {
		if r.Table == "" {
			return false
		}
		t := strings.ToLower(r.Table)
		if !outerTables[t] || innerTables[t] {
			return false
		}
	}
	return true
}

// liftBodyOuterConditions takes every conjunct of an INNER join's ON in the
// body that names the enclosing query OUT of that ON, and returns them for the
// caller to classify beside the WHERE conjuncts. It reports the clause that
// BLOCKS the rewrite, or "" when nothing does.
//
// info is the caller's own fresh parse of the subquery text, so rewriting the
// ON here is local to this attempt: a decline leaves the original SQL, which
// is what the per-outer-row re-run reads.
func liftBodyOuterConditions(info *plansql.SelectInfo, outerTables, innerTables map[string]bool) (lifted []plansql.Node, blocked string) {
	if info == nil {
		return nil, ""
	}
	for i := range info.Joins {
		j := &info.Joins[i]
		if j.CondExpr == nil {
			if namesEnclosingQuery(rawCondNode(j.Condition), outerTables, innerTables) {
				return nil, "a JOIN's ON"
			}
			continue
		}
		var conjuncts []plansql.Node
		flattenASTNodes(j.CondExpr, &conjuncts)
		var keep, take []plansql.Node
		for _, c := range conjuncts {
			if namesEnclosingQuery(c, outerTables, innerTables) {
				take = append(take, c)
				continue
			}
			keep = append(keep, c)
		}
		if len(take) == 0 {
			continue
		}
		if !isInnerOrCrossJoin(j.Type) {
			// A conjunct of an OUTER join's ON is not a WHERE conjunct: it
			// decides which rows PAIR, and the preserved side keeps its row
			// NULL-extended either way. Lifting it would delete the padding
			// PostgreSQL produces.
			return nil, "an outer join's ON"
		}
		if len(keep) == 0 {
			// Nothing would be left to join on. An inner join with an empty
			// condition is a cross join to every reader below this, so the
			// rewrite declines rather than changing what the body means.
			return nil, "a JOIN's ON that names only the enclosing query"
		}
		lifted = append(lifted, take...)
		j.CondExpr = andAll(keep)
		texts := make([]string, len(keep))
		for k, c := range keep {
			texts[k] = c.String()
		}
		j.Condition = strings.Join(texts, " AND ")
	}
	// Everything else a body can hold. Each is a place PostgreSQL reads the
	// outer row and this rewrite has nowhere to put it.
	if namesEnclosingQuery(info.HavingExpr, outerTables, innerTables) {
		return nil, "HAVING"
	}
	if namesEnclosingQuery(info.QualifyExpr, outerTables, innerTables) {
		return nil, "QUALIFY"
	}
	for _, g := range info.GroupByExprs {
		if namesEnclosingQuery(g, outerTables, innerTables) {
			return nil, "GROUP BY"
		}
	}
	for _, ob := range info.OrderBy {
		if namesEnclosingQuery(ob.Expr, outerTables, innerTables) {
			return nil, "ORDER BY"
		}
	}
	for _, c := range info.Columns {
		if namesEnclosingQuery(c.ASTExpr, outerTables, innerTables) {
			return nil, "the SELECT list"
		}
	}
	return lifted, ""
}

// rawCondNode parses an ON clause the parser kept only as TEXT. A clause that
// will not parse is treated as one that may name anything, which is what the
// caller's decline is for.
func rawCondNode(text string) plansql.Node {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	expr, err := plansql.ParseExpression(text)
	if err != nil {
		return &plansql.ColRef{Table: "\x00unparsed", Column: "\x00"}
	}
	return expr
}

// andAll rebuilds a conjunction from its parts.
func andAll(parts []plansql.Node) plansql.Node {
	out := parts[0]
	for _, p := range parts[1:] {
		out = &plansql.AndNode{Left: out, Right: p}
	}
	return out
}

// outerOnlyDisposition says what a decorrelation may do with a condition
// inside the body that names only the ENCLOSING row.
//
// PostgreSQL evaluates the body once per outer row, so such a condition
// decides whether the body produces ANY row for that outer row:
//
//	WHERE o.id IN (SELECT z.id FROM t z WHERE P(o))
//	    ≡ WHERE P(o) AND o.id IN (SELECT z.id FROM t z)
//	WHERE EXISTS (SELECT 1 FROM t z WHERE Q(z) AND P(o))
//	    ≡ WHERE P(o) AND EXISTS (SELECT 1 FROM t z WHERE Q(z))
//
// Both hold in a WHERE, where only TRUE passes: when P(o) is FALSE the body is
// empty, IN over an empty set is FALSE and EXISTS is FALSE, so the row is
// rejected — which is also what the conjunction does, NULL included.
//
// The NEGATED spellings are NOT a conjunction and there is nothing to hoist
// them into:
//
//	WHERE o.id NOT IN (SELECT … WHERE P(o))   ≡ NOT P(o) OR o.id NOT IN (…)
//	WHERE NOT EXISTS (SELECT … WHERE P(o))    ≡ NOT P(o) OR NOT EXISTS (…)
//
// An outer row for which P is false passes BOTH, because the body it would
// have to contradict is empty. So a negated operator declines and the
// subquery is re-run per outer row with the condition where the query wrote it.
//
// Before this, the IN rewrite put the condition in the build side's filter
// with its QUALIFIER STRIPPED — `o.total > 100` became `total > 100` read
// against the subquery's own relation — and the EXISTS rewrite dropped it
// outright under a comment saying the shape "shouldn't happen" (#1104).
func outerOnlyDisposition(negated bool) (hoist bool) { return !negated }
