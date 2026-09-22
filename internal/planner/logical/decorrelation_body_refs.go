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
//     predicate re-run per outer row, which answers PostgreSQL's rows;
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
// A qualifier is read only when it names an outer relation and NO inner one:
// `dc_out o` outside and `dc_out z` inside makes `o.total` outer and `z.id`
// inner, while `dc_out.total` names both and is not decided here.
//
// It asks nodeTableRefs — the correlation classifier's own reader — with
// bodyOuter as the column map rather than the enclosing one, so an unqualified
// name counts only when the body provably cannot supply it. Reading the clause any other way costs a
// right answer. plansql.ColumnRefs is the strict walker and REFUSES the three
// nodes that carry raw SQL rather than a parsed subtree (a subquery, an
// EXISTS, a window call), so a clause holding one would have to be treated as
// "may name anything" — and `EXISTS (SELECT 1 FROM z WHERE z.k = o.id ORDER BY
// ROW_NUMBER() OVER ())`, whose ORDER BY names nothing outer at all, would
// stop decorrelating and be refused by the per-row rebuild instead of
// answering PostgreSQL's rows (arc L1's `EXISTS/*/winord`). nodeTableRefs has
// a case for a window call and for a nested block, which is the reader this
// question needs.
//
// bodyOuter is the one exception, and it is exact rather than name-only: the
// enclosing columns the body's OWN relations cannot supply (see
// bodyOuterColumns). An unqualified name in it binds to the enclosing row in
// PostgreSQL, and reading it that way here is what keeps `ON b.k = id`, `HAVING
// SUM(b.amt) > total` and `SELECT total` from travelling into the body as a
// name no relation of the body publishes. nil means "not known", which is the
// qualifier-only reading.
func namesEnclosingQuery(node plansql.Node, outerTables, innerTables map[string]bool, bodyOuter map[string]string) bool {
	if node == nil {
		return false
	}
	hasOuter, _ := nodeTableRefs(node, outerTables, innerTables, bodyOuter)
	return hasOuter
}

// bodyOuterColumns is the part of the enclosing query's column map that names
// a column the subquery body's OWN FROM clause does not publish — the
// unqualified names PostgreSQL binds to the enclosing row, because it resolves
// a name innermost-first and the body has nothing of that name.
//
// It is nil when the body's namespace cannot be named COMPLETELY (a table the
// catalog does not answer, a table function, a star over one): absent from a
// partial list is not absent from the body, and reading it as outer would move
// an inner column outward. nil keeps the qualifier-only reading every caller
// had before.
//
// The namespace is read from the catalog through annotate, run on a throwaway
// Scan per relation and never on the plan being built: annotating the real
// inner subtree hands reorderJoins statistics it did not have and moved TPC-H
// Q2's join order (decorrelatedInnerPlan). The rule cannot be the enclosing
// map alone either — Q2 writes `p_partkey = ps_partkey` unqualified with BOTH
// names in it — and it is not: `ps_partkey` is the body's, so it is not here.
//
// undecided is the other half of the answer: when the namespace is NOT known,
// it is the whole enclosing map, and an unqualified name in it is one this
// pass cannot place. The body walker then DECLINES wherever such a name sits
// in a clause the rewrite has no classification for (see
// liftBodyOuterConditions) — the per-row rerun resolves it with the binder's
// own scope — rather than let it travel into the body as a column no relation
// there may publish. A table function in the body's FROM is the ordinary way
// to get here: its columns are its CALL's or its INPUT's, and reading an input
// a second time to answer this question is not a cost this pass may impose.
func bodyOuterColumns(info *plansql.SelectInfo, outerColMap map[string]string,
	ctes []plansql.CTEDef, annotate func(*Node)) (bodyOuter, undecided map[string]string) {
	if info == nil || len(outerColMap) == 0 {
		return nil, nil
	}
	if annotate == nil {
		return nil, outerColMap
	}
	catalog := func(table string) []string {
		n := &Node{Type: NodeScan, TableName: table}
		annotate(n)
		return n.ScanColumns
	}
	own := plansql.FromClauseColumns(info,
		plansql.CTEColumns(scopeCTEs(ctes, info.CTEs), catalog))
	if own == nil {
		return nil, outerColMap
	}
	bodyOuter = make(map[string]string)
	for col, tbl := range outerColMap {
		if !own[strings.ToLower(col)] {
			bodyOuter[col] = tbl
		}
	}
	return bodyOuter, nil
}

// namesUndecided reports whether node holds an UNQUALIFIED name in undecided —
// a name the enclosing query has and the body's namespace, being unknown,
// cannot be asked about.
func namesUndecided(node plansql.Node, undecided map[string]string) bool {
	if node == nil || undecided == nil {
		return false
	}
	hasOuter, _ := nodeTableRefs(node, nil, nil, undecided)
	return hasOuter
}

// provablyOuterOnly reports whether EVERY column reference in node is
// qualified by an enclosing-only relation — the one spelling this pass can
// prove names the outer row and nothing else.
//
// A qualifier makes it provable. The enclosing query's column map alone does
// NOT: it cannot tell the outer column `total` from an inner one of the same
// name — TPC-H Q2's official spelling writes `p_partkey = ps_partkey` inside
// the subquery with no qualifier, and BOTH names are in the enclosing map
// because the enclosing query reads those relations too, so hoisting on the
// map alone would change Q2's row set (ADR-0021 §1r).
//
// bodyOuter (bodyOuterColumns) is what makes an unqualified name provable too:
// a name the body's own relations cannot supply binds to the enclosing row,
// so `WHERE total > 100` over a body with no `total` is the same condition as
// `WHERE o.total > 100`. nil proves nothing unqualified.
func provablyOuterOnly(node plansql.Node, outerTables, innerTables map[string]bool, bodyOuter map[string]string) bool {
	if node == nil {
		return false
	}
	refs, err := plansql.ColumnRefs(node)
	if err != nil || len(refs) == 0 {
		return false
	}
	for _, r := range refs {
		if r.Table == "" {
			if _, ok := bodyOuter[strings.ToLower(r.Column)]; ok {
				continue
			}
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
// readsSelectList says whether the CALLER reads the body's SELECT list. An IN
// and a scalar comparison do — the item is the membership value or the value
// itself — and an EXISTS does not: it asks whether a row exists, PostgreSQL
// does not evaluate the target list for it, and the plan this rewrite builds
// (Scan → [Join …] → [Filter]) never materializes it. Checking it for an
// EXISTS costs a right answer: `EXISTS (SELECT SUM(o.id) OVER () FROM z WHERE
// z.k = o.id)` decorrelates and answers PostgreSQL's rows, and blocking on the
// outer reference in that item refuses it (arc L1's `EXISTS/*/winsel`).
func liftBodyOuterConditions(info *plansql.SelectInfo, outerTables, innerTables map[string]bool, bodyOuter, undecided map[string]string, readsSelectList bool) (lifted []plansql.Node, blocked string) {
	if info == nil {
		return nil, ""
	}
	for i := range info.Joins {
		j := &info.Joins[i]
		if j.CondExpr == nil {
			// An ON the parser kept only as TEXT. Parse it to ask the
			// question; a clause that will not parse is one that may name
			// anything, so it blocks rather than reading as "names nothing".
			cond, ok := parseCondText(j.Condition)
			if !ok || namesEnclosingQuery(cond, outerTables, innerTables, bodyOuter) ||
				namesUndecided(cond, undecided) {
				return nil, "a JOIN's ON"
			}
			continue
		}
		var conjuncts []plansql.Node
		flattenASTNodes(j.CondExpr, &conjuncts)
		var keep, take []plansql.Node
		for _, c := range conjuncts {
			if namesEnclosingQuery(c, outerTables, innerTables, bodyOuter) {
				take = append(take, c)
				continue
			}
			if namesUndecided(c, undecided) {
				return nil, "an unqualified name in a JOIN's ON the body's namespace cannot place"
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
	if namesEnclosingQuery(info.HavingExpr, outerTables, innerTables, bodyOuter) || namesUndecided(info.HavingExpr, undecided) {
		return nil, "HAVING"
	}
	if namesEnclosingQuery(info.QualifyExpr, outerTables, innerTables, bodyOuter) || namesUndecided(info.QualifyExpr, undecided) {
		return nil, "QUALIFY"
	}
	for _, g := range info.GroupByExprs {
		if namesEnclosingQuery(g, outerTables, innerTables, bodyOuter) || namesUndecided(g, undecided) {
			return nil, "GROUP BY"
		}
	}
	// An ORDER BY decides WHICH rows only beside a bound. Without one it
	// changes neither a membership set, nor whether a row exists, nor an
	// aggregate — so an outer reference there is not a reference this rewrite
	// has to carry, and blocking on it would decline a shape that answers.
	if strings.TrimSpace(info.Limit) != "" || strings.TrimSpace(info.Offset) != "" {
		for _, ob := range info.OrderBy {
			if namesEnclosingQuery(ob.Expr, outerTables, innerTables, bodyOuter) || namesUndecided(ob.Expr, undecided) {
				return nil, "ORDER BY beside a bound"
			}
		}
	}
	if readsSelectList {
		for _, c := range info.Columns {
			if namesEnclosingQuery(c.ASTExpr, outerTables, innerTables, bodyOuter) || namesUndecided(c.ASTExpr, undecided) {
				return nil, "the SELECT list"
			}
		}
	}
	return lifted, ""
}

// parseCondText parses an ON clause the parser kept only as TEXT. ok=false
// means it could not be read at all, which the caller treats as "may name
// anything" — an empty clause (a cross join) reads as naming nothing, which it
// does.
func parseCondText(text string) (plansql.Node, bool) {
	if strings.TrimSpace(text) == "" {
		return nil, true
	}
	expr, err := plansql.ParseExpression(text)
	if err != nil {
		return nil, false
	}
	return expr, true
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
