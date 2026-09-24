// SPDX-License-Identifier: MIT

package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// The decorrelation body walk accounts for outer references in every clause.
// Inner-join ON conjuncts may join the WHERE classification; an outer-join
// ON or another clause the rewrite cannot preserve declines to per-row
// execution. No reference may disappear or bind a different relation.
// See ADR-0021 §1r.

// namesEnclosingQuery detects enclosing references through nodeTableRefs.
// A qualifier must name an enclosing relation and no body relation.
// An unqualified name counts only in bodyOuter, the enclosing columns that
// the complete body namespace cannot supply; nil permits qualified names only.
// It must not use plansql.ColumnRefs: that walker refuses subquery, EXISTS
// and window nodes, and a clause holding one would stop decorrelating
// (arc L1's EXISTS/*/winord answers only through nodeTableRefs).
func namesEnclosingQuery(node plansql.Node, outerTables, innerTables map[string]bool, bodyOuter map[string]string) bool {
	if node == nil {
		return false
	}
	hasOuter, _ := nodeTableRefs(node, outerTables, innerTables, bodyOuter)
	return hasOuter
}

// bodyOuterColumns returns enclosing names absent from the complete body
// namespace. annotate reads throwaway scans so it cannot change join costs.
// An incomplete namespace returns nil and the full enclosing map as undecided;
// callers decline unclassifiable clauses without re-reading reader inputs.
// See ADR-0021 §1r.
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

// outerOnlyDisposition permits hoisting an outer-only condition into a
// positive IN or EXISTS filter, preserving its qualifier and NULL behavior.
// Negated forms decline to per-row execution: an empty body makes them true
// even when the outer-only condition is false or unknown (ADR-0021 §1r).
func outerOnlyDisposition(negated bool) (hoist bool) { return !negated }

// bodyWithShadowsEnclosing reports whether the subquery's OWN WITH declares an
// item with the name of an enclosing WITH item. Such a body is not
// decorrelated: the build side would be planned with the enclosing item's
// definition (scopeCTEs puts the body's items after the enclosing ones, and the
// builder takes the first match), which is not the relation PostgreSQL reads.
// Declined, the subquery reaches the per-row re-run, which refuses it by name
// (expr.ShadowingWithError).
func bodyWithShadowsEnclosing(info *plansql.SelectInfo, ctes []plansql.CTEDef) bool {
	if info == nil || len(info.CTEs) == 0 || len(ctes) == 0 {
		return false
	}
	outer := make(map[string]bool, len(ctes))
	for _, c := range ctes {
		outer[strings.ToLower(c.Name)] = true
	}
	for _, c := range info.CTEs {
		if outer[strings.ToLower(c.Name)] {
			return true
		}
	}
	return false
}
