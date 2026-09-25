// SPDX-License-Identifier: MIT

package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A DECORRELATED LATERAL CARRIES THE OUTER ROW INTO ITS `WHERE` AND NOWHERE
// ELSE, AND THE OTHER CLAUSES SAY SO OUT LOUD.
//
// `buildLateralSubquery` decorrelates by splitting the body's WHERE into the
// predicates that name the enclosing row and the ones that do not, promoting
// the first into the JOIN condition. That is the whole of the mechanism: the
// body below the join is then planned over its OWN relations, where the outer
// query's columns do not exist.
//
// An outer reference in any OTHER clause of the body therefore resolves
// against the inner relation — to the inner column of that bare name where one
// exists, and to nothing where it does not. Measured against live PostgreSQL
// 17.11 over the `lat_ord` / `lat_item` rows, before this refusal, PER ARM
// (single / spilled512k on the left of the slash, the three DAG arms on the
// right — they are not the same, which the first version of this comment said
// they were):
//
//	SELECT s.m FROM lat_ord o JOIN LATERAL (
//	  SELECT o.total + i.amount AS m FROM lat_item i WHERE i.order_id = o.id) s ON true
//	-- PostgreSQL  200, 250, 275, 325     before  NULL ×4 on all five arms
//
//	  SELECT o.id AS m …          1, 1, 2, 2   1, 2, 3, 4 (`i.id`) / RIGHT on the DAG
//	  … GROUP BY o.id             150, 200     100, 50, 125, 75 on all five
//	  … HAVING SUM(…) > o.total   no rows      3 rows of NULL on all five
//	  SELECT CASE WHEN o.id > 1 … 0, 0, 1, 1   0, 1, 1, 1 on all five
//
// **THE `SELECT o.id AS m` ROW IS A REGRESSION ON THREE ARMS AND IT IS TAKEN
// DELIBERATELY.** The three DAG arms carried PostgreSQL's exact row set there
// and are loud now; so are `LIFTED/leftArm`'s three DAG arms and
// `OUTERREF/whereInequality`'s `dag-shuffled`, seven (cell, arm) results in
// all, measured at `c34cdbcb` by this arc. A refusal is a property of the
// PLAN and there is no per-arm spelling of one; and an answer that is right on
// three arms and wrong on two is a two-path split, which this engine does not
// ship either. Loud on five is the only disposition available, and the cells
// are in the L1 table with PostgreSQL's answer beside them so the day the
// single-process arms can answer, all five do.
//
// **One cell moves from right to loud, and its rightness was the fixture's.**
// An outer reference in the body's own `ORDER BY` — `ORDER BY i.amount *
// o.total LIMIT 1` — answered PostgreSQL's rows because multiplying every row
// of one outer key by that key's own constant does not change their order.
// The term reads NULL for the outer column just like the others; the ordering
// it produced was the unordered one, and the fixture's ties fell the right
// way. It is refused with its siblings rather than left as an accident.
//
// **What is NOT refused**, and each for its own reason:
//
//   - a body with NO FROM clause. It is a PROJECTION OVER THE OUTER ROW and is
//     lowered as one (ADR-0021 §1n, lateral_dual_body.go) — that path never
//     reaches here.
//   - the WHERE clause itself, which is what the decorrelation reads.
//   - a reference whose qualifier the BODY'S OWN FROM ITEM or CTE shadows.
//     `buildLateralSubquery` subtracts those names from `leftAliases` before
//     the WHERE split, because SQL scoping resolves them to the inner item:
//     without the subtraction `FROM lat_ord x, LATERAL (SELECT SUM(x.amount)
//     … FROM lat_item x …)` was refused for reading an outer row it never
//     touches (round-2 review).
//   - an UNCORRELATED body, which names no outer column anywhere.
func refuseLateralOuterReferenceOutsideWhere(info *plansql.SelectInfo, leftAliases map[string]bool) error {
	if info == nil || len(leftAliases) == 0 {
		return nil
	}
	clauses := []struct {
		name  string
		nodes []plansql.Node
	}{
		{"SELECT list", lateralSelectListNodes(info)},
		{"GROUP BY", lateralGroupByNodes(info)},
		{"HAVING", []plansql.Node{info.HavingExpr}},
		{"ORDER BY", lateralSortNodes(info)},
		{"QUALIFY", []plansql.Node{info.QualifyExpr}},
	}
	for _, c := range clauses {
		for _, n := range c.nodes {
			if ref := lateralOuterRefIn(n, leftAliases); ref != "" {
				return sqlerr.New("0A000",
					"LATERAL body's %s reads %s from the enclosing query: the "+
						"correlation is lowered into a JOIN, so the body is planned over "+
						"its own relations and a reference to the outer row resolves "+
						"there — to the inner column of that name, or to nothing. Only "+
						"the body's WHERE clause carries the outer row today. Write the "+
						"expression in the ENCLOSING query's SELECT list over the "+
						"lateral's own output, or move the reference into the body's "+
						"WHERE clause",
					c.name, sqlerr.Quote(ref))
			}
		}
	}
	return nil
}

// lateralSelectListNodes is every tree a body's SELECT list holds: the item's
// own expression and, for an aggregate, its ARGUMENTS — which are a FIELD of
// the item rather than a node under it and are missed by a walk that reads
// ASTExpr alone (ADR-0021 §1h's own lesson about `AggArgs`).
func lateralSelectListNodes(info *plansql.SelectInfo) []plansql.Node {
	var out []plansql.Node
	for _, c := range info.Columns {
		out = append(out, c.ASTExpr, c.AggArgExpr)
		out = append(out, c.AggArgs...)
	}
	return out
}

func lateralGroupByNodes(info *plansql.SelectInfo) []plansql.Node {
	var out []plansql.Node
	out = append(out, info.GroupByExprs...)
	for _, g := range info.GroupBy {
		if parsed, err := plansql.ParseExpression(strings.TrimSpace(g)); err == nil {
			out = append(out, parsed)
		}
	}
	return out
}

// lateralSortNodes is the body's ORDER BY, and it is EMPTY for a body with no
// FROM clause.
//
// Such a body yields at most one row, so a sort over it is the IDENTITY and an
// outer name in it does not make the body correlated — arc C1's measured
// position (`LATERAL (SELECT 7 AS v ORDER BY u.id)` answers PostgreSQL's rows
// on five arms, and so does a second lateral sorting on the first's output).
// The clause decides nothing there, so there is nothing to refuse.
func lateralSortNodes(info *plansql.SelectInfo) []plansql.Node {
	if len(info.Tables) == 0 {
		return nil
	}
	return lateralOrderByNodes(info)
}

func lateralOrderByNodes(info *plansql.SelectInfo) []plansql.Node {
	var out []plansql.Node
	for _, ob := range info.OrderBy {
		if ob.Expr != nil {
			out = append(out, ob.Expr)
			continue
		}
		if parsed, err := plansql.ParseExpression(strings.TrimSpace(ob.Column)); err == nil {
			out = append(out, parsed)
		}
	}
	return out
}

// lateralOuterRefIn returns the first QUALIFIED reference in n whose qualifier
// is one of the enclosing query's relation names, or "".
//
// Only a qualified reference counts. A BARE name binds the inner relation when
// the inner supplies it and the enclosing row when it does not (ADR-0021 §1k),
// and this walk has no schema to tell those apart — so it speaks about the
// spelling that is unambiguous and leaves the other to the resolvers that can.
func lateralOuterRefIn(n plansql.Node, leftAliases map[string]bool) string {
	if n == nil {
		return ""
	}
	found := ""
	walkExprNodes(n, func(x plansql.Node) {
		if found != "" {
			return
		}
		ref, ok := x.(*plansql.ColRef)
		if !ok || ref.Table == "" {
			return
		}
		if leftAliases[strings.ToLower(ref.Table)] {
			found = ref.Table + "." + ref.Column
		}
	})
	return found
}

// refuseOuterReferenceThroughLateralSubquery refuses a correlated predicate of
// a LATERAL body that reaches the enclosing relation THROUGH a subquery whose
// own FROM holds a LATERAL join — `… LATERAL (SELECT q.qid FROM jp_q q WHERE
// … AND EXISTS (SELECT 1 FROM jp_j j JOIN LATERAL (…) t ON true WHERE j.id =
// q.qid AND t.xv > o.id)) s`. A subquery with a LATERAL join does not keep its
// correlation with the query around it on any execution path (the EXISTS
// admits every row even at top level — filed, arc JP round 4 N1), so the
// reference to `o` is not evaluated per outer row and the lateral answered
// every pair for PostgreSQL's two. Until 2026-09-24 the text path refused it
// by accident (it split the key at the first `=` inside the EXISTS); the
// parsed equality let the plan build (b2070cbb). Loud until N1 is fixed
// (arc JP round 4, B4).
func refuseOuterReferenceThroughLateralSubquery(correlatedParts []string) error {
	for _, cp := range correlatedParts {
		node, err := plansql.ParseExpression(cp)
		if err != nil || node == nil {
			continue
		}
		found := false
		walkExprNodes(node, func(x plansql.Node) {
			var body string
			switch e := x.(type) {
			case *plansql.ExistsNode:
				body = e.SQL
			case *plansql.SubqueryNode:
				body = e.SQL
			default:
				return
			}
			if !found && subqueryJoinsLaterally(body) {
				found = true
			}
		})
		if found {
			return sqlerr.New("0A000",
				"LATERAL body's correlated predicate %s reads the enclosing relation inside a "+
					"subquery whose FROM holds a LATERAL join, and such a subquery does not keep "+
					"its correlation with the query around it on this engine, so the reference "+
					"would not be evaluated per outer row. Join the subquery's relations without "+
					"LATERAL, or move the condition out of the lateral body",
				sqlerr.Quote(strings.TrimSpace(cp)))
		}
	}
	return nil
}

// subqueryJoinsLaterally reports whether a subquery's own FROM holds a LATERAL
// join (a subquery the parser cannot read is judged by its text).
func subqueryJoinsLaterally(sql string) bool {
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return strings.Contains(strings.ToUpper(sql), "LATERAL")
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil || info == nil {
		return strings.Contains(strings.ToUpper(sql), "LATERAL")
	}
	for _, j := range info.Joins {
		if j.Lateral {
			return true
		}
	}
	return false
}
