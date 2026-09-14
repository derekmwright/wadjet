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
// 17.11 over the `lat_ord` / `lat_item` rows, before this refusal:
//
//	SELECT s.m FROM lat_ord o JOIN LATERAL (
//	  SELECT o.total + i.amount AS m FROM lat_item i WHERE i.order_id = o.id) s ON true
//	-- PostgreSQL  200, 250, 275, 325      this engine  NULL, NULL, NULL, NULL
//
//	  SELECT o.id AS m …                     1, 1, 2, 2          1, 2, 3, 4  (i.id)
//	  … GROUP BY o.id                        150, 200            100, 50, 125, 75
//	  … HAVING SUM(i.amount) > o.total       no rows             3 rows of NULL
//	  SELECT CASE WHEN o.id > 1 …            0, 0, 1, 1          0, 1, 1, 1
//
// Every one of them is a SILENT wrong value on every arm. `0A000` —
// PostgreSQL's feature_not_supported — is the honest disposition until the
// body is evaluated per outer row or its outer-reading items are computed
// ABOVE the join, which is ADR-0021's dependent-join layer and not a rewrite
// this pass can make.
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
		{"ORDER BY", lateralOrderByNodes(info)},
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
