// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import "fmt"

// THE ARC DC CORPUS — one cell per (where the outer reference is written) ×
// (what the operator is) × (what the enclosing query is).
//
// The rule it enumerates: a correlated subquery's outer references are a SET
// collected over the WHOLE body, and the decorrelation either carries every one
// of them into the join it builds — as a key, as a residual over the (outer,
// inner) row, or as a filter on the OUTER side — or declines to decorrelate and
// runs the body per outer row. Never drops one, never strips its qualifier.
//
// dcSite is one PLACE an outer reference can sit. `item` is what the body
// SELECTs for the set-valued and scalar spellings; `body` is everything from
// FROM onwards, written with `o.` for the enclosing row.
type dcSite struct {
	name    string
	item    string
	body    string
	grouped bool // a GROUP BY body has no scalar spelling here (it is not one row)
}

// dcSites is the position dimension. The names group them:
//
//	where*   — the outer reference is in the body's WHERE
//	on*      — in a JOIN's ON inside the body (#1232)
//	having*  — in the body's HAVING
//	select*  — in the body's SELECT list
//	nested*  — one level down, in a derived table or a nested subquery
//	two*     — two outer references in two different places
//	null*    — the value dimension: a NULL key on one side or the other
func dcSites() []dcSite {
	return []dcSite{
		// --- the body's WHERE, with an inner column (the shape R1 measured) ---
		{name: "whereEq", item: "b.k", body: "FROM dc_in b WHERE b.k = o.id"},
		{name: "whereNe", item: "b.k", body: "FROM dc_in b WHERE b.amt > o.total"},
		{name: "whereFn", item: "b.k", body: "FROM dc_in b WHERE b.k = ABS(o.id)"},
		{name: "whereCast", item: "b.k", body: "FROM dc_in b WHERE b.k = CAST(o.id AS BIGINT)"},
		{name: "whereIDF", item: "b.k", body: "FROM dc_in b WHERE b.k IS DISTINCT FROM o.id"},
		// --- the body's WHERE, naming ONLY the outer row (#1104) ---
		{name: "whereOuterOnly", item: "b.k", body: "FROM dc_in b WHERE o.total > 100"},
		{name: "whereOuterEq", item: "b.k", body: "FROM dc_in b WHERE o.grp = 10"},
		{name: "whereOuterNull", item: "b.k", body: "FROM dc_in b WHERE o.id IS NULL"},
		{name: "whereOuterFn", item: "b.k", body: "FROM dc_in b WHERE ABS(o.total) > 100"},
		{name: "whereBoth", item: "b.k", body: "FROM dc_in b WHERE b.k = o.id AND o.total > 100"},
		{name: "whereEmpty", item: "b.k", body: "FROM dc_in b WHERE b.k = -999 AND o.total > 100"},
		// --- a JOIN's ON inside the body (#1232) ---
		{name: "onEq", item: "b.k", body: "FROM dc_in b JOIN dc_side c ON c.j = b.k AND o.id = b.k"},
		{name: "onNe", item: "b.k", body: "FROM dc_in b JOIN dc_side c ON c.j = b.k AND o.total > b.amt"},
		{name: "onFn", item: "b.k", body: "FROM dc_in b JOIN dc_side c ON c.j = b.k AND b.k = ABS(o.id)"},
		{name: "onIDF", item: "b.k", body: "FROM dc_in b JOIN dc_side c ON c.j = b.k AND b.k IS DISTINCT FROM o.id"},
		{name: "onOuterOnly", item: "b.k", body: "FROM dc_in b JOIN dc_side c ON c.j = b.k AND o.total > 100"},
		{name: "onOuterNull", item: "b.k", body: "FROM dc_in b JOIN dc_side c ON c.j = b.k AND o.id IS NULL"},
		{name: "onSelf", item: "b.k", body: "FROM dc_in b JOIN dc_in c ON c.k = b.k AND o.total > b.amt"},
		{name: "onWhereSplit", item: "b.k", body: "FROM dc_in b JOIN dc_side c ON c.j = b.k AND b.k = o.id WHERE o.total > 100"},
		// --- a LEFT JOIN's ON inside the body: the padding is the answer ---
		{name: "onLeftPad", item: "c.id", body: "FROM dc_in b LEFT JOIN dc_side c ON c.j = b.k AND o.total > c.amt2"},
		{name: "onLeftOuter", item: "c.id", body: "FROM dc_in b LEFT JOIN dc_side c ON c.j = b.k AND o.total > 100"},
		{name: "onLeftEq", item: "c.id", body: "FROM dc_in b LEFT JOIN dc_side c ON c.j = b.k AND c.j = o.id"},
		// --- the body's HAVING ---
		{name: "having", item: "b.k", body: "FROM dc_in b GROUP BY b.k HAVING SUM(b.amt) > o.total", grouped: true},
		{name: "havingOuter", item: "b.k", body: "FROM dc_in b GROUP BY b.k HAVING o.total > 100", grouped: true},
		// --- the body's SELECT list ---
		{name: "selectOuter", item: "o.grp", body: "FROM dc_in b WHERE b.tag = o.grp"},
		{name: "selectOuterOnly", item: "o.grp", body: "FROM dc_in b"},
		// --- one level down ---
		{name: "nestedDerived", item: "b.k", body: "FROM (SELECT d.k FROM dc_in d WHERE d.amt > o.total) b"},
		{name: "nestedSubq", item: "b.k", body: "FROM dc_in b WHERE b.k IN (SELECT c.j FROM dc_side c WHERE c.amt2 > o.total)"},
		{name: "nestedSubqOuter", item: "b.k", body: "FROM dc_in b WHERE b.k IN (SELECT c.j FROM dc_side c WHERE o.total > 100)"},
		// --- two outer references in two places ---
		{name: "twoOnWhere", item: "b.k", body: "FROM dc_in b JOIN dc_side c ON c.j = b.k AND o.total > c.amt2 WHERE b.k = o.id"},
		{name: "twoWhere", item: "b.k", body: "FROM dc_in b WHERE b.k = o.id AND b.amt < o.total"},
		{name: "twoOuterOnly", item: "b.k", body: "FROM dc_in b WHERE b.k = o.id AND o.total > 100 AND o.grp = 10"},
		// --- the same conditions where the inner relation HAS the outer's
		// column name, so stripping the qualifier reads a REAL column and the
		// wrong answer is silent rather than loud ---
		{name: "whereOuterShadow", item: "z.id", body: "FROM dc_out z WHERE o.total > 100"},
		{name: "onOuterShadow", item: "z.id", body: "FROM dc_out z JOIN dc_side c ON c.j = z.id AND o.total > 100"},
		{name: "havingShadow", item: "z.id", body: "FROM dc_out z GROUP BY z.id HAVING o.total > 100", grouped: true},
		{name: "whereShadowCorr", item: "z.id", body: "FROM dc_out z WHERE z.id = o.id AND o.total > 100"},
		// --- an outer reference written WITHOUT a qualifier, and an
		// outer-only condition between TWO outer columns ---
		{name: "whereOuterBare", item: "b.k", body: "FROM dc_in b WHERE total > 100"},
		{name: "whereOuterTwoCols", item: "b.k", body: "FROM dc_in b WHERE o.id = o.grp"},
		{name: "onBareOuter", item: "b.k", body: "FROM dc_in b JOIN dc_side c ON c.j = b.k AND total > 100"},
		// --- a COMMA join in the body: TPC-H Q2's spelling ---
		{name: "commaCorr", item: "b.k", body: "FROM dc_in b, dc_side c WHERE c.j = b.k AND b.k = o.id"},
		{name: "commaNe", item: "b.k", body: "FROM dc_in b, dc_side c WHERE c.j = b.k AND o.total > b.amt"},
		{name: "commaOuterOnly", item: "b.k", body: "FROM dc_in b, dc_side c WHERE c.j = b.k AND o.total > 100"},
		// --- HAVING beside a correlated WHERE, so the body decorrelates and
		// the HAVING's outer reference has to travel with it ---
		{name: "havingWithWhere", item: "b.k", body: "FROM dc_in b WHERE b.tag = o.grp GROUP BY b.k HAVING SUM(b.amt) > o.total", grouped: true},
		{name: "havingOuterWithWhere", item: "b.k", body: "FROM dc_in b WHERE b.tag = o.grp GROUP BY b.k HAVING o.total > 100", grouped: true},
		// --- a RIGHT JOIN's ON: the padded side is the LEFT one ---
		{name: "onRightPad", item: "b.k", body: "FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND o.total > c.amt2"},
		// --- the select list beside a body JOIN ---
		{name: "selectOuterJoin", item: "o.grp", body: "FROM dc_in b JOIN dc_side c ON c.j = b.k"},
		// --- the value dimension: a NULL on the INNER key ---
		{name: "nullInner", item: "n.k", body: "FROM dc_nul n"},
		{name: "nullInnerCorr", item: "n.k", body: "FROM dc_nul n WHERE n.amt < o.total"},
		{name: "nullInnerOuter", item: "n.k", body: "FROM dc_nul n WHERE o.total > 100"},
	}
}

// dcOuter is the ENCLOSING query. `noJoin` reads one relation; `onOuter` is a
// JOIN whose two arms BOTH publish a column named `id`, which is the probe-side
// collision ADR-0021 §1p records — a decorrelated join's probe key is a
// reference, not a bare name.
type dcOuter struct {
	name string
	tmpl string // one %s for the predicate
}

func dcOuters() []dcOuter {
	return []dcOuter{
		{"noJoin", "SELECT o.id AS a FROM dc_out o WHERE %s ORDER BY a"},
		{"onOuter", "SELECT o.id AS a, s.id AS b FROM dc_out o JOIN dc_side s ON s.j = o.id WHERE %s ORDER BY a, b"},
	}
}

type dcCase struct{ name, sql string }

// dcCases writes the table out. The name is the KEY into the recorded
// PostgreSQL row sets and into the pin lists.
func dcCases() []dcCase {
	var out []dcCase
	add := func(name, sql string) { out = append(out, dcCase{name, sql}) }
	for _, oq := range dcOuters() {
		for _, s := range dcSites() {
			set := "SELECT " + s.item + " " + s.body
			exists := "SELECT 1 " + s.body
			p := func(pred string) string { return fmt.Sprintf(oq.tmpl, pred) }
			n := func(op string) string { return op + "/" + oq.name + "/" + s.name }
			add(n("IN"), p("o.id IN ("+set+")"))
			add(n("NOTIN"), p("o.id NOT IN ("+set+")"))
			add(n("EXISTS"), p("EXISTS ("+exists+")"))
			add(n("NOTEXISTS"), p("NOT EXISTS ("+exists+")"))
			// = ANY / <> ALL are IN / NOT IN written the other way; a
			// spelling that takes a different path in the planner is exactly
			// what this table exists to find.
			add(n("ANY"), p("o.id = ANY ("+set+")"))
			add(n("ALL"), p("o.id <> ALL ("+set+")"))
			if !s.grouped {
				scalar := "SELECT MAX(" + s.item + ") " + s.body
				add(n("SCALARWHERE"), p("o.id = ("+scalar+")"))
			}
		}
		// A scalar subquery in the SELECT LIST takes its own enclosing
		// template: there is no predicate to substitute, and the VALUE is the
		// answer rather than which outer rows survive.
		for _, s := range dcSites() {
			if s.grouped {
				continue
			}
			scalar := "SELECT MAX(" + s.item + ") " + s.body
			tmpl := "SELECT o.id AS a, (%s) AS v FROM dc_out o ORDER BY a, v"
			if oq.name == "onOuter" {
				tmpl = "SELECT o.id AS a, s.id AS b, (%s) AS v FROM dc_out o JOIN dc_side s ON s.j = o.id ORDER BY a, b, v"
			}
			add("SCALARSELECT/"+oq.name+"/"+s.name, fmt.Sprintf(tmpl, scalar))
		}
	}
	// The two issues' own shapes, verbatim, over the fixture they were filed
	// against, with the four discriminators arc RS measured beside #1232.
	add("ISSUE/1232", "SELECT o.id AS a FROM lat_ord o WHERE o.id IN (SELECT b.order_id FROM lat_item b JOIN lat_item c ON c.id = b.id AND o.total > b.amount) ORDER BY a")
	add("ISSUE/1232-whereCtl", "SELECT o.id AS a FROM lat_ord o WHERE o.id IN (SELECT b.order_id FROM lat_item b JOIN lat_item c ON c.id = b.id WHERE o.total > b.amount) ORDER BY a")
	add("ISSUE/1232-existsCtl", "SELECT o.id AS a FROM lat_ord o WHERE EXISTS (SELECT 1 FROM lat_item b JOIN lat_item c ON c.id = b.id AND o.total > b.amount AND b.order_id = o.id) ORDER BY a")
	add("ISSUE/1232-existsWhereCtl", "SELECT o.id AS a FROM lat_ord o WHERE EXISTS (SELECT 1 FROM lat_item b JOIN lat_item c ON c.id = b.id WHERE o.total > b.amount AND b.order_id = o.id) ORDER BY a")
	add("ISSUE/1232-plainJoinCtl", "SELECT o.id AS a, b.id AS b FROM lat_ord o JOIN lat_item b ON b.order_id = o.id AND o.total > b.amount ORDER BY a, b")
	add("ISSUE/1104-in", "SELECT o.id AS a FROM lat_ord o WHERE o.id IN (SELECT z.id FROM lat_ord z WHERE o.total > 100) ORDER BY a")
	add("ISSUE/1104-notin", "SELECT o.id AS a FROM lat_ord o WHERE o.id NOT IN (SELECT z.id FROM lat_ord z WHERE o.total > 100) ORDER BY a")
	add("ISSUE/1104-exists", "SELECT o.id AS a FROM lat_ord o WHERE EXISTS (SELECT 1 FROM lat_ord z WHERE z.id = o.id AND o.total > 100) ORDER BY a")
	add("ISSUE/1104-notexists", "SELECT o.id AS a FROM lat_ord o WHERE NOT EXISTS (SELECT 1 FROM lat_ord z WHERE z.id = o.id AND o.total > 100) ORDER BY a")
	add("ISSUE/1104-existsOnly", "SELECT o.id AS a FROM lat_ord o WHERE EXISTS (SELECT 1 FROM lat_ord z WHERE o.total > 100) ORDER BY a")
	add("ISSUE/1104-notexistsOnly", "SELECT o.id AS a FROM lat_ord o WHERE NOT EXISTS (SELECT 1 FROM lat_ord z WHERE o.total > 100) ORDER BY a")
	add("ISSUE/1104-scalar", "SELECT o.id AS a FROM lat_ord o WHERE o.id = (SELECT MAX(z.id) FROM lat_ord z WHERE o.total > 100) ORDER BY a")
	return out
}
