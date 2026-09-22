// SPDX-License-Identifier: MIT

package logical

import (
	"sort"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// THE SEAM, AS A TABLE: every clause of a subquery body × what an outer
// reference written there does to the rewrite.
//
// The row sets are gated on five arms by
// coordinator.TestArcDCADecorrelatedBodyKeepsEveryOuterReferenceOnEveryArm,
// which is the gate that fails at the base commit. This is the same table one
// layer down, where a disposition can be read directly instead of inferred
// from an answer — so a future change that stops LIFTING an inner join's ON,
// or starts lifting an OUTER one, names the clause here rather than showing up
// as a row count somewhere else.
func boolPtr(b bool) *bool { return &b }

// dcBodyOuter is bodyOuterColumns' answer for a dc_out (id, grp, total)
// enclosing query over a body reading dc_in (k, tag, amt) and dc_nul (k, amt):
// all three enclosing names are absent from the body. dcBodyOuterSide is the
// answer when the body reads dc_side (id, j, amt2) instead — `id` is the
// body's there, so it is not outer.
var dcBodyOuterSide = map[string]string{"total": "dc_out", "grp": "dc_out"}

var dcBodyOuter = map[string]string{"total": "dc_out", "grp": "dc_out", "id": "dc_out"}

func TestEveryBodyClauseSaysWhatItDoesWithAnOuterReference(t *testing.T) {
	outer := map[string]bool{"dc_out": true, "o": true}
	inner := map[string]bool{"dc_in": true, "b": true, "dc_side": true, "c": true}

	cases := []struct {
		name string
		body string
		// lifted is the ON conjunct text the walker hands to the
		// classification; blocked is the clause that declines the rewrite.
		lifted  []string
		blocked string
		// readsSelectList is the caller's own question — an IN and a scalar
		// comparison read the body's SELECT item, an EXISTS does not. Default
		// true, so a row says so only when it is testing the EXISTS side.
		readsSelectList *bool
		// bodyOuter is the enclosing columns the body cannot supply
		// (bodyOuterColumns); nil is the qualifier-only reading.
		bodyOuter map[string]string
	}{
		{name: "whereOnly", body: "SELECT b.k FROM dc_in b WHERE b.k = o.id"},
		{name: "innerJoinON",
			body:   "SELECT b.k FROM dc_in b JOIN dc_side c ON c.j = b.k AND o.total > b.amt",
			lifted: []string{"o.total > b.amt"}},
		{name: "innerJoinONOuterOnly",
			body:   "SELECT b.k FROM dc_in b JOIN dc_side c ON c.j = b.k AND o.total > 100",
			lifted: []string{"o.total > 100"}},
		{name: "innerJoinONTwo",
			body:   "SELECT b.k FROM dc_in b JOIN dc_side c ON c.j = b.k AND o.total > b.amt AND b.k = o.id",
			lifted: []string{"o.total > b.amt", "b.k = o.id"}},
		{name: "innerJoinONUnqualified",
			body: "SELECT b.k FROM dc_in b JOIN dc_side c ON c.j = b.k AND total > 100"},
		{name: "crossJoinON",
			body:   "SELECT b.k FROM dc_in b CROSS JOIN dc_side c ON c.j = b.k AND o.id = b.k",
			lifted: []string{"o.id = b.k"}},
		{name: "leftJoinON",
			body:    "SELECT b.k FROM dc_in b LEFT JOIN dc_side c ON c.j = b.k AND o.total > c.amt2",
			blocked: "an outer join's ON"},
		{name: "rightJoinON",
			body:    "SELECT b.k FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND o.total > c.amt2",
			blocked: "an outer join's ON"},
		{name: "innerJoinONNothingLeft",
			body:    "SELECT b.k FROM dc_in b JOIN dc_side c ON o.total > b.amt",
			blocked: "a JOIN's ON that names only the enclosing query"},
		{name: "having",
			body:    "SELECT b.k FROM dc_in b GROUP BY b.k HAVING SUM(b.amt) > o.total",
			blocked: "HAVING"},
		{name: "groupBy",
			body:    "SELECT b.k FROM dc_in b GROUP BY o.total",
			blocked: "GROUP BY"},
		{name: "selectList",
			body:    "SELECT o.grp FROM dc_in b",
			blocked: "the SELECT list"},
		// …and an EXISTS does not read it, so the same body blocks nothing
		// for the caller that discards the item.
		{name: "selectListUnreadByExists",
			body:            "SELECT o.grp FROM dc_in b",
			readsSelectList: boolPtr(false)},
		{name: "selectListWindowUnreadByExists",
			body:            "SELECT SUM(o.id) OVER () FROM dc_in b",
			readsSelectList: boolPtr(false)},
		{name: "selectListWindowReadByIn",
			body:            "SELECT SUM(o.id) OVER () FROM dc_in b",
			readsSelectList: boolPtr(true),
			blocked:         "the SELECT list"},
		// A window call with NO outer reference in a clause the walker reads
		// must not block: the strict walker refuses a window node outright,
		// which is what made arc L1's EXISTS/*/winord stop answering.
		{name: "orderByWindowNoOuterRef",
			body: "SELECT 1 FROM dc_in b ORDER BY ROW_NUMBER() OVER () LIMIT 1"},
		// An ORDER BY with no bound under it changes no membership set, no
		// existence and no aggregate, so an outer reference there is not one
		// this rewrite has to carry. Beside a LIMIT it is.
		{name: "orderByNoBound",
			body: "SELECT b.k FROM dc_in b ORDER BY o.total"},
		{name: "orderByBounded",
			body:    "SELECT b.k FROM dc_in b ORDER BY o.total LIMIT 1",
			blocked: "ORDER BY beside a bound"},
		{name: "qualify",
			body:    "SELECT b.k FROM dc_in b QUALIFY ROW_NUMBER() OVER (PARTITION BY b.k) > o.total",
			blocked: "QUALIFY"},
		// A body with no outer reference outside its WHERE moves nothing and
		// blocks nothing — the control that keeps every row above from being
		// the same answer twice.
		{name: "innerJoinONInnerOnly",
			body: "SELECT b.k FROM dc_in b JOIN dc_side c ON c.j = b.k AND b.amt > 10"},
		{name: "havingInnerOnly",
			body: "SELECT b.k FROM dc_in b GROUP BY b.k HAVING SUM(b.amt) > 10"},
		// An UNQUALIFIED name the body cannot supply is an outer reference in
		// every clause (round 2, B1): lifted from an inner join's ON, and
		// blocking where the rewrite cannot carry it.
		{name: "innerJoinONUnqualifiedOuter",
			body:      "SELECT b.k FROM dc_in b JOIN dc_nul n ON n.k = b.k AND b.k = id",
			bodyOuter: dcBodyOuter,
			lifted:    []string{"b.k = id"}},
		{name: "leftJoinONUnqualifiedOuter",
			body:      "SELECT b.k FROM dc_in b LEFT JOIN dc_side c ON c.j = b.k AND total > 100",
			bodyOuter: dcBodyOuterSide,
			blocked:   "an outer join's ON"},
		{name: "havingUnqualifiedOuter",
			body:      "SELECT b.k FROM dc_in b GROUP BY b.k HAVING SUM(b.amt) > total",
			bodyOuter: dcBodyOuter,
			blocked:   "HAVING"},
		{name: "groupByUnqualifiedOuter",
			body:      "SELECT b.k FROM dc_in b GROUP BY b.k, total",
			bodyOuter: dcBodyOuter,
			blocked:   "GROUP BY"},
		{name: "selectListUnqualifiedOuter",
			body:      "SELECT total FROM dc_in b",
			bodyOuter: dcBodyOuter,
			blocked:   "the SELECT list"},
		// …and a name the body DOES publish is the body's, even though the
		// enclosing query has one too: bodyOuterColumns leaves it out.
		{name: "innerJoinONUnqualifiedShadowed",
			body:      "SELECT b.k FROM dc_in b JOIN dc_side c ON c.j = b.k AND id > 7",
			bodyOuter: dcBodyOuterSide},
		{name: "havingUnqualifiedInner",
			body:      "SELECT b.k FROM dc_in b GROUP BY b.k HAVING SUM(amt) > 10",
			bodyOuter: dcBodyOuter},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := plansql.Parse(tc.body)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			info, err := plansql.ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			reads := true
			if tc.readsSelectList != nil {
				reads = *tc.readsSelectList
			}
			lifted, blocked := liftBodyOuterConditions(info, outer, inner, tc.bodyOuter, nil, reads)
			if blocked != tc.blocked {
				t.Fatalf("blocked = %q, want %q", blocked, tc.blocked)
			}
			got := make([]string, len(lifted))
			for i, n := range lifted {
				got[i] = n.String()
			}
			if strings.Join(got, " ; ") != strings.Join(tc.lifted, " ; ") {
				t.Fatalf("lifted = %v, want %v", got, tc.lifted)
			}
			// What stays in the ON is the other half of the claim: a lift that
			// leaves the conjunct behind would double-apply it.
			for _, j := range info.Joins {
				for _, l := range tc.lifted {
					if strings.Contains(j.Condition, l) {
						t.Errorf("the lifted conjunct %q is still in the ON %q", l, j.Condition)
					}
				}
			}
		})
	}
}

// provablyOuterOnly reads a QUALIFIER and never a bare name. The unqualified
// row is the one that protects TPC-H Q02, whose correlated keys are all
// unqualified and whose names are all in the enclosing column map.
func TestOnlyAQualifiedReferenceIsReadAsOuterOnly(t *testing.T) {
	outer := map[string]bool{"dc_out": true, "o": true}
	inner := map[string]bool{"dc_in": true, "b": true}

	cases := []struct {
		name      string
		expr      string
		want      bool
		bodyOuter map[string]string
	}{
		{"qualifiedOuterOnly", "o.total > 100", true, nil},
		{"qualifiedOuterOnlyTwoColumns", "o.id = o.grp", true, nil},
		{"qualifiedOuterOnlyFunction", "ABS(o.total) > 100", true, nil},
		{"qualifiedOuterOnlyIsNull", "o.id IS NULL", true, nil},
		{"unqualified", "total > 100", false, nil},
		{"mixed", "o.total > b.amt", false, nil},
		{"innerOnly", "b.amt > 10", false, nil},
		{"noColumnAtAll", "1 = 1", false, nil},
		{"holdsASubquery", "o.id IN (SELECT b.k FROM dc_in b)", false, nil},
		// With the body's namespace known, an unqualified name the body
		// cannot supply is provably the enclosing row's (round 2, B1)…
		{"unqualifiedBodyCannotSupply", "total > 100", true, dcBodyOuter},
		{"unqualifiedAndQualified", "total > o.grp", true, dcBodyOuter},
		// …and one the body DOES supply is not, whatever the enclosing
		// query also has.
		{"unqualifiedBodySupplies", "amt > 10", false, dcBodyOuter},
		{"unqualifiedShadowed", "id > 7", false, dcBodyOuterSide},
		{"unqualifiedMixed", "total > b.amt", false, dcBodyOuter},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node, err := plansql.ParseExpression(tc.expr)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.expr, err)
			}
			if got := provablyOuterOnly(node, outer, inner, tc.bodyOuter); got != tc.want {
				t.Fatalf("provablyOuterOnly(%q) = %v, want %v", tc.expr, got, tc.want)
			}
		})
	}
}

// An outer-only condition is hoisted for IN and EXISTS and declined for their
// negations, because only the first is a conjunction. See outerOnlyDisposition.
func TestOnlyTheUnnegatedOperatorHoistsAnOuterOnlyCondition(t *testing.T) {
	if !outerOnlyDisposition(false) {
		t.Error("IN / EXISTS must hoist: `WHERE P(o) AND x IN (…)` is the same predicate")
	}
	if outerOnlyDisposition(true) {
		t.Error("NOT IN / NOT EXISTS must decline: an outer row P rejects PASSES them, " +
			"because the body it would have to contradict is empty")
	}
}

// bodyOuterColumns reads the body's namespace from the catalog and answers
// only when it can name ALL of it: a name absent from a partial list is not
// absent from the body.
func TestTheBodysNamespaceIsCompleteOrUnknown(t *testing.T) {
	catalog := map[string][]string{
		"dc_in":   {"k", "tag", "amt"},
		"dc_side": {"id", "j", "amt2"},
	}
	annotate := func(n *Node) {
		if n.Type == NodeScan {
			n.ScanColumns = catalog[n.TableName]
		}
	}
	outer := map[string]string{"id": "dc_out", "grp": "dc_out", "total": "dc_out", "k": "dc_out"}
	cases := []struct {
		name, body string
		ctes       []plansql.CTEDef
		want       string // sorted keys, "<nil>" for unknown
	}{
		{name: "oneTable", body: "SELECT 1 FROM dc_in b", want: "grp id total"},
		{name: "join", body: "SELECT 1 FROM dc_in b JOIN dc_side c ON c.j = b.k", want: "grp total"},
		{name: "unknownTable", body: "SELECT 1 FROM dc_in b JOIN nowhere z ON z.k = b.k", want: "<nil>"},
		{name: "derived", body: "SELECT 1 FROM (SELECT k AS id FROM dc_in) d", want: "grp k total"},
		{name: "aliasListOverUnknown", body: "SELECT 1 FROM nowhere AS z(total)", want: "<nil>"},
		{name: "aliasListRenames", body: "SELECT 1 FROM dc_in AS z(id)", want: "grp k total"},
		{name: "tableFunction", body: "SELECT 1 FROM generate_series(1, 3) g", want: "<nil>"},
		// A WITH item in scope is a relation with a schema: `c` publishes
		// `total`, so the enclosing `total` is not outer in its body.
		{name: "cteInScope", body: "SELECT 1 FROM c",
			ctes: []plansql.CTEDef{{Name: "c", SQL: "SELECT k AS total FROM dc_in"}},
			want: "grp id k"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := plansql.Parse(tc.body)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			info, err := plansql.ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			got, _ := bodyOuterColumns(info, outer, tc.ctes, annotate)
			s := "<nil>"
			if got != nil {
				keys := make([]string, 0, len(got))
				for k := range got {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				s = strings.Join(keys, " ")
			}
			if s != tc.want {
				t.Fatalf("bodyOuterColumns = %s, want %s", s, tc.want)
			}
		})
	}
	if b, u := bodyOuterColumns(nil, outer, nil, annotate); b != nil || u != nil {
		t.Fatal("no body: nothing to decide")
	}
	if b, u := bodyOuterColumns(&plansql.SelectInfo{}, outer, nil, nil); b != nil || len(u) != len(outer) {
		t.Fatal("no catalog: nothing is provably outer and every enclosing name is undecided")
	}
}
