// SPDX-License-Identifier: MIT

package logical

import (
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
		{name: "orderBy",
			body:    "SELECT b.k FROM dc_in b ORDER BY o.total",
			blocked: "ORDER BY"},
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
			lifted, blocked := liftBodyOuterConditions(info, outer, inner)
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
		name string
		expr string
		want bool
	}{
		{"qualifiedOuterOnly", "o.total > 100", true},
		{"qualifiedOuterOnlyTwoColumns", "o.id = o.grp", true},
		{"qualifiedOuterOnlyFunction", "ABS(o.total) > 100", true},
		{"qualifiedOuterOnlyIsNull", "o.id IS NULL", true},
		{"unqualified", "total > 100", false},
		{"mixed", "o.total > b.amt", false},
		{"innerOnly", "b.amt > 10", false},
		{"noColumnAtAll", "1 = 1", false},
		{"holdsASubquery", "o.id IN (SELECT b.k FROM dc_in b)", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node, err := plansql.ParseExpression(tc.expr)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.expr, err)
			}
			if got := provablyOuterOnly(node, outer, inner); got != tc.want {
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
