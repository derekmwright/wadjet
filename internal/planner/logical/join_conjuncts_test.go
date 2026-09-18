// SPDX-License-Identifier: MIT

package logical

import (
	"strings"
	"testing"
)

// THE ON CLAUSE SPLITS ON ITS AST, NOT ON ITS TEXT (#1178).
//
// The table is every spelling that CARRIES the word AND inside one condition,
// beside the spellings that really are two conditions — the distinction the
// textual split could not make. `ON a.x BETWEEN b.lo AND b.hi` was cut into
// `a.x BETWEEN b.lo` and `b.hi`; the first parses as nothing and the second as
// a literal, so the join was refused for a condition PostgreSQL evaluates.
//
// Each case asserts the COUNT and the terms, and that re-joining the terms
// with " AND " re-parses to a tree with the same term count — the property
// that makes the split safe to undo, which is what both join sites do with
// the key half of the clause.
func TestSplitJoinConjunctsSplitsOnTheASTNotTheText(t *testing.T) {
	cases := []struct {
		name string
		cond string
		want []string
	}{
		// --- one condition that CONTAINS the word AND.
		{"between", "a.x BETWEEN b.lo AND b.hi", []string{"a.x BETWEEN b.lo AND b.hi"}},
		{"not between", "a.x NOT BETWEEN b.lo AND b.hi", []string{"a.x NOT BETWEEN b.lo AND b.hi"}},
		{"between lowercase", "a.x between b.lo and b.hi", []string{"a.x between b.lo and b.hi"}},
		{"between of expressions", "a.x + 1 BETWEEN b.lo - 1 AND b.hi * 2",
			[]string{"a.x + 1 BETWEEN b.lo - 1 AND b.hi * 2"}},
		{"string literal holding AND", "a.s = ' AND '", []string{"a.s = ' AND '"}},
		{"string literal holding and", "a.s = 'x and y'", []string{"a.s = 'x and y'"}},
		{"identifier holding and", "a.brand = b.brand", []string{"a.brand = b.brand"}},
		{"case with AND", "CASE WHEN a.x > 1 AND b.y > 1 THEN true ELSE false END",
			[]string{"CASE WHEN a.x > 1 AND b.y > 1 THEN true ELSE false END"}},
		{"parenthesised OR", "(a.x = b.y OR a.z = b.w)", []string{"(a.x = b.y OR a.z = b.w)"}},
		{"parenthesised AND under OR", "(a.x = b.y AND a.z = b.w) OR a.q = b.q",
			[]string{"(a.x = b.y AND a.z = b.w) OR a.q = b.q"}},
		{"function call with two args", "COALESCE(a.x, b.y) = 1", []string{"COALESCE(a.x, b.y) = 1"}},

		// --- genuinely two (or three) conditions.
		{"two equalities", "a.x = b.x AND a.y = b.y", []string{"a.x = b.x", "a.y = b.y"}},
		{"three equalities", "a.x = b.x AND a.y = b.y AND a.z = b.z",
			[]string{"a.x = b.x", "a.y = b.y", "a.z = b.z"}},
		{"equality and between", "a.x = b.x AND a.n BETWEEN b.lo AND b.hi",
			[]string{"a.x = b.x", "a.n between b.lo and b.hi"}},
		{"between and equality", "a.n BETWEEN b.lo AND b.hi AND a.x = b.x",
			[]string{"a.n between b.lo and b.hi", "a.x = b.x"}},
		{"equality and string literal", "a.x = b.x AND a.s = ' AND '",
			[]string{"a.x = b.x", "a.s = ' AND '"}},
		{"parenthesised conjunction", "(a.x = b.x AND a.y = b.y)",
			[]string{"a.x = b.x", "a.y = b.y"}},
		{"equality and parenthesised OR", "a.x = b.x AND (a.y = b.y OR a.z = b.z)",
			[]string{"a.x = b.x", "(a.y = b.y or a.z = b.z)"}},
		{"equality and LIKE", "a.x = b.x AND b.s LIKE 'a%'",
			[]string{"a.x = b.x", "b.s like 'a%'"}},
		{"equality and IN", "a.x = b.x AND b.s IN ('p', 'q')",
			[]string{"a.x = b.x", "b.s in ('p', 'q')"}},
		{"equality and CASE with AND",
			"a.x = b.x AND CASE WHEN a.y > 1 AND b.y > 1 THEN true ELSE false END",
			[]string{"a.x = b.x", "case when a.y > 1 and b.y > 1 then true else false end"}},

		// --- what does not parse keeps the textual split, which is what the
		// physical key parser still refuses loudly.
		{"unparseable", "a.x = AND b.y", []string{"a.x =", "b.y"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := splitJoinConjuncts(tc.cond)
			var texts []string
			for _, c := range got {
				texts = append(texts, strings.TrimSpace(c.text))
			}
			if len(texts) != len(tc.want) {
				t.Fatalf("%q split into %d terms %q, want %d %q",
					tc.cond, len(texts), texts, len(tc.want), tc.want)
			}
			for i := range texts {
				if texts[i] != tc.want[i] {
					t.Errorf("%q term %d: got %q, want %q", tc.cond, i, texts[i], tc.want[i])
				}
			}
			// Re-joining must not re-associate: the rebuilt clause splits
			// back into the same number of terms. Both join sites rebuild
			// JoinCond exactly this way.
			if rejoined := strings.Join(texts, " AND "); len(splitJoinConjuncts(rejoined)) != len(texts) {
				t.Errorf("%q rejoined as %q re-splits into %d terms, want %d",
					tc.cond, rejoined, len(splitJoinConjuncts(rejoined)), len(texts))
			}
		})
	}
}

// A condition with nothing to split keeps its ORIGINAL text, byte for byte:
// the overwhelmingly common ON clause must reach the physical planner exactly
// as the parser handed it over, so this change cannot move a spelling that
// some other site matches on.
func TestSplitJoinConjunctsKeepsASingleTermVerbatim(t *testing.T) {
	for _, cond := range []string{
		`a."WatchID" = b."WatchID"`,
		"a.x = b.y",
		"A.X = B.Y",
		"a.n BETWEEN b.lo AND b.hi",
		"1 = 1",
	} {
		got := splitJoinConjuncts(cond)
		if len(got) != 1 || got[0].text != cond {
			t.Errorf("%q became %d terms %v; a single term must stay verbatim", cond, len(got), got)
		}
	}
}
