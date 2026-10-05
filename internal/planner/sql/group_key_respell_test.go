// SPDX-License-Identifier: MIT

package sql

import (
	"strings"
	"testing"
)

// RespellGroupKeyTerms' contract (#1524): in a single-relation block a term
// that IS a key once the block's OWN qualifier is erased is spelled as the
// key; nothing else is touched — not an outer relation's qualifier, not a
// join's, not an aggregate's argument, not a SELECT alias named in HAVING or
// ORDER BY, and not a term that differs from the key in anything but that
// qualifier.
func TestRespellGroupKeyTermsErasesOnlyTheBlocksOwnQualifier(t *testing.T) {
	for _, c := range []struct {
		name, sql string
		items     []string // the items' Expr after the pass
		having    string
		orderBy   []string
	}{
		{"itemQualified", "SELECT t.i + 1, count(*) FROM ss_t t GROUP BY i + 1",
			[]string{"i + 1", "count(*)"}, "", nil},
		{"keyQualified", "SELECT i + 1 AS k FROM ss_t t GROUP BY t.i + 1",
			[]string{"t.i + 1"}, "", nil},
		{"insideALargerExpression", "SELECT (t.i + 1) * 2 AS k FROM ss_t t GROUP BY i + 1",
			[]string{"(i + 1) * 2"}, "", nil},
		{"bareColumnKey", "SELECT n AS k FROM ss_t t GROUP BY t.n",
			[]string{"t.n"}, "", nil},
		{"tableNameUnaliased", "SELECT ss_t.n * 2 AS k FROM ss_t GROUP BY n * 2",
			[]string{"n * 2"}, "", nil},
		{"havingAndOrderBy", "SELECT 2 * t.n AS k FROM ss_t t GROUP BY 2 * n HAVING 2 * t.n > 0 ORDER BY 2 * t.n, k",
			[]string{"2 * n"}, "(2 * n) > 0", []string{"2 * n", "k"}},
		{"ordinalUnaliased", "SELECT 2 * t.n, count(*) FROM ss_t t GROUP BY 2 * n ORDER BY 1",
			[]string{"2 * n", "count(*)"}, "", []string{"2 * n"}},
		{"aggregateArgumentUntouched", "SELECT sum(t.i + 1) AS s FROM ss_t t GROUP BY i + 1",
			[]string{"sum(t.i + 1)"}, "", nil},
		{"commutedIsNotTheKey", "SELECT 1 + t.i AS k FROM ss_t t GROUP BY i + 1",
			[]string{"1 + t.i"}, "", nil},
		{"anOuterQualifierIsNotSpelling", "SELECT 2 * o.n AS k FROM ss_t x GROUP BY 2 * n",
			[]string{"2 * o.n"}, "", nil},
		{"aJoinIsNotOneRelation", "SELECT t.i + 1 AS k FROM ss_t t JOIN ss_i x ON x.id = t.id GROUP BY i + 1",
			[]string{"t.i + 1"}, "", nil},
		{"anAliasInHavingNamesTheOutput", "SELECT 2 * t.n AS n FROM ss_t t GROUP BY t.n, 2 * n HAVING n > 0",
			[]string{"2 * n"}, "n > 0", nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			pq, err := Parse(c.sql)
			if err != nil {
				t.Fatal(err)
			}
			info, err := ExtractSelect(pq)
			if err != nil {
				t.Fatal(err)
			}
			RespellGroupKeyTerms(info)
			RespellGroupKeyTerms(info) // idempotent
			for i, want := range c.items {
				if got := info.Columns[i].Expr; got != want {
					t.Errorf("item %d: %q, want %q", i, got, want)
				}
				if got := info.Columns[i].ASTExpr.String(); got != want {
					t.Errorf("item %d AST: %q, want %q", i, got, want)
				}
			}
			if c.having != "" && info.Having != c.having {
				t.Errorf("HAVING %q, want %q", info.Having, c.having)
			}
			for i, want := range c.orderBy {
				if got := strings.TrimSpace(info.OrderBy[i].Column); got != want {
					t.Errorf("ORDER BY %d: %q, want %q", i, got, want)
				}
			}
		})
	}
}

// BlockIdentity is ExprIdentity with the one qualifier erased, and nothing
// else: an outer qualifier keeps its identity apart.
func TestBlockIdentityErasesOneQualifier(t *testing.T) {
	parse := func(s string) Node {
		n, err := ParseExpression(s)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if a, b := BlockIdentity(parse("T.I + 1"), "t"), BlockIdentity(parse("(i+1)"), "t"); a != b {
		t.Errorf("%q != %q", a, b)
	}
	if a, b := BlockIdentity(parse("o.i + 1"), "t"), BlockIdentity(parse("i + 1"), "t"); a == b {
		t.Errorf("an outer qualifier was erased: %q", a)
	}
	if a, b := BlockIdentity(parse("t.i + 1"), ""), ExprIdentity(parse("t.i + 1")); a != b {
		t.Errorf("no own relation: %q, want ExprIdentity %q", a, b)
	}
}
