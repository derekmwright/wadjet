// SPDX-License-Identifier: MIT

package sql

import (
	"strings"
	"testing"
)

// ARC PT / #1180 / #1183 — the PREDICATE band is a precedence LOOP.
//
// PostgreSQL's §4.1.6 table, from tighter to looser:
//
//	(any other operator)            `#`
//	BETWEEN IN LIKE ILIKE SIMILAR
//	< > = <= >= <>                  NONASSOCIATIVE
//	IS ISNULL NOTNULL
//
// Every one of those constructs used to RETURN from parseComparison as soon
// as it was read, so anything after it was "trailing input after the end of
// the statement" — `5 BETWEEN 10 AND 1 = true` (#1180), `1 = 1 IS TRUE`,
// `1 IS NULL = false` (#1183's other half). The forms PostgreSQL REFUSES are
// half this table: comparison is nonassociative there, and reading the band
// as a loop with no bound would accept `1 = 1 = true`, which 17.11 does not.
//
// Every row was measured on PostgreSQL 17.11.
func TestArcPTTheP4PredicateBandParsesAtPostgresPrecedence(t *testing.T) {
	for _, f := range []struct {
		name   string
		sql    string
		reject bool
		pg     string
	}{
		// ---- BETWEEN binds tighter than a comparison (#1180) ------------
		{name: "between_then_equals", sql: `SELECT 5 BETWEEN 10 AND 1 = true`,
			pg: "f — (5 BETWEEN 10 AND 1) = true"},
		{name: "between_then_equals_false", sql: `SELECT 5 BETWEEN 10 AND 1 = false`, pg: "t"},
		{name: "not_between_then_equals", sql: `SELECT 5 NOT BETWEEN 10 AND 1 = true`, pg: "t"},
		{name: "between_then_not_equals", sql: `SELECT 1 BETWEEN 0 AND 2 <> false`, pg: "t"},
		{name: "symmetric_between_then_equals", sql: `SELECT 5 BETWEEN SYMMETRIC 10 AND 1 = true`, pg: "t"},
		{name: "between_then_equals_under_a_conjunction",
			sql: `SELECT 5 BETWEEN 1 AND 10 = true AND 1=1`, pg: "t"},
		{name: "between_then_equals_a_parenthesized_predicate",
			sql: `SELECT 2 BETWEEN 1 AND 3 = (1=1)`, pg: "t"},
		{name: "between_in_a_where_clause",
			sql: `SELECT COUNT(*) FROM t WHERE n BETWEEN 1 AND 6 = true`, pg: "2 over the fixture"},
		{name: "a_comparisons_right_side_may_be_a_between",
			sql: `SELECT true = 1 BETWEEN 0 AND 2`, pg: "t — true = (1 BETWEEN 0 AND 2)"},

		// ---- the other members of the band ------------------------------
		{name: "in_then_equals", sql: `SELECT 1 IN (1,2) = true`, pg: "t"},
		{name: "like_then_equals", sql: `SELECT 'a' LIKE 'a' = true`, pg: "t"},
		{name: "similar_to_then_equals", sql: `SELECT 'abc' SIMILAR TO 'a%' = true`, pg: "t"},

		// ---- IS is looser than a comparison, and may be followed by one --
		{name: "comparison_then_is_true", sql: `SELECT 1=1 IS TRUE`, pg: "t — (1=1) IS TRUE"},
		{name: "comparison_then_is_not_unknown", sql: `SELECT 1 = 1 IS NOT UNKNOWN`, pg: "t"},
		{name: "arithmetic_comparison_then_is_true", sql: `SELECT 1 + 1 = 2 IS TRUE`, pg: "t"},
		{name: "is_null_then_equals", sql: `SELECT 1 IS NULL = false`, pg: "t — (1 IS NULL) = false"},
		{name: "is_null_then_is_false", sql: `SELECT 1 IS NULL IS FALSE`, pg: "t"},
		{name: "is_true_chained", sql: `SELECT 1 = 1 IS TRUE IS FALSE`, pg: "f"},
		{name: "between_then_is_true", sql: `SELECT 5 BETWEEN 10 AND 1 IS TRUE`, pg: "f"},
		{name: "between_then_is_not_true", sql: `SELECT 5 BETWEEN 10 AND 1 IS NOT TRUE`, pg: "t"},
		{name: "in_then_is_true", sql: `SELECT 1 IN (1) IS TRUE`, pg: "t"},

		// ---- IS [NOT] UNKNOWN over a parenthesized predicate (#1183) ----
		{name: "paren_predicate_is_not_unknown", sql: `SELECT (1=1) IS NOT UNKNOWN`, pg: "t"},
		{name: "paren_conjunction_is_not_unknown", sql: `SELECT ((1=1) AND (2=2)) IS NOT UNKNOWN`, pg: "t"},
		{name: "paren_like_is_not_unknown", sql: `SELECT (('a' LIKE 'a')) IS NOT UNKNOWN`, pg: "t"},
		{name: "in_a_where_clause",
			sql: `SELECT COUNT(*) FROM t WHERE ((b) AND (n > 1)) IS NOT UNKNOWN`, pg: "3"},
		{name: "in_a_join_on_clause",
			sql: `SELECT COUNT(*) FROM t a JOIN t c ON ((a.s) LIKE (c.s)) IS NOT UNKNOWN`, pg: "9"},
		{name: "in_a_group_by", sql: `SELECT COUNT(*) FROM t GROUP BY (b) IS NOT UNKNOWN`, pg: "3"},
		{name: "in_an_order_by", sql: `SELECT id FROM t ORDER BY (b) IS NOT UNKNOWN, id`, pg: "1;2;3"},
		{name: "in_a_select_list", sql: `SELECT (b) IS NOT UNKNOWN FROM t`, pg: "t;t;t"},
		{name: "in_a_having", sql: `SELECT COUNT(*) FROM t HAVING (COUNT(*) > 0) IS NOT UNKNOWN`, pg: "3"},

		// ---- REFUSED by PostgreSQL 17.11 too ----------------------------
		// Comparison is NONASSOCIATIVE there: the loop must stop after one.
		{name: "reject_chained_equals", sql: `SELECT 1 = 1 = true`, reject: true,
			pg: `42601 syntax error at or near "="`},
		{name: "reject_chained_less_than", sql: `SELECT 1 < 2 < 3`, reject: true,
			pg: `42601 syntax error at or near "<"`},
		{name: "reject_two_comparisons_after_is", sql: `SELECT 1 IS NULL = false = true`, reject: true,
			pg: `42601 syntax error at or near "="`},
		// BETWEEN is nonassociative too.
		{name: "reject_chained_between", sql: `SELECT 5 BETWEEN 3 AND 10 BETWEEN 0 AND 1`, reject: true,
			pg: `42601 syntax error at or near "BETWEEN"`},
		{name: "reject_is_not_without_a_check", sql: `SELECT 1 IS NOT`, reject: true,
			pg: "42601 syntax error at end of input"},
		{name: "reject_is_distinct_without_from", sql: `SELECT 1 IS DISTINCT`, reject: true,
			pg: "42601 syntax error at end of input"},
		// A comparison after an IS postfix is allowed even when one was read
		// before it — `1 = 1 IS TRUE = true` is `t` on 17.11 — which is why
		// the loop RESETS at an IS rather than refusing outright.
		{name: "comparison_after_is_after_comparison", sql: `SELECT 1 = 1 IS TRUE = true`,
			pg: "t"},
	} {
		t.Run(f.name, func(t *testing.T) {
			_, err := Parse(f.sql)
			if f.reject {
				if err == nil {
					t.Errorf("parsed a form PostgreSQL 17.11 refuses\n  SQL: %s\n  PostgreSQL 17.11: %s",
						f.sql, f.pg)
				}
				return
			}
			if err != nil {
				t.Errorf("refused a form PostgreSQL 17.11 answers: %v\n  SQL: %s\n  PostgreSQL 17.11: %s",
					err, f.sql, f.pg)
			}
		})
	}
}

// TestArcPTThePredicateBandRendersBackAtItsOwnPrecedence is the renderer
// half: the join clause RE-PARSES its own rendered condition, so a rendering
// that regroups under a different precedence is a wrong answer rather than a
// cosmetic difference (the property arc PS's SYMMETRIC expansion states).
func TestArcPTThePredicateBandRendersBackAtItsOwnPrecedence(t *testing.T) {
	for _, tc := range []struct{ name, sql, want string }{
		{"between_then_equals", `SELECT 1 FROM t WHERE a BETWEEN 1 AND 2 = true`,
			`a between 1 and 2 = true`},
		{"comparison_then_is_true", `SELECT 1 FROM t WHERE a = 1 IS TRUE`,
			`a = 1 is true`},
		{"is_null_then_equals", `SELECT 1 FROM t WHERE a IS NULL = false`,
			`a is null = false`},
		{"hash_xor", `SELECT 1 FROM t WHERE a # 3 = 6`,
			`bitwise_xor(a, 3) = 6`},
		{"like_escape", `SELECT 1 FROM t WHERE a LIKE 'x!%' ESCAPE '!'`,
			`like_escape(a, 'x!%', '!')`},
		{"not_like_escape", `SELECT 1 FROM t WHERE a NOT LIKE 'x!%' ESCAPE '!'`,
			`not like_escape(a, 'x!%', '!')`},
		{"similar_to", `SELECT 1 FROM t WHERE a SIMILAR TO 'x%'`,
			`similar_to(a, 'x%')`},
		{"similar_to_escape", `SELECT 1 FROM t WHERE a SIMILAR TO 'x#%' ESCAPE '#'`,
			`similar_to(a, 'x#%', '#')`},
		// The SQL-standard spellings render as the CALLS they lower to, and a
		// worker fragment re-parses that text: `localtimestamp()` is the
		// rendering of a niladic call and the parser refused it, so the DAG
		// arms failed a query the single arm answered (five-arm gate).
		{"substring_from_for", `SELECT 1 FROM t WHERE substring(a FROM 2 FOR 3) = 'x'`,
			`substring(a, 2, 3) = 'x'`},
		{"substring_for", `SELECT 1 FROM t WHERE substring(a FOR 3) = 'x'`,
			`substring(a, 1, 3) = 'x'`},
		{"overlay", `SELECT 1 FROM t WHERE overlay(a PLACING 'X' FROM 2) = 'x'`,
			`overlay(a, 'X', 2) = 'x'`},
		{"normalize_form", `SELECT 1 FROM t WHERE normalize(a, NFD) = 'x'`,
			`normalize(a, 'NFD') = 'x'`},
		{"localtimestamp", `SELECT 1 FROM t WHERE a < localtimestamp`,
			`a < localtimestamp()`},
		{"left_call", `SELECT 1 FROM t WHERE left(a, 2) = 'x'`, `left(a, 2) = 'x'`},
		{"right_call", `SELECT 1 FROM t WHERE right(a, 2) = 'x'`, `right(a, 2) = 'x'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := psWhere(t, tc.sql)
			got := info.String()
			if got != tc.want {
				t.Fatalf("rendered\n  got  %s\n  want %s", got, tc.want)
			}
			again := psWhere(t, "SELECT 1 FROM t WHERE "+got)
			if second := again.String(); second != tc.want {
				t.Errorf("re-parsing the rendering changed it\n  got  %s\n  want %s", second, tc.want)
			}
			if strings.Contains(got, "regexp_like") {
				t.Errorf("SIMILAR TO still renders as regexp_like, which is a different "+
					"pattern language (#1168): %s", got)
			}
		})
	}
}
