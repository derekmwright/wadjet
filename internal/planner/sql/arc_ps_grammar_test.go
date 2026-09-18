// SPDX-License-Identifier: MIT

package sql

import (
	"strings"
	"testing"
)

// ARC PS — the parser's half of five documented PostgreSQL grammars this
// engine did not read (#1154 BETWEEN SYMMETRIC, #1155 `^`, #1158 and #959
// column-alias lists, #655 JOIN USING / NATURAL JOIN / the leading-dot
// numeric literal).
//
// The table is PostgreSQL 17.11's DOCUMENTED grammar, enumerated from
// §4.1.6 (operator precedence), §9.1 (comparison predicates), §7.2.1.2 (table
// and column aliases) and §7.2.1.1 (joined tables) — every accepted form and
// every rejected form, with case, whitespace and padding families at each
// boundary — never the issues' example spellings. Each `want` below was
// measured against a live postgres:17.11-alpine, not remembered.
//
// This file asserts what the PARSER does: accepts, rejects, and renders back.
// What the engine ANSWERS is wadjet.TestArcPSGrammarAnswersPostgreSQLsValues
// and coordinator.TestArcPSGrammarOnEveryArm; a form that parses here and
// answers wrong there is a defect neither file alone can see.

type psForm struct {
	name string
	sql  string
	// reject is true when PostgreSQL 17.11 REFUSES this spelling and so must
	// this parser. The class is asserted by the answer gates, which can see
	// SQLSTATE; here the question is only accept-or-refuse.
	reject bool
	pg     string // what PostgreSQL 17.11 does, measured
}

// --- #1154: a [NOT] BETWEEN [SYMMETRIC|ASYMMETRIC] b AND c ---------------

func psBetweenForms() []psForm {
	return []psForm{
		{name: "symmetric_reversed", sql: `SELECT 5 BETWEEN SYMMETRIC 10 AND 1`, pg: "t"},
		{name: "symmetric_ordered", sql: `SELECT 5 BETWEEN SYMMETRIC 1 AND 10`, pg: "t"},
		{name: "asymmetric_ordered", sql: `SELECT 5 BETWEEN ASYMMETRIC 1 AND 10`, pg: "t"},
		{name: "asymmetric_reversed", sql: `SELECT 5 BETWEEN ASYMMETRIC 10 AND 1`, pg: "f"},
		{name: "not_symmetric", sql: `SELECT 5 NOT BETWEEN SYMMETRIC 10 AND 1`, pg: "f"},
		{name: "not_asymmetric", sql: `SELECT 5 NOT BETWEEN ASYMMETRIC 10 AND 1`, pg: "t"},
		// SQLancer's spelling, which read as a call to a function named
		// `symmetric` before this arc (#1154).
		{name: "symmetric_parenthesized_bounds", sql: `SELECT 5 BETWEEN SYMMETRIC (10) AND (1)`, pg: "t"},
		{name: "symmetric_one_parenthesized_bound", sql: `SELECT 5 BETWEEN SYMMETRIC (10) AND 1`, pg: "t"},
		{name: "symmetric_lowercase", sql: `SELECT 5 BETWEEN symmetric 10 AND 1`, pg: "t"},
		{name: "symmetric_mixed_case", sql: `SELECT 5 BETWEEN SyMmEtRiC 10 AND 1`, pg: "t"},
		{name: "symmetric_extra_whitespace", sql: "SELECT 5 BETWEEN   SYMMETRIC\t10 AND 1", pg: "t"},
		{name: "symmetric_newline_padded", sql: "SELECT 5 BETWEEN\nSYMMETRIC\n10 AND 1", pg: "t"},
		{name: "symmetric_null_left", sql: `SELECT NULL BETWEEN SYMMETRIC 10 AND 1`, pg: "NULL"},
		{name: "symmetric_null_low", sql: `SELECT 5 BETWEEN SYMMETRIC NULL AND 1`, pg: "NULL"},
		{name: "symmetric_null_high", sql: `SELECT 5 BETWEEN SYMMETRIC 1 AND NULL`, pg: "NULL"},
		{name: "symmetric_both_null", sql: `SELECT 5 BETWEEN SYMMETRIC NULL AND NULL`, pg: "NULL"},
		{name: "symmetric_strings", sql: `SELECT 'b' BETWEEN SYMMETRIC 'c' AND 'a'`, pg: "t"},
		{name: "symmetric_mixed_numeric", sql: `SELECT 1.5 BETWEEN SYMMETRIC 3 AND 1`, pg: "t"},
		{name: "symmetric_dates",
			sql: `SELECT DATE '2020-01-05' BETWEEN SYMMETRIC DATE '2020-01-10' AND DATE '2020-01-01'`, pg: "t"},
		{name: "symmetric_operand_is_an_expression", sql: `SELECT 1 + 2 BETWEEN SYMMETRIC 5 AND 1`, pg: "t"},
		{name: "symmetric_bounds_are_expressions", sql: `SELECT 5 BETWEEN SYMMETRIC 1 + 1 AND 10 - 8`, pg: "f"},
		{name: "symmetric_in_where",
			sql: `SELECT id FROM zzp WHERE id BETWEEN SYMMETRIC 3 AND 2 ORDER BY id`, pg: "2, 3"},
		{name: "symmetric_in_case",
			sql: `SELECT CASE WHEN 5 BETWEEN SYMMETRIC 10 AND 1 THEN 'y' ELSE 'n' END`, pg: "y"},
		{name: "symmetric_in_join_on",
			sql: `SELECT COUNT(*) FROM zzp a JOIN zzj b ON a.id BETWEEN SYMMETRIC b.id AND b.id`, pg: "3"},
		{name: "symmetric_conjunct_right", sql: `SELECT 5 BETWEEN SYMMETRIC 10 AND 1 AND true`, pg: "t"},
		{name: "symmetric_conjunct_left", sql: `SELECT true AND 5 BETWEEN SYMMETRIC 10 AND 1`, pg: "t"},
		// A QUOTED "symmetric" is an identifier, not the modifier — the
		// double quotes say so, and consuming it would read a column as a
		// keyword.
		{name: "quoted_symmetric_is_a_column",
			sql: `SELECT 5 BETWEEN "symmetric" AND 1 FROM zzp`, pg: `a column reference`},

		// Rejected by PostgreSQL 17.11.
		{name: "reject_symmetric_without_high", sql: `SELECT 5 BETWEEN SYMMETRIC 10`,
			reject: true, pg: "42601 syntax error at end of input"},
		{name: "reject_symmetric_alone", sql: `SELECT 5 BETWEEN SYMMETRIC`,
			reject: true, pg: "42601 syntax error at end of input"},
		{name: "reject_both_modifiers", sql: `SELECT 5 BETWEEN SYMMETRIC ASYMMETRIC 1 AND 2`,
			reject: true, pg: `42601 syntax error at or near "ASYMMETRIC"`},
	}
}

func TestArcPSParserReadsPostgreSQLsGrammar(t *testing.T) {
	groups := map[string][]psForm{
		"#1154/between": psBetweenForms(),
	}
	for group, forms := range groups {
		for _, f := range forms {
			t.Run(group+"/"+f.name, func(t *testing.T) {
				_, err := Parse(f.sql)
				if f.reject {
					if err == nil {
						t.Errorf("parsed a spelling this engine must refuse\n  SQL: %s\n  PostgreSQL 17.11: %s",
							f.sql, f.pg)
					}
					return
				}
				if err != nil {
					t.Errorf("refused a spelling PostgreSQL 17.11 accepts: %v\n  SQL: %s\n  PostgreSQL 17.11: %s",
						err, f.sql, f.pg)
				}
			})
		}
	}
}

// TestArcPSRenderingRoundTrips is the renderer half.
//
// Both constructs are EXPANDED at parse time — SYMMETRIC into the pair of
// ordinary BETWEENs PostgreSQL's own grammar expands it to, `^` into the
// `power()` call whose kernel already answers PostgreSQL's values and error
// classes — so `String()` renders the expansion rather than the written form.
// What has to hold is that the rendering RE-READS AS THE SAME TREE: the join
// clause re-parses its own rendered condition, and a rendering that regroups
// under a different precedence is a wrong answer rather than a cosmetic one.
func TestArcPSRenderingRoundTrips(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want string // the rendered WHERE expression
	}{
		{"symmetric", `SELECT 1 FROM t WHERE a BETWEEN SYMMETRIC 3 AND 2`,
			`(a between 3 and 2 or a between 2 and 3)`},
		{"not_symmetric", `SELECT 1 FROM t WHERE a NOT BETWEEN SYMMETRIC 3 AND 2`,
			`(not (a between 3 and 2 or a between 2 and 3))`},
		{"asymmetric_is_the_plain_form", `SELECT 1 FROM t WHERE a BETWEEN ASYMMETRIC 3 AND 2`,
			`a between 3 and 2`},
		// The parentheses are load-bearing: `x AND y BETWEEN SYMMETRIC …`
		// rendered without them re-reads as `(x AND …) OR …`, because AND
		// binds tighter than OR.
		{"symmetric_under_a_conjunction", `SELECT 1 FROM t WHERE x AND a BETWEEN SYMMETRIC 3 AND 2`,
			`x and (a between 3 and 2 or a between 2 and 3)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := psWhere(t, tc.sql)
			if got := info.String(); got != tc.want {
				t.Fatalf("rendered\n  got  %s\n  want %s", got, tc.want)
			}
			// And the rendering re-reads as the same tree, which is the
			// property the join clause depends on.
			again := psWhere(t, "SELECT 1 FROM t WHERE "+info.String())
			if got := again.String(); got != tc.want {
				t.Errorf("re-parsing the rendering changed it\n  got  %s\n  want %s", got, tc.want)
			}
		})
	}
}

func psWhere(t *testing.T, sql string) Node {
	t.Helper()
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect(%q): %v", sql, err)
	}
	if info.WhereExpr == nil {
		t.Fatalf("no WHERE expression in %q", sql)
	}
	return info.WhereExpr
}

// TestArcPSSymmetricIsNotTheLeastGreatestForm is the NULL discriminator, and
// the reason this arc did not implement the equivalence the prose states.
//
// The PostgreSQL documentation describes BETWEEN SYMMETRIC as swapping the
// bounds so a nonempty range is always implied, which reads as
// `BETWEEN least(b,c) AND greatest(b,c)`. It is not that: least() and
// greatest() IGNORE NULL operands, so the least/greatest form answers TRUE for
// `1 BETWEEN SYMMETRIC NULL AND 1` where PostgreSQL 17.11 answers NULL. The
// grammar's own expansion — a disjunction of two ordinary BETWEENs — is what
// this parser emits, and this test fails if anybody replaces it with the
// shorter reading.
func TestArcPSSymmetricIsNotTheLeastGreatestForm(t *testing.T) {
	got := psWhere(t, `SELECT 1 FROM t WHERE a BETWEEN SYMMETRIC b AND c`).String()
	if strings.Contains(got, "least") || strings.Contains(got, "greatest") {
		t.Fatalf("expanded to the least/greatest form, which answers TRUE where "+
			"PostgreSQL 17.11 answers NULL for `1 BETWEEN SYMMETRIC NULL AND 1`: %s", got)
	}
	want := `(a between b and c or a between c and b)`
	if got != want {
		t.Fatalf("expansion\n  got  %s\n  want %s", got, want)
	}
}
