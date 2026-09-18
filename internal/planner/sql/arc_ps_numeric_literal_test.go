// SPDX-License-Identifier: MIT

package sql

import "testing"

// ARC PS, #655 — the LEADING-DOT numeric literal.
//
// `.5` was `unexpected token "."` before v0.18.x; the lexer reads a dot
// followed by a digit as the start of a number now. This file is the grammar
// table that face of #655 never had: PostgreSQL §4.1.2.1's numeric-constant
// grammar, every accepted form and every rejected one, including the forms
// where the dot is NOT a number (a qualifier separator) and the forms where a
// digit run is followed by junk. Measured on 17.11.
//
// The shared runner is in arc_ps_grammar_test.go.

func psNumericLiteralForms() []psForm {
	return []psForm{
		{name: "leading_dot", sql: `SELECT .5`, pg: "0.5"},
		{name: "leading_dot_negated", sql: `SELECT -.5`, pg: "-0.5"},
		{name: "leading_dot_zero", sql: `SELECT .0`, pg: "0.0"},
		{name: "leading_dot_two_digits", sql: `SELECT .25`, pg: "0.25"},
		{name: "leading_dot_exponent", sql: `SELECT .5e3`, pg: "500"},
		{name: "leading_dot_exponent_upper", sql: `SELECT .5E3`, pg: "500"},
		{name: "trailing_dot", sql: `SELECT 1.`, pg: "1"},
		{name: "trailing_dot_exponent", sql: `SELECT 1.e3`, pg: "1000"},
		{name: "leading_dot_in_arithmetic", sql: `SELECT .5 + 1`, pg: "1.5"},
		{name: "leading_dot_in_a_comparison", sql: `SELECT 5 > .5`, pg: "t"},
		{name: "leading_dot_with_a_digit_separator", sql: `SELECT .5_0`, pg: "0.50"},
		{name: "digit_separator", sql: `SELECT 1_000`, pg: "1000"},
		// The dot is still the QUALIFIER separator when a digit does not
		// follow it, which is the control the lexer rule must not move.
		{name: "qualifier_dot_is_still_a_dot", sql: `SELECT a.b FROM (SELECT 1 AS b) a`, pg: "1"},

		// Rejected by PostgreSQL 17.11.
		{name: "reject_bare_dot", sql: `SELECT .`, reject: true, pg: `42601 syntax error at or near "."`},
		{name: "reject_double_dot", sql: `SELECT ..5`, reject: true, pg: `42601 syntax error at or near ".."`},
		{name: "reject_dot_after_a_number", sql: `SELECT 1..5`, reject: true,
			pg: `42601 syntax error at or near ".."`},
		{name: "reject_two_dotted_parts", sql: `SELECT .5.5`, reject: true,
			pg: `42601 syntax error at or near ".5"`},
		{name: "reject_trailing_junk", sql: `SELECT .5abc`, reject: true,
			pg: "42601 trailing junk after numeric literal"},
	}
}

func TestArcPSParserReadsTheNumericLiteralGrammar(t *testing.T) {
	for _, f := range psNumericLiteralForms() {
		t.Run(f.name, func(t *testing.T) {
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
