// SPDX-License-Identifier: MIT

package sql

import "testing"

// ARC PS, #1155 — PostgreSQL's `^` exponentiation operator, whose character
// this lexer did not read at all (`unexpected character: ^`).
//
// The table is PostgreSQL 17.11's DOCUMENTED grammar: §4.1.6's precedence
// table for where `^` sits and which way it associates, and §9.3's `^`
// operator for what it answers and what it refuses. Every `pg` note was
// measured against a live postgres:17.11-alpine.
//
// The shared runner and the round-trip harness are in arc_ps_grammar_test.go.

// --- #1155: the `^` exponentiation operator ------------------------------

func psPowerForms() []psForm {
	return []psForm{
		{name: "basic", sql: `SELECT 2 ^ 3`, pg: "8 (double precision)"},
		{name: "no_spaces", sql: `SELECT 2^3`, pg: "8"},
		{name: "left_associative", sql: `SELECT 2 ^ 3 ^ 2`, pg: "64 — (2^3)^2, not 512"},
		{name: "tighter_than_star", sql: `SELECT 2 ^ 3 * 2`, pg: "16"},
		{name: "tighter_than_star_right", sql: `SELECT 2 * 3 ^ 2`, pg: "18"},
		{name: "tighter_than_plus", sql: `SELECT 2 + 3 ^ 2`, pg: "11"},
		{name: "looser_than_unary_minus", sql: `SELECT -2 ^ 2`, pg: "4 — (-2)^2, not -4"},
		{name: "negative_exponent", sql: `SELECT 2 ^ -1`, pg: "0.5"},
		{name: "zero_to_zero", sql: `SELECT 0 ^ 0`, pg: "1"},
		{name: "numeric_operands", sql: `SELECT 2.0 ^ 3.0`, pg: "8.0000000000000000 (numeric)"},
		{name: "explicit_numeric_cast", sql: `SELECT 2::numeric ^ 3::numeric`, pg: "8.0000000000000000"},
		{name: "explicit_float8_cast", sql: `SELECT 2::float8 ^ 3::float8`, pg: "8"},
		{name: "fractional_exponent", sql: `SELECT 2 ^ 0.5`, pg: "1.4142135623730950"},
		{name: "null_right", sql: `SELECT 2 ^ NULL`, pg: "NULL"},
		{name: "null_left", sql: `SELECT NULL ^ 2`, pg: "NULL"},
		{name: "parenthesized_right_assoc", sql: `SELECT 2 ^ (3 ^ 2)`, pg: "512"},
		{name: "over_a_column", sql: `SELECT id ^ 2 FROM zzp`, pg: "1, 4, 9 (double precision)"},
		{name: "over_a_decimal_column", sql: `SELECT d92 ^ 2 FROM zzp`, pg: "numeric"},
		{name: "inside_a_comparison", sql: `SELECT 2 ^ 3 = 8`, pg: "t"},
		{name: "negative_base_fractional_exponent", sql: `SELECT (-2.0) ^ 0.5`,
			pg: "2201F a negative number raised to a non-integer power"},
		{name: "zero_to_negative", sql: `SELECT 0 ^ -1`, pg: "2201F zero raised to a negative power"},
		{name: "float8_overflow", sql: `SELECT 1e308::float8 ^ 2::float8`, pg: "22003 value out of range"},

		// Rejected by PostgreSQL 17.11.
		{name: "reject_prefix", sql: `SELECT ^ 2`, reject: true, pg: `42601 syntax error at or near "^"`},
		{name: "reject_dangling", sql: `SELECT 2 ^`, reject: true, pg: "42601 syntax error at end of input"},
		{name: "reject_doubled", sql: `SELECT 2 ^^ 3`, reject: true,
			pg: "42883 operator does not exist: integer ^^ integer"},
		// `#` is PostgreSQL's integer XOR and is a DIFFERENT operator: `^` must
		// not have taken its meaning. This engine has neither spelling of XOR,
		// so the assertion is that `#` is still refused, never answered as 6.
		{name: "reject_hash_is_not_power", sql: `SELECT 5 # 3`, reject: true,
			pg: "6 — PostgreSQL's integer XOR, which this engine does not implement"},
	}
}

func TestArcPSParserReadsThePowerOperatorsGrammar(t *testing.T) {
	for _, f := range psPowerForms() {
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

// TestArcPSPowerRenderingRoundTrips: `^` is expanded to the `power()` call
// whose kernel already answers PostgreSQL's values and error classes, so
// `String()` renders the call. What has to hold is that the rendering re-reads
// as the same tree at the same precedence.
func TestArcPSPowerRenderingRoundTrips(t *testing.T) {
	for _, tc := range []struct{ name, sql, want string }{
		{"power", `SELECT 1 FROM t WHERE a ^ 2 > 4`, `power(a, 2) > 4`},
		{"power_chain_is_left_associative", `SELECT 1 FROM t WHERE a ^ 2 ^ 3 > 4`,
			`power(power(a, 2), 3) > 4`},
		{"power_binds_tighter_than_multiplication", `SELECT 1 FROM t WHERE a ^ 2 * 3 > 4`,
			`power(a, 2) * 3 > 4`},
		{"unary_minus_binds_tighter_than_power", `SELECT 1 FROM t WHERE -a ^ 2 > 4`,
			`power(-a, 2) > 4`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := psWhere(t, tc.sql)
			if got := info.String(); got != tc.want {
				t.Fatalf("rendered\n  got  %s\n  want %s", got, tc.want)
			}
			again := psWhere(t, "SELECT 1 FROM t WHERE "+info.String())
			if got := again.String(); got != tc.want {
				t.Errorf("re-parsing the rendering changed it\n  got  %s\n  want %s", got, tc.want)
			}
		})
	}
}

// TestArcPSPowerPublishesTheOperatorsName pins what a client reads out of
// RowDescription: PostgreSQL publishes an OPERATOR result under `?column?`,
// not under the name of the function the operator is implemented by. The
// expansion to `power()` would otherwise have renamed the column.
func TestArcPSPowerPublishesTheOperatorsName(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{`SELECT 2 ^ 3`, UnnamedOutputColumn},
		{`SELECT 2 ^ 3 AS v`, "v"},
		{`SELECT power(2, 3)`, "power"},
	} {
		parsed, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.sql, err)
		}
		info, err := ExtractSelect(parsed)
		if err != nil {
			t.Fatal(err)
		}
		if len(info.Columns) != 1 {
			t.Fatalf("%s: %d columns", tc.sql, len(info.Columns))
		}
		if got := OutputColumnName(info.Columns[0]); got != tc.want {
			t.Errorf("%s published %q, want %q (PostgreSQL 17.11)", tc.sql, got, tc.want)
		}
	}
}
