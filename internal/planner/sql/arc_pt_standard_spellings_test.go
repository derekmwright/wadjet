// SPDX-License-Identifier: MIT

package sql

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// ARC PT / #1169 / #1179 — the SQL-standard spellings whose grammar is
// KEYWORDS rather than commas, and the `#` operator.
//
// Each is a rewrite into a call this engine already answers, enumerated in
// ADR-0038's table (physical.TestEveryGrammarRewriteProducesACallItsSignatureAccepts).
// What this file walks is the GRAMMAR: every documented accepted form, every
// documented rejected one, and the case, whitespace and padding families at
// the boundary — measured on PostgreSQL 17.11, never taken from the issue's
// example spellings.
func TestArcPTParserReadsTheStandardFunctionSpellings(t *testing.T) {
	for _, f := range []struct {
		name   string
		sql    string
		reject bool
		code   string
		pg     string
	}{
		// ---- SUBSTRING --------------------------------------------------
		{name: "substring_from_for", sql: `SELECT substring('abcdef' FROM 2 FOR 3)`, pg: "bcd"},
		{name: "substring_from", sql: `SELECT substring('abcdef' FROM 2)`, pg: "bcdef"},
		{name: "substring_for", sql: `SELECT substring('abcdef' FOR 3)`, pg: "abc"},
		{name: "substring_lowercase_keywords", sql: `SELECT SUBSTRING('abcdef' from 2 for 3)`, pg: "bcd"},
		{name: "substring_padding", sql: `SELECT substring( 'abcdef'  FROM  2  FOR  3 )`, pg: "bcd"},
		{name: "substring_newline", sql: "SELECT substring('abcdef'\nFROM 2\nFOR 3)", pg: "bcd"},
		{name: "substring_comma", sql: `SELECT substring('abcdef', 2, 3)`, pg: "bcd"},
		{name: "substring_column_operands", sql: `SELECT substring(s FROM n FOR n) FROM t`, pg: "per row"},
		{name: "substring_expression_operands",
			sql: `SELECT substring('abcdef' FROM 1+1 FOR 2*1)`, pg: "bc"},
		{name: "substring_regex", sql: `SELECT substring('abcdef' FROM 'b.d')`, pg: "bcd"},
		// The standard's OTHER spelling, refused by name: the `#"` capture
		// markers have no expression in this engine's SIMILAR TO translation.
		{name: "substring_similar_is_refused",
			sql:    `SELECT substring('abcdef' SIMILAR 'a#"b_d#"ef' ESCAPE '#')`,
			reject: true, code: "0A000", pg: "bcd — a spelling this engine refuses by name"},
		{name: "reject_substring_from_nothing", sql: `SELECT substring('abcdef' FROM)`,
			reject: true, pg: "42601 syntax error"},

		// ---- OVERLAY ----------------------------------------------------
		{name: "overlay_placing_from_for",
			sql: `SELECT overlay('Txxxxas' PLACING 'hom' FROM 2 FOR 4)`, pg: "Thomas"},
		{name: "overlay_placing_from",
			sql: `SELECT overlay('Txxxxas' PLACING 'hom' FROM 2)`, pg: "Thomxas"},
		{name: "overlay_lowercase", sql: `SELECT OVERLAY('abc' placing 'X' from 2)`, pg: "aXc"},
		{name: "overlay_comma", sql: `SELECT overlay('abc', 'X', 2)`, pg: "aXc"},
		{name: "overlay_comma_four", sql: `SELECT overlay('abc', 'X', 2, 1)`, pg: "aXc"},
		{name: "overlay_column_operands", sql: `SELECT overlay(s PLACING s FROM n) FROM t`, pg: "per row"},
		{name: "reject_overlay_without_placing", sql: `SELECT overlay('abc' FROM 2)`,
			reject: true, pg: `42601 syntax error at or near "FROM"`},
		{name: "reject_overlay_without_from", sql: `SELECT overlay('abc' PLACING 'X')`,
			reject: true, pg: "42601 syntax error at or near )"},

		// ---- LIKE / ILIKE / SIMILAR TO … ESCAPE -------------------------
		{name: "like_escape", sql: `SELECT 'a%b' LIKE 'a!%b' ESCAPE '!'`, pg: "t"},
		{name: "not_like_escape", sql: `SELECT 'a%b' NOT LIKE 'a!%b' ESCAPE '!'`, pg: "f"},
		{name: "ilike_escape", sql: `SELECT 'A%B' ILIKE 'a!%b' ESCAPE '!'`, pg: "t"},
		{name: "like_escape_lowercase", sql: `SELECT 'a%b' LIKE 'a!%b' escape '!'`, pg: "t"},
		{name: "like_escape_empty", sql: `SELECT 'a%b' LIKE 'a%b' ESCAPE ''`, pg: "t"},
		{name: "like_escape_null", sql: `SELECT 'a%b' LIKE 'a!%b' ESCAPE NULL`, pg: "NULL"},
		{name: "like_escape_in_a_where", sql: `SELECT id FROM t WHERE s LIKE 'a!%b' ESCAPE '!'`, pg: "2"},
		{name: "similar_to_escape", sql: `SELECT 'a_c' SIMILAR TO 'a#_c' ESCAPE '#'`, pg: "t"},
		{name: "not_similar_to_escape", sql: `SELECT 'a_c' NOT SIMILAR TO 'a#_c' ESCAPE '#'`, pg: "f"},
		// A second ESCAPE clause is not a spelling PostgreSQL has.
		{name: "reject_two_escape_clauses",
			sql: `SELECT 'a' LIKE 'a' ESCAPE '!' ESCAPE '!'`, reject: true,
			pg: `42601 syntax error at or near "ESCAPE"`},
		// A column named "escape" is a column: only the BARE word is the
		// clause, so the quoted spelling must not be eaten.
		{name: "quoted_escape_is_a_column",
			sql: `SELECT id FROM t WHERE s LIKE "escape"`, pg: "a column reference"},

		// ---- LOCALTIMESTAMP ---------------------------------------------
		{name: "localtimestamp", sql: `SELECT localtimestamp`, pg: "timestamp without time zone"},
		{name: "localtimestamp_uppercase", sql: `SELECT LOCALTIMESTAMP`, pg: "same"},
		{name: "localtimestamp_precision", sql: `SELECT localtimestamp(3)`, pg: "same"},
		{name: "localtimestamp_in_a_predicate",
			sql: `SELECT id FROM t WHERE localtimestamp > localtimestamp`, pg: "no rows"},
		{name: "localtimestamp_cast", sql: `SELECT localtimestamp::date`, pg: "today"},
		{name: "reject_localtimestamp_text_precision", sql: `SELECT localtimestamp('x')`,
			reject: true, pg: "42601 syntax error"},
		// The RENDERED form. A worker fragment re-parses the filter text this
		// planner rendered, so a spelling the renderer emits and the parser
		// refuses fails the query on the DAG arms and answers on the single
		// one — which is what the five-arm gate found.
		{name: "localtimestamp_rendered_form", sql: `SELECT localtimestamp()`,
			pg: "42601 there; this engine's own rendering of the niladic call"},
		{name: "localtimestamp_rendered_in_a_predicate",
			sql: `SELECT id FROM t WHERE localtimestamp() >= localtimestamp()`,
			pg:  "the rendering a worker fragment re-parses"},

		// ---- NORMALIZE ---------------------------------------------------
		{name: "normalize_bare", sql: `SELECT normalize('abc')`, pg: "abc"},
		{name: "normalize_nfc", sql: `SELECT normalize('abc', NFC)`, pg: "abc"},
		{name: "normalize_nfd", sql: `SELECT normalize('abc', NFD)`, pg: "abc"},
		{name: "normalize_nfkc", sql: `SELECT normalize('abc', NFKC)`, pg: "abc"},
		{name: "normalize_nfkd", sql: `SELECT normalize('abc', NFKD)`, pg: "abc"},
		{name: "normalize_lowercase_form", sql: `SELECT normalize('abc', nfc)`, pg: "abc"},
		{name: "reject_unknown_form", sql: `SELECT normalize('abc', NFZ)`, reject: true,
			code: "42601", pg: `42601 syntax error at or near "NFZ"`},

		// ---- LEFT / RIGHT as function names ------------------------------
		{name: "left_call", sql: `SELECT left('abcdef', 2)`, pg: "ab"},
		{name: "right_call", sql: `SELECT right('abcdef', 2)`, pg: "ef"},
		{name: "left_uppercase", sql: `SELECT LEFT('abcdef', 2)`, pg: "ab"},
		{name: "left_and_right_together", sql: `SELECT LEFT('abc',1) || RIGHT('abc',1)`, pg: "ac"},
		{name: "left_over_a_column", sql: `SELECT left(s, 2) FROM t`, pg: "per row"},
		{name: "left_in_a_predicate", sql: `SELECT id FROM t WHERE left(s, 1) = 'a'`, pg: "1;2"},
		// The join keywords are untouched: LEFT is only a call when a '('
		// follows it, which is where PostgreSQL's own grammar puts it.
		{name: "left_join_still_parses", sql: `SELECT 1 FROM t LEFT JOIN t u ON t.id = u.id`,
			pg: "a join"},
		{name: "right_outer_join_still_parses",
			sql: `SELECT 1 FROM t RIGHT OUTER JOIN t u ON t.id = u.id`, pg: "a join"},
		{name: "left_as_an_output_name", sql: `SELECT 1 AS left`, pg: "1"},
		{name: "reject_left_without_parens", sql: `SELECT left`, reject: true,
			pg: "42601 syntax error at end of input — LEFT is reserved there too"},

		// ---- `#`, the integer XOR operator (#1179) -----------------------
		{name: "hash_xor", sql: `SELECT 5 # 3`, pg: "6"},
		{name: "hash_xor_left_associative", sql: `SELECT 5 # 3 # 2`, pg: "4"},
		{name: "hash_xor_negative_right", sql: `SELECT 5 # -3`, pg: "-8"},
		{name: "hash_xor_unary_left", sql: `SELECT - 5 # 3`, pg: "-8"},
		{name: "hash_xor_null", sql: `SELECT 5 # NULL`, pg: "NULL"},
		{name: "hash_xor_over_columns", sql: `SELECT n # 1 FROM t`, pg: "per row"},
		{name: "hash_xor_no_spaces", sql: `SELECT 5#3`, pg: "6"},
		{name: "hash_xor_under_a_comparison", sql: `SELECT 5 # 3 = 6`, pg: "t"},
		{name: "hash_xor_in_a_between_bound", sql: `SELECT 5 # 3 BETWEEN 6 AND 6`, pg: "t"},
		{name: "hash_xor_in_a_where", sql: `SELECT COUNT(*) FROM t WHERE n # 1 = 4`, pg: "1"},
		{name: "reject_hash_prefix", sql: `SELECT # 3`, reject: true,
			pg: "42883 operator does not exist: # integer — there is no prefix `#`"},
		{name: "reject_hash_dangling", sql: `SELECT 3 #`, reject: true,
			pg: "42601 syntax error at end of input"},
	} {
		t.Run(f.name, func(t *testing.T) {
			_, err := Parse(f.sql)
			if f.reject {
				if err == nil {
					t.Errorf("parsed a form PostgreSQL 17.11 refuses\n  SQL: %s\n  PostgreSQL 17.11: %s",
						f.sql, f.pg)
					return
				}
				if f.code != "" {
					if got := sqlerr.StateOf(err); got != f.code {
						t.Errorf("SQLSTATE %q, want %q: %v", got, f.code, err)
					}
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
