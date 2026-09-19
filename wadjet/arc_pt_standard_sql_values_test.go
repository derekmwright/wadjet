// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ARC PT — what the new spellings ANSWER, measured against PostgreSQL 17.11
// over the same three rows.
//
// The grammar tables next door (internal/planner/sql/arc_pt_*) assert what
// PARSES. This file asserts the VALUE, which is the half that carries #1168:
// SIMILAR TO parsed all along and answered the opposite truth value on six
// cells, because it was rewritten to `regexp_like` — a different pattern
// language, matched against a SUBSTRING rather than the whole string.
//
// The fixture is a real table so the vec kernels run over columns as well as
// literals: `LEFT(c, 2)` and `SUBSTRING(c FROM p)` have a row path and a
// vector path and the two must answer the same thing.
func TestArcPTTheStandardSpellingsAnswerPostgresValues(t *testing.T) {
	ctx := context.Background()
	db := ptOpenFixture(t, ctx)

	for _, c := range []struct {
		name, sql, want, code, pg string
	}{
		// ---- #1168: SIMILAR TO is the SQL pattern language ---------------
		// The six inverted cells the issue names, plus the rest of the
		// documented language.
		{name: "similar_percent_is_like_not_regex", sql: `SELECT 'abc' SIMILAR TO 'a%' AS v`,
			want: "true", pg: "t"},
		{name: "similar_percent_both_sides", sql: `SELECT 'abc' SIMILAR TO '%b%' AS v`,
			want: "true", pg: "t"},
		{name: "similar_percent_in_the_middle", sql: `SELECT 'abc' SIMILAR TO 'a%c' AS v`,
			want: "true", pg: "t"},
		{name: "similar_underscore", sql: `SELECT 'abc' SIMILAR TO 'a_c' AS v`,
			want: "true", pg: "t"},
		{name: "similar_dot_is_a_literal", sql: `SELECT 'abc' SIMILAR TO 'a.c' AS v`,
			want: "false", pg: "f"},
		{name: "similar_dot_matches_a_dot", sql: `SELECT 'a.c' SIMILAR TO 'a.c' AS v`,
			want: "true", pg: "t"},
		{name: "similar_is_anchored", sql: `SELECT 'abc' SIMILAR TO 'ab' AS v`,
			want: "false", pg: "f"},
		{name: "not_similar_is_anchored", sql: `SELECT 'abc' NOT SIMILAR TO 'ab' AS v`,
			want: "true", pg: "t"},
		{name: "similar_caret_is_a_literal", sql: `SELECT 'abc' SIMILAR TO '^abc' AS v`,
			want: "false", pg: "f"},
		{name: "similar_caret_matches_a_caret", sql: `SELECT '^abc' SIMILAR TO '^abc' AS v`,
			want: "true", pg: "t"},
		{name: "similar_dollar_is_a_literal", sql: `SELECT 'abc' SIMILAR TO 'abc$' AS v`,
			want: "false", pg: "f"},
		{name: "similar_alternation", sql: `SELECT 'abc' SIMILAR TO '(b|a)%' AS v`,
			want: "true", pg: "t"},
		{name: "similar_alternation_no_match", sql: `SELECT 'abc' SIMILAR TO '(b|c)%' AS v`,
			want: "false", pg: "f"},
		{name: "similar_star", sql: `SELECT 'bc' SIMILAR TO 'a*bc' AS v`, want: "true", pg: "t"},
		{name: "similar_plus", sql: `SELECT 'bc' SIMILAR TO 'a+bc' AS v`, want: "false", pg: "f"},
		{name: "similar_question", sql: `SELECT 'bc' SIMILAR TO 'a?bc' AS v`, want: "true", pg: "t"},
		{name: "similar_repeat_exact", sql: `SELECT 'aabc' SIMILAR TO 'a{2}bc' AS v`,
			want: "true", pg: "t"},
		{name: "similar_repeat_range", sql: `SELECT 'aabc' SIMILAR TO 'a{1,2}bc' AS v`,
			want: "true", pg: "t"},
		{name: "similar_repeat_range_no_match", sql: `SELECT 'aabc' SIMILAR TO 'a{3,}bc' AS v`,
			want: "false", pg: "f"},
		{name: "similar_bracket", sql: `SELECT 'abc' SIMILAR TO '[abc]bc' AS v`, want: "true", pg: "t"},
		{name: "similar_bracket_negated", sql: `SELECT 'abc' SIMILAR TO '[^a]bc' AS v`,
			want: "false", pg: "f"},
		{name: "similar_bracket_class", sql: `SELECT 'abc' SIMILAR TO '[[:alpha:]]bc' AS v`,
			want: "true", pg: "t"},
		{name: "similar_empty_pattern", sql: `SELECT '' SIMILAR TO '' AS v`, want: "true", pg: "t"},
		{name: "similar_empty_input_percent", sql: `SELECT '' SIMILAR TO '%' AS v`, want: "true", pg: "t"},
		{name: "similar_null_pattern", sql: `SELECT 'abc' SIMILAR TO NULL AS v`, want: "NULL", pg: "NULL"},
		{name: "similar_null_input", sql: `SELECT NULL SIMILAR TO 'a' AS v`, want: "NULL", pg: "NULL"},
		{name: "similar_default_escape_is_backslash",
			sql: `SELECT 'a%b' SIMILAR TO 'a\%b' AS v`, want: "true", pg: "t"},
		{name: "similar_default_escape_no_match",
			sql: `SELECT 'axb' SIMILAR TO 'a\%b' AS v`, want: "false", pg: "f"},
		{name: "similar_escape_clause", sql: `SELECT 'a%c' SIMILAR TO 'a#%c' ESCAPE '#' AS v`,
			want: "true", pg: "t"},
		{name: "similar_escape_clause_no_match", sql: `SELECT 'abc' SIMILAR TO 'a#%c' ESCAPE '#' AS v`,
			want: "false", pg: "f"},
		// The escape and the character after it go to the regex as `\c`,
		// which is what similar_to_escape emits on the server: `#b` is the
		// word-boundary escape there and answers FALSE, not "literal b".
		{name: "similar_escape_before_a_letter_is_the_regex_escape",
			sql: `SELECT 'abc' SIMILAR TO 'a#bc' ESCAPE '#' AS v`, want: "false", pg: "f"},
		{name: "similar_trailing_escape_matches_nothing",
			sql: `SELECT 'abc' SIMILAR TO 'a\' AS v`, want: "false", pg: "f"},
		// A pattern the language cannot express is a REFUSAL, never NULL.
		{name: "similar_bad_quantifier_refuses", sql: `SELECT 'abc' SIMILAR TO '*' AS v`,
			code: "2201B", pg: "2201B invalid regular expression: quantifier operand invalid"},
		{name: "similar_unbalanced_bracket_refuses", sql: `SELECT 'abc' SIMILAR TO '[' AS v`,
			code: "2201B", pg: "2201B invalid regular expression: brackets [] not balanced"},
		// The WHERE clause the issue measured: COUNT(*) was 0 where
		// PostgreSQL answers 1.
		{name: "similar_in_a_where_clause",
			sql: `SELECT COUNT(*) AS v FROM ptt WHERE s SIMILAR TO 'a%'`, want: "2", pg: "2"},
		{name: "similar_underscore_in_a_where_clause",
			sql: `SELECT COUNT(*) AS v FROM ptt WHERE s SIMILAR TO 'a_c'`, want: "1", pg: "1"},
		{name: "not_similar_in_a_where_clause",
			sql: `SELECT COUNT(*) AS v FROM ptt WHERE s NOT SIMILAR TO 'a%'`, want: "2", pg: "2"},

		// ---- #1169: SUBSTRING -------------------------------------------
		{name: "substring_from_for", sql: `SELECT substring('abcdef' FROM 2 FOR 3) AS v`,
			want: "bcd", pg: "bcd"},
		{name: "substring_from", sql: `SELECT substring('abcdef' FROM 2) AS v`, want: "bcdef", pg: "bcdef"},
		{name: "substring_for", sql: `SELECT substring('abcdef' FOR 3) AS v`, want: "abc", pg: "abc"},
		{name: "substring_zero_length", sql: `SELECT substring('abcdef' FROM 2 FOR 0) AS v`,
			want: "", pg: "the empty string"},
		{name: "substring_from_zero", sql: `SELECT substring('abcdef' FROM 0 FOR 3) AS v`,
			want: "ab", pg: "ab"},
		{name: "substring_from_negative", sql: `SELECT substring('abcdef' FROM -1 FOR 3) AS v`,
			want: "a", pg: "a"},
		{name: "substring_negative_length_refuses",
			sql: `SELECT substring('abcdef' FROM 2 FOR -1) AS v`, code: "22011",
			pg: "22011 negative substring length not allowed"},
		{name: "substring_null_input", sql: `SELECT substring(NULL FROM 2 FOR 3) AS v`,
			want: "NULL", pg: "NULL"},
		{name: "substring_regex", sql: `SELECT substring('abcdef' FROM 'b.d') AS v`,
			want: "bcd", pg: "bcd"},
		{name: "substring_regex_first_group",
			sql: `SELECT substring('abcdef' FROM '(b)(c)') AS v`, want: "b", pg: "b"},
		{name: "substring_regex_no_match", sql: `SELECT substring('abcdef' FROM 'x') AS v`,
			want: "NULL", pg: "NULL"},
		{name: "substring_regex_comma_spelling", sql: `SELECT substring('abcdef', '2') AS v`,
			want: "NULL", pg: "NULL — a text second operand is a PATTERN, not a position"},
		{name: "substring_over_a_column",
			sql: `SELECT substring(s FROM 2 FOR 1) AS v FROM ptt WHERE id = 1`, want: "b", pg: "b"},
		{name: "substring_regex_over_a_column",
			sql: `SELECT substring(s FROM 'b') AS v FROM ptt WHERE id = 1`, want: "b", pg: "b"},

		// ---- #1169: OVERLAY ---------------------------------------------
		{name: "overlay_placing_from_for",
			sql: `SELECT overlay('Txxxxas' PLACING 'hom' FROM 2 FOR 4) AS v`, want: "Thomas", pg: "Thomas"},
		{name: "overlay_placing_from",
			sql: `SELECT overlay('Txxxxas' PLACING 'hom' FROM 2) AS v`, want: "Thomxas", pg: "Thomxas"},
		{name: "overlay_zero_count", sql: `SELECT overlay('abc' PLACING 'XY' FROM 1 FOR 0) AS v`,
			want: "XYabc", pg: "XYabc"},
		{name: "overlay_empty_replacement", sql: `SELECT overlay('abc' PLACING '' FROM 2 FOR 1) AS v`,
			want: "ac", pg: "ac"},
		{name: "overlay_from_zero_refuses", sql: `SELECT overlay('abc' PLACING 'X' FROM 0) AS v`,
			code: "22011", pg: "22011 negative substring length not allowed"},
		{name: "overlay_past_the_end", sql: `SELECT overlay('abc' PLACING 'X' FROM 10) AS v`,
			want: "abcX", pg: "abcX"},
		{name: "overlay_negative_count", sql: `SELECT overlay('abc' PLACING 'X' FROM 2 FOR -1) AS v`,
			want: "aXabc", pg: "aXabc"},
		{name: "overlay_null", sql: `SELECT overlay(NULL PLACING 'X' FROM 2) AS v`,
			want: "NULL", pg: "NULL"},
		{name: "overlay_comma_spelling", sql: `SELECT overlay('abc', 'X', 2) AS v`, want: "aXc", pg: "aXc"},
		{name: "overlay_comma_spelling_four", sql: `SELECT overlay('abc', 'X', 2, 1) AS v`,
			want: "aXc", pg: "aXc"},

		// ---- #1169: LIKE … ESCAPE ---------------------------------------
		{name: "like_escape_matches", sql: `SELECT 'a%b' LIKE 'a!%b' ESCAPE '!' AS v`,
			want: "true", pg: "t"},
		{name: "like_escape_does_not_match_a_wildcard",
			sql: `SELECT 'axb' LIKE 'a!%b' ESCAPE '!' AS v`, want: "false", pg: "f"},
		{name: "not_like_escape", sql: `SELECT 'a%b' NOT LIKE 'a!%b' ESCAPE '!' AS v`,
			want: "false", pg: "f"},
		{name: "ilike_escape", sql: `SELECT 'A%B' ILIKE 'a!%b' ESCAPE '!' AS v`, want: "true", pg: "t"},
		{name: "like_escape_underscore", sql: `SELECT 'a_b' LIKE 'a!_b' ESCAPE '!' AS v`,
			want: "true", pg: "t"},
		{name: "like_escape_escapes_itself", sql: `SELECT 'a!b' LIKE 'a!!b' ESCAPE '!' AS v`,
			want: "true", pg: "t"},
		{name: "like_escape_empty_disables_escaping",
			sql: `SELECT 'a%b' LIKE 'a%b' ESCAPE '' AS v`, want: "true", pg: "t"},
		{name: "like_escape_too_long_refuses", sql: `SELECT 'a%b' LIKE 'a!%b' ESCAPE '!!' AS v`,
			code: "22019", pg: "22019 invalid escape string"},
		{name: "like_escape_null", sql: `SELECT 'a%b' LIKE 'a!%b' ESCAPE NULL AS v`,
			want: "NULL", pg: "NULL"},
		{name: "like_escape_in_a_where",
			sql: `SELECT COUNT(*) AS v FROM ptt WHERE s LIKE 'a!%b' ESCAPE '!'`, want: "1", pg: "1"},

		// ---- #1169: LEFT / RIGHT ----------------------------------------
		{name: "left", sql: `SELECT left('abcdef', 2) AS v`, want: "ab", pg: "ab"},
		{name: "right", sql: `SELECT right('abcdef', 2) AS v`, want: "ef", pg: "ef"},
		{name: "left_negative", sql: `SELECT left('abcdef', -2) AS v`, want: "abcd", pg: "abcd"},
		{name: "right_negative", sql: `SELECT right('abcdef', -2) AS v`, want: "cdef", pg: "cdef"},
		{name: "left_past_the_end", sql: `SELECT left('abc', 10) AS v`, want: "abc", pg: "abc"},
		{name: "left_negative_past_the_end", sql: `SELECT left('abc', -10) AS v`, want: "", pg: "empty"},
		{name: "left_null_count", sql: `SELECT (left('abc', NULL) IS NULL) AS v`,
			want: "true", pg: "NULL — a NULL count is a NULL result, not the empty string"},
		{name: "substring_null_length", sql: `SELECT (substring('abcdef' FROM 2 FOR NULL) IS NULL) AS v`,
			want: "true", pg: "NULL"},
		{name: "substring_null_start", sql: `SELECT (substring('abcdef' FROM NULL) IS NULL) AS v`,
			want: "true", pg: "NULL"},
		{name: "left_null_count_over_a_column",
			sql: `SELECT (left(s, NULL) IS NULL) AS v FROM ptt WHERE id = 1`,
			want: "true", pg: "NULL"},
		{name: "substring_null_start_over_a_column",
			sql: `SELECT (substring(s FROM NULL) IS NULL) AS v FROM ptt WHERE id = 1`,
			want: "true", pg: "NULL"},
		{name: "left_over_a_column", sql: `SELECT left(s, 2) AS v FROM ptt WHERE id = 1`,
			want: "ab", pg: "ab"},
		{name: "right_over_a_column_negative", sql: `SELECT right(s, -1) AS v FROM ptt WHERE id = 1`,
			want: "bc", pg: "bc"},
		// The vec kernel indexed BYTES while the row path counted characters:
		// one of them cut a multibyte character in half.
		{name: "left_over_a_multibyte_column",
			sql: `SELECT left(s, 2) AS v FROM ptt WHERE id = 4`, want: "日本", pg: "日本"},
		{name: "right_over_a_multibyte_column",
			sql: `SELECT right(s, 2) AS v FROM ptt WHERE id = 4`, want: "本語", pg: "本語"},

		// ---- #1169: NORMALIZE -------------------------------------------
		// The one-argument spelling used to STRIP non-printing characters and
		// call that NFC: a combining sequence came back unchanged.
		{name: "normalize_composes", sql: `SELECT length(normalize('e' || chr(769), NFC)) AS v`,
			want: "1", pg: "1"},
		{name: "normalize_decomposes", sql: `SELECT length(normalize('é', NFD)) AS v`,
			want: "2", pg: "2"},
		{name: "normalize_default_is_nfc",
			sql: `SELECT length(normalize('e' || chr(769))) AS v`, want: "1", pg: "1"},
		{name: "normalize_nfkc", sql: `SELECT normalize('abc', NFKC) AS v`, want: "abc", pg: "abc"},
		{name: "normalize_nfkd", sql: `SELECT normalize('abc', NFKD) AS v`, want: "abc", pg: "abc"},

		// ---- #1169: LOCALTIMESTAMP --------------------------------------
		{name: "localtimestamp_is_an_instant",
			sql: `SELECT (localtimestamp::date = current_date) AS v`, want: "true", pg: "t"},

		// ---- #1179: the `#` operator ------------------------------------
		{name: "hash_xor", sql: `SELECT 5 # 3 AS v`, want: "6", pg: "6"},
		{name: "hash_xor_left_associative", sql: `SELECT 5 # 3 # 2 AS v`, want: "4", pg: "4"},
		{name: "hash_xor_negative", sql: `SELECT 5 # -3 AS v`, want: "-8", pg: "-8"},
		{name: "hash_xor_unary_minus_binds_tighter", sql: `SELECT - 5 # 3 AS v`, want: "-8", pg: "-8"},
		{name: "hash_xor_null", sql: `SELECT 5 # NULL AS v`, want: "NULL", pg: "NULL"},
		{name: "hash_xor_big", sql: `SELECT 9223372036854775807 # 1 AS v`,
			want: "9223372036854775806", pg: "9223372036854775806"},
		{name: "hash_xor_is_looser_than_plus", sql: `SELECT 1 + 2 # 3 AS v`, want: "0", pg: "0"},
		{name: "hash_xor_is_looser_than_times", sql: `SELECT 2 * 3 # 1 AS v`, want: "7", pg: "7"},
		{name: "hash_xor_is_tighter_than_comparison", sql: `SELECT (5 # 3 = 6) AS v`,
			want: "true", pg: "t"},
		{name: "hash_xor_over_a_column",
			sql: `SELECT COUNT(*) AS v FROM ptt WHERE n # 1 = 4`, want: "1", pg: "1"},

		// ---- #1180 / #1183: the predicate band's VALUES ------------------
		{name: "between_then_equals", sql: `SELECT (5 BETWEEN 10 AND 1 = true) AS v`,
			want: "false", pg: "f"},
		{name: "between_then_equals_false", sql: `SELECT (5 BETWEEN 10 AND 1 = false) AS v`,
			want: "true", pg: "t"},
		{name: "not_between_then_equals", sql: `SELECT (5 NOT BETWEEN 10 AND 1 = true) AS v`,
			want: "true", pg: "t"},
		{name: "comparison_then_is_true", sql: `SELECT (1 = 1 IS TRUE) AS v`, want: "true", pg: "t"},
		{name: "is_null_then_equals", sql: `SELECT (1 IS NULL = false) AS v`, want: "true", pg: "t"},
		{name: "paren_predicate_is_not_unknown", sql: `SELECT ((1=1) IS NOT UNKNOWN) AS v`,
			want: "true", pg: "t"},
		{name: "null_predicate_is_unknown", sql: `SELECT ((NULL=1) IS UNKNOWN) AS v`,
			want: "true", pg: "t"},
		{name: "is_not_unknown_in_a_where",
			sql: `SELECT COUNT(*) AS v FROM ptt WHERE ((b) AND (n > 1)) IS NOT UNKNOWN`,
			want: "4", pg: "4"},
		{name: "between_comparison_in_a_where",
			sql: `SELECT COUNT(*) AS v FROM ptt WHERE n BETWEEN 1 AND 6 = true`, want: "2", pg: "2"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := ptScalar(ctx, db, c.sql)
			if c.code != "" {
				if err == nil {
					t.Fatalf("answered %q where PostgreSQL 17.11 raises %s\n  SQL: %s", got, c.pg, c.sql)
				}
				if st := sqlerr.StateOf(err); st != c.code {
					t.Errorf("SQLSTATE %q, want %q: %v\n  SQL: %s", st, c.code, err, c.sql)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused a statement PostgreSQL 17.11 answers %s: %v\n  SQL: %s",
					c.pg, err, c.sql)
			}
			if got != c.want {
				t.Errorf("answered %q, want %q (PostgreSQL 17.11: %s)\n  SQL: %s",
					got, c.want, c.pg, c.sql)
			}
		})
	}
}

// ptOpenFixture loads the three rows every cell above is measured over, plus
// a multibyte row for the character-versus-byte kernels.
func ptOpenFixture(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
		{Name: "n", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeBool},
	}}
	if err := db.CreateTable(ctx, "ptt", schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := db.NewIngester("ptt", schema, nil, ingest.Config{MaxBufferRows: 64, RowGroupSize: 64})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "s": "abc", "n": int64(5), "b": true},
		{"id": int64(2), "s": "a%b", "n": int64(3), "b": false},
		{"id": int64(3), "s": "xyz", "n": int64(10), "b": true},
		{"id": int64(4), "s": "日本語", "n": int64(7), "b": true},
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}

// ptScalar answers a one-row, one-column query as text, with NULL spelled
// "NULL" so a NULL and an empty string cannot be confused for each other.
func ptScalar(ctx context.Context, db *DB, sql string) (string, error) {
	res, err := db.Query(ctx, sql)
	if err != nil {
		return "", err
	}
	if len(res.Rows) != 1 {
		return "", sqlerr.New("XX000", "expected one row, got %d", len(res.Rows))
	}
	if len(res.Columns) != 1 {
		return "", sqlerr.New("XX000", "expected one column, got %d", len(res.Columns))
	}
	v, ok := res.Rows[0][res.Columns[0]]
	if !ok || v == nil {
		return "NULL", nil
	}
	if s, ok := v.(string); ok {
		return s, nil
	}
	return ptFormat(v), nil
}

// ptFormat renders a non-string value the way the cells above spell it.
func ptFormat(v any) string { return fmt.Sprintf("%v", v) }
