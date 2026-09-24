// SPDX-License-Identifier: MIT

package sql

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

func TestParseDelete_Basic(t *testing.T) {
	q, err := Parse("DELETE FROM users WHERE id = 5")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if q.Type != QueryDelete {
		t.Fatalf("expected QueryDelete, got %v", q.Type)
	}
	if q.Delete.Table != "users" {
		t.Fatalf("expected table 'users', got %q", q.Delete.Table)
	}
	if q.Delete.WhereSQL != "id = 5" {
		t.Fatalf("expected WHERE 'id = 5', got %q", q.Delete.WhereSQL)
	}
}

func TestParseDelete_NoWhere(t *testing.T) {
	q, err := Parse("DELETE FROM events")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if q.Type != QueryDelete {
		t.Fatalf("expected QueryDelete, got %v", q.Type)
	}
	if q.Delete.Table != "events" {
		t.Fatalf("expected table 'events', got %q", q.Delete.Table)
	}
	if q.Delete.WhereSQL != "" {
		t.Fatalf("expected empty WHERE, got %q", q.Delete.WhereSQL)
	}
}

func TestParseDelete_ComplexWhere(t *testing.T) {
	q, err := Parse("DELETE FROM users WHERE name = 'Alice' AND status != 'active'")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if q.Delete.WhereSQL != "name = 'Alice' AND status != 'active'" {
		t.Fatalf("expected compound WHERE, got %q", q.Delete.WhereSQL)
	}
}

func TestParseUpdate_Basic(t *testing.T) {
	q, err := Parse("UPDATE users SET name = 'Bob' WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if q.Type != QueryUpdate {
		t.Fatalf("expected QueryUpdate, got %v", q.Type)
	}
	if q.Update.Table != "users" {
		t.Fatalf("expected table 'users', got %q", q.Update.Table)
	}
	if len(q.Update.SetClauses) != 1 {
		t.Fatalf("expected 1 SET clause, got %d", len(q.Update.SetClauses))
	}
	if q.Update.SetClauses[0].Column != "name" {
		t.Fatalf("expected column 'name', got %q", q.Update.SetClauses[0].Column)
	}
	// The value is the expression's SQL TEXT, quotes included. It used to be
	// the lexer's unquoted token value, which made `SET name = 'Bob'` and
	// `SET name = Bob` the same string — so no consumer could tell a string
	// LITERAL from a COLUMN REFERENCE, and the SET resolver had to guess
	// (#678). The reversal is deliberate.
	if q.Update.SetClauses[0].Value != "'Bob'" {
		t.Fatalf("expected value \"'Bob'\", got %q", q.Update.SetClauses[0].Value)
	}
	if q.Update.WhereSQL != "id = 1" {
		t.Fatalf("expected WHERE 'id = 1', got %q", q.Update.WhereSQL)
	}
}

func TestParseUpdate_MultipleSets(t *testing.T) {
	q, err := Parse("UPDATE users SET name = 'Bob', age = 30, status = 'active' WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if len(q.Update.SetClauses) != 3 {
		t.Fatalf("expected 3 SET clauses, got %d", len(q.Update.SetClauses))
	}
	expected := []struct {
		col string
		val string
	}{
		{"name", "'Bob'"},
		{"age", "30"},
		{"status", "'active'"},
	}
	for i, exp := range expected {
		if q.Update.SetClauses[i].Column != exp.col {
			t.Errorf("clause %d: expected column %q, got %q", i, exp.col, q.Update.SetClauses[i].Column)
		}
		if q.Update.SetClauses[i].Value != exp.val {
			t.Errorf("clause %d: expected value %q, got %q", i, exp.val, q.Update.SetClauses[i].Value)
		}
	}
}

func TestParseUpdate_NoWhere(t *testing.T) {
	q, err := Parse("UPDATE events SET status = 'archived'")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if q.Update.Table != "events" {
		t.Fatalf("expected table 'events', got %q", q.Update.Table)
	}
	if q.Update.WhereSQL != "" {
		t.Fatalf("expected empty WHERE, got %q", q.Update.WhereSQL)
	}
}

func TestParseInsert_Basic(t *testing.T) {
	q, err := Parse("INSERT INTO users (name, age) VALUES ('Alice', 30)")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if q.Type != QueryInsert {
		t.Fatalf("expected QueryInsert, got %v", q.Type)
	}
	if q.Insert.Table != "users" {
		t.Fatalf("expected table 'users', got %q", q.Insert.Table)
	}
	if len(q.Insert.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(q.Insert.Columns))
	}
	if q.Insert.Columns[0] != "name" || q.Insert.Columns[1] != "age" {
		t.Fatalf("expected columns [name, age], got %v", q.Insert.Columns)
	}
	if len(q.Insert.Values) != 1 {
		t.Fatalf("expected 1 value row, got %d", len(q.Insert.Values))
	}
	if len(q.Insert.Values[0]) != 2 {
		t.Fatalf("expected 2 values, got %d", len(q.Insert.Values[0]))
	}
}

func TestParseInsert_MultipleRows(t *testing.T) {
	q, err := Parse("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if len(q.Insert.Values) != 3 {
		t.Fatalf("expected 3 value rows, got %d", len(q.Insert.Values))
	}
	// RE-QUOTED: a string literal's KIND has to survive a []string, and the
	// quotes are the only thing that carries it. Bare, `'NULL'` and the NULL
	// keyword were the same four letters and both stored a SQL NULL (#690).
	if q.Insert.Values[1][0] != "'Bob'" {
		t.Fatalf("expected row 1 val 0 = \"'Bob'\", got %q", q.Insert.Values[1][0])
	}
}

func TestParseInsert_NoColumns(t *testing.T) {
	q, err := Parse("INSERT INTO events VALUES ('click', 100)")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if len(q.Insert.Columns) != 0 {
		t.Fatalf("expected 0 columns (implicit), got %d", len(q.Insert.Columns))
	}
	if len(q.Insert.Values) != 1 {
		t.Fatalf("expected 1 value row, got %d", len(q.Insert.Values))
	}
}

func TestParseDelete_MissingFrom(t *testing.T) {
	_, err := Parse("DELETE users WHERE id = 1")
	if err == nil {
		t.Fatal("expected error for missing FROM")
	}
}

func TestParseUpdate_MissingSet(t *testing.T) {
	_, err := Parse("UPDATE users name = 'Bob'")
	if err == nil {
		t.Fatal("expected error for missing SET")
	}
}

func TestParseInsert_MissingValues(t *testing.T) {
	_, err := Parse("INSERT INTO users (name)")
	if err == nil {
		t.Fatal("expected error for missing VALUES")
	}
}

// #447: the VALUES tuple was split one LEXER TOKEN per value, with commas
// merely skipped, so the entry count was the token count. A unary minus is its
// own token — the lexer is right to make it one — and `VALUES (4, -3)`
// therefore produced ["4","-","3"] and failed with "expected 2 values, got 3".
//
// The same loop broke on the first ')' at ANY depth, so a nested parenthesis
// left the tuple's own ')' unconsumed and the statement parsed SUCCESSFULLY
// with truncated values.
func TestParseInsert_ValueSplitting(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want [][]string
	}{
		{"unary_minus", `INSERT INTO t (a, b) VALUES (4, -3)`, [][]string{{"4", "-3"}}},
		{"unary_plus", `INSERT INTO t (a, b) VALUES (4, +3)`, [][]string{{"4", "3"}}},
		{"both_negative", `INSERT INTO t (a, b) VALUES (-4, -3)`, [][]string{{"-4", "-3"}}},
		{"negative_float", `INSERT INTO t (a, b) VALUES (-4.5, 3)`, [][]string{{"-4.5", "3"}}},
		{"negative_multi_row", `INSERT INTO t (a, b) VALUES (-1, 2), (3, -4)`,
			[][]string{{"-1", "2"}, {"3", "-4"}}},
		{"redundant_parens", `INSERT INTO t (a, b) VALUES ((1), 2)`, [][]string{{"1", "2"}}},
		{"nested_parens_around_sign", `INSERT INTO t (a, b) VALUES (((-3)), 2)`,
			[][]string{{"-3", "2"}}},
		// A string literal arrives RE-QUOTED, commas and parens inside it
		// included: it is one lexer token and always was, but the quotes are
		// what tell the converter it is a STRING rather than the NULL keyword
		// or an identifier. Unquoted, `'NULL'` and `NULL` were the same four
		// letters and both stored a SQL NULL (#690). The doubled apostrophe
		// the re-quote writes is the one convertValue un-doubles.
		{"string_with_comma", `INSERT INTO t (a, b) VALUES (1, 'a, b')`,
			[][]string{{"1", "'a, b'"}}},
		{"string_with_paren", `INSERT INTO t (a, b) VALUES (1, 'has (paren)')`,
			[][]string{{"1", "'has (paren)'"}}},
		{"string_with_escaped_quote", `INSERT INTO t (a, b) VALUES (1, 'it''s')`,
			[][]string{{"1", "'it''s'"}}},
		{"a string spelling NULL is not the keyword", `INSERT INTO t (a, b) VALUES (1, 'NULL')`,
			[][]string{{"1", "'NULL'"}}},
		{"null_keyword", `INSERT INTO t (a, b) VALUES (1, NULL)`, [][]string{{"1", "NULL"}}},
		{"single_column", `INSERT INTO t (a) VALUES (-7)`, [][]string{{"-7"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if q.Insert == nil {
				t.Fatal("no InsertInfo")
			}
			if len(q.Insert.Values) != len(tc.want) {
				t.Fatalf("got %d rows, want %d: %#v", len(q.Insert.Values), len(tc.want), q.Insert.Values)
			}
			for i, wantRow := range tc.want {
				gotRow := q.Insert.Values[i]
				if len(gotRow) != len(wantRow) {
					t.Fatalf("row %d: got %d values %#v, want %d %#v",
						i, len(gotRow), gotRow, len(wantRow), wantRow)
				}
				for j := range wantRow {
					if gotRow[j] != wantRow[j] {
						t.Errorf("row %d value %d = %q, want %q", i, j, gotRow[j], wantRow[j])
					}
				}
			}
		})
	}
}

// VALUES accepts a full scalar expression in each position (#1252) — the
// same grammar SELECT's expression parser reads — so the parser's job is
// only to find each value's own SOURCE TEXT, not to decide what shapes are
// legal. The old loop refused every one of these ("VALUES accepts literals,
// not the expression …"); this parser reconstructs the text instead and
// leaves evaluating it — and refusing the shapes this engine cannot
// evaluate with no FROM, a column reference or a subquery — to
// wadjet/dml.go's assignInsertValue, one layer up where the target column's
// declaration is in hand (TestAssignInsertValue* there covers that half).
func TestParseInsert_AcceptsExpressionsAndCapturesSourceText(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want [][]string
	}{
		{"arithmetic", `INSERT INTO t (a) VALUES (2 * 3)`, [][]string{{"2 * 3"}}},
		{"function_call", `INSERT INTO t (a) VALUES (coalesce(1, 2))`,
			[][]string{{"coalesce ( 1 , 2 )"}}},
		{"column_ref_reconstructs_too", `INSERT INTO t (a, b) VALUES (1, a + 1)`,
			[][]string{{"1", "a + 1"}}},
		{"typed_literal", `INSERT INTO t (a) VALUES (TIMESTAMP '2026-01-01 00:00:00')`,
			[][]string{{"TIMESTAMP '2026-01-01 00:00:00'"}}},
		{"cast", `INSERT INTO t (a) VALUES (CAST('1' AS INTEGER))`,
			[][]string{{"CAST ( '1' AS INTEGER )"}}},
		{"second_row_restarts_count", `INSERT INTO t (a, b) VALUES (1, 2), (3, 4 * 5)`,
			[][]string{{"1", "2"}, {"3", "4 * 5"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if len(q.Insert.Values) != len(tc.want) {
				t.Fatalf("got %d rows, want %d: %#v", len(q.Insert.Values), len(tc.want), q.Insert.Values)
			}
			for i, wantRow := range tc.want {
				gotRow := q.Insert.Values[i]
				if len(gotRow) != len(wantRow) {
					t.Fatalf("row %d: got %d values %#v, want %d %#v",
						i, len(gotRow), gotRow, len(wantRow), wantRow)
				}
				for j := range wantRow {
					if gotRow[j] != wantRow[j] {
						t.Errorf("row %d value %d = %q, want %q", i, j, gotRow[j], wantRow[j])
					}
				}
			}
		})
	}
}

// An entry with NO tokens at all is still refused at parse time: there is no
// expression to reconstruct, and VALUES () is not a shape PostgreSQL accepts
// for a table with columns either.
func TestParseInsert_StillRefusesEmptyValue(t *testing.T) {
	if q, err := Parse(`INSERT INTO t (a) VALUES ()`); err == nil {
		t.Fatalf("VALUES () parsed with no error: %#v", q.Insert)
	}
}

// A refusal that describes WHAT is wrong but not WHICH entry it was leaves the
// author of `VALUES (1, 'a', <bad>, 4)` to find the value by inspection. Every
// per-value refusal carries the value's 1-based position in the tuple, and the
// position must be the value's own — an off-by-one is exactly as unhelpful as
// no position at all, so these cases pin a middle and a last entry, and the
// count restarts with each tuple.
//
// The "not the expression" reason this test used to pin for a bare
// arithmetic or function-call VALUES entry is gone (#1252): those shapes
// parse now, covered by TestParseInsert_AcceptsExpressionsAndCapturesSourceText
// instead. Only the two shapes that remain genuinely unparseable here — an
// entry with no tokens at all, and a tuple whose ')' never arrives — still
// refuse at THIS layer.
var valuesOrdinalRE = regexp.MustCompile(`value (\d+) of the VALUES tuple`)

func TestParseInsert_RefusalNamesValueOrdinal(t *testing.T) {
	for _, tc := range []struct {
		name       string
		sql        string
		wantOrd    int
		wantReason string
	}{
		// An entry with no tokens at all: the reason is "empty value".
		{"empty_value_first", `INSERT INTO t (a, b, c) VALUES (, 2, 3)`, 1, "empty value"},
		{"empty_value_middle", `INSERT INTO t (a, b, c) VALUES (1, , 3)`, 2, "empty value"},
		{"empty_value_last", `INSERT INTO t (a, b, c) VALUES (1, 2, )`, 3, "empty value"},
		// The tuple's ')' never arrives: the position is the value being read
		// when the input ran out, not the count of completed values.
		{"unterminated_first", `INSERT INTO t (a, b) VALUES (1`, 1, "unterminated VALUES row"},
		{"unterminated_third", `INSERT INTO t (a, b, c) VALUES (1, 2, 3`, 3, "unterminated VALUES row"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.sql)
			if err == nil {
				t.Fatalf("%s parsed with no error", tc.sql)
			}
			msg := err.Error()
			if !strings.Contains(msg, tc.wantReason) {
				t.Errorf("error %q does not keep the reason %q", msg, tc.wantReason)
			}
			m := valuesOrdinalRE.FindStringSubmatch(msg)
			if m == nil {
				t.Fatalf("error %q names no value position", msg)
			}
			got, convErr := strconv.Atoi(m[1])
			if convErr != nil {
				t.Fatalf("position %q in %q is not a number", m[1], msg)
			}
			if got != tc.wantOrd {
				t.Errorf("error names value %d, want value %d: %q", got, tc.wantOrd, msg)
			}
		})
	}
}

// TestParseInsert_ValuesRowKeepsLexerErrorSQLState is round-2 review P1:
// parseValuesRow folded TokenEOF and TokenError into the SAME arm, so a
// lexer error inside a VALUES row — #1307's own Unicode-escape refusals
// among them — lost its own sentence and SQLSTATE to the generic
// "unterminated VALUES row" 42601, the message meant for the OTHER case
// (the ')' never arriving). SELECT already keeps the lexer's own error
// (selectParser.syntaxFailure); this pins the same rule at the VALUES door,
// covering a #1307 surrogate-pair escape (42601, but with the RIGHT
// sentence) and a malformed-escape class that carries a DIFFERENT SQLSTATE
// (22025) to prove the code itself passes through, not only the text.
func TestParseInsert_ValuesRowKeepsLexerErrorSQLState(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sql      string
		wantCode string
		wantMsg  string
	}{
		{
			name:     "unicode surrogate pair — #1307's own message and code",
			sql:      `INSERT INTO vd (id, s) VALUES (1, E'\uD83Dx')`,
			wantCode: "42601",
			wantMsg:  `invalid Unicode surrogate pair at or near "x"`,
		},
		{
			name:     "too few hex digits — a DIFFERENT SQLSTATE than the generic 42601",
			sql:      `INSERT INTO vd (id, s) VALUES (1, E'\u12')`,
			wantCode: "22025",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.sql)
			if err == nil {
				t.Fatalf("%s parsed with no error", tc.sql)
			}
			if got := sqlerr.StateOf(err); got != tc.wantCode {
				t.Errorf("SQLSTATE %q, want %q (message: %q)", got, tc.wantCode, err.Error())
			}
			if strings.Contains(err.Error(), "unterminated VALUES row") {
				t.Errorf("lexer error %q was folded into the generic unterminated-row refusal", err.Error())
			}
			if tc.wantMsg != "" && err.Error() != tc.wantMsg {
				t.Errorf("got %q, want %q", err.Error(), tc.wantMsg)
			}
		})
	}
}

// An ARRAY constructor's commas belong to the constructor, not to the SET
// list: `SET a = ARRAY[7, 8], b = 1` is two clauses (arc CW round 2, N4 —
// the value ended at the first comma and `8]` was read as the next column).
func TestParseUpdate_ArrayConstructorValue(t *testing.T) {
	q, err := Parse("UPDATE t SET a = ARRAY[7, 8], b = ARRAY[ARRAY[1, 2]][1], c = 1 WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	want := []struct{ col, val string }{
		{"a", "array [ 7 , 8 ]"},
		{"b", "array [ array [ 1 , 2 ] ] [ 1 ]"},
		{"c", "1"},
	}
	if len(q.Update.SetClauses) != len(want) {
		t.Fatalf("got %d SET clauses %+v, want %d", len(q.Update.SetClauses), q.Update.SetClauses, len(want))
	}
	for i, w := range want {
		if got := q.Update.SetClauses[i]; got.Column != w.col || got.Value != w.val {
			t.Errorf("clause %d: got %q = %q, want %q = %q", i, got.Column, got.Value, w.col, w.val)
		}
	}
}
