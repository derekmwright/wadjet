package sql

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
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
	if q.Update.SetClauses[0].Value != "Bob" {
		t.Fatalf("expected value \"Bob\", got %q", q.Update.SetClauses[0].Value)
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
		{"name", "Bob"},
		{"age", "30"},
		{"status", "active"},
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
	if q.Insert.Values[1][0] != "Bob" {
		t.Fatalf("expected row 1 val 0 = \"Bob\", got %q", q.Insert.Values[1][0])
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
		// A string literal keeps arriving unquoted, commas and parens inside
		// it included: it is one lexer token and always was.
		{"string_with_comma", `INSERT INTO t (a, b) VALUES (1, 'a, b')`,
			[][]string{{"1", "a, b"}}},
		{"string_with_paren", `INSERT INTO t (a, b) VALUES (1, 'has (paren)')`,
			[][]string{{"1", "has (paren)"}}},
		{"string_with_escaped_quote", `INSERT INTO t (a, b) VALUES (1, 'it''s')`,
			[][]string{{"1", "it's"}}},
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

// An expression this path cannot evaluate is a NAMED error. The old loop
// answered `VALUES (coalesce(a, b))` with a truncated row, no error, and the
// tuple's closing paren left in the stream.
func TestParseInsert_RefusesExpressionsItCannotEvaluate(t *testing.T) {
	for _, sql := range []string{
		`INSERT INTO t (a) VALUES (2 * 3)`,
		`INSERT INTO t (a) VALUES (coalesce(1, 2))`,
		`INSERT INTO t (a, b) VALUES (1, a + 1)`,
		`INSERT INTO t (a) VALUES ()`,
	} {
		if q, err := Parse(sql); err == nil {
			t.Errorf("%s parsed with no error: %#v", sql, q.Insert)
		}
	}
}

// A refusal that describes WHAT is wrong but not WHICH entry it was leaves the
// author of `VALUES (1, 'a', <bad>, 4)` to find the value by inspection. Every
// per-value refusal carries the value's 1-based position in the tuple, and the
// position must be the value's own — an off-by-one is exactly as unhelpful as
// no position at all, so these cases pin the first, a middle and the last
// entry, and the count restarts with each tuple.
var valuesOrdinalRE = regexp.MustCompile(`value (\d+) of the VALUES tuple`)

func TestParseInsert_RefusalNamesValueOrdinal(t *testing.T) {
	for _, tc := range []struct {
		name       string
		sql        string
		wantOrd    int
		wantReason string
	}{
		{"expression_first", `INSERT INTO t (a, b, c, d) VALUES (2 * 3, 'x', 3, 4)`,
			1, "not the expression"},
		{"expression_middle", `INSERT INTO t (a, b, c, d) VALUES (1, 'x', 2 * 3, 4)`,
			3, "not the expression"},
		{"expression_last", `INSERT INTO t (a, b, c, d) VALUES (1, 'x', 3, 2 * 3)`,
			4, "not the expression"},
		{"function_call_middle", `INSERT INTO t (a, b, c) VALUES (1, coalesce(1, 2), 3)`,
			2, "not the expression"},
		// An entry with no tokens at all: the reason is still "empty value".
		{"empty_value_first", `INSERT INTO t (a, b, c) VALUES (, 2, 3)`, 1, "empty value"},
		{"empty_value_middle", `INSERT INTO t (a, b, c) VALUES (1, , 3)`, 2, "empty value"},
		{"empty_value_last", `INSERT INTO t (a, b, c) VALUES (1, 2, )`, 3, "empty value"},
		// The tuple's ')' never arrives: the position is the value being read
		// when the input ran out, not the count of completed values.
		{"unterminated_first", `INSERT INTO t (a, b) VALUES (1`, 1, "unterminated VALUES row"},
		{"unterminated_third", `INSERT INTO t (a, b, c) VALUES (1, 2, 3`, 3, "unterminated VALUES row"},
		// The count is per TUPLE, not per statement.
		{"second_row_restarts_count", `INSERT INTO t (a, b) VALUES (1, 2), (3, 4 * 5)`,
			2, "not the expression"},
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
