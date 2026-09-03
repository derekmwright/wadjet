package sql

import (
	"reflect"
	"strings"
	"testing"
	"unicode"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The splitter's whole job is deciding which semicolons SEPARATE and which are
// text. Everything in the "not a separator" half below is a semicolon a
// strings.Split would have cut on, and cutting on any of them turns one
// statement into two malformed ones (#711).
func TestSplitStatements(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want []string
	}{
		// --- nothing to run -----------------------------------------------
		{name: "empty", sql: "", want: nil},
		{name: "whitespace", sql: "   \n\t ", want: nil},
		{name: "only a semicolon", sql: ";", want: nil},
		{name: "only semicolons", sql: " ; ;; ", want: nil},

		// --- one statement ------------------------------------------------
		{name: "bare", sql: "SELECT 1", want: []string{"SELECT 1"}},
		{name: "trailing semicolon", sql: "SELECT 1;", want: []string{"SELECT 1"}},
		{name: "doubled trailing", sql: "SELECT 1;;", want: []string{"SELECT 1"}},
		{name: "leading semicolon", sql: "; SELECT 1", want: []string{"SELECT 1"}},

		// --- a semicolon that is NOT a separator --------------------------
		{name: "inside a string literal",
			sql:  "UPDATE t SET name = 'a;b' WHERE id = 1",
			want: []string{"UPDATE t SET name = 'a;b' WHERE id = 1"}},
		{name: "inside a string literal with an escaped quote",
			sql:  "UPDATE t SET name = 'it''s a;b' WHERE id = 1",
			want: []string{"UPDATE t SET name = 'it''s a;b' WHERE id = 1"}},
		{name: "inside a quoted identifier",
			sql:  `SELECT "a;b" FROM t`,
			want: []string{`SELECT "a;b" FROM t`}},
		{name: "inside a dollar-quoted string",
			sql:  "SELECT $$a;b$$",
			want: []string{"SELECT $$a;b$$"}},
		{name: "inside a line comment",
			sql:  "SELECT 1 -- ; not a separator",
			want: []string{"SELECT 1 -- ; not a separator"}},
		{name: "inside a block comment",
			sql:  "SELECT /* ; */ 1",
			want: []string{"SELECT /* ; */ 1"}},
		{name: "inside a nested block comment",
			sql:  "SELECT /* a /* ; */ b */ 1",
			want: []string{"SELECT /* a /* ; */ b */ 1"}},
		{name: "inside parentheses",
			sql:  "SELECT (1;2)",
			want: []string{"SELECT (1;2)"}},

		// --- a semicolon that IS a separator -------------------------------
		{name: "two statements",
			sql:  "SELECT 1; SELECT 2",
			want: []string{"SELECT 1", "SELECT 2"}},
		{name: "three statements",
			sql:  "SELECT 1; SELECT 2; SELECT 3",
			want: []string{"SELECT 1", "SELECT 2", "SELECT 3"}},
		{name: "two DML statements",
			sql:  "DELETE FROM t WHERE id = 1; DELETE FROM t WHERE id = 2",
			want: []string{"DELETE FROM t WHERE id = 1", "DELETE FROM t WHERE id = 2"}},
		{name: "separator after a literal that contains one",
			sql:  "UPDATE t SET name = 'a;b'; SELECT 1",
			want: []string{"UPDATE t SET name = 'a;b'", "SELECT 1"}},
		{name: "separator after a comment that contains one",
			sql:  "SELECT /* ; */ 1; SELECT 2",
			want: []string{"SELECT /* ; */ 1", "SELECT 2"}},
		{name: "separator after a closed subquery",
			sql:  "SELECT (SELECT 1); SELECT 2",
			want: []string{"SELECT (SELECT 1)", "SELECT 2"}},
		{name: "trailing separator after two",
			sql:  "SELECT 1; SELECT 2;",
			want: []string{"SELECT 1", "SELECT 2"}},
		{name: "a statement that is only garbage still comes back whole",
			sql:  "SELECT 1; ZZZ NOT SQL",
			want: []string{"SELECT 1", "ZZZ NOT SQL"}},

		// --- malformed input is NOT cut up --------------------------------
		//
		// An unterminated literal or an unbalanced paren has an error, and the
		// error belongs to the statement. Splitting would report the halves'
		// errors instead of it.
		{name: "unterminated string literal",
			sql:  "SELECT 'a; SELECT 2",
			want: []string{"SELECT 'a; SELECT 2"}},
		{name: "unterminated quoted identifier",
			sql:  `SELECT "a; SELECT 2`,
			want: []string{`SELECT "a; SELECT 2`}},
		{name: "unterminated block comment",
			sql:  "SELECT /* a; SELECT 2",
			want: []string{"SELECT /* a; SELECT 2"}},
		{name: "unbalanced open paren",
			sql:  "SELECT (1; SELECT 2",
			want: []string{"SELECT (1; SELECT 2"}},
		{name: "unbalanced close paren does not go negative",
			sql:  "SELECT 1); SELECT 2",
			want: []string{"SELECT 1)", "SELECT 2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SplitStatements(tc.sql); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("SplitStatements(%q)\n  = %#v\n  want %#v", tc.sql, got, tc.want)
			}
		})
	}
}

func TestIsMultiStatement(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want bool
	}{
		{"", false},
		{";", false},
		{"SELECT 1", false},
		{"SELECT 1;", false},
		{"SELECT 1;;", false},
		{"UPDATE t SET name = 'a;b'", false},
		{"SELECT 1; SELECT 2", true},
		{"SELECT 1; SELECT 2;", true},
		{"INSERT INTO t (id) VALUES (1); ZZZ", true},
	} {
		if got := IsMultiStatement(tc.sql); got != tc.want {
			t.Errorf("IsMultiStatement(%q) = %v, want %v", tc.sql, got, tc.want)
		}
	}
}

// FuzzSplitStatements: the pieces must reassemble into the input with only
// semicolons and whitespace removed, so no split can lose or invent SQL. A
// splitter that dropped a byte would silently drop part of a statement.
func FuzzSplitStatements(f *testing.F) {
	for _, s := range []string{
		"", ";", "SELECT 1", "SELECT 1; SELECT 2", "UPDATE t SET n = 'a;b'",
		"SELECT $$a;b$$", "SELECT /* ; */ 1", `SELECT "a;b"`, "SELECT 'a; b",
		"SELECT (1;2)", "-- ;", "SELECT 1 -- ;\n; SELECT 2",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, sql string) {
		parts := SplitStatements(sql)
		var joined string
		for _, p := range parts {
			joined += p
		}
		// Semicolons and whitespace are what a split is allowed to remove.
		// Whitespace is unicode.IsSpace because that is what TrimSpace uses;
		// a narrower set here tests the trim rather than the splitter.
		strip := func(s string) string {
			var b []rune
			for _, r := range s {
				if r == ';' || unicode.IsSpace(r) {
					continue
				}
				b = append(b, r)
			}
			return string(b)
		}
		if strip(joined) != strip(sql) {
			t.Fatalf("SplitStatements(%q) = %#v\n  reassembles to %q, want %q",
				sql, parts, strip(joined), strip(sql))
		}
		for _, p := range parts {
			if p == "" {
				t.Fatalf("SplitStatements(%q) returned an empty piece: %#v", sql, parts)
			}
		}
	})
}

// Parse is the ONE-STATEMENT entry point, and a string carrying two statements
// is refused there with PostgreSQL's class and PostgreSQL's message.
//
// Before this, what happened depended on which sub-parser swallowed the tail:
// the SELECT path refused it (42601, "trailing input"), the DELETE path with a
// WHERE refused it, the DELETE path WITHOUT a WHERE tripped the
// empty-predicate backstop as XX000, and the INSERT path — which has no
// end-of-statement check at all — silently DROPPED the second statement and
// ran the first (#711).
func TestParseRefusesMultipleStatements(t *testing.T) {
	const multi = "cannot insert multiple commands into a prepared statement"
	for _, tc := range []struct {
		name  string
		sql   string
		state string
		// msg is a substring the error must contain. PostgreSQL reports a
		// SYNTAX error rather than the multi-command message when one of the
		// statements does not parse, because it parses the whole string first.
		msg string
		// notMsg is a substring the error must NOT contain.
		notMsg string
	}{
		{name: "two INSERTs", state: "42601", msg: multi,
			sql: "INSERT INTO t (id) VALUES (1); INSERT INTO t (id) VALUES (2)"},
		{name: "two DELETEs", state: "42601", msg: multi,
			sql: "DELETE FROM t WHERE id = 1; DELETE FROM t WHERE id = 2"},
		{name: "DELETE with no WHERE then DELETE", state: "42601", msg: multi,
			sql: "DELETE FROM t; DELETE FROM t WHERE id = 2"},
		{name: "two SELECTs", state: "42601", msg: multi,
			sql: "SELECT 1; SELECT 2"},
		{name: "DELETE then SELECT", state: "42601", msg: multi,
			sql: "DELETE FROM t WHERE id = 1; SELECT id FROM t"},
		{name: "SELECT then DELETE", state: "42601", msg: multi,
			sql: "SELECT id FROM t; DELETE FROM t WHERE id = 1"},
		{name: "three statements", state: "42601", msg: multi,
			sql: "SELECT 1; SELECT 2; SELECT 3"},
		// A syntax error anywhere outranks the multi-command refusal, which is
		// what "parse the whole string first" means: PostgreSQL answers
		// `INSERT …; ZZZ NOT SQL` with `syntax error at or near "ZZZ"`, not
		// with the multi-command message. This parser words its own syntax
		// errors differently (it says what it expected rather than what it
		// found, which is its own pre-existing shape); what is asserted here
		// is the class, and that the multi-command message does NOT win —
		// telling a client "you sent two commands" when one of them is
		// unparseable names the wrong problem.
		{name: "a statement then garbage", state: "42601", notMsg: multi,
			sql: "INSERT INTO t (id) VALUES (1); ZZZ NOT SQL"},
		{name: "garbage then a statement", state: "42601", notMsg: multi,
			sql: "ZZZ NOT SQL; SELECT 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.sql)
			if err == nil {
				t.Fatalf("Parse(%q) returned no error — a statement was silently dropped", tc.sql)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("Parse(%q) SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
			}
			if tc.msg != "" && !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("Parse(%q) error does not contain %q:\n  %v", tc.sql, tc.msg, err)
			}
			if tc.notMsg != "" && strings.Contains(err.Error(), tc.notMsg) {
				t.Errorf("Parse(%q) reports %q, but one of its statements does not parse "+
					"and the syntax error is the answer:\n  %v", tc.sql, tc.notMsg, err)
			}
		})
	}
}

// The other half: a single statement with a semicolon in it, or a trailing
// one, must still parse. A splitter that cut on those would refuse statements
// that work today.
func TestParseAcceptsSingleStatementsWithSemicolons(t *testing.T) {
	for _, sql := range []string{
		"SELECT 1",
		"SELECT 1;",
		"SELECT 1;;",
		"SELECT 1 -- ; a comment",
		"SELECT /* ; */ 1",
		"DELETE FROM t WHERE id = 1;",
		"UPDATE t SET name = 'a;b' WHERE id = 1",
		"INSERT INTO t (id, name) VALUES (1, 'a;b')",
		`SELECT "a;b" FROM t`,
	} {
		if _, err := Parse(sql); err != nil {
			t.Errorf("Parse(%q) = %v, want it to parse", sql, err)
		}
	}
}
