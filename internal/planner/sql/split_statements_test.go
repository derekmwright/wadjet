package sql

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

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

		// --- A COMMENT IS NOT A STATEMENT ---------------------------------
		//
		// Trimming whitespace alone left `-- trailing comment` standing as a
		// second statement, so `SELECT 1; -- c` became a MULTI-statement string
		// and every one-statement door refused it 42601. PostgreSQL 17.11 runs
		// all of these (measured), and so did wadjet before the splitter
		// existed.
		{name: "a trailing line comment",
			sql: "SELECT 1; -- a comment", want: []string{"SELECT 1"}},
		{name: "a trailing line comment on its own line",
			sql: "SELECT 1;\n-- a comment", want: []string{"SELECT 1"}},
		{name: "a bare trailing --",
			sql: "SELECT 1; --", want: []string{"SELECT 1"}},
		{name: "a trailing block comment",
			sql: "SELECT 1; /* c */", want: []string{"SELECT 1"}},
		{name: "a trailing nested block comment",
			sql: "SELECT 1; /* a /* b */ c */", want: []string{"SELECT 1"}},
		{name: "a DML statement with an audit comment",
			sql:  "DELETE FROM t WHERE id = 1; -- audit note",
			want: []string{"DELETE FROM t WHERE id = 1"}},
		{name: "a comment-only piece in the MIDDLE",
			sql:  "SELECT 1; -- middle\n; SELECT 2",
			want: []string{"SELECT 1", "SELECT 2"}},
		{name: "a block-comment-only piece in the middle",
			sql:  "SELECT 1; /* mid */ ; SELECT 2",
			want: []string{"SELECT 1", "SELECT 2"}},
		{name: "a comment between two statements rides on the second",
			sql:  "SELECT 1; /* c */ SELECT 2",
			want: []string{"SELECT 1", "/* c */ SELECT 2"}},
		{name: "a semicolon inside a comment between two statements",
			sql:  "SELECT 1; /* ; */ SELECT 2",
			want: []string{"SELECT 1", "/* ; */ SELECT 2"}},
		{name: "a leading comment rides on the first statement",
			sql: "-- lead\nSELECT 1", want: []string{"-- lead\nSELECT 1"}},
		{name: "only a line comment",
			sql: "-- nothing to run", want: nil},
		{name: "only a block comment",
			sql: "/* nothing to run */", want: nil},
		{name: "only comments and semicolons",
			sql: " -- a\n ; /* b */ ; ", want: nil},
		// An UNTERMINATED comment reads to end of input, so the piece holds
		// nothing and is dropped. PostgreSQL raises `unterminated /* comment`;
		// wadjet answered `SELECT 1` before this arc and still does — a
		// superset this function neither widens nor narrows.
		{name: "an unterminated trailing block comment",
			sql: "SELECT 1; /* never closed", want: []string{"SELECT 1"}},

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

// FuzzSplitStatements: the pieces carry exactly the TOKENS the input carried,
// minus the semicolons, in order — so no split can lose SQL or invent it.
//
// The invariant is over tokens rather than bytes because a piece that holds
// only whitespace and COMMENTS is dropped, and a byte-level invariant would
// then have to know what a comment is — which is the lexer's job and the
// thing this function exists to defer to. Tokens say the load-bearing part
// directly: whatever the splitter dropped could not have been executed.
func FuzzSplitStatements(f *testing.F) {
	for _, s := range []string{
		"", ";", "SELECT 1", "SELECT 1; SELECT 2", "UPDATE t SET n = 'a;b'",
		"SELECT $$a;b$$", "SELECT /* ; */ 1", `SELECT "a;b"`, "SELECT 'a; b",
		"SELECT (1;2)", "-- ;", "SELECT 1 -- ;\n; SELECT 2",
		"SELECT 1; -- c", "SELECT 1; /* c */", "/* only */", "SELECT 1; /* open",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, sql string) {
		parts := SplitStatements(sql)
		for _, p := range parts {
			if p == "" {
				t.Fatalf("SplitStatements(%q) returned an empty piece: %#v", sql, parts)
			}
		}
		want, wantOK := lexTokens(sql)
		var got []string
		for _, p := range parts {
			pt, ok := lexTokens(p)
			if !ok {
				// A piece that does not lex is the statement's own error to
				// report; the table tests cover those shapes.
				return
			}
			got = append(got, pt...)
		}
		if !wantOK {
			return
		}
		if len(got) != len(want) {
			t.Fatalf("SplitStatements(%q) = %#v\n  carries %d tokens, the input carries %d\n  got  %v\n  want %v",
				sql, parts, len(got), len(want), got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("SplitStatements(%q) = %#v\n  token %d is %q, the input has %q",
					sql, parts, i, got[i], want[i])
			}
		}
	})
}

// lexTokens returns every token of sql except semicolons, as type:value
// strings, and whether the whole string lexed. Semicolons are excluded because
// they are exactly what a split consumes.
func lexTokens(sql string) ([]string, bool) {
	l := newLexer(sql)
	var out []string
	for {
		tok := l.nextToken()
		switch tok.typ {
		case TokenEOF:
			return out, true
		case TokenError:
			return out, false
		case TokenSemicolon:
		default:
			out = append(out, fmt.Sprintf("%d:%s", tok.typ, tok.val))
		}
	}
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
		{name: "two statements separated by a comment-only piece", state: "42601", msg: multi,
			sql: "SELECT 1; -- middle\n; SELECT 2"},
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

// The other half: a single statement with a semicolon in it, a trailing one, or
// a trailing COMMENT, must still parse. A splitter that cut on any of those
// would refuse statements that work.
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
		// A COMMENT AFTER THE SEMICOLON. This list had the comment BEFORE it
		// ("SELECT 1 -- ; a comment") and not after, and the missing half is
		// exactly what regressed: `SELECT 1; -- c` became a two-statement
		// string that every one-statement door refused with 42601, where
		// PostgreSQL 17.11 runs it and so did this parser before the arc.
		"SELECT 1; -- a comment",
		"SELECT 1;\n-- a comment",
		"SELECT 1; --",
		"SELECT 1; /* c */",
		"SELECT 1; /* a /* nested */ b */",
		"DELETE FROM t WHERE id = 1; -- audit note",
		"UPDATE t SET n = 1 WHERE id = 1; /* audit */",
		"INSERT INTO t (id) VALUES (1); -- note",
		"SELECT 1; /* never closed",
	} {
		if _, err := Parse(sql); err != nil {
			t.Errorf("Parse(%q) = %v, want it to parse", sql, err)
		}
	}
}
