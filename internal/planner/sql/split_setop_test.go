package sql

import "testing"

// SplitLastTopLevelUnionAll is asked for the text of a set operation's two arms
// by a caller that has already decided the FORM from the parsed tree, so its
// only contract is "find the LAST top-level `UNION ALL`, and see what the lexer
// sees". Both halves of that are gated here: a byte scan matched the letters
// inside identifiers, strings and comments (round-3 review, B1), and the first
// lexer version did not count a parenthesis it consumed as its lookahead, so a
// parenthesised arm hid every later operator (round-4 review, B1).
func TestSplitLastTopLevelUnionAll(t *testing.T) {
	for _, tc := range []struct {
		name, sql   string
		left, right string
		ok          bool
	}{
		{name: "two arms", sql: "SELECT 1 UNION ALL SELECT 2",
			left: "SELECT 1", right: "SELECT 2", ok: true},
		{name: "the LAST of three", sql: "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3",
			left: "SELECT 1 UNION ALL SELECT 2", right: "SELECT 3", ok: true},
		{name: "a plain UNION is not a split point", sql: "SELECT 1 UNION SELECT 2",
			ok: false},
		{name: "UNION then UNION ALL", sql: "SELECT 1 UNION SELECT 2 UNION ALL SELECT 3",
			left: "SELECT 1 UNION SELECT 2", right: "SELECT 3", ok: true},

		// THE LEXER SEES TOKENS, not characters.
		{name: "the letters inside an identifier", sql: "SELECT 1 UNION ALL SELECT x AS unionall",
			left: "SELECT 1", right: "SELECT x AS unionall", ok: true},
		{name: "the letters inside a delimited identifier",
			sql:  `SELECT 1 UNION ALL SELECT x AS "union all"`,
			left: "SELECT 1", right: `SELECT x AS "union all"`, ok: true},
		{name: "the letters inside a string literal",
			sql:  "SELECT 1 UNION ALL SELECT x WHERE 'UNION ALL' <> 'z'",
			left: "SELECT 1", right: "SELECT x WHERE 'UNION ALL' <> 'z'", ok: true},
		{name: "the letters inside a block comment",
			sql:  "SELECT 1 UNION ALL SELECT x /* UNION ALL */",
			left: "SELECT 1", right: "SELECT x /* UNION ALL */", ok: true},
		{name: "the letters inside a line comment",
			sql:  "SELECT 1 UNION ALL SELECT x -- UNION ALL\n",
			left: "SELECT 1", right: "SELECT x -- UNION ALL", ok: true},
		{name: "a comment BETWEEN the two keywords",
			sql:  "SELECT 1 UNION /* c */ ALL SELECT 2",
			left: "SELECT 1", right: "SELECT 2", ok: true},

		// DEPTH: an operator inside parentheses is not top level, and a
		// parenthesised ARM must not hide the operators after it.
		{name: "an operator inside parentheses is not top level",
			sql:  "SELECT 1 UNION ALL SELECT x FROM (SELECT 2 UNION ALL SELECT 3) z",
			left: "SELECT 1", right: "SELECT x FROM (SELECT 2 UNION ALL SELECT 3) z", ok: true},
		{name: "a parenthesised arm after UNION does not hide the next operator",
			sql:  "SELECT 1 UNION (SELECT 2) UNION ALL SELECT 3",
			left: "SELECT 1 UNION (SELECT 2)", right: "SELECT 3", ok: true},
		{name: "a doubly parenthesised arm",
			sql:  "SELECT 1 UNION ((SELECT 2)) UNION ALL SELECT 3",
			left: "SELECT 1 UNION ((SELECT 2))", right: "SELECT 3", ok: true},
		{name: "a parenthesised arm holding its own operator",
			sql:  "SELECT 1 UNION (SELECT 2 UNION ALL SELECT 9) UNION ALL SELECT 3",
			left: "SELECT 1 UNION (SELECT 2 UNION ALL SELECT 9)", right: "SELECT 3", ok: true},
		{name: "two parenthesised arms",
			sql:  "SELECT 1 UNION (SELECT 2) UNION (SELECT 3) UNION ALL SELECT 4",
			left: "SELECT 1 UNION (SELECT 2) UNION (SELECT 3)", right: "SELECT 4", ok: true},
		{name: "a parenthesised arm after UNION ALL",
			sql:  "SELECT 1 UNION ALL (SELECT 2) UNION ALL SELECT 3",
			left: "SELECT 1 UNION ALL (SELECT 2)", right: "SELECT 3", ok: true},
		{name: "EXCEPT before a parenthesised arm",
			sql:  "SELECT 1 EXCEPT (SELECT 2) UNION ALL SELECT 3",
			left: "SELECT 1 EXCEPT (SELECT 2)", right: "SELECT 3", ok: true},

		{name: "no set operation at all", sql: "SELECT 1", ok: false},
		{name: "empty", sql: "", ok: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left, right, ok := SplitLastTopLevelUnionAll(tc.sql)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v (left %q right %q)", ok, tc.ok, left, right)
			}
			if !tc.ok {
				return
			}
			if left != tc.left || right != tc.right {
				t.Errorf("split\n  got  %q | %q\n  want %q | %q", left, right, tc.left, tc.right)
			}
		})
	}
}
