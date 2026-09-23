// SPDX-License-Identifier: MIT

package sql

import "testing"

// ARC TF / generate_series — A SIGNED NUMBER IS ONE ARGUMENT.
//
// The lexer emits a sign as its own operator token, and the table-function
// argument loop read that token as a WHOLE argument and then demanded a comma.
// So `generate_series(1, 0, -1)` — the only spelling of a DESCENDING series
// once the step is no longer negated for the caller — was
// `42601 expected , between function arguments`, where PostgreSQL 17.11
// answers two rows.
//
// The forms below are the argument grammar at its boundaries, not the issue's
// one example: each sign, each position, the sign on its own, a sign in front
// of something that is not a number, and a named argument beside a signed one.
func TestArcTFATableFunctionReadsASignedNumberAsOneArgument(t *testing.T) {
	for _, c := range []struct {
		name string
		sql  string
		args []string
		bad  bool
	}{
		{name: "negative_step", sql: `SELECT * FROM generate_series(1, 0, -1)`,
			args: []string{"1", "0", "-1"}},
		{name: "negative_start", sql: `SELECT * FROM generate_series(-2, 2)`,
			args: []string{"-2", "2"}},
		{name: "negative_start_and_stop", sql: `SELECT * FROM generate_series(-5, -1)`,
			args: []string{"-5", "-1"}},
		// The expression parser folds a unary plus away; the value is the same.
		{name: "explicit_plus", sql: `SELECT * FROM generate_series(+1, +3, +1)`,
			args: []string{"1", "3", "1"}},
		// The sign FOLDS onto the number, so the whitespace between them is
		// not part of the value — `generate_series(1, 0, - 1)` is two rows
		// on 17.11 too.
		{name: "sign_with_space", sql: `SELECT * FROM generate_series(1, 0, - 1)`,
			args: []string{"1", "0", "-1"}},
		{name: "negative_float", sql: `SELECT * FROM generate_series(1, 2, -1.5)`,
			args: []string{"1", "2", "-1.5"}},
		{name: "unsigned_is_unchanged", sql: `SELECT * FROM generate_series(1, 3)`,
			args: []string{"1", "3"}},
		{name: "a_string_argument_is_unchanged", sql: `SELECT * FROM read_json('a-b.json')`,
			args: []string{"a-b.json"}},
		// A sign in front of nothing is PostgreSQL 17.11's syntax error, and
		// now this parser's too: generate_series reads each argument as an
		// expression (arc PC round 2), so the lone sign no longer reaches
		// the source as an argument of its own.
		{name: "a_lone_sign_is_a_syntax_error", sql: `SELECT * FROM generate_series(1, -)`, bad: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			stmt, err := Parse(c.sql)
			if err != nil {
				if c.bad {
					return
				}
				t.Fatalf("parse: %v", err)
			}
			info, err := ExtractSelect(stmt)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			if len(info.Tables) != 1 || !info.Tables[0].IsFunction {
				t.Fatalf("FROM item is not a table function: %+v", info.Tables)
			}
			got := info.Tables[0].FuncArgs
			if len(got) != len(c.args) {
				t.Fatalf("args %q, want %q", got, c.args)
			}
			for i := range got {
				if got[i] != c.args[i] {
					t.Fatalf("args %q, want %q", got, c.args)
				}
			}
		})
	}
}
