// SPDX-License-Identifier: MIT

package sql

import "testing"

// #1307: the three Unicode-escape syntax errors carry PostgreSQL's
// SQLSTATE 42601 on every door already; only the message TEXT was missing
// PostgreSQL's `at or near "…"` location suffix every other 42601 message in
// this lexer carries. Measured live against PostgreSQL 17.11
// (postgres:17.11-alpine): each `want` here is the exact sentence
// `\gdesc`/psql prints for the matching SQL.
//
// A fourth shape — a HIGH surrogate paired with a syntactically valid \u/\U
// escape whose VALUE is not in the low-surrogate range — reaches the same
// base message through a THIRD call site (lexer.go's `lo < 0xDC00 || lo >
// 0xDFFF` arm) that the issue's own three examples do not exercise; it is
// pinned here too since it shares the mechanism.
func TestUnicodeEscapeErrorsNameTheirLocation(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "high surrogate with nothing to pair — names what follows it",
			sql:  `SELECT E'\uD83D' AS v`,
			want: `invalid Unicode surrogate pair at or near "'"`,
		},
		{
			name: "bare low surrogate — names its own escape text",
			sql:  `SELECT E'\uDE00' AS v`,
			want: `invalid Unicode surrogate pair at or near "\uDE00"`,
		},
		{
			name: "code point past U+10FFFF — names its own escape text",
			sql:  `SELECT E'\U00110000' AS v`,
			want: `invalid Unicode escape value at or near "\U00110000"`,
		},
		{
			name: "high surrogate paired with a valid escape outside the low range",
			sql:  `SELECT E'\uD83D` + `\u0041' AS v`,
			want: `invalid Unicode surrogate pair at or near "\u0041"`,
		},
		{
			name: "8-digit \\U form, bare low surrogate — the suffix carries the form",
			sql:  `SELECT E'\U0000DE00' AS v`,
			want: `invalid Unicode surrogate pair at or near "\U0000DE00"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			toks := collectTokens(tc.sql)
			last := toks[len(toks)-1]
			if last.typ != TokenError {
				t.Fatalf("%s: last token is %v, want TokenError", tc.sql, last.typ)
			}
			if last.code != "42601" {
				t.Errorf("%s: SQLSTATE %q, want 42601", tc.sql, last.code)
			}
			if last.val != tc.want {
				t.Errorf("%s:\n got  %q\n want %q", tc.sql, last.val, tc.want)
			}
		})
	}
}

// A VALID surrogate pair is unaffected: the two escapes still fold to one
// code point, and no error — let alone a spurious location suffix — is
// raised over correct input.
func TestUnicodeEscapeValidPairStillFolds(t *testing.T) {
	toks := collectTokens(`SELECT E'\uD83D\uDE00' AS v`)
	if len(toks) < 2 {
		t.Fatalf("too few tokens: %#v", toks)
	}
	str := toks[1]
	if str.typ != TokenString {
		t.Fatalf("token 1 is %v (val %q), want TokenString", str.typ, str.val)
	}
	if str.val != "\U0001F600" {
		t.Errorf("got %q, want U+1F600", str.val)
	}
}
