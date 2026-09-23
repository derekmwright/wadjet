// SPDX-License-Identifier: MIT

package expr

import (
	"errors"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestPatternMatchOperatorsAnswerAsPostgreSQL is the pattern-match
// operators' grammar gate: every documented ARE form (PostgreSQL 17 §9.7.3.1
// – §9.7.3.5) — atoms, bracket expressions and their classes, quantifiers and
// bounds, constraints, every escape family inside and outside brackets, the
// metasyntax directors and embedded options — plus the malformed patterns
// PostgreSQL refuses, each answered by 17.11 (the `pg` column is measured,
// not written: `SELECT (E'<subject>' <op> '<pattern>')::text`).
//
// A cell answers what PostgreSQL answers — true, false, or its 2201B — or is
// REFUSED with 0A000 where RE2 has no form for the ARE construct; the refused
// forms are listed by name below and a refusal of anything else fails.
func TestPatternMatchOperatorsAnswerAsPostgreSQL(t *testing.T) {
	refused := map[string]bool{
		`(ab)\1`: true, `a(?=b)`: true, `a(?!b)`: true, `(?<=a)b`: true, `(?<!a)b`: true,
		`\mc`: true, `b\M`: true, `[[:<:]]b`: true, `a[[:>:]]`: true, `[[.a.]]`: true,
		`[[=a=]]`: true, `(?n)a.b`: true, `(?x)a b c`: true, `(?b)abc`: true, `(?e)abc`: true,
	}
	cells := []struct{ subject, pattern, op, pg string }{
		{"abc", "abc", "~", "true"},
		{"abc", "b", "~", "true"},
		{"abc", "^b", "~", "false"},
		{"abc", "c$", "~", "true"},
		{"abc", "^abc$", "~", "true"},
		{"ABC", "abc", "~", "false"},
		{"ABC", "abc", "~*", "true"},
		{"abc", "x", "!~", "true"},
		{"abc", "b", "!~", "false"},
		{"ABC", "b", "!~*", "false"},
		{"abc", "B", "!~*", "false"},
		{"a\nb", "a.b", "~", "true"},
		{"a\nb", "^b", "~", "false"},
		{"a\nb", "a$", "~", "false"},
		{"", "", "~", "true"},
		{"", "^$", "~", "true"},
		{"abc", "", "~", "true"},
		{"abc", "[b]", "~", "true"},
		{"abc", "[^abc]", "~", "false"},
		{"a]c", "[]]", "~", "true"},
		{"a-c", "[a-]", "~", "true"},
		{"a-c", "[-]", "~", "true"},
		{"b", "[a-c]", "~", "true"},
		{"d", "[a-c]", "~", "false"},
		{"x1", "[[:digit:]]", "~", "true"},
		{"xy", "[[:digit:]]", "~", "false"},
		{"aB", "[[:upper:]]", "~", "true"},
		{"a b", "[[:space:]]", "~", "true"},
		{"a_b", "[[:alnum:]_]+$", "~", "true"},
		{"ab", "[[:bogus:]]", "~", "E:2201B"},
		{"ab", "[a", "~", "E:2201B"},
		{"ab", "[[.a.]]", "~", "true"},
		{"ab", "[[=a=]]", "~", "true"},
		{"a b", "[[:<:]]b", "~", "true"},
		{"ab", "a[[:>:]]", "~", "false"},
		{"a[b", "[[]", "~", "true"},
		{"a\\b", "[\\\\]", "~", "true"},
		{"a.b", "[.]", "~", "true"},
		{"aaa", "a*", "~", "true"},
		{"aaa", "^a+$", "~", "true"},
		{"aaa", "^a?$", "~", "false"},
		{"aaa", "^a{3}$", "~", "true"},
		{"aaa", "^a{2}$", "~", "false"},
		{"aaa", "^a{2,}$", "~", "true"},
		{"aaa", "^a{1,2}$", "~", "false"},
		{"aaa", "^a{2,4}?$", "~", "true"},
		{"aaa", "a*?", "~", "true"},
		{"aaa", "^a{256}$", "~", "E:2201B"},
		{"aaa", "a{2,1}", "~", "E:2201B"},
		{"aaa", "a{", "~", "false"},
		{"aaa", "a{x}", "~", "false"},
		{"a{b", "a{b", "~", "true"},
		{"ab", "**", "~", "E:2201B"},
		{"ab", "a**", "~", "E:2201B"},
		{"ab", "+a", "~", "E:2201B"},
		{"abc", "(b|x)", "~", "true"},
		{"abc", "^(a|b)+c$", "~", "true"},
		{"abc", "(?:ab)c", "~", "true"},
		{"abc", "(ab", "~", "E:2201B"},
		{"abc", "ab)", "~", "E:2201B"},
		{"abc", "()", "~", "true"},
		{"abc", "a|", "~", "true"},
		{"abab", "(ab)\\1", "~", "true"},
		{"abc", "a(?=b)", "~", "true"},
		{"abc", "a(?!b)", "~", "false"},
		{"abc", "(?<=a)b", "~", "true"},
		{"abc", "(?<!a)b", "~", "false"},
		{"a1", "\\d", "~", "true"},
		{"ab", "\\d", "~", "false"},
		{"a b", "\\s", "~", "true"},
		{"ab", "\\s", "~", "false"},
		{"a_", "^\\w+$", "~", "true"},
		{"a-", "^\\w+$", "~", "false"},
		{"ab", "\\D", "~", "true"},
		{"12", "\\D", "~", "false"},
		{"a\tb", "a\\tb", "~", "true"},
		{"a\nb", "a\\nb", "~", "true"},
		{"a\rb", "a\\rb", "~", "true"},
		{"a\bb", "a\\bb", "~", "true"},
		{"a b", "a\\bb", "~", "false"},
		{"a\\b", "a\\Bb", "~", "true"},
		{"abc", "\\Aabc\\Z", "~", "true"},
		{"ab c", "\\yc", "~", "true"},
		{"abc", "\\yc", "~", "false"},
		{"abc", "\\Yc", "~", "true"},
		{"ab c", "\\mc", "~", "true"},
		{"abc", "b\\M", "~", "false"},
		{"a\u00e9", "\\u00e9", "~", "true"},
		{"a\u00e9", "\\U000000e9", "~", "true"},
		{"aA", "\\x41", "~", "true"},
		{"aA", "\\x0041", "~", "true"},
		{"a", "\\x", "~", "E:2201B"},
		{"aA", "\\101", "~", "true"},
		{"a", "\\0", "~", "false"},
		{"a.b", "a\\.b", "~", "true"},
		{"axb", "a\\.b", "~", "false"},
		{"a*b", "a\\*b", "~", "true"},
		{"ab", "\\q", "~", "E:2201B"},
		{"a\u0001b", "\\cA", "~", "true"},
		{"a\u001bb", "\\e", "~", "true"},
		{"a\u000bb", "\\v", "~", "true"},
		{"a\u0007b", "\\a", "~", "true"},
		{"a\fb", "\\f", "~", "true"},
		{"a1", "[\\d]", "~", "true"},
		{"ab", "[\\D]", "~", "true"},
		{"a", "\\", "~", "E:2201B"},
		{"a.c", "***=a.c", "~", "true"},
		{"abc", "***=a.c", "~", "false"},
		{"abc", "***:a.c", "~", "true"},
		{"ABC", "(?i)abc", "~", "true"},
		{"abc", "(?c)ABC", "~*", "false"},
		{"a.c", "(?q)a.c", "~", "true"},
		{"abc", "(?q)a.c", "~", "false"},
		{"a\nb", "(?n)a.b", "~", "false"},
		{"abc", "(?x)a b c", "~", "true"},
		{"abc", "(?b)abc", "~", "true"},
		{"abc", "(?e)abc", "~", "true"},
		{"abc", "(?z)abc", "~", "E:2201B"},
		{"abc", "(?s)a.c", "~", "true"},
		{"abc", "(?t)abc", "~", "true"},
		{"abc", "(?i", "~", "E:2201B"},
		{" abc", "^abc", "~", "false"},
		{"abc ", "abc$", "~", "false"},
		{" abc ", "^ abc $", "~", "true"},
		{"a b", "a b", "~", "true"},
		{"a  b", "a b", "~", "false"},
		{"a\tb", "a[[:blank:]]b", "~", "true"},
		{"ABC", "[a-c]+", "~*", "true"},
		{"abc", "^[A-C]+$", "~*", "true"},
		{"xyz", "[^A-C]", "~*", "true"},
		{"ABC", "[^a-c]", "~*", "false"},
		{"a", "A", "~", "false"},
		{"A", "[[:lower:]]", "~*", "true"},
		{"A", "[[:lower:]]", "~", "false"},
		{"ABC", "ABC", "!~*", "false"},
		{"abc", "(?i)A(?c)", "~", "E:2201B"},
		{"a.c", "***=A.C", "~*", "true"},
		{"Q", "\\x51", "~*", "true"},
		{"q", "\\x51", "~*", "true"},
		{"aaa", "a{0}", "~", "true"},
		{"aaa", "^a{0,0}$", "~", "false"},
		{"", "a{0}", "~", "true"},
		{"ab", "a{,2}", "~", "false"},
		{"a{,2}", "a{,2}", "~", "true"},
		{"a1b2", "[0-9]", "~", "true"},
		{"ab", "[z-a]", "~", "E:2201B"},
		{"a", "[a-a]", "~", "true"},
		{"-", "[a\\-z]", "~", "true"},
		{"b", "[a\\-z]", "~", "false"},
		{"\u00c9", "\u00e9", "~*", "false"},
		{"stra\u00dfe", "STRASSE", "~*", "false"},
	}
	fn := map[string]string{"~": "textregexeq", "~*": "texticregexeq", "!~": "textregexne", "!~*": "texticregexne"}
	answered := 0
	for _, c := range cells {
		got := pgRegexOutcome(fn[c.op], c.subject, c.pattern)
		name := fmt.Sprintf("%q %s %q", c.subject, c.op, c.pattern)
		if got == "E:0A000" {
			if !refused[c.pattern] {
				t.Errorf("%s: refused, PostgreSQL 17.11 answers %s", name, c.pg)
			}
			continue
		}
		if refused[c.pattern] {
			t.Errorf("%s: answered %s, but the form is listed as refused", name, got)
			continue
		}
		answered++
		if got != c.pg {
			t.Errorf("%s: %s, PostgreSQL 17.11 answers %s", name, got, c.pg)
		}
	}
	if answered < 90 {
		t.Errorf("only %d cells answered; the table stopped reaching the operator", answered)
	}
	// NULL on either side is NULL, on all four operators.
	for _, name := range fn {
		for _, args := range [][]any{{nil, "a"}, {"a", nil}} {
			f := DefaultRegistry.Lookup(name)
			if v := f(args); v != nil {
				t.Errorf("%s(%v) = %v, want NULL", name, args, v)
			}
		}
	}
}

// pgRegexOutcome evaluates one operator and renders its outcome the way the
// table records PostgreSQL's: true, false, or E:<SQLSTATE>.
func pgRegexOutcome(fn, subject, pattern string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			fe, ok := r.(fatalEval)
			if !ok {
				panic(r)
			}
			var coded *sqlerr.Error
			if errors.As(fe.err, &coded) {
				out = "E:" + coded.Code
				return
			}
			out = "E:?"
		}
	}()
	f := DefaultRegistry.Lookup(fn)
	return fmt.Sprint(f([]any{subject, pattern}))
}
