// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Arc RX (#1499): every construct that takes a SQL pattern compiles it
// through translateAndCompile, so the engine reads one dialect —
// PostgreSQL's ARE. The cells' expected answers are PostgreSQL 17.11's,
// measured (tooling/arcs/rx_regexp_translation/rx_author/pg.tsv); a
// non-PostgreSQL function's answer is its PostgreSQL equivalent's
// (regexp_extract ≡ regexp_substr, regexp_extract_all ≡ the array of
// regexp_substr occurrences, regexp_split ≡ regexp_split_to_array,
// payload_matches ≡ `~`).

// TestArcRXTranslateAndCompileRows: unit rows on the one compile function —
// a pattern and flags in, PostgreSQL's match (or its refusal) out.
func TestArcRXTranslateAndCompileRows(t *testing.T) {
	cells := []struct {
		pattern string
		flags   reFlags
		subject string
		want    string // "t" / "f" / "E:<state>"
	}{
		{`\b`, reFlags{}, "abc", "f"},               // backspace, not a word boundary (#1499)
		{`a\bc`, reFlags{}, "a\bc", "t"},            // the backspace it is
		{`\ya`, reFlags{}, "b a", "t"},              // the word boundary is \y
		{`b\Y`, reFlags{}, "ba", "t"},               // \Y: not a boundary
		{`a\Bb`, reFlags{}, `a\b`, "t"},             // \B is a backslash
		{`b\Z`, reFlags{}, "ab", "t"},               // \Z ends the string
		{`b\z`, reFlags{}, "ab", "E:2201B"},         // RE2-only spelling
		{`\pL`, reFlags{}, "a", "E:2201B"},          // RE2-only spelling
		{`\Qa.\E`, reFlags{}, "a.", "E:2201B"},      // RE2-only spelling
		{`(?P<n>a)`, reFlags{}, "a", "E:2201B"},     // RE2-only spelling
		{`a(?i)b`, reFlags{}, "aB", "E:2201B"},      // an option only at the start
		{`a.b`, reFlags{}, "a\nb", "t"},             // . matches a newline
		{`\s`, reFlags{}, "\v", "t"},                // \s holds the vertical tab
		{`A`, reFlags{}, "A", "t"},                  // \uXXXX
		{`***=a.c`, reFlags{}, "abc", "f"},          // literal director
		{`***:a.c`, reFlags{}, "abc", "t"},          // ARE director
		{`(?i)A`, reFlags{}, "a", "t"},              // embedded option
		{`A`, reFlags{icase: true}, "a", "t"},       // the i flag
		{`a.c`, reFlags{literal: true}, "abc", "f"}, // the q flag
		{`a.c`, reFlags{literal: true}, "a.c", "t"}, // the q flag
		{`(a`, reFlags{}, "a", "E:2201B"},           // unbalanced
		{`[a`, reFlags{}, "a", "E:2201B"},           // unbalanced
		{`*a`, reFlags{}, "a", "E:2201B"},           // quantifier operand
		{`a{256}`, reFlags{}, "a", "E:2201B"},       // bound > 255
		{``, reFlags{}, "abc", "t"},                 // the empty pattern
		{`(a)\1`, reFlags{}, "aa", "E:0A000"},       // back reference: refused
		{`a(?=b)`, reFlags{}, "ab", "E:0A000"},      // lookahead: refused
		{`(?<=a)b`, reFlags{}, "ab", "E:0A000"},     // lookbehind: refused
		{`\ma`, reFlags{}, "b a", "E:0A000"},        // \m: refused
		{`[[:<:]]a`, reFlags{}, "b a", "E:0A000"},   // [[:<:]]: refused
		{`(?x) a b`, reFlags{}, "ab", "t"},          // expanded syntax
		{`(?x)a\ b`, reFlags{}, "a b", "t"},
		{`(?n)a.b`, reFlags{}, "a\nb", "f"},
		{`(?m)^b`, reFlags{}, "a\nb", "t"},
		{`(?p)^b`, reFlags{}, "a\nb", "f"},
		{`(?w)a.b`, reFlags{}, "a\nb", "t"},
		{`a[^x]b`, reFlags{nlStop: true}, "a\nb", "f"},
		{`a\Wb`, reFlags{nlStop: true}, "a\nb", "t"},
		{`(?b)a`, reFlags{}, "a", "E:0A000"},
		{`äbc`, reFlags{icase: true}, "ÄBC", "t"},
		{`σ`, reFlags{icase: true}, "ς", "f"},
		{`ς`, reFlags{icase: true}, "Σ", "t"},
		{`i`, reFlags{icase: true}, "İ", "f"},
		{`İ`, reFlags{icase: true}, "i", "t"},
		{`ß`, reFlags{icase: true}, "ẞ", "t"},
		{`[à-æ]`, reFlags{icase: true}, "À", "t"},
		{`[^ä]`, reFlags{icase: true}, "Ä", "f"},
		{`\u00e4`, reFlags{icase: true}, "Ä", "t"},
		{`äbc`, reFlags{icase: true, literal: true}, "ÄBC", "t"},
		{`[[:alpha:]][[:digit:]]`, reFlags{}, "a1", "t"}, // POSIX classes
	}
	for _, c := range cells {
		got := func() (out string) {
			re, err := translateAndCompile(c.pattern, c.flags)
			if err != nil {
				return "E:" + sqlerr.StateOf(err)
			}
			if re.re.MatchString(c.subject) {
				return "t"
			}
			return "f"
		}()
		if got != c.want {
			t.Errorf("translateAndCompile(%q, %+v) over %q = %s, PostgreSQL 17.11 %s", c.pattern, c.flags, c.subject, got, c.want)
		}
	}
}

// TestArcRXFlagsRows: the flags argument as parse_re_flags reads it.
func TestArcRXFlagsRows(t *testing.T) {
	cells := []struct{ fn, flags, want string }{
		{"regexp_like", "i", "i"},
		{"regexp_like", "ic", ""},
		{"regexp_like", "ci", "i"},
		{"regexp_like", "g", "E:22023"},
		{"regexp_count", "g", "E:22023"},
		{"regexp_replace", "g", "g"},
		{"regexp_like", "q", "q"},
		{"regexp_like", "s", ""},
		{"regexp_like", "t", ""},
		{"regexp_like", "", ""},
		{"regexp_like", "z", "E:22023"},
		{"regexp_like", "n", "NA"},
		{"regexp_like", "m", "NA"},
		{"regexp_like", "p", "N"},
		{"regexp_like", "w", "A"},
		{"regexp_like", "x", "X"},
		{"regexp_like", "xt", ""},
		{"regexp_like", "ms", ""},
		{"regexp_like", "b", "E:0A000"},
		{"regexp_like", "e", "E:0A000"},
		{"regexp_like", "ns", ""}, // s after n restores the default
		{"regexp_like", "sn", "NA"},
	}
	for _, c := range cells {
		f, err := parseREFlags(c.fn, c.flags)
		got := ""
		if err != nil {
			got = "E:" + sqlerr.StateOf(err)
		} else {
			if f.global {
				got += "g"
			}
			if f.icase {
				got += "i"
			}
			if f.literal {
				got += "q"
			}
			if f.nlStop {
				got += "N"
			}
			if f.nlAnchor {
				got += "A"
			}
			if f.expanded {
				got += "X"
			}
		}
		if got != c.want {
			t.Errorf("parseREFlags(%s, %q) = %s, want %s", c.fn, c.flags, got, c.want)
		}
	}
}

// TestArcRXCompileCacheIsBounded: a literal pattern compiles once however
// many rows evaluate it, and a column of distinct patterns cannot grow the
// cache past regexCacheBound.
func TestArcRXCompileCacheIsBounded(t *testing.T) {
	const pattern = `arc-rx-literal-[0-9]+`
	before := regexCompiles.Load()
	for i := 0; i < 10000; i++ {
		fnRegexpLike([]any{fmt.Sprintf("arc-rx-literal-%d", i), pattern})
	}
	if n := regexCompiles.Load() - before; n != 1 {
		t.Errorf("10000 rows over one literal pattern compiled it %d times, want 1", n)
	}
	for i := 0; i < 3*regexCacheBound; i++ {
		fnRegexpLike([]any{"x", fmt.Sprintf("arc-rx-distinct-%d", i)})
		if n := regexCacheLen(); n > regexCacheBound+8 {
			t.Fatalf("after %d distinct patterns the cache holds %d entries, bound %d", i+1, n, regexCacheBound)
		}
	}
}

// TestArcRXCacheEvictsOneEntryAtATime: past the bound the cache drops its
// oldest entry, not all of them — a working set of 1500 patterns compiles
// once and then hits on every later pass, and the cache never holds more
// than regexCacheBound entries.
func TestArcRXCacheEvictsOneEntryAtATime(t *testing.T) {
	for i := 0; i < regexCacheBound+100; i++ {
		fnRegexpLike([]any{"x", fmt.Sprintf("arc-rx-evict-fill-%d", i)})
	}
	if n := regexCacheLen(); n > regexCacheBound {
		t.Fatalf("cache holds %d entries, bound %d", n, regexCacheBound)
	}
	ws := make([]string, 1500)
	for i := range ws {
		ws[i] = fmt.Sprintf(`arc-rx-evict-ws-%d\y`, i)
	}
	for pass := 0; pass < 3; pass++ {
		before := regexCompiles.Load()
		for _, p := range ws {
			pgRegexMatch([]any{"x", p}, false, false)
		}
		n := regexCompiles.Load() - before
		want := int64(0)
		if pass == 0 {
			want = int64(len(ws))
		}
		if n != want {
			t.Errorf("pass %d over a working set of %d patterns compiled %d, want %d", pass, len(ws), n, want)
		}
	}
	if n := regexCacheLen(); n > regexCacheBound {
		t.Fatalf("cache holds %d entries, bound %d", n, regexCacheBound)
	}
}
