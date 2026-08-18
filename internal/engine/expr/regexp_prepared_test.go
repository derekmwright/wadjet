package expr

import (
	"math/rand"
	"regexp"
	"testing"
)

// The prepared path must be byte-identical to the generic path
// (ReplaceAllString over the sqlBackrefsToGo-converted template) for every
// pattern/replacement/input combination.
func TestPreparedReplaceMatchesGeneric(t *testing.T) {
	cases := []struct{ pattern, repl string }{
		{`^https?://(?:www\.)?([^/]+)/.*$`, `\1`},          // ClickBench Q29
		{`(\w+)@(\w+)`, `\2 at \1`},                        // multiple groups, reordered
		{`a+`, `X`},                                        // no groups, multiple matches
		{`(b)?c`, `[\1]`},                                  // optional group (unmatched → empty)
		{`x*`, `<>`},                                       // empty matches (advancement rules)
		{`(\d+)`, `n=\1$`},                                 // literal dollar in template
		{`(.)(.)`, `\2\1`},                                 // swap pairs
		{`\\`, `/`},                                        // escaped backslash pattern
		{`(a)(b)(c)(d)(e)(f)(g)(h)(i)`, `\9\1`},            // high group numbers
		{`q`, `\\1`},                                       // escaped backslash then digit → literal \1
		{`(z)`, `pre\1post\7`},                             // out-of-range group → empty
	}
	inputs := []string{
		"",
		"http://www.example.com/path/to/page",
		"https://example.org/",
		"no match at all",
		"aaa bbb aaabc c x",
		"user@host and other@place",
		"12 and 345",
		`back\slash`,
		"abcdefghi abcdefghi",
		"zzz",
		"ab", "abab", "xxabxx",
	}
	// Deterministic fuzz-ish extras.
	r := rand.New(rand.NewSource(29))
	alphabet := "abcx/:.@\\w1 "
	for range 200 {
		var b []byte
		for range r.Intn(24) {
			b = append(b, alphabet[r.Intn(len(alphabet))])
		}
		inputs = append(inputs, string(b))
	}

	for _, c := range cases {
		prep := prepareRegexpReplace(c.pattern, c.repl)
		if !prep.ok {
			t.Fatalf("pattern %q failed to prepare", c.pattern)
		}
		re := regexp.MustCompile(c.pattern)
		goRepl := sqlBackrefsToGo(c.repl)
		for _, in := range inputs {
			want := re.ReplaceAllString(in, goRepl)
			got := prep.replaceAll(in)
			if got != want {
				t.Errorf("pattern %q repl %q input %q:\n  generic:  %q\n  prepared: %q",
					c.pattern, c.repl, in, want, got)
			}
		}
	}
}

// Anchor detection decides whether replaceAll may ask for one match
// instead of all of them. A false positive silently drops replacements, so
// the table covers the shapes a "pattern starts with ^" test would get
// wrong.
func TestAnchoredAtTextStart(t *testing.T) {
	cases := []struct {
		pattern string
		want    bool
		why     string
	}{
		{`^https?://(?:www\.)?([^/]+)/.*$`, true, "ClickBench Q29"},
		{`^abc`, true, "plain anchor"},
		{`\Aabc`, true, `\A is the same op as ^`},
		{`^`, true, "bare anchor, empty match at 0 only"},
		{`^a*`, true, "anchored, can match empty"},
		{`(^a)b`, true, "anchor inside a leading capture"},
		{`(?i)^abc`, true, "flag group before the anchor"},
		{`^a|^b`, true, "alternation anchored on every branch"},
		{`(^a|^b)c`, true, "captured alternation, all branches anchored"},
		{`(^a)+`, true, "one-or-more of an anchored sub"},

		{`abc`, false, "unanchored"},
		{`[^/]+`, false, "^ inside a character class is a negation"},
		{`\^abc`, false, "escaped literal caret"},
		{`^a|b`, false, "only the first branch is anchored"},
		{`a|^b`, false, "only the second branch is anchored"},
		{`(?m)^a`, false, "multiline ^ is OpBeginLine and matches after newlines"},
		{`(^a)*`, false, "star may skip the anchored sub entirely"},
		{`(^a)?b`, false, "optional anchored sub"},
		{`.*^a`, false, "anchor is not at the start of the concat"},
		{`$`, false, "end anchor says nothing about start"},
		{`\bfoo`, false, "word boundary is not a text anchor"},
		{`(`, false, "unparseable pattern falls back"},
	}
	for _, c := range cases {
		if got := anchoredAtTextStart(c.pattern); got != c.want {
			t.Errorf("anchoredAtTextStart(%q) = %v, want %v (%s)", c.pattern, got, c.want, c.why)
		}
	}
}

// Whatever the detector says, the two splice paths must agree with Go's
// own ReplaceAllString — including on patterns the detector accepts, where
// only one match is ever consulted.
func TestPreparedReplaceAnchoredPathParity(t *testing.T) {
	cases := []struct{ pattern, repl string }{
		{`^https?://(?:www\.)?([^/]+)/.*$`, `\1`},   // ClickBench Q29
		{`^(\w+)`, `[\1]`},                          // anchored, match is a prefix only
		{`^a*`, `<>`},                               // anchored empty matches
		{`^(a)|^(b)`, `\1\2`},                       // anchored alternation, one group unmatched
		{`(?m)^(\w+)`, `<\1>`},                      // multiline: several matches, NOT anchored
		{`^`, `X`},                                  // pure anchor, empty match
		{`^(.*)$`, `\1`},                            // whole-subject extract
		{`^(\w+)@(\w+)$`, `\2/\1`},                  // multi-group whole-subject
		{`^(z)`, `pre\1post\7`},                     // out-of-range group
		{`^(?:(a)|(b))c`, `\1\2!`},                  // unmatched optional group
		{`\A(\d+)`, `n=\1`},                         // \A form
		{`^x(y)?z`, `[\1]`},                         // optional group inside anchored match
	}
	inputs := []string{
		"", "a", "aaa", "abc", "xz", "xyz", "zzz",
		"http://www.example.com/path/to/page",
		"https://example.org/",
		"HTTP://Example.COM/x",
		"no match at all",
		"user@host",
		"12345 and more",
		"line one\nline two\nline three",
		"ünïcøde://www.münchen.example.de/straße/übersicht",
		"日本語://www.例え.jp/パス/ページ",
		"a\nb\n",
		"\n\n",
	}
	r := rand.New(rand.NewSource(302))
	alphabet := "abz@/.:\n0１ü日 "
	for range 300 {
		var b []byte
		for range r.Intn(20) {
			b = append(b, alphabet[r.Intn(len(alphabet))])
		}
		inputs = append(inputs, string(b))
	}

	for _, c := range cases {
		prep := prepareRegexpReplace(c.pattern, c.repl)
		if !prep.ok {
			t.Fatalf("pattern %q failed to prepare", c.pattern)
		}
		// Same prepared state with the fast path disabled — isolates the
		// anchored branch from the generic one.
		generic := &preparedRegexp{re: prep.re, segs: prep.segs, ok: true}
		re := regexp.MustCompile(c.pattern)
		goRepl := sqlBackrefsToGo(c.repl)
		for _, in := range inputs {
			want := re.ReplaceAllString(in, goRepl)
			if got := prep.replaceAll(in); got != want {
				t.Errorf("anchored=%v pattern %q repl %q input %q:\n  regexp:   %q\n  prepared: %q",
					prep.anchored, c.pattern, c.repl, in, want, got)
			}
			if got := generic.replaceAll(in); got != want {
				t.Errorf("generic path, pattern %q repl %q input %q:\n  regexp: %q\n  got:    %q",
					c.pattern, c.repl, in, want, got)
			}
		}
	}
}

// The Q29 shape returns a substring of its input with no allocation at
// all; the guard is that it stays a correct substring, not merely cheap.
func TestPreparedReplaceWholeMatchExtractIsZeroAlloc(t *testing.T) {
	prep := prepareRegexpReplace(`^https?://(?:www\.)?([^/]+)/.*$`, `\1`)
	src := "http://www.example.com/some/fairly/long/path?with=query"
	if got := prep.replaceAll(src); got != "example.com" {
		t.Fatalf("replaceAll = %q, want %q", got, "example.com")
	}
	// Stated against the match itself rather than an absolute count (the
	// race detector inflates both): finding the match is all this shape
	// costs, the splice adds nothing.
	match := testing.AllocsPerRun(200, func() { _ = prep.re.FindStringSubmatchIndex(src) })
	full := testing.AllocsPerRun(200, func() { _ = prep.replaceAll(src) })
	if full > match {
		t.Errorf("whole-match extract allocated %v per call vs %v for the match alone", full, match)
	}
}

func TestPreparedReplaceKillSwitch(t *testing.T) {
	e := &FuncCall{Name: "regexp_replace", Args: []Expr{
		&ColRef{Name: "s"},
		&Lit{Val: `(\w+)`},
		&Lit{Val: `<\1>`},
	}}
	if p := e.preparedReplace(); p == nil {
		t.Fatal("qualifying call did not prepare")
	}
	prev := preparedRegexpToggle.Set(false)
	defer preparedRegexpToggle.Set(prev)
	e2 := &FuncCall{Name: "regexp_replace", Args: []Expr{
		&ColRef{Name: "s"},
		&Lit{Val: `(\w+)`},
		&Lit{Val: `<\1>`},
	}}
	if p := e2.preparedReplace(); p != nil {
		t.Fatal("kill switch off but call still prepared")
	}
}

func BenchmarkRegexpReplaceGeneric(b *testing.B) {
	in := "http://www.example.com/some/fairly/long/path?with=query&and=params"
	args := []any{in, `^https?://(?:www\.)?([^/]+)/.*$`, `\1`}
	for i := 0; i < b.N; i++ {
		_ = fnRegexpReplace(args)
	}
}

func BenchmarkRegexpReplacePrepared(b *testing.B) {
	in := "http://www.example.com/some/fairly/long/path?with=query&and=params"
	prep := prepareRegexpReplace(`^https?://(?:www\.)?([^/]+)/.*$`, `\1`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = prep.replaceAll(in)
	}
}
