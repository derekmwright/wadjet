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
