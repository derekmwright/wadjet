// SPDX-License-Identifier: MIT

package scan

import (
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// The pushed matcher must agree with the residual path byte-for-byte: both
// are kernel.CompileLike (#1209), and its specialized contains/prefix/
// suffix/equality forms must agree with the generic matcher, kernel.LikeMatch,
// escaped literals included.
func TestCompileLikeMatchesGeneric(t *testing.T) {
	patterns := []string{
		"%google%", "google%", "%google", "google",
		"%", "%%", "", "a%b%c", "a_c", "%a_c%", "__", "%_%",
		"g%e", "a%", "%a", "http://%",
		`a\%%`, `%\%`, `%\%%`, `\%`, `a\_c`, `\\%`, `%\\`, `a\bc%`, `%g\o%`,
	}
	inputs := []string{
		"", "google", "googol", "xgooglex", "agc", "abc", "aXbYc",
		"http://google.com/", "a", "ab", "ggoogle", "googlegoogle",
	}
	r := rand.New(rand.NewSource(24))
	alphabet := "gole%_abc/:.\\"
	for range 300 {
		var b []byte
		for range r.Intn(20) {
			b = append(b, alphabet[r.Intn(len(alphabet))])
		}
		inputs = append(inputs, string(b))
	}
	for _, pat := range patterns {
		generic := func(s []byte) bool {
			m, err := kernel.LikeMatch(string(s), pat)
			if err != nil {
				t.Fatalf("pattern %q: %v", pat, err)
			}
			return m
		}
		for _, negate := range []bool{false, true} {
			m, err := compileLike(pat, negate)
			if err != nil {
				t.Fatal(err)
			}
			for _, in := range inputs {
				want := generic([]byte(in)) != negate
				if got := m([]byte(in)); got != want {
					t.Fatalf("pattern %q negate=%v input %q: specialized=%v generic=%v",
						pat, negate, in, got, want)
				}
			}
		}
	}
}

// A pattern that can raise 22025 is never matched without its error: the
// planner does not push one, and compileLike refuses it (#1209).
func TestCompileLikeRefusesARaisingPattern(t *testing.T) {
	for _, pat := range []string{`\`, `abc\`, `%\`, `a\\\`} {
		if _, err := compileLike(pat, false); err == nil {
			t.Errorf("compileLike(%q) compiled a pattern that can raise 22025", pat)
		}
	}
}
