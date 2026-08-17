package scan

import (
	"math/rand"
	"testing"
)

// The pushed matcher must agree with the residual path byte-for-byte;
// likeMatchBytes mirrors expr.matchLikeRecur, and the specialized
// contains/prefix/suffix/equality forms must agree with the generic form.
func TestCompileLikeMatchesGeneric(t *testing.T) {
	patterns := []string{
		"%google%", "google%", "%google", "google",
		"%", "%%", "", "a%b%c", "a_c", "%a_c%", "__", "%_%",
		"g%e", "a%", "%a", "http://%",
	}
	inputs := []string{
		"", "google", "googol", "xgooglex", "agc", "abc", "aXbYc",
		"http://google.com/", "a", "ab", "ggoogle", "googlegoogle",
	}
	r := rand.New(rand.NewSource(24))
	alphabet := "gole%_abc/:."
	for range 300 {
		var b []byte
		for range r.Intn(20) {
			b = append(b, alphabet[r.Intn(len(alphabet))])
		}
		inputs = append(inputs, string(b))
	}
	for _, pat := range patterns {
		generic := func(s []byte) bool { return likeMatchBytes(s, []byte(pat), 0, 0) }
		for _, negate := range []bool{false, true} {
			m := compileLike(pat, negate)
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
