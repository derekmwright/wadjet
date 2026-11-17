// SPDX-License-Identifier: MIT

package kernel

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// BenchmarkLikeKernel measures the WHERE-clause LIKE kernel over one 2048-row
// string batch, per pattern shape: the fast forms (equality, prefix, suffix,
// contains) and the general matcher. The escaped shapes are #1209's: the
// backslash is the escape, and an escaped prefix must still take the prefix
// form.
func BenchmarkLikeKernel(b *testing.B) {
	vec := batch.NewVector(batch.TypeString, 0)
	for i := 0; i < batch.DefaultBatchSize; i++ {
		vec.AppendValue(fmt.Sprintf("http://www.example%d.com/path/%d?q=google", i%97, i))
	}
	out := make([]uint32, 0, vec.Len)
	for _, c := range []struct{ name, pat string }{
		{"exact", "http://www.example5.com/path/5?q=google"},
		{"prefix", "http://www.example1%"},
		{"suffix", "%=google"},
		{"contains", "%example42%"},
		{"general", "http://%example_2.com%"},
		{"escaped-prefix", `http:\/\/www%`},
		{"escaped-contains", `%q\=google%`},
	} {
		k := ResolveLikeFilterKernel(batch.TypeString, c.pat, false)
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out = k(vec, nil, vec.Len, out)
			}
		})
	}
}
