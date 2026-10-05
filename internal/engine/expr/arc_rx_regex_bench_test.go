// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"testing"
)

// Arc RX benchmarks (#1499): a literal pattern over many rows (compiled once
// — the seam's cache — and looked up per row) and a column of distinct
// patterns (one compile per distinct pattern; the cache bounded).
var rxBenchSubjects = func() []string {
	out := make([]string, 2048)
	for i := range out {
		out[i] = fmt.Sprintf("GET /api/v1/items/%d?user=u%d HTTP/1.1", i, i%97)
	}
	return out
}()

func BenchmarkArcRXRegexpLikeLiteral(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fnRegexpLike([]any{rxBenchSubjects[i%len(rxBenchSubjects)], `items/[0-9]+\?user=u1`})
	}
}

func BenchmarkArcRXRegexpCountLiteral(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fnRegexpCount([]any{rxBenchSubjects[i%len(rxBenchSubjects)], `[0-9]+`})
	}
}

func BenchmarkArcRXRegexpExtractLiteral(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fnRegexpExtract([]any{rxBenchSubjects[i%len(rxBenchSubjects)], `user=(u[0-9]+)`, int64(1)})
	}
}

func BenchmarkArcRXRegexMatchOperatorLiteral(b *testing.B) {
	f := DefaultRegistry.Lookup("textregexeq")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		f([]any{rxBenchSubjects[i%len(rxBenchSubjects)], `items/[0-9]+\?user=u1`})
	}
}

// A column of 100k distinct patterns, cycled.
func BenchmarkArcRXRegexpLikeColumnDistinct(b *testing.B) {
	pats := make([]string, 100000)
	for i := range pats {
		pats[i] = fmt.Sprintf(`items/%d\?user=u[0-9]+`, i)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fnRegexpLike([]any{rxBenchSubjects[i%len(rxBenchSubjects)], pats[i%len(pats)]})
	}
}

// The `~` operator over a column of distinct patterns, cycled: a working set
// under the cache's bound (500, 1500) and one far over it (100k).
func benchRXOpColumn(b *testing.B, n int) {
	f := DefaultRegistry.Lookup("textregexeq")
	pats := make([]string, n)
	for i := range pats {
		pats[i] = fmt.Sprintf(`items/%d\?user=u[0-9]+`, i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f([]any{rxBenchSubjects[i%len(rxBenchSubjects)], pats[i%n]})
	}
}

func BenchmarkArcRXRegexOpColumn500(b *testing.B)  { benchRXOpColumn(b, 500) }
func BenchmarkArcRXRegexOpColumn1500(b *testing.B) { benchRXOpColumn(b, 1500) }
func BenchmarkArcRXRegexOpColumn100k(b *testing.B) { benchRXOpColumn(b, 100000) }

// The literal pattern under parallel evaluation (morsel workers share the
// cache).
func BenchmarkArcRXRegexOpLiteralParallel(b *testing.B) {
	f := DefaultRegistry.Lookup("textregexeq")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			f([]any{rxBenchSubjects[i%len(rxBenchSubjects)], `items/[0-9]+\?user=u1`})
			i++
		}
	})
}

// A column of 1500 distinct patterns under parallel evaluation.
func BenchmarkArcRXRegexOpColumn1500Parallel(b *testing.B) {
	f := DefaultRegistry.Lookup("textregexeq")
	pats := make([]string, 1500)
	for i := range pats {
		pats[i] = fmt.Sprintf(`items/%d\?user=u[0-9]+`, i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			f([]any{rxBenchSubjects[i%len(rxBenchSubjects)], pats[i%len(pats)]})
			i++
		}
	})
}
