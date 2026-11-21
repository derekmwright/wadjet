// SPDX-License-Identifier: MIT

package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// lower() and ILIKE over ASCII and over mixed (non-ASCII) text, a value at a
// time and over a column: the one C-locale case mapping (#1209) must not be
// slower than the Unicode folding it replaced.

var caseFoldBenchASCII = []string{"Hello World", "FOO bar BAZ", "testing 123", "Alpha Beta GAMMA"}
var caseFoldBenchMixed = []string{"École ÉTÉ Été", "ПРИВЕТ мир", "Straße GROSS", "Ünïcödé ÅSCII mix"}

func caseFoldBenchBatch(vals []string, n int) *batch.RecordBatch {
	schema := []parquet.Column{{Name: "s", Type: parquet.TypeString}}
	b := batch.NewRecordBatch(schema, n)
	for i := 0; i < n; i++ {
		b.Columns[0].BytesData.Set(i, []byte(vals[i%len(vals)]))
	}
	return b
}

func benchCaseFoldRow(b *testing.B, fn string, vals []string) {
	bb := caseFoldBenchBatch(vals, 2048)
	fc := &FuncCall{Name: fn, Args: []Expr{&ColRef{Name: "s"}}}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = fc.Eval(bb, row)
		}
	}
}

func benchCaseFoldVec(b *testing.B, fn string, vals []string) {
	bb := caseFoldBenchBatch(vals, 2048)
	fc := &FuncCall{Name: fn, Args: []Expr{&ColRef{Name: "s"}}}
	out := batch.NewVector(batch.TypeString, 2048)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out.BytesData.Reset()
		fc.EvalVec(bb, out, 2048)
	}
}

func benchCaseFoldILike(b *testing.B, vals []string, pattern string) {
	bb := caseFoldBenchBatch(vals, 2048)
	node, err := plansql.ParseExpression("s ILIKE '" + pattern + "'")
	if err != nil {
		b.Fatal(err)
	}
	e, err := Compile(node)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.Eval(bb, row)
		}
	}
}

func BenchmarkCaseFoldLowerRowASCII(b *testing.B) { benchCaseFoldRow(b, "lower", caseFoldBenchASCII) }
func BenchmarkCaseFoldLowerRowMixed(b *testing.B) { benchCaseFoldRow(b, "lower", caseFoldBenchMixed) }
func BenchmarkCaseFoldUpperRowASCII(b *testing.B) { benchCaseFoldRow(b, "upper", caseFoldBenchASCII) }
func BenchmarkCaseFoldUpperRowMixed(b *testing.B) { benchCaseFoldRow(b, "upper", caseFoldBenchMixed) }
func BenchmarkCaseFoldLowerVecASCII(b *testing.B) { benchCaseFoldVec(b, "lower", caseFoldBenchASCII) }
func BenchmarkCaseFoldLowerVecMixed(b *testing.B) { benchCaseFoldVec(b, "lower", caseFoldBenchMixed) }
func BenchmarkCaseFoldILikeASCII(b *testing.B)    { benchCaseFoldILike(b, caseFoldBenchASCII, "%world%") }
func BenchmarkCaseFoldILikeMixed(b *testing.B)    { benchCaseFoldILike(b, caseFoldBenchMixed, "%été%") }
