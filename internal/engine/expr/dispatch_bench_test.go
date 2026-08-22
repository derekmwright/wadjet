package expr

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The benchmarks here mirror how a scan-pushed WHERE and a projection
// actually reach the expression tree: through an INTERFACE held by a
// closure, one call per row, over a full 2048-row batch. Calling the
// concrete method directly (as most benches in this package do) lets the
// compiler devirtualize and inline the whole node, which is exactly the
// shape the filter loop does NOT have — the dispatch cost these measure
// would vanish.

// dispatchBenchBatch has one column per operand shape the filter leaves
// take, with a NULL every 17th row so the UNKNOWN branch is exercised at a
// realistic rate rather than never.
func dispatchBenchBatch(n int) *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "d", Type: parquet.TypeDate},
		{Name: "n", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeFloat64},
		{Name: "s", Type: parquet.TypeString},
	}
	b := batch.NewRecordBatch(schema, n)
	for i := 0; i < n; i++ {
		b.Columns[0].SetValue(i, int32(10000+i%400))
		b.Columns[1].SetValue(i, int64(i%1000))
		b.Columns[2].SetValue(i, float64(i)*1.5)
		b.Columns[3].SetValue(i, "abcdefghij")
	}
	for i := 0; i < n; i += 17 {
		b.Columns[0].Nulls.SetNull(i)
		b.Columns[1].Nulls.SetNull(i)
	}
	return b
}

// benchFilterLoop runs the exec.Filter row loop over the compiled predicate,
// reporting nanoseconds per ROW rather than per batch.
func benchFilterLoop(b *testing.B, sql string, want any) {
	b.Helper()
	rb := dispatchBenchBatch(2048)
	e := compileExprSQL(b, sql)
	if want != nil {
		if got, wantT := typeName(e), typeName(want); got != wantT {
			b.Fatalf("%q compiled to %s, want %s", sql, got, wantT)
		}
	}
	pred := FilterPredicate(e)
	b.ReportAllocs()
	b.ResetTimer()
	kept := 0
	for i := 0; i < b.N; i++ {
		for row := 0; row < rb.Len; row++ {
			if pred(rb, row) {
				kept++
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rb.Len), "ns/row")
	if kept == 0 {
		b.Fatalf("%q selected no rows — the benchmark is measuring the reject path only", sql)
	}
}

func typeName(v any) string { return fmt.Sprintf("%T", v) }

func BenchmarkFilterCmpTemporalLit(b *testing.B) {
	benchFilterLoop(b, "d >= '1998-01-01'", &CmpTemporalLit{})
}

func BenchmarkFilterIn(b *testing.B) {
	benchFilterLoop(b, "n IN (1, 7, 11, 23, 99, 500)", &In{})
}

func BenchmarkFilterLike(b *testing.B) {
	benchFilterLoop(b, "s LIKE '%cde%'", &Like{})
}

func BenchmarkFilterBetween(b *testing.B) {
	benchFilterLoop(b, "n BETWEEN 100 AND 900", &Between{})
}

func BenchmarkFilterCmp(b *testing.B) {
	benchFilterLoop(b, "f > 100.0", nil)
}

func BenchmarkFilterAnd(b *testing.B) {
	benchFilterLoop(b, "n > 10 AND d >= '1998-01-01'", &And{})
}

// benchEvalLoop measures a projection's per-row cost through the Expr
// interface, the shape exec.Project's ProjectColumn.Expr closure has.
func benchEvalLoop(b *testing.B, sql string, want any) {
	b.Helper()
	rb := dispatchBenchBatch(2048)
	e := compileExprSQL(b, sql)
	if want != nil {
		if got, wantT := typeName(e), typeName(want); got != wantT {
			b.Fatalf("%q compiled to %s, want %s", sql, got, wantT)
		}
	}
	proj := func(b *batch.RecordBatch, row int) any { return e.Eval(b, row) }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for row := 0; row < rb.Len; row++ {
			sinkAny = proj(rb, row)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rb.Len), "ns/row")
}

var sinkAny any

func BenchmarkBinOpNumericEvalInt(b *testing.B) {
	benchEvalLoop(b, "n * 2 + 1", &BinOpNumeric{})
}

func BenchmarkBinOpNumericEvalFloat(b *testing.B) {
	benchEvalLoop(b, "f * 2 + 1", &BinOpNumeric{})
}

// The typed protocols are what an aggregate input and a typed comparison
// take, and they are where the redundant mode resolution compounded.
func BenchmarkBinOpNumericEvalInt64(b *testing.B) {
	rb := dispatchBenchBatch(2048)
	e := compileExprSQL(b, "n * 2 + 1").(Int64Expr)
	b.ReportAllocs()
	b.ResetTimer()
	var sink int64
	for i := 0; i < b.N; i++ {
		for row := 0; row < rb.Len; row++ {
			v, _ := e.EvalInt64(rb, row)
			sink += v
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rb.Len), "ns/row")
	sinkInt = sink
}

func BenchmarkBinOpNumericEvalFloat64(b *testing.B) {
	rb := dispatchBenchBatch(2048)
	e := compileExprSQL(b, "f * 2 + 1").(Float64Expr)
	b.ReportAllocs()
	b.ResetTimer()
	var sink float64
	for i := 0; i < b.N; i++ {
		for row := 0; row < rb.Len; row++ {
			v, _ := e.EvalFloat64(rb, row)
			sink += v
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rb.Len), "ns/row")
	sinkFloat = sink
}

var (
	sinkInt   int64
	sinkFloat float64
)
