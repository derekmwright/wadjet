// SPDX-License-Identifier: MIT

package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The timestamp-difference guard's cost (#1713d round 8): the minus over
// numeric columns (BinOpNumeric's typed path, BinOpFloat64's vectorized
// loop) and the boxed timestamp ± interval shift, one 2048-row batch per
// iteration. Compared against b0a3e59d with the same file (1713d_r8.md).
func tdgBenchBatch() *batch.RecordBatch {
	n := batch.DefaultBatchSize
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "c", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeFloat64},
		{Name: "g", Type: parquet.TypeFloat64},
		{Name: "ts", Type: parquet.TypeTimestamp},
	}, n)
	for i := 0; i < n; i++ {
		b.Columns[0].SetValue(i, int64(i*7))
		b.Columns[1].SetValue(i, int64(i))
		b.Columns[2].SetValue(i, float64(i)*1.5)
		b.Columns[3].SetValue(i, float64(i)*0.25)
		b.Columns[4].SetValue(i, int64(1704067200000+i*1000))
	}
	b.Len = n
	return b
}

func benchRows(b *testing.B, sql string, run func(e Expr, rb *batch.RecordBatch)) {
	e := compileSelectColB(b, sql)
	rb := tdgBenchBatch()
	run(e, rb)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		run(e, rb)
	}
}

func evalFloatRows(e Expr, rb *batch.RecordBatch) {
	f := e.(Float64Expr)
	for r := 0; r < rb.Len; r++ {
		f.EvalFloat64(rb, r)
	}
}

func evalIntRows(e Expr, rb *batch.RecordBatch) {
	f := e.(Int64Expr)
	for r := 0; r < rb.Len; r++ {
		f.EvalInt64(rb, r)
	}
}

func evalBoxedRows(e Expr, rb *batch.RecordBatch) {
	for r := 0; r < rb.Len; r++ {
		e.Eval(rb, r)
	}
}

func BenchmarkTDGMinusInt64Cols(b *testing.B)   { benchRows(b, "SELECT a - c", evalIntRows) }
func BenchmarkTDGMinusFloat64Cols(b *testing.B) { benchRows(b, "SELECT f - g", evalFloatRows) }
func BenchmarkTDGMinusFloatVec(b *testing.B) {
	buf := make([]float64, batch.DefaultBatchSize)
	// The vectorized kernel the projection planner selects for a FLOAT64
	// output (projection_plan.go), built directly: no SQL spelling over
	// two columns compiles to it.
	e := &BinOpFloat64{Left: &ColRef{Name: "f"}, Right: &ColRef{Name: "g"}, Op: "-"}
	rb := tdgBenchBatch()
	e.EvalFloat64Vec(rb, buf, rb.Len)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.EvalFloat64Vec(rb, buf, rb.Len)
	}
}
func BenchmarkTDGMinusBoxedInt(b *testing.B) { benchRows(b, "SELECT a - c", evalBoxedRows) }
func BenchmarkTDGTimestampPlusInterval(b *testing.B) {
	benchRows(b, "SELECT ts + INTERVAL '1 day'", evalBoxedRows)
}
func BenchmarkTDGTimestampMinusInterval(b *testing.B) {
	benchRows(b, "SELECT ts - INTERVAL '1 day'", evalBoxedRows)
}

// compileSelectColB compiles a statement's first select item.
func compileSelectColB(b *testing.B, sql string) Expr {
	b.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		b.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		b.Fatal(err)
	}
	e, err := Compile(info.Columns[0].ASTExpr)
	if err != nil {
		b.Fatal(err)
	}
	return e
}
