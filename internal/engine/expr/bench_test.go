package expr

import (
	"testing"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func benchBatch(n int) *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
		{Name: "status", Type: parquet.TypeInt32},
	}
	b := batch.NewRecordBatch(schema, n)
	for i := 0; i < n; i++ {
		b.Columns[0].SetValue(i, int64(i))
		b.Columns[1].SetValue(i, "alice")
		b.Columns[2].SetValue(i, float64(i)*1.5)
		b.Columns[3].SetValue(i, int32(i%3))
	}
	return b
}

func BenchmarkColRef(b *testing.B) {
	bb := benchBatch(2048)
	e := &ColRef{Name: "amount"}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.Eval(bb, row)
		}
	}
}

func BenchmarkLiteral(b *testing.B) {
	bb := benchBatch(1)
	e := &Lit{Val: float64(42)}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = e.Eval(bb, 0)
	}
}

func BenchmarkArithExpr(b *testing.B) {
	bb := benchBatch(2048)
	// amount * 2 + 10
	e := &BinOp{
		Left: &BinOp{
			Left:  &ColRef{Name: "amount"},
			Right: &Lit{Val: float64(2)},
			Op:    "*",
		},
		Right: &Lit{Val: float64(10)},
		Op:    "+",
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.Eval(bb, row)
		}
	}
}

func BenchmarkComparison(b *testing.B) {
	bb := benchBatch(2048)
	e := &Cmp{
		Left:  &ColRef{Name: "amount"},
		Right: &Lit{Val: float64(100)},
		Op:    CmpGt,
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.EvalBool(bb, row)
		}
	}
}

func BenchmarkAndPredicate(b *testing.B) {
	bb := benchBatch(2048)
	e := &And{
		Left:  &Cmp{Left: &ColRef{Name: "id"}, Right: &Lit{Val: int64(100)}, Op: CmpGt},
		Right: &Cmp{Left: &ColRef{Name: "amount"}, Right: &Lit{Val: float64(500)}, Op: CmpLt},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.EvalBool(bb, row)
		}
	}
}

func BenchmarkFuncCallUpper(b *testing.B) {
	bb := benchBatch(2048)
	e := &FuncCall{Name: "upper", Args: []Expr{&ColRef{Name: "name"}}}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.Eval(bb, row)
		}
	}
}

func BenchmarkFuncCallConcat(b *testing.B) {
	bb := benchBatch(2048)
	e := &FuncCall{
		Name: "concat",
		Args: []Expr{&ColRef{Name: "name"}, &Lit{Val: "_"}, &ColRef{Name: "name"}},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.Eval(bb, row)
		}
	}
}

func BenchmarkCaseWhen(b *testing.B) {
	bb := benchBatch(2048)
	e := &Case{
		Whens: []CaseWhen{
			{Cond: &Cmp{Left: &ColRef{Name: "status"}, Right: &Lit{Val: int64(0)}, Op: CmpEq}, Result: &Lit{Val: "inactive"}},
			{Cond: &Cmp{Left: &ColRef{Name: "status"}, Right: &Lit{Val: int64(1)}, Op: CmpEq}, Result: &Lit{Val: "active"}},
		},
		Else: &Lit{Val: "unknown"},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.Eval(bb, row)
		}
	}
}

func BenchmarkInExpr(b *testing.B) {
	bb := benchBatch(2048)
	e := &In{
		Expr: &ColRef{Name: "status"},
		Values: []Expr{
			&Lit{Val: int64(0)},
			&Lit{Val: int64(1)},
			&Lit{Val: int64(5)},
			&Lit{Val: int64(10)},
		},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.EvalBool(bb, row)
		}
	}
}

func BenchmarkCompileAndEval(b *testing.B) {
	bb := benchBatch(2048)
	sql := "SELECT * FROM t WHERE amount > 100 AND id < 1000"
	stmt, _ := sqlparser.Parse(sql)
	sel := stmt.(*sqlparser.Select)
	compiled, _ := Compile(sel.Where.Expr)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = compiled.Eval(bb, row)
		}
	}
}

func BenchmarkCompileComplexExpr(b *testing.B) {
	bb := benchBatch(2048)
	sql := "SELECT amount * 2 + id FROM t"
	stmt, _ := sqlparser.Parse(sql)
	sel := stmt.(*sqlparser.Select)
	aliased := sel.SelectExprs[0].(*sqlparser.AliasedExpr)
	compiled, _ := Compile(aliased.Expr)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = compiled.Eval(bb, row)
		}
	}
}

func BenchmarkLikePattern(b *testing.B) {
	bb := benchBatch(2048)
	e := &Like{
		Expr:    &ColRef{Name: "name"},
		Pattern: &Lit{Val: "al%"},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.EvalBool(bb, row)
		}
	}
}
