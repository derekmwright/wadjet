package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// tempLitBatch builds a batch with a date, timestamp, string, and int64
// column for the comparison matrix. Values are set per test.
func tempLitBatch(n int) *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "d", Type: parquet.TypeDate},
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "s", Type: parquet.TypeString},
		{Name: "n", Type: parquet.TypeInt64},
	}
	return batch.NewRecordBatch(schema, n)
}

func TestCmpTemporalLitCompiles(t *testing.T) {
	e := compileCmp(&ColRef{Name: "d"}, &Lit{Val: "1998-09-02"}, CmpLe)
	if _, ok := e.(*CmpTemporalLit); !ok {
		t.Fatalf("date-literal comparison compiled to %T, want *CmpTemporalLit", e)
	}
	// Flipped operand order also specializes.
	e = compileCmp(&Lit{Val: "1998-09-02"}, &ColRef{Name: "d"}, CmpGt)
	tl, ok := e.(*CmpTemporalLit)
	if !ok || !tl.Flip {
		t.Fatalf("flipped literal compiled to %T, want flipped *CmpTemporalLit", e)
	}
	// Non-temporal strings stay generic.
	e = compileCmp(&ColRef{Name: "s"}, &Lit{Val: "BUILDING"}, CmpEq)
	if _, ok := e.(*Cmp); !ok {
		t.Fatalf("plain string literal compiled to %T, want *Cmp", e)
	}
	// Numeric literals keep their existing typed paths.
	e = compileCmp(&Lit{Val: int64(3)}, &Lit{Val: int64(4)}, CmpLt)
	if _, ok := e.(*CmpInt64); !ok {
		t.Fatalf("int literals compiled to %T, want *CmpInt64", e)
	}
}

// TestCmpTemporalLitMatchesGeneric is the semantics gate: for every
// (column type, column value, literal, op, operand order) cell the
// specialized node must return exactly what the generic Cmp returns —
// including the epoch-zero literal guard, NULLs, string columns, and
// non-temporal columns falling back.
func TestCmpTemporalLitMatchesGeneric(t *testing.T) {
	lits := []string{"1998-09-02", "1970-01-01", "1995-01-01T00:00:00", "2049-12-31"}
	ops := []CmpOp{CmpEq, CmpNe, CmpLt, CmpLe, CmpGt, CmpGe}

	dates := []int32{0, 10471, 10472, 10473, -30, 29220}
	tss := []int64{0, 904694400000, 904694400001, 788918400000, -1, 2524521600000}

	n := len(dates)
	b := tempLitBatch(n)
	for i := 0; i < n; i++ {
		b.Columns[0].SetValue(i, dates[i])
		b.Columns[1].SetValue(i, tss[i])
		b.Columns[2].SetValue(i, "1998-09-02")
		b.Columns[3].SetValue(i, int64(dates[i]))
	}
	b.Columns[0].Nulls.SetNull(n - 1) // one NULL date row

	for _, colName := range []string{"d", "ts", "s", "n"} {
		for _, lit := range lits {
			for _, op := range ops {
				for _, flip := range []bool{false, true} {
					var generic, specialized Expr
					if flip {
						generic = &Cmp{Left: &Lit{Val: lit}, Right: &ColRef{Name: colName}, Op: op}
						specialized = compileCmp(&Lit{Val: lit}, &ColRef{Name: colName}, op)
					} else {
						generic = &Cmp{Left: &ColRef{Name: colName}, Right: &Lit{Val: lit}, Op: op}
						specialized = compileCmp(&ColRef{Name: colName}, &Lit{Val: lit}, op)
					}
					if _, ok := specialized.(*CmpTemporalLit); !ok {
						t.Fatalf("col %s lit %q op %v flip %v: compiled %T, want *CmpTemporalLit",
							colName, lit, op, flip, specialized)
					}
					for row := 0; row < n; row++ {
						g := generic.(BoolExpr).EvalBool(b, row)
						s := specialized.(BoolExpr).EvalBool(b, row)
						if g != s {
							t.Fatalf("col %s row %d lit %q op %v flip %v: generic=%v specialized=%v",
								colName, row, lit, op, flip, g, s)
						}
					}
				}
			}
		}
	}
}

func BenchmarkCmpDateLit(b *testing.B) {
	rb := tempLitBatch(2048)
	for i := 0; i < 2048; i++ {
		rb.Columns[0].SetValue(i, int32(10000+i%1000))
	}
	b.Run("generic", func(b *testing.B) {
		e := &Cmp{Left: &ColRef{Name: "d"}, Right: &Lit{Val: "1998-09-02"}, Op: CmpLe}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.EvalBool(rb, i%2048)
		}
	})
	b.Run("temporal-lit", func(b *testing.B) {
		e := compileCmp(&ColRef{Name: "d"}, &Lit{Val: "1998-09-02"}, CmpLe).(*CmpTemporalLit)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.EvalBool(rb, i%2048)
		}
	})
}
