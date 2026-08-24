package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// decimalGuardBatch is DECIMAL(9,2): 0.00, 5.00, -3.50. Row 0 holds the
// column's zero value, which is exactly what a nil literal must NOT match.
func decimalGuardBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2},
	}, 3)
	for i, u := range []int64{0, 500, -350} {
		b.Columns[0].DecimalData.Data[i] = batch.Int128From(u)
	}
	return b
}

// TestKernelFilterLitNilDecimalMatchesNothing is the floor guard for
// decimalLitValue: a nil value carrying non-empty litText used to bypass
// ResolveFilterKernel's nil guard, because decimalLitValue substituted the
// litText whenever the column was DECIMAL and litText != "", regardless of
// whether value itself was nil. NewKernelFilterLit("d", OpEq, nil, "null")
// then resolved a real (non-nil) kernel comparing against the garbage text
// "null", which parsed as zero and silently selected row 0 (0.00) instead of
// matching nothing.
func TestKernelFilterLitNilDecimalMatchesNothing(t *testing.T) {
	f := NewKernelFilterLit("d", OpEq, nil, "null")
	out, err := f.Execute(context.Background(), decimalGuardBatch(t))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := selOf(out); len(got) != 0 {
		t.Fatalf("Sel = %v, want no rows (a NULL-literal comparison is UNKNOWN for every row)", got)
	}
}

// TestKernelFilterLitNilNonDecimalMatchesNothing pins the same guard on a
// type decimalLitValue passes straight through: nil must stay nil (and thus
// keep hitting ResolveFilterKernel's own nil guard) rather than depending on
// this function's typ/litText branch.
func TestKernelFilterLitNilNonDecimalMatchesNothing(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "n", Type: parquet.TypeInt64},
	}, 2)
	copy(b.Columns[0].Int64Data, []int64{0, 7})
	f := NewKernelFilterLit("n", OpEq, nil, "")
	out, err := f.Execute(context.Background(), b)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := selOf(out); len(got) != 0 {
		t.Fatalf("Sel = %v, want no rows", got)
	}
}

// TestColumnCompareLitNilMatchesNothing is the same class of bug in the
// row-at-a-time evaluator: ColumnCompareLit computed strVal, intVal, floatVal
// etc. from `value` unconditionally, so a nil value read as
// fmt.Sprint(nil)="<nil>" / toInt64(nil)=0 / toBool(nil)=false and could
// match a real row instead of none. Exercised over several column types
// since each type reads a different one of those hoisted conversions.
func TestColumnCompareLitNilMatchesNothing(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "i", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
		{Name: "bl", Type: parquet.TypeBool},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2},
	}, 2)
	copy(b.Columns[0].Int64Data, []int64{0, 7})
	b.Columns[1].BytesData.Set(0, []byte(""))
	b.Columns[1].BytesData.Set(1, []byte("x"))
	b.Columns[2].BoolData[0] = false
	b.Columns[2].BoolData[1] = true
	b.Columns[3].DecimalData.Data[0] = batch.Int128From(0)
	b.Columns[3].DecimalData.Data[1] = batch.Int128From(500)

	cases := []struct {
		name string
		col  string
		op   CompareOp
	}{
		{"int64 eq nil", "i", OpEq},
		{"string eq nil", "s", OpEq},
		{"decimal eq nil", "d", OpEq},
		// TypeBool's case does `value.(bool)` with no comma-ok: a nil value
		// reaching it (pre-fix) would PANIC on the type assertion rather
		// than merely answering wrong.
		{"bool eq nil", "bl", OpEq},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			pred := ColumnCompareLit(tc.col, tc.op, nil, "")
			for row := 0; row < b.Len; row++ {
				if pred(b, row) {
					t.Fatalf("row %d matched a nil literal comparison; want no row to match", row)
				}
			}
		})
	}
}
