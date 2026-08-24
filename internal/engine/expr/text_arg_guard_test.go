package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// mixedTextBatch is #509's table: one TEXT column, one BIGINT column.
func mixedTextBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "c0", Type: parquet.TypeString},
		{Name: "c1", Type: parquet.TypeInt64},
	}, 2)
	b.Columns[0].BytesData.Set(0, []byte("x"))
	b.Columns[0].BytesData.Set(1, []byte("9"))
	b.Columns[1].Int64Data[0] = 5
	b.Columns[1].Int64Data[1] = 7
	return b
}

// TestVecTextKernelsGuardEveryTextArgument is #509's regression.
//
// A vec kernel that reads an argument as text indexes that vector's offsets
// array directly. A BIGINT column has none — a zero-length slice — so the
// kernel walked off the end and took the whole server process down, not just
// the query. Before the fix only ARGUMENT 0 was checked, which covers
// UPPER(int_col) and misses every function that reads a later argument as
// text: concat (all of them), replace (three), starts_with / ends_with /
// contains (two).
//
// The right answer is PostgreSQL's: stringify the argument. The per-row path
// already does that correctly; the fix is that the vec dispatch now routes
// there instead of handing the kernel a vector it cannot read.
func TestVecTextKernelsGuardEveryTextArgument(t *testing.T) {
	text := &ColRef{Name: "c0"}
	num := &ColRef{Name: "c1"}

	cases := []struct {
		name    string
		call    *FuncCall
		outType batch.TypeID
		want    []any
	}{
		{
			// The filed repro: SELECT CONCAT(t0.c0, t0.c1) FROM t0.
			name:    "concat_text_then_int",
			call:    &FuncCall{Name: "concat", Args: []Expr{text, num}},
			outType: batch.TypeString,
			want:    []any{"x5", "97"},
		},
		{
			name:    "concat_int_then_text",
			call:    &FuncCall{Name: "concat", Args: []Expr{num, text}},
			outType: batch.TypeString,
			want:    []any{"5x", "79"},
		},
		{
			name:    "concat_int_only",
			call:    &FuncCall{Name: "concat", Args: []Expr{num, num}},
			outType: batch.TypeString,
			want:    []any{"55", "77"},
		},
		{
			name:    "replace_int_needle",
			call:    &FuncCall{Name: "replace", Args: []Expr{text, num, &Lit{Val: "Z"}}},
			outType: batch.TypeString,
			want:    []any{"x", "9"},
		},
		{
			name:    "starts_with_int_prefix",
			call:    &FuncCall{Name: "starts_with", Args: []Expr{text, num}},
			outType: batch.TypeBool,
			want:    []any{false, false},
		},
		{
			name:    "ends_with_int_suffix",
			call:    &FuncCall{Name: "ends_with", Args: []Expr{text, num}},
			outType: batch.TypeBool,
			want:    []any{false, false},
		},
		{
			name:    "contains_int_needle",
			call:    &FuncCall{Name: "contains", Args: []Expr{text, num}},
			outType: batch.TypeBool,
			want:    []any{false, false},
		},
		{
			// Position 0 stayed guarded — the #273/UPPER(int_col) case.
			name:    "upper_int",
			call:    &FuncCall{Name: "upper", Args: []Expr{num}},
			outType: batch.TypeString,
			want:    []any{"5", "7"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := mixedTextBatch(t)
			out := batch.NewVector(tc.outType, 2)
			tc.call.EvalVec(b, out, 2)
			for i, want := range tc.want {
				if got := out.GetValue(i); got != want {
					t.Errorf("row %d: got %v (%T), want %v", i, got, got, want)
				}
			}
		})
	}
}

// TestVecTypedArgumentsKeepTheVecPath is the other half of the split: a
// function whose trailing arguments are integers BY DESIGN must not lose the
// vec kernel to them. Guarding every position unconditionally would have
// pushed SUBSTR(s, 1, 4) — TPC-H's date-prefix shape — onto the per-row path.
func TestVecTypedArgumentsKeepTheVecPath(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "s", Type: parquet.TypeString},
		{Name: "n", Type: parquet.TypeInt64},
	}, 2)
	b.Columns[0].BytesData.Set(0, []byte("1994-03-15"))
	b.Columns[0].BytesData.Set(1, []byte("1998-08-02"))
	b.Columns[1].Int64Data[0] = 4
	b.Columns[1].Int64Data[1] = 4

	// An INTEGER COLUMN in a typed position: the guard must not reject it,
	// and the answer must still be right.
	call := &FuncCall{Name: "substr", Args: []Expr{
		&ColRef{Name: "s"}, &Lit{Val: float64(1)}, &ColRef{Name: "n"},
	}}
	out := batch.NewVector(batch.TypeString, 2)
	call.EvalVec(b, out, 2)
	for i, want := range []string{"1994", "1998"} {
		if got, _ := out.GetString(i); got != want {
			t.Errorf("row %d: got %q, want %q", i, got, want)
		}
	}
	if call.vecTypedArgs == nil || !call.vecTypedArgs[1] || !call.vecTypedArgs[2] {
		t.Errorf("substr typed positions = %v, want {1,2}", call.vecTypedArgs)
	}
}

// TestTypedArgPositionsOnlyNamesStringInputFuncs keeps the two tables in
// step: a typed-position entry for a function the vec guard never consults
// exempts nothing and is a silent lie about the function's shape.
func TestTypedArgPositionsOnlyNamesStringInputFuncs(t *testing.T) {
	for name := range typedArgPositions {
		if !stringInputFuncs[name] {
			t.Errorf("typedArgPositions names %q, which is not a stringInputFuncs entry", name)
		}
	}
}
