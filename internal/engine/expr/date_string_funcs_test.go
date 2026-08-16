package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Issue #273 regression: string functions over a TypeDate column must see
// the canonical ISO rendering (batch.FormatDate), not the epoch-day
// digits. At SF100 (date32 parquet) SUBSTR(o_orderdate,1,4) grouped
// day-granular — Q09 returned 50,250 rows (= 25 nations × 2,010 distinct
// 4-digit epoch-day prefixes) instead of the canonical 175. String-date
// test data never exercised the path, which is why every local gate
// passed; keep this test on a real Date-typed vector.
func dateBatch(t *testing.T, days ...int32) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{{Name: "d", Type: parquet.TypeDate}}, len(days))
	for i, d := range days {
		b.Columns[0].Int32Data[i] = d
	}
	return b
}

func TestStringFuncsOverDateColumn(t *testing.T) {
	// 1994-03-15 = epoch day 8839; 1998-08-02 = 10440 (SF100 o_orderdate max).
	b := dateBatch(t, 8839, 10440)

	col := &ColRef{Name: "d"}
	cases := []struct {
		name string
		expr Expr
		want [2]any
	}{
		{"substr_year", &FuncCall{Name: "substr", Args: []Expr{col, &Lit{Val: float64(1)}, &Lit{Val: float64(4)}}},
			[2]any{"1994", "1998"}},
		{"substring_alias", &FuncCall{Name: "SUBSTRING", Args: []Expr{col, &Lit{Val: float64(6)}, &Lit{Val: float64(2)}}},
			[2]any{"03", "08"}},
		{"upper_passthrough", &FuncCall{Name: "upper", Args: []Expr{col}},
			[2]any{"1994-03-15", "1998-08-02"}},
		{"length_iso", &FuncCall{Name: "length", Args: []Expr{col}},
			[2]any{int64(10), int64(10)}},
		{"concat", &FuncCall{Name: "concat", Args: []Expr{col, &Lit{Val: "!"}}},
			[2]any{"1994-03-15!", "1998-08-02!"}},
		{"cast_string", &FuncCall{Name: "cast_string", Args: []Expr{col}},
			[2]any{"1994-03-15", "1998-08-02"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for row := 0; row < 2; row++ {
				got := tc.expr.Eval(b, row)
				// length returns whatever integer width fnLength picks;
				// normalize for comparison.
				if wi, ok := tc.want[row].(int64); ok {
					if ToInt64(got) != wi {
						t.Errorf("row %d: got %v want %v", row, got, wi)
					}
					continue
				}
				if got != tc.want[row] {
					t.Errorf("row %d: got %v (%T) want %v", row, got, got, tc.want[row])
				}
			}
		})
	}
}

// TestStringFuncsOverDateColumn_Vec pins the vectorized entry point: a
// string kernel must not read a Date vector's (empty) BytesData — it
// routes through the fixed per-row path instead.
func TestStringFuncsOverDateColumn_Vec(t *testing.T) {
	b := dateBatch(t, 8839, 10440)
	fc := &FuncCall{Name: "substr", Args: []Expr{&ColRef{Name: "d"}, &Lit{Val: float64(1)}, &Lit{Val: float64(4)}}}
	out := batch.NewVector(batch.TypeString, 2)
	fc.EvalVec(b, out, 2)
	for i, want := range []string{"1994", "1998"} {
		got, _ := out.GetString(i)
		if got != want {
			t.Errorf("vec row %d: got %q want %q", i, got, want)
		}
	}
}

// Numeric and comparison contexts must keep seeing raw epoch days — the
// rendering applies ONLY at string-function boundaries.
func TestDateColumnNumericContextsUnchanged(t *testing.T) {
	b := dateBatch(t, 8839, 10440)
	col := &ColRef{Name: "d"}
	if v := col.Eval(b, 0); v != int64(8839) {
		t.Errorf("ColRef.Eval changed: got %v (%T) want int64(8839)", v, v)
	}
	if f, ok := col.EvalFloat64(b, 1); !ok || f != 10440 {
		t.Errorf("EvalFloat64 changed: got %v %v", f, ok)
	}
	// A non-string function over a date arg keeps numeric semantics.
	abs := &FuncCall{Name: "abs", Args: []Expr{col}}
	if got := ToFloat64(abs.Eval(b, 0)); got != 8839 {
		t.Errorf("abs over date: got %v want 8839", got)
	}
}
