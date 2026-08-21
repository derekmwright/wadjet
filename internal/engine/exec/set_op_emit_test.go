package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// setOpEmitInput builds the shape SetOpEmit consumes: one row per distinct
// result row with its per-arm counts. v is the result value, l/r the counts.
func setOpEmitInput(vals []int64, ls, rs []int64) *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "v", Type: parquet.TypeInt64},
		{Name: "__l", Type: parquet.TypeInt64},
		{Name: "__r", Type: parquet.TypeInt64},
	}
	rows := make([]map[string]any, len(vals))
	for i := range vals {
		rows[i] = map[string]any{"v": vals[i], "__l": ls[i], "__r": rs[i]}
	}
	return batch.FromRows(schema, rows)
}

// drainSetOpEmit collects the emitted v values, expanding the selection
// vector if the operator set one.
func drainSetOpEmit(t *testing.T, out *batch.RecordBatch) []int64 {
	t.Helper()
	if out == nil {
		return nil
	}
	if len(out.Schema) != 1 || out.Schema[0].Name != "v" {
		t.Fatalf("output schema %v — the count columns must be dropped and the result column kept", out.Schema)
	}
	var got []int64
	for i := 0; i < out.ActiveLen(); i++ {
		row := i
		if out.Sel != nil {
			row = int(out.Sel[i])
		}
		x, ok := out.Columns[0].GetInt64(row)
		if !ok {
			t.Fatalf("row %d of the output is NULL", i)
		}
		got = append(got, x)
	}
	return got
}

// TestSetOpEmitCountRules pins the four operations' count rules on one
// input: rows with every relevant (countA, countB) shape.
func TestSetOpEmitCountRules(t *testing.T) {
	// v:      1  2  3  4  5
	// countA: 2  3  1  0  2
	// countB: 1  3  0  2  5
	vals := []int64{1, 2, 3, 4, 5}
	ls := []int64{2, 3, 1, 0, 2}
	rs := []int64{1, 3, 0, 2, 5}

	cases := []struct {
		name string
		op   string
		all  bool
		want []int64
	}{
		// Distinct rows in both arms, once each.
		{name: "Intersect", op: "intersect", want: []int64{1, 2, 5}},
		// min(countA, countB) copies.
		{name: "IntersectAll", op: "intersect", all: true, want: []int64{1, 2, 2, 2, 5, 5}},
		// Distinct rows of A absent from B.
		{name: "Except", op: "except", want: []int64{3}},
		// max(0, countA−countB) copies — row 1 survives once (2−1), row 3
		// once (1−0), rows 2/4/5 not at all.
		{name: "ExceptAll", op: "except", all: true, want: []int64{1, 3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			emit, err := NewSetOpEmit(tc.op, tc.all, "__l", "__r")
			if err != nil {
				t.Fatal(err)
			}
			out, err := emit.Execute(context.Background(), setOpEmitInput(vals, ls, rs))
			if err != nil {
				t.Fatal(err)
			}
			got := drainSetOpEmit(t, out)
			if len(got) != len(tc.want) {
				t.Fatalf("emitted %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("emitted %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestSetOpEmitDistinctIsSelection: the distinct forms must not copy rows —
// the output shares the input's vectors under a selection vector, minus the
// count columns.
func TestSetOpEmitDistinctIsSelection(t *testing.T) {
	in := setOpEmitInput([]int64{1, 2}, []int64{1, 0}, []int64{1, 1})
	emit, err := NewSetOpEmit("intersect", false, "__l", "__r")
	if err != nil {
		t.Fatal(err)
	}
	out, err := emit.Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if out.Sel == nil {
		t.Fatal("distinct form materialized instead of selecting")
	}
	if out.Columns[0] != in.Columns[0] {
		t.Error("result column was copied; the distinct form must share the input vector")
	}
}

// TestSetOpEmitEmpty: a fully filtered batch answers nil (the pipeline's
// "nothing survived" convention), and a nil input passes through.
func TestSetOpEmitEmpty(t *testing.T) {
	emit, err := NewSetOpEmit("except", false, "__l", "__r")
	if err != nil {
		t.Fatal(err)
	}
	// Every row of A also appears in B → EXCEPT emits nothing.
	out, err := emit.Execute(context.Background(), setOpEmitInput([]int64{1}, []int64{3}, []int64{1}))
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		t.Fatalf("want nil (fully filtered), got %d rows", out.ActiveLen())
	}
	if out, err := emit.Execute(context.Background(), nil); err != nil || out != nil {
		t.Fatalf("nil input: got (%v, %v), want (nil, nil)", out, err)
	}
}

// TestSetOpEmitSelInput: an input batch already carrying a selection vector
// (a defensive shape — the aggregate drain does not produce one today) is
// read through it.
func TestSetOpEmitSelInput(t *testing.T) {
	in := setOpEmitInput([]int64{1, 2, 3}, []int64{1, 1, 1}, []int64{1, 1, 1})
	in.Sel = []uint32{0, 2} // row 1 is inactive
	emit, err := NewSetOpEmit("intersect", true, "__l", "__r")
	if err != nil {
		t.Fatal(err)
	}
	out, err := emit.Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	got := drainSetOpEmit(t, out)
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("emitted %v, want [1 3] — the inactive row leaked through", got)
	}
}

// TestSetOpEmitNullValues: a NULL result value is a row like any other by
// the time it reaches this operator (the upstream GROUP BY matched NULLs);
// the emit must carry it through, not drop or de-NULL it.
func TestSetOpEmitNullValues(t *testing.T) {
	schema := []parquet.Column{
		{Name: "v", Type: parquet.TypeInt64},
		{Name: "__l", Type: parquet.TypeInt64},
		{Name: "__r", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"v": nil, "__l": int64(2), "__r": int64(1)},
		{"v": int64(7), "__l": int64(1), "__r": int64(1)},
	}
	in := batch.FromRows(schema, rows)

	emit, err := NewSetOpEmit("intersect", true, "__l", "__r")
	if err != nil {
		t.Fatal(err)
	}
	out, err := emit.Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if out.ActiveLen() != 2 {
		t.Fatalf("emitted %d rows, want 2 (NULL once — min(2,1) — and 7 once)", out.ActiveLen())
	}
	if !out.Columns[0].Nulls.IsNull(0) {
		t.Error("the NULL row lost its NULL")
	}
	if x, _ := out.Columns[0].GetInt64(1); x != 7 {
		t.Errorf("row 1 = %d, want 7", x)
	}
}

// TestSetOpEmitMissingCountColumns: an input without the declared count
// columns is a wiring bug and must fail loudly, not emit garbage.
func TestSetOpEmitMissingCountColumns(t *testing.T) {
	schema := []parquet.Column{{Name: "v", Type: parquet.TypeInt64}}
	in := batch.FromRows(schema, []map[string]any{{"v": int64(1)}})
	emit, err := NewSetOpEmit("intersect", false, "__l", "__r")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := emit.Execute(context.Background(), in); err == nil {
		t.Fatal("missing count columns accepted")
	}
}

// TestSetOpEmitInvalidSpec: unknown operations and degenerate column specs
// are refused at construction.
func TestSetOpEmitInvalidSpec(t *testing.T) {
	if _, err := NewSetOpEmit("union", false, "__l", "__r"); err == nil {
		t.Error("op union accepted — SetOpEmit answers intersect/except only")
	}
	if _, err := NewSetOpEmit("intersect", false, "__l", "__l"); err == nil {
		t.Error("identical count columns accepted")
	}
	if _, err := NewSetOpEmit("intersect", false, "", "__r"); err == nil {
		t.Error("empty count column accepted")
	}
}
