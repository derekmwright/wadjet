package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The value functions — LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE — copy a
// value out of their input column instead of computing one, so their output
// type IS that column's type. Window allocates batch.NewVector(OutputType) and
// writes through Vector.SetValue, which DROPS a value the vector cannot hold:
// a string written into a Float64 vector leaves the slot at 0 and marks it
// valid, so the row renders as the integer 0 rather than as an error or a NULL.
//
// #345 is that drop reached from SQL — the planner declared all five float64
// from a hand-maintained name list. windowSpecOutputType now resolves them from
// the catalog, and Window re-types from the input vector as well (the way
// Project resolves a projection's type from its input batch), so a spec that
// arrives wrong is corrected instead of silently corrupting the column.
//
// This table feeds Window the WRONG declaration on purpose: it is the exec
// half of the fix, and it is what covers any caller whose spec carries no
// resolvable type at all.
func TestWindowValueFunctionsRetypeFromInput(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
		{Name: "d", Type: parquet.TypeDate},
		{Name: "i", Type: parquet.TypeInt32},
	}
	rows := []map[string]any{
		{"k": int64(1), "s": "alpha", "d": int32(19000), "i": int32(10)},
		{"k": int64(2), "s": "bravo", "d": int32(19001), "i": int32(20)},
		{"k": int64(3), "s": "charlie", "d": int32(19002), "i": int32(30)},
	}
	order := []SortKey{{Column: "k", Order: Ascending}}

	tests := []struct {
		name string
		col  WindowColumn
		want []any // one per row, in k order; nil means NULL
	}{
		{
			name: "lag over a string column",
			col:  WindowColumn{Func: WinLag, InputCol: "s", OutputCol: "w", OutputType: parquet.TypeFloat64, OrderBy: order},
			want: []any{nil, "alpha", "bravo"},
		},
		{
			name: "lead over a string column",
			col:  WindowColumn{Func: WinLead, InputCol: "s", OutputCol: "w", OutputType: parquet.TypeFloat64, OrderBy: order},
			want: []any{"bravo", "charlie", nil},
		},
		{
			name: "first_value over a string column",
			col:  WindowColumn{Func: WinFirstValue, InputCol: "s", OutputCol: "w", OutputType: parquet.TypeFloat64, OrderBy: order},
			want: []any{"alpha", "alpha", "alpha"},
		},
		{
			name: "last_value over a string column",
			col:  WindowColumn{Func: WinLastValue, InputCol: "s", OutputCol: "w", OutputType: parquet.TypeFloat64, OrderBy: order},
			want: []any{"alpha", "bravo", "charlie"},
		},
		{
			// NTH_VALUE over the whole partition: an explicit frame is what
			// makes the answer constant, and the frame is where window bugs
			// hide, so it is spelled out rather than left to the default.
			name: "nth_value over a string column, whole-partition frame",
			col: WindowColumn{Func: WinNthValue, InputCol: "s", OutputCol: "w", OutputType: parquet.TypeFloat64,
				OrderBy: order, NthValueN: 2,
				Frame: &WindowFrameSpec{Mode: "rows",
					Start: WindowBound{Type: "unbounded_preceding"},
					End:   WindowBound{Type: "unbounded_following"}}},
			want: []any{"bravo", "bravo", "bravo"},
		},
		{
			// A DATE column's rendering depends on the declared type just as
			// much as a string's: declared float64, the same drop turns every
			// date into 0.
			// (day 19000 of the epoch; a DATE vector reads back as its
			// rendered date, which is exactly what the declared type buys.)
			name: "first_value over a DATE column",
			col:  WindowColumn{Func: WinFirstValue, InputCol: "d", OutputCol: "w", OutputType: parquet.TypeFloat64, OrderBy: order},
			want: []any{"2022-01-08", "2022-01-08", "2022-01-08"},
		},
		{
			// The numeric control. Float64.SetValue has no int32 case either,
			// so a narrow int was dropped exactly like a string.
			name: "lag over an INT32 column",
			col:  WindowColumn{Func: WinLag, InputCol: "i", OutputCol: "w", OutputType: parquet.TypeFloat64, OrderBy: order},
			want: []any{nil, int32(10), int32(20)},
		},
		{
			// A declaration that is already right is left alone.
			name: "lag over an INT64 column declared correctly",
			col:  WindowColumn{Func: WinLag, InputCol: "k", OutputCol: "w", OutputType: parquet.TypeInt64, OrderBy: order},
			want: []any{nil, int64(1), int64(2)},
		},
		{
			// The rank family computes its answer and must NOT be re-typed:
			// row_number writes Int64Data directly, so an input-derived type
			// would panic rather than correct anything.
			name: "row_number is unaffected",
			col:  WindowColumn{Func: WinRowNumber, OutputCol: "w", OutputType: parquet.TypeInt64, OrderBy: order},
			want: []any{int64(1), int64(2), int64(3)},
		},
		{
			name: "rank is unaffected",
			col:  WindowColumn{Func: WinRank, OutputCol: "w", OutputType: parquet.TypeInt64, OrderBy: order},
			want: []any{int64(1), int64(2), int64(3)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			win := NewWindow([]WindowColumn{tc.col})
			pipe := &Pipeline{Source: NewSliceSource(schema, rows), Sink: win}
			ctx := context.Background()
			if err := pipe.Run(ctx); err != nil {
				t.Fatal(err)
			}
			b, err := win.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				t.Fatal("window produced no batch")
			}
			got := b.ToRows()
			if len(got) != len(tc.want) {
				t.Fatalf("got %d rows, want %d", len(got), len(tc.want))
			}
			byKey := make(map[int64]any, len(got))
			for _, r := range got {
				byKey[r["k"].(int64)] = r["w"]
			}
			for i, want := range tc.want {
				k := int64(i + 1)
				if byKey[k] != want {
					t.Errorf("k=%d: window column is %#v, want %#v", k, byKey[k], want)
				}
			}
		})
	}
}

// A NULL in a variable-length output column still owes its offset slot. The
// value functions used to mark the null in the bitmap and stop there, which
// was harmless while their output was always a Float64 vector — and became a
// silent corruption the moment the output followed the input's type: with
// Offsets[i+1] left at zero, the NEXT row reads back from the start of the
// arena and returns everything written so far, concatenated.
//
// Two partitions are what makes it visible. A single leading NULL at index 0
// is correct by accident (Offsets[0] is zero anyway); the second partition's
// leading NULL sits after real data, so every row behind it is wrong.
func TestWindowValueFunctionNullsAdvanceBytesOffsets(t *testing.T) {
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
	}
	rows := []map[string]any{
		{"g": int64(0), "k": int64(1), "s": "alpha"},
		{"g": int64(0), "k": int64(2), "s": "bravo"},
		{"g": int64(1), "k": int64(3), "s": "charlie"},
		{"g": int64(1), "k": int64(4), "s": "delta"},
		{"g": int64(2), "k": int64(5), "s": "echo"},
		{"g": int64(2), "k": int64(6), "s": "foxtrot"},
	}
	want := map[int64]any{1: nil, 2: "alpha", 3: nil, 4: "charlie", 5: nil, 6: "echo"}

	win := NewWindow([]WindowColumn{
		{Func: WinLag, InputCol: "s", OutputCol: "prev", OutputType: parquet.TypeString,
			PartitionBy: []string{"g"}, OrderBy: []SortKey{{Column: "k", Order: Ascending}}},
	})
	ctx := context.Background()
	if err := (&Pipeline{Source: NewSliceSource(schema, rows), Sink: win}).Run(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := win.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b == nil {
		t.Fatal("window produced no batch")
	}
	for _, r := range b.ToRows() {
		k := r["k"].(int64)
		if r["prev"] != want[k] {
			t.Errorf("k=%d: LAG = %#v, want %#v — a NULL left its offset slot unwritten, so this row "+
				"read back from the start of the bytes arena", k, r["prev"], want[k])
		}
	}
}

// The spill path builds its output vectors from the same declaration, through
// a different route (sorted columnar runs, merged one partition at a time), so
// the re-typing has to hold there too — otherwise the same query answers
// correctly in memory and in zeros once it spills.
//
// runWindowBothPaths only proves the two paths AGREE, which a wrong
// declaration satisfies by making both wrong, so the values are asserted
// outright here.
func TestWindowValueFunctionsRetypeUnderSpill(t *testing.T) {
	forceTinyRuns(t)
	schema := []parquet.Column{
		{Name: "grp", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
	}
	const n = 300
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"grp": int64(i % 3), "k": int64(i), "s": fmt.Sprintf("s%03d", i)}
	}

	// Declared float64 on purpose: the whole point is that the operator no
	// longer takes the declaration's word for it.
	w := newWindowSpillHarness(t, []WindowColumn{
		{Func: WinLag, InputCol: "s", OutputCol: "prev", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"grp"}, OrderBy: []SortKey{{Column: "k", Order: Ascending}}},
	}, 512)

	ctx := context.Background()
	for i := 0; i < n; i += 16 {
		end := i + 16
		if end > n {
			end = n
		}
		if err := w.Consume(ctx, batch.FromRows(schema, rows[i:end])); err != nil {
			t.Fatal(err)
		}
	}
	if len(w.runFiles) == 0 {
		t.Fatal("window run-spill path was never exercised; budget/floor setup is wrong")
	}
	if err := w.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	got := drainWindowRows(t, w)
	if len(got) != n {
		t.Fatalf("got %d rows, want %d", len(got), n)
	}
	prevByKey := make(map[int64]any, len(got))
	for _, r := range got {
		prevByKey[r["k"].(int64)] = r["prev"]
	}
	for i := 0; i < n; i++ {
		var want any
		if i >= 3 { // three partitions, so the previous row of grp is k-3
			want = fmt.Sprintf("s%03d", i-3)
		}
		if prevByKey[int64(i)] != want {
			t.Fatalf("k=%d: LAG is %#v, want %#v — a spilled string window column came back numeric", i, prevByKey[int64(i)], want)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}
