package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #406 on the operator's three execution paths. A window value function
// declares its output with a bare TypeID, so the output vector for a
// container had no Child/Children/Dimension and for a DECIMAL no scale —
// and Vector.SetValue returns early on exactly those, over a null mask that
// was pre-set all-null. Every write was dropped in silence.
//
// The in-memory arm asserts ABSOLUTE values, because the spill arm's
// reference is the in-memory arm: two paths that drop the same writes agree
// on NULL, which is how the whole defect stayed invisible.

func wvcSchema() []parquet.Column {
	return []parquet.Column{
		{Name: "grp", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		{Name: "rec", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeString, Nullable: true},
			{Name: "b", Type: parquet.TypeInt64, Nullable: true},
		}},
		{Name: "mp", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64, Nullable: true},
			}}},
		{Name: "vec", Type: parquet.TypeVector, Nullable: true, Dimension: 3},
		{Name: "dec", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
	}
}

func wvcCols(partitionBy []string) []WindowColumn {
	order := []SortKey{{Column: "ts", Order: Ascending}}
	var out []WindowColumn
	for _, in := range []string{"arr", "rec", "mp", "vec", "dec"} {
		out = append(out, WindowColumn{
			Func: WinLastValue, InputCol: in, OutputCol: "l_" + in,
			OutputType:  parquet.TypeFloat64,
			PartitionBy: partitionBy,
			OrderBy:     order,
		})
		out = append(out, WindowColumn{
			Func: WinFirstValue, InputCol: in, OutputCol: "f_" + in,
			// The planner's declaration for a parameterized type is
			// FLOAT64 (physical.windowSpecOutputType declines them), so
			// this is what actually reaches exec — retypeValueColumns is
			// what corrects the TypeID, and windowOutputColumn is what
			// now supplies the parameterisation with it.
			OutputType:  parquet.TypeFloat64,
			PartitionBy: partitionBy,
			OrderBy:     order,
		})
	}
	return out
}

// wvcRows: 120 rows over 4 partitions. Row ts=i belongs to partition i%4, so
// the FIRST row of partition p is ts=p and its values are known.
func wvcRows(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]any{
			"grp": int64(i % 4),
			"ts":  int64(i),
			"arr": []any{fmt.Sprintf("a%03d", i), "tail"},
			"rec": map[string]any{"a": fmt.Sprintf("r%03d", i), "b": int64(i) * 7},
			"mp":  map[string]any{"k": int64(i)},
			"vec": []float32{float32(i), float32(i) + 0.5, -1},
			"dec": fmt.Sprintf("%d.0003", i),
		}
	}
	return rows
}

// wvcRowsWithNulls nulls every container on row 9 and beyond in a stride, so
// LAST_VALUE (whose default frame ends at the current row) has to emit a NULL
// container — the case where the difference between "this row is NULL" and
// "this row is a ROW of NULL fields" becomes visible.
func wvcRowsWithNulls(n int) []map[string]any {
	rows := wvcRows(n)
	for i := 9; i < n; i += 9 {
		for _, c := range []string{"arr", "rec", "mp", "vec", "dec"} {
			rows[i][c] = nil
		}
	}
	return rows
}

// TestWindowValueContainersNullRow: a NULL container must come back NULL, not
// as an empty container or a ROW of NULL fields, on every path.
func TestWindowValueContainersNullRow(t *testing.T) {
	rows := wvcRowsWithNulls(120)
	out := wvcRunInMemory(t, wvcCols([]string{"grp"}), rows)
	byTS := map[int64]map[string]any{}
	for _, r := range out {
		byTS[r["ts"].(int64)] = r
	}
	for i := 9; i < 120; i += 9 {
		r := byTS[int64(i)]
		for _, in := range []string{"arr", "rec", "mp", "vec", "dec"} {
			if got := r["l_"+in]; got != nil {
				t.Errorf("LAST_VALUE(%s) at a NULL row (ts=%d) = %#v, want nil", in, i, got)
			}
		}
	}
	compare := []string{"grp", "ts", "l_arr", "l_rec", "l_mp", "l_vec", "l_dec"}
	runWindowBothPaths(t, wvcSchema(), wvcCols([]string{"grp"}), wvcRowsWithNulls(180), 16, compare)
}

func wvcRunInMemory(t *testing.T, cols []WindowColumn, rows []map[string]any) []map[string]any {
	t.Helper()
	ctx := context.Background()
	w := NewWindow(cols)
	if err := w.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	schema := wvcSchema()
	const per = 16
	for i := 0; i < len(rows); i += per {
		end := i + per
		if end > len(rows) {
			end = len(rows)
		}
		if err := w.Consume(ctx, batch.FromRows(schema, rows[i:end])); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	return drainWindowRows(t, w)
}

// wvcAssertFirstValues checks every output row against the value of its
// partition's first row, computed here rather than read off another path.
func wvcAssertFirstValues(t *testing.T, out []map[string]any, rows []map[string]any, partitioned bool) {
	t.Helper()
	// first[partition] = the input row with the least ts in that partition.
	first := map[int64]map[string]any{}
	for _, r := range rows {
		p := int64(0)
		if partitioned {
			p = r["grp"].(int64)
		}
		cur, ok := first[p]
		if !ok || r["ts"].(int64) < cur["ts"].(int64) {
			first[p] = r
		}
	}
	if len(out) != len(rows) {
		t.Fatalf("window emitted %d rows, want %d", len(out), len(rows))
	}
	for _, r := range out {
		p := int64(0)
		if partitioned {
			p = r["grp"].(int64)
		}
		src := first[p]
		// Rendered comparison: the boxed forms are []any / map[string]any /
		// []float32 / a formatted decimal string, and what matters is that
		// the value ARRIVED and arrived intact.
		for _, in := range []string{"arr", "rec", "mp", "vec", "dec"} {
			got := r["f_"+in]
			if got == nil {
				t.Fatalf("FIRST_VALUE(%s) is NULL at ts=%v — the output vector could not hold the value",
					in, r["ts"])
			}
			// The reference is the input value read back through a
			// projection of the same batch, so a DECIMAL renders the same
			// way on both sides.
			wantRow := batch.FromRows(wvcSchema(), []map[string]any{src}).ToRows()[0]
			if fmt.Sprint(got) != fmt.Sprint(wantRow[in]) {
				t.Errorf("FIRST_VALUE(%s) at ts=%v = %v, want %v", in, r["ts"], got, wantRow[in])
			}
		}
	}
}

// TestWindowValueContainersInMemory: the columnar in-memory path.
func TestWindowValueContainersInMemory(t *testing.T) {
	rows := wvcRows(120)
	out := wvcRunInMemory(t, wvcCols([]string{"grp"}), rows)
	wvcAssertFirstValues(t, out, rows, true)
}

// TestWindowValueContainersGlobal: the empty-PARTITION-BY path, which has its
// own output-vector construction (window_global.go) and its own writes.
func TestWindowValueContainersGlobal(t *testing.T) {
	rows := wvcRows(120)
	out := wvcRunInMemory(t, wvcCols(nil), rows)
	wvcAssertFirstValues(t, out, rows, false)
}

// TestWindowValueContainersExternal: the spilling paths must agree with the
// in-memory one, which the assertions above have already pinned to the right
// values — so agreement here is agreement on a CHECKED answer.
func TestWindowValueContainersExternal(t *testing.T) {
	compare := []string{"grp", "ts", "f_arr", "f_rec", "f_mp", "f_vec", "f_dec"}
	t.Run("partitioned", func(t *testing.T) {
		runWindowBothPaths(t, wvcSchema(), wvcCols([]string{"grp"}), wvcRows(180), 16, compare)
	})
	t.Run("global", func(t *testing.T) {
		runWindowBothPaths(t, wvcSchema(), wvcCols(nil), wvcRows(180), 16, compare)
	})
}
