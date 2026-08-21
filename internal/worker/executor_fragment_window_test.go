package worker

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestExecuteFragment_ScanWindowUnpartitioned runs the OpWindow breaker
// end to end: scan → window(ROW_NUMBER + SUM over a partition) →
// unpartitioned sink.
//
// Before #349 no such fragment could exist — there was no OpWindow, the
// coordinator built no window fragment, and every window stage reached the
// worker with Operators == nil and died in executeStage. This asserts the
// values, not just that the task returns: a window computed over the wrong
// grain still produces the right number of rows.
func TestExecuteFragment_ScanWindowUnpartitioned(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-fragment-window"

	// Two partitions by parity of id: odd ids {1,3,5} and even {2,4,6}.
	// Values are shuffled so a pass-through read order cannot pass.
	rows := [][2]int64{
		{1, 50}, {2, 10}, {3, 90}, {4, 30}, {5, 70}, {6, 20},
	}
	data := makeScanWshf(t, rows)

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	key := "in/window/t0.wshf"
	if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)

	task := distributed.Task{
		ID:           "frag-window-0",
		QueryID:      "q-frag-window",
		StageID:      "window-0",
		Type:         distributed.TaskTypeStage,
		StageType:    "window",
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/window/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpScan,
				InputAlias:  "t",
				InputFiles:  []string{key},
				InputBucket: bucket,
				Columns:     []string{"id", "val"},
			},
			{
				Type: distributed.OpWindow,
				WindowCols: []distributed.WindowColSpec{
					{
						Func:       "row_number",
						OutputCol:  "rn",
						OutputType: distributed.WindowTypePtr(int(parquet.TypeInt64)),
						OrderBy:    []distributed.SortKeySpec{{Column: "val", Desc: true}},
					},
					{
						Func:       "sum",
						InputCol:   "val",
						OutputCol:  "total",
						OutputType: distributed.WindowTypePtr(int(parquet.TypeFloat64)),
					},
				},
			},
			{Type: distributed.OpUnpartitionedSink},
		},
	}
	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != int64(len(rows)) {
		t.Fatalf("NumRows = %d, want %d — a window emits its input's rows, widened", result.NumRows, len(rows))
	}
	if len(result.ResultFiles) != 1 {
		t.Fatalf("expected 1 unpartitioned output, got %d", len(result.ResultFiles))
	}

	rc, _, err := store.Get(ctx, bucket, result.ResultFiles[0])
	if err != nil {
		t.Fatalf("get output: %v", err)
	}
	defer rc.Close()
	out, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	r, err := newShuffleChunkReader(out)
	if err != nil {
		t.Fatalf("parse output: %v", err)
	}
	// val → (rn, total), so the assertion does not depend on emission order.
	got := map[int64][2]float64{}
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatalf("chunk next: %v", err)
		}
		if b == nil {
			break
		}
		idx := map[string]int{}
		for i, c := range b.Schema {
			idx[c.Name] = i
		}
		for _, name := range []string{"val", "rn", "total"} {
			if _, ok := idx[name]; !ok {
				t.Fatalf("output schema missing %q: %+v", name, b.Schema)
			}
		}
		for i := 0; i < b.ActiveLen(); i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			got[b.Columns[idx["val"]].Int64Data[row]] = [2]float64{
				float64(b.Columns[idx["rn"]].Int64Data[row]),
				b.Columns[idx["total"]].Float64Data[row],
			}
		}
	}
	// ROW_NUMBER over val DESC: 90, 70, 50, 30, 20, 10.
	// SUM with no ORDER BY and no PARTITION BY is the whole-input total.
	const wantTotal = 50 + 10 + 90 + 30 + 70 + 20
	want := map[int64][2]float64{
		90: {1, wantTotal}, 70: {2, wantTotal}, 50: {3, wantTotal},
		30: {4, wantTotal}, 20: {5, wantTotal}, 10: {6, wantTotal},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d distinct vals, want %d: %v", len(got), len(want), got)
	}
	for val, w := range want {
		g, ok := got[val]
		if !ok {
			t.Fatalf("val %d missing from output: %v", val, got)
		}
		if g[0] != w[0] {
			t.Errorf("val %d: ROW_NUMBER = %v, want %v (full: %v)", val, g[0], w[0], got)
		}
		if g[1] != w[1] {
			t.Errorf("val %d: SUM() OVER () = %v, want %v", val, g[1], w[1])
		}
	}
}

// TestBuildFragmentWindow_Translation covers the wire→operator conversion:
// every field the planner resolved has to arrive, and a name the operator
// does not implement has to fail rather than compute something else.
func TestBuildFragmentWindow_Translation(t *testing.T) {
	e := NewExecutor(objstore.NewMemStore(), NewLRUCache(1024), nil)

	t.Run("full spec", func(t *testing.T) {
		nullsFirst := false
		win, err := e.buildFragmentWindow(context.Background(), distributed.OpSpec{
			Type: distributed.OpWindow,
			WindowCols: []distributed.WindowColSpec{{
				Func:        "lag",
				InputCol:    "n_name",
				OutputCol:   "prev",
				OutputType:  distributed.WindowTypePtr(int(parquet.TypeString)),
				PartitionBy: []string{"n_regionkey"},
				OrderBy: []distributed.SortKeySpec{
					{Column: "n_nationkey", Desc: true, NullsLast: &nullsFirst},
				},
				Frame: &distributed.WindowFrameSpec{
					Mode:  "rows",
					Start: distributed.WindowBoundSpec{Type: "preceding", Offset: 3},
					End:   distributed.WindowBoundSpec{Type: "current_row"},
				},
				LagLeadOffset:  2,
				LagLeadDefault: "none",
			}},
		})
		if err != nil {
			t.Fatalf("buildFragmentWindow: %v", err)
		}
		if len(win.Columns) != 1 {
			t.Fatalf("got %d columns, want 1", len(win.Columns))
		}
		c := win.Columns[0]
		if c.Func != exec.WinLag {
			t.Errorf("Func = %v, want WinLag", c.Func)
		}
		if c.InputCol != "n_name" || c.OutputCol != "prev" {
			t.Errorf("columns = (%q → %q), want (n_name → prev)", c.InputCol, c.OutputCol)
		}
		// The declared type is the whole of #345: an operator that
		// allocates a Float64 vector drops every string write in silence.
		if c.OutputType != parquet.TypeString {
			t.Errorf("OutputType = %v, want String", c.OutputType)
		}
		if len(c.PartitionBy) != 1 || c.PartitionBy[0] != "n_regionkey" {
			t.Errorf("PartitionBy = %v, want [n_regionkey]", c.PartitionBy)
		}
		if len(c.OrderBy) != 1 || c.OrderBy[0].Column != "n_nationkey" ||
			c.OrderBy[0].Order != exec.Descending || c.OrderBy[0].NullsLast {
			t.Errorf("OrderBy = %+v, want n_nationkey DESC NULLS FIRST", c.OrderBy)
		}
		if c.Frame == nil || c.Frame.Mode != "rows" ||
			c.Frame.Start.Type != "preceding" || c.Frame.Start.Offset != 3 ||
			c.Frame.End.Type != "current_row" {
			t.Errorf("Frame = %+v, want rows 3 PRECEDING → CURRENT ROW", c.Frame)
		}
		if c.LagLeadOffset != 2 {
			t.Errorf("LagLeadOffset = %d, want 2", c.LagLeadOffset)
		}
		if c.LagLeadDefault != "none" {
			t.Errorf("LagLeadDefault = %v, want \"none\"", c.LagLeadDefault)
		}
	})

	t.Run("undeclared type keeps the conservative float64", func(t *testing.T) {
		win, err := e.buildFragmentWindow(context.Background(), distributed.OpSpec{
			Type:       distributed.OpWindow,
			WindowCols: []distributed.WindowColSpec{{Func: "row_number", OutputCol: "rn"}},
		})
		if err != nil {
			t.Fatalf("buildFragmentWindow: %v", err)
		}
		// Nil means "no coordinator declaration" — an older coordinator, or
		// a type the planner declined. float64 is what the planner's own
		// fallback declares.
		if win.Columns[0].OutputType != parquet.TypeFloat64 {
			t.Errorf("OutputType = %v, want Float64 for an undeclared type", win.Columns[0].OutputType)
		}
	})

	t.Run("a declared BOOL is not read as undeclared", func(t *testing.T) {
		// parquet.TypeID's zero value is BOOL, so an int OutputType could
		// not tell LAG(bool_col) from a spec that declares nothing — and
		// the operator would allocate a Float64 vector and drop every
		// write, which is #345 for exactly one type.
		win, err := e.buildFragmentWindow(context.Background(), distributed.OpSpec{
			Type: distributed.OpWindow,
			WindowCols: []distributed.WindowColSpec{{
				Func:       "lag",
				InputCol:   "flag",
				OutputCol:  "prev_flag",
				OutputType: distributed.WindowTypePtr(int(parquet.TypeBool)),
			}},
		})
		if err != nil {
			t.Fatalf("buildFragmentWindow: %v", err)
		}
		if win.Columns[0].OutputType != parquet.TypeBool {
			t.Errorf("OutputType = %v, want Bool", win.Columns[0].OutputType)
		}
	})

	t.Run("unknown function fails", func(t *testing.T) {
		_, err := e.buildFragmentWindow(context.Background(), distributed.OpSpec{
			Type:       distributed.OpWindow,
			WindowCols: []distributed.WindowColSpec{{Func: "median_over_the_moon", OutputCol: "x"}},
		})
		if err == nil {
			t.Fatal("expected an error for an unimplemented window function; " +
				"exec.ParseWindowFunc's fallback is ROW_NUMBER, which would answer the wrong question silently")
		}
	})

	t.Run("no columns fails", func(t *testing.T) {
		if _, err := e.buildFragmentWindow(context.Background(), distributed.OpSpec{Type: distributed.OpWindow}); err == nil {
			t.Fatal("expected an error for an OpWindow carrying no columns")
		}
	})
}
