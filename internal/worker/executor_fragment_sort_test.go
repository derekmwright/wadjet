package worker

import (
	"bytes"
	"context"
	"io"
	"sort"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// TestExecuteFragment_ScanSortUnpartitioned exercises the OpSort breaker
// with a 3-op fragment: scan → sort (descending by val, limit=3) →
// unpartitioned sink. Verifies the breaker chain materialises the input,
// sorts in the expected order, and Truncate fires when SortLimit is set.
func TestExecuteFragment_ScanSortUnpartitioned(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-fragment-sort"

	// 10 rows with shuffled values so a non-stable read order can't pass.
	rows := [][2]int64{
		{1, 50}, {2, 10}, {3, 90}, {4, 30}, {5, 70},
		{6, 20}, {7, 80}, {8, 40}, {9, 60}, {10, 100},
	}
	data := makeScanWshf(t, rows)

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	key := "in/sort/t0.wshf"
	if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)

	task := distributed.Task{
		ID:           "frag-sort-0",
		QueryID:      "q-frag-sort",
		StageID:      "sort-0",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/sort/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpScan,
				InputAlias:  "t",
				InputFiles:  []string{key},
				InputBucket: bucket,
				Columns:     []string{"id", "val"},
			},
			{
				Type: distributed.OpSort,
				SortKeySpecs: []distributed.SortKeySpec{
					{Column: "val", Desc: true},
				},
				SortLimit: 3,
			},
			{
				Type: distributed.OpUnpartitionedSink,
			},
		},
	}
	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != 3 {
		t.Fatalf("NumRows = %d, want 3 (limited)", result.NumRows)
	}
	if len(result.ResultFiles) != 1 {
		t.Fatalf("expected 1 unpartitioned output, got %d", len(result.ResultFiles))
	}

	// Decode output and verify the top-3 desc-by-val rows.
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
	var gotVals []int64
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatalf("chunk next: %v", err)
		}
		if b == nil {
			break
		}
		valIdx := -1
		for i, c := range b.Schema {
			if c.Name == "val" {
				valIdx = i
			}
		}
		if valIdx < 0 {
			t.Fatalf("output schema missing val: %+v", b.Schema)
		}
		for i := 0; i < b.ActiveLen(); i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			gotVals = append(gotVals, b.Columns[valIdx].Int64Data[row])
		}
	}
	wantTopThree := []int64{100, 90, 80}
	if len(gotVals) != len(wantTopThree) {
		t.Fatalf("got %d rows, want %d", len(gotVals), len(wantTopThree))
	}
	if !sort.SliceIsSorted(gotVals, func(i, j int) bool { return gotVals[i] > gotVals[j] }) {
		t.Errorf("output not sorted desc: %v", gotVals)
	}
	for i, want := range wantTopThree {
		if gotVals[i] != want {
			t.Errorf("row %d: got val=%d, want %d (full sequence: %v)", i, gotVals[i], want, gotVals)
		}
	}
}

// TestExecuteFragment_ThreeOps_AggFilterSort regresses the Sel-clobber bug
// between drain phases. The pipeline:
//
//	scan → HashAggregate(GROUP BY g) → Filter(total > 25) → Sort(total DESC)
//
// exec.Filter mutates input.Sel and reuses its internal selBuf across calls.
// Sort retains every batch it Consumes. Without snapshotting Sel between
// Filter and Sort.Consume, every previously-stored batch's Sel gets clobbered
// by the next Filter execution, so the final sort output reflects only the
// LAST batch's surviving rows — the SF0.1 Q11 row-set drift that bisected to
// drainThroughBreaker missing the Sel snapshot.
//
// Inputs: 4 groups with sums {30, 20, 80, 25}. Filter `total > 25` keeps
// {30, 80}. Sort DESC: [80, 30].
func TestExecuteFragment_ThreeOps_AggFilterSort(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-fragment-agg-filt-sort"

	data := makeGroupedWshf(t, [][2]int64{
		{1, 10}, {1, 20}, // g=1 → sum=30 (passes >25)
		{2, 5}, {2, 15}, // g=2 → sum=20 (rejected)
		{3, 50}, {3, 30}, // g=3 → sum=80 (passes)
		{4, 25}, // g=4 → sum=25 (rejected, not strictly greater)
	})

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	key := "in/agg-filt-sort/t0.wshf"
	if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)

	task := distributed.Task{
		ID:           "frag-agg-filt-sort-0",
		QueryID:      "q-frag-afs",
		StageID:      "afs-0",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/afs/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpScan,
				InputAlias:  "src",
				InputFiles:  []string{key},
				InputBucket: bucket,
				Columns:     []string{"g", "v"},
			},
			{
				Type:        distributed.OpHashAggregate,
				GroupByCols: []string{"g"},
				Aggregates: []distributed.AggSpec{
					{Func: "sum", InputCol: "v", OutputCol: "total"},
				},
			},
			{
				Type:       distributed.OpFilter,
				Predicates: []string{"total > 25"},
			},
			{
				Type: distributed.OpSort,
				SortKeySpecs: []distributed.SortKeySpec{
					{Column: "total", Desc: true},
				},
			},
			{
				Type: distributed.OpUnpartitionedSink,
			},
		},
	}
	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != 2 {
		t.Fatalf("NumRows = %d, want 2 (groups passing total>25)", result.NumRows)
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
	type pair struct {
		g     int64
		total float64
	}
	var got []pair
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatalf("chunk next: %v", err)
		}
		if b == nil {
			break
		}
		gIdx, totalIdx := -1, -1
		for i, c := range b.Schema {
			switch c.Name {
			case "g":
				gIdx = i
			case "total":
				totalIdx = i
			}
		}
		for i := 0; i < b.ActiveLen(); i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			got = append(got, pair{
				g:     b.Columns[gIdx].Int64Data[row],
				total: b.Columns[totalIdx].Float64Data[row],
			})
		}
	}
	want := []pair{{g: 3, total: 80}, {g: 1, total: 30}}
	if len(got) != len(want) {
		t.Fatalf("rows: got %d, want %d (got=%v)", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].g != w.g || got[i].total != w.total {
			t.Errorf("row %d: got (g=%d, total=%v), want (g=%d, total=%v); full got=%v", i, got[i].g, got[i].total, w.g, w.total, got)
		}
	}
}

// TestExecuteFragment_TwoBreakers_AggThenSort exercises the multi-breaker
// generalization: scan → hash_aggregate (GROUP BY g) → sort (DESC by total)
// → unpartitioned sink. Validates that runFragmentWithBreakers correctly
// chains two pipeline-breakers: aggregate's drain feeds sort's consume, and
// sort's drain feeds the terminal sink.
//
// No current dispatch path emits this shape, but the worker must support
// it ahead of any planner-level fusion that does (e.g. final_aggregate +
// sort folded into one stage).
func TestExecuteFragment_TwoBreakers_AggThenSort(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-fragment-agg-sort"

	// 4 groups with deliberately out-of-order values so the sort phase
	// has to actually re-order the aggregate's output.
	data := makeGroupedWshf(t, [][2]int64{
		{1, 10}, {1, 20}, // g=1 → sum=30
		{2, 5}, {2, 15}, // g=2 → sum=20
		{3, 50}, {3, 30}, // g=3 → sum=80
		{4, 25}, // g=4 → sum=25
	})

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	key := "in/agg-sort/t0.wshf"
	if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)

	task := distributed.Task{
		ID:           "frag-agg-sort-0",
		QueryID:      "q-frag-agg-sort",
		StageID:      "as-0",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/agg-sort/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpScan,
				InputAlias:  "src",
				InputFiles:  []string{key},
				InputBucket: bucket,
				Columns:     []string{"g", "v"},
			},
			{
				Type:        distributed.OpHashAggregate,
				GroupByCols: []string{"g"},
				Aggregates: []distributed.AggSpec{
					{Func: "sum", InputCol: "v", OutputCol: "total"},
				},
			},
			{
				Type: distributed.OpSort,
				SortKeySpecs: []distributed.SortKeySpec{
					{Column: "total", Desc: true},
				},
			},
			{
				Type: distributed.OpUnpartitionedSink,
			},
		},
	}

	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != 4 {
		t.Fatalf("NumRows = %d, want 4 (groups)", result.NumRows)
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
	type pair struct {
		g     int64
		total float64
	}
	var got []pair
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatalf("chunk next: %v", err)
		}
		if b == nil {
			break
		}
		gIdx, totalIdx := -1, -1
		for i, c := range b.Schema {
			switch c.Name {
			case "g":
				gIdx = i
			case "total":
				totalIdx = i
			}
		}
		if gIdx < 0 || totalIdx < 0 {
			t.Fatalf("output schema missing g/total: %+v", b.Schema)
		}
		for i := 0; i < b.ActiveLen(); i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			got = append(got, pair{
				g:     b.Columns[gIdx].Int64Data[row],
				total: b.Columns[totalIdx].Float64Data[row],
			})
		}
	}
	want := []pair{
		{g: 3, total: 80},
		{g: 1, total: 30},
		{g: 4, total: 25},
		{g: 2, total: 20},
	}
	if len(got) != len(want) {
		t.Fatalf("rows: got %d, want %d (got=%v)", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].g != w.g || got[i].total != w.total {
			t.Errorf("row %d: got (g=%d, total=%v), want (g=%d, total=%v); full got=%v", i, got[i].g, got[i].total, w.g, w.total, got)
		}
	}
}

// TestExecuteFragment_TwoBreakers_SpillUnderPressure exercises the
// multi-breaker shape under a tight shared memory budget so the
// HashAggregate spills mid-fragment via the partial-state external-merge
// path. Validates that the fragment runner correctly drives Finalize ->
// Next() -> Sort.Consume -> Sort.Finalize -> sink even when the aggregate
// is in streaming-merger mode rather than fully-materialized.
//
// Regression for the Gap 1 + 2 spill audit work: ensures runFragmentWithBreakers
// chains an aggregate that consumed via canUseExternalMerge into a
// downstream Sort without losing groups or ordering.
func TestExecuteFragment_TwoBreakers_SpillUnderPressure(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-fragment-agg-sort-spill"

	// Build enough groups that the aggregate's tracked memory crosses the
	// 60% spill threshold under the tight budget below. 200 groups × ~64
	// bytes per group state (with small accumulator arrays + int hash
	// table overhead) is well over 1KB, comfortably triggering the spill.
	rows := make([][2]int64, 0, 200*3)
	for g := int64(1); g <= 200; g++ {
		for v := int64(1); v <= 3; v++ {
			rows = append(rows, [2]int64{g, v * 10})
		}
	}
	data := makeGroupedWshf(t, rows)

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	key := "in/agg-sort-spill/t0.wshf"
	if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
	// 1KB budget — the aggregate's group state will cross the 60%
	// (SpillCheap) threshold partway through Consume and route through
	// the external-merge partial-state spill path.
	executor.SetMemoryBudget(1024, t.TempDir())

	task := distributed.Task{
		ID:           "frag-agg-sort-spill-0",
		QueryID:      "q-frag-agg-sort-spill",
		StageID:      "ass-0",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/agg-sort-spill/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpScan,
				InputAlias:  "src",
				InputFiles:  []string{key},
				InputBucket: bucket,
				Columns:     []string{"g", "v"},
			},
			{
				Type:        distributed.OpHashAggregate,
				GroupByCols: []string{"g"},
				Aggregates: []distributed.AggSpec{
					{Func: "sum", InputCol: "v", OutputCol: "total"},
				},
			},
			{
				Type: distributed.OpSort,
				SortKeySpecs: []distributed.SortKeySpec{
					{Column: "g", Desc: false},
				},
			},
			{
				Type: distributed.OpUnpartitionedSink,
			},
		},
	}

	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != 200 {
		t.Fatalf("NumRows = %d, want 200 groups", result.NumRows)
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

	type pair struct {
		g     int64
		total float64
	}
	var got []pair
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatalf("chunk next: %v", err)
		}
		if b == nil {
			break
		}
		gIdx, totalIdx := -1, -1
		for i, c := range b.Schema {
			switch c.Name {
			case "g":
				gIdx = i
			case "total":
				totalIdx = i
			}
		}
		if gIdx < 0 || totalIdx < 0 {
			t.Fatalf("output schema missing g/total: %+v", b.Schema)
		}
		for i := 0; i < b.ActiveLen(); i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			got = append(got, pair{
				g:     b.Columns[gIdx].Int64Data[row],
				total: b.Columns[totalIdx].Float64Data[row],
			})
		}
	}
	// Each group's expected sum is 10 + 20 + 30 = 60.
	if len(got) != 200 {
		t.Fatalf("rows: got %d, want 200 (got=%v)", len(got), got[:min(len(got), 5)])
	}
	for i, p := range got {
		wantG := int64(i + 1)
		if p.g != wantG {
			t.Errorf("row %d: got g=%d, want %d (output not sorted ascending)", i, p.g, wantG)
		}
		if p.total != 60 {
			t.Errorf("row %d g=%d: got total=%v, want 60", i, p.g, p.total)
		}
	}
}
