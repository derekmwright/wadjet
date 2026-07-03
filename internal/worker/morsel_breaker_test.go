package worker

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// readGroupTotals decodes an unpartitioned agg output (g, total) into a map.
func readGroupTotals(t *testing.T, store *objstore.MemStore, bucket, key string) map[int64]float64 {
	t.Helper()
	rc, _, err := store.Get(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("get output: %v", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	r, err := newShuffleChunkReader(data)
	if err != nil {
		t.Fatalf("parse output: %v", err)
	}
	got := map[int64]float64{}
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatalf("chunk next: %v", err)
		}
		if b == nil {
			return got
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
			got[b.Columns[gIdx].Int64Data[row]] += b.Columns[totalIdx].Float64Data[row]
		}
	}
}

// putGroupedFiles writes numFiles wshf inputs with (g, v) rows where
// g = id % groups and v = id, and returns the keys plus the expected
// per-group sums for rows passing `v >= minV`.
func putGroupedFiles(t *testing.T, store *objstore.MemStore, bucket string, numFiles, rowsPerFile, groups int, minV int64) ([]string, map[int64]float64) {
	t.Helper()
	ctx := context.Background()
	keys := make([]string, numFiles)
	want := map[int64]float64{}
	for f := 0; f < numFiles; f++ {
		rows := make([][2]int64, rowsPerFile)
		for i := range rows {
			id := int64(f*rowsPerFile + i)
			g := id % int64(groups)
			rows[i] = [2]int64{g, id}
			if id >= minV {
				want[g] += float64(id)
			}
		}
		data := makeGroupedWshf(t, rows)
		key := "in/agg/t" + strconv.Itoa(f) + ".wshf"
		if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatalf("Put: %v", err)
		}
		keys[f] = key
	}
	return keys, want
}

func aggFragmentTask(bucket string, keys []string, minV int64) distributed.Task {
	return distributed.Task{
		ID:           "frag-morsel-agg",
		QueryID:      "q-morsel-agg",
		StageID:      "agg-0",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/agg-0/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpScan,
				InputAlias:  "src",
				InputFiles:  keys,
				InputBucket: bucket,
				Columns:     []string{"g", "v"},
			},
			{
				Type:       distributed.OpFilter,
				Predicates: []string{"v >= " + strconv.FormatInt(minV, 10)},
			},
			{
				Type:        distributed.OpHashAggregate,
				GroupByCols: []string{"g"},
				Aggregates: []distributed.AggSpec{
					{Func: "sum", InputCol: "v", OutputCol: "total"},
				},
				MergeMode:    false,
				BuildProject: true,
			},
			{
				Type: distributed.OpUnpartitionedSink,
			},
		},
	}
}

// TestExecuteFragment_MorselParallel_AggParity runs the same
// scan→filter→aggregate→sink breaker fragment serial and morsel-parallel
// (CloneSink partials + barrier merge) and asserts identical group sums.
func TestExecuteFragment_MorselParallel_AggParity(t *testing.T) {
	const numFiles = 8
	const rowsPerFile = 1000
	const groups = 50
	const minV = 1000

	run := func(t *testing.T, bucket string, morselWorkers int) map[int64]float64 {
		store := objstore.NewMemStore()
		if err := store.MakeBucket(context.Background(), bucket); err != nil {
			t.Fatalf("MakeBucket: %v", err)
		}
		keys, _ := putGroupedFiles(t, store, bucket, numFiles, rowsPerFile, groups, minV)

		executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
		executor.SetMorselWorkers(morselWorkers)
		if executor.cpuTokens != nil {
			executor.cpuTokens = newCPUTokens(8)
		}
		task := aggFragmentTask(bucket, keys, minV)
		result := &distributed.ResultNotification{TaskID: task.ID}
		if err := executor.executeStage(context.Background(), task, result); err != nil {
			t.Fatalf("executeStage (morsel=%d): %v", morselWorkers, err)
		}
		if executor.cpuTokens != nil {
			if held := executor.cpuTokens.InUse(); held != 0 {
				t.Fatalf("cpu tokens leaked: %d", held)
			}
		}
		if len(result.ResultFiles) != 1 {
			t.Fatalf("expected 1 output file, got %d", len(result.ResultFiles))
		}
		return readGroupTotals(t, store, bucket, result.ResultFiles[0])
	}

	serial := run(t, "morsel-agg-serial", 1)
	parallel := run(t, "morsel-agg-parallel", 4)

	if len(serial) != groups {
		t.Fatalf("serial groups = %d, want %d", len(serial), groups)
	}
	if len(parallel) != len(serial) {
		t.Fatalf("parallel groups = %d, serial = %d", len(parallel), len(serial))
	}
	for g, s := range serial {
		if parallel[g] != s {
			t.Errorf("group %d: parallel total %v != serial %v", g, parallel[g], s)
		}
	}
}

// TestExecuteFragment_MorselParallel_AggFloatDrift documents and bounds the
// one acceptable serial-vs-parallel divergence: float SUM order. Clone
// partials sum disjoint row subsets and merge, so non-associative float64
// addition can drift in the last ulps (this is why the harness row
// CHECKSUMS differ on float-aggregate TPC-H queries while row counts and
// values match). Anything beyond relative 1e-9 is a real bug, not drift.
func TestExecuteFragment_MorselParallel_AggFloatDrift(t *testing.T) {
	const numFiles = 8
	const rowsPerFile = 1000
	const groups = 10

	run := func(t *testing.T, bucket string, morselWorkers int) map[int64]float64 {
		store := objstore.NewMemStore()
		if err := store.MakeBucket(context.Background(), bucket); err != nil {
			t.Fatalf("MakeBucket: %v", err)
		}
		keys := make([]string, numFiles)
		for f := 0; f < numFiles; f++ {
			rows := make([][2]int64, rowsPerFile)
			for i := range rows {
				id := int64(f*rowsPerFile + i)
				rows[i] = [2]int64{id % groups, id*7919 + 13} // large odd values → inexact float sums
			}
			data := makeGroupedWshf(t, rows)
			key := "in/agg/t" + strconv.Itoa(f) + ".wshf"
			if _, err := store.Put(context.Background(), bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
				t.Fatalf("Put: %v", err)
			}
			keys[f] = key
		}
		executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
		executor.SetMorselWorkers(morselWorkers)
		if executor.cpuTokens != nil {
			executor.cpuTokens = newCPUTokens(8)
		}
		task := aggFragmentTask(bucket, keys, 0)
		result := &distributed.ResultNotification{TaskID: task.ID}
		if err := executor.executeStage(context.Background(), task, result); err != nil {
			t.Fatalf("executeStage: %v", err)
		}
		return readGroupTotals(t, store, bucket, result.ResultFiles[0])
	}

	serial := run(t, "morsel-drift-serial", 1)
	parallel := run(t, "morsel-drift-parallel", 4)
	if len(serial) != groups || len(parallel) != groups {
		t.Fatalf("groups: serial %d, parallel %d, want %d", len(serial), len(parallel), groups)
	}
	for g, s := range serial {
		p := parallel[g]
		diff := s - p
		if diff < 0 {
			diff = -diff
		}
		if diff > 1e-9*s {
			t.Errorf("group %d: parallel %v vs serial %v — beyond float-order drift", g, p, s)
		}
	}
}

// TestExecuteFragment_MorselParallel_SortParity: order-sensitive breaker.
// The parallel consume feeds per-worker Sort partials in nondeterministic
// interleave; the Finalize sort must still produce the exact serial output
// sequence.
func TestExecuteFragment_MorselParallel_SortParity(t *testing.T) {
	const numFiles = 8
	const rowsPerFile = 1000

	run := func(t *testing.T, bucket string, morselWorkers int) []int64 {
		store := objstore.NewMemStore()
		if err := store.MakeBucket(context.Background(), bucket); err != nil {
			t.Fatalf("MakeBucket: %v", err)
		}
		keys := make([]string, numFiles)
		for f := 0; f < numFiles; f++ {
			rows := make([][2]int64, rowsPerFile)
			for i := range rows {
				id := int64(f*rowsPerFile + i)
				rows[i] = [2]int64{id, id * 7 % 9973} // shuffled-ish values, unique ids
			}
			data := makeScanWshf(t, rows)
			key := "in/sort/t" + strconv.Itoa(f) + ".wshf"
			if _, err := store.Put(context.Background(), bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
				t.Fatalf("Put: %v", err)
			}
			keys[f] = key
		}

		executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
		executor.SetMorselWorkers(morselWorkers)
		if executor.cpuTokens != nil {
			executor.cpuTokens = newCPUTokens(8)
		}
		task := distributed.Task{
			ID:           "frag-morsel-sort",
			QueryID:      "q-morsel-sort",
			StageID:      "sort-0",
			Type:         distributed.TaskTypeStage,
			DataBucket:   bucket,
			ResultBucket: bucket,
			ResultPrefix: "out/sort-0/",
			Operators: []distributed.OpSpec{
				{
					Type:        distributed.OpScan,
					InputAlias:  "t",
					InputFiles:  keys,
					InputBucket: bucket,
					Columns:     []string{"id", "val"},
				},
				{
					Type: distributed.OpSort,
					SortKeySpecs: []distributed.SortKeySpec{
						{Column: "val", Desc: true},
						{Column: "id", Desc: false},
					},
				},
				{
					Type: distributed.OpUnpartitionedSink,
				},
			},
		}
		result := &distributed.ResultNotification{TaskID: task.ID}
		if err := executor.executeStage(context.Background(), task, result); err != nil {
			t.Fatalf("executeStage (morsel=%d): %v", morselWorkers, err)
		}
		if result.NumRows != int64(numFiles*rowsPerFile) {
			t.Fatalf("NumRows = %d, want %d", result.NumRows, numFiles*rowsPerFile)
		}
		return readMemStoreInts(t, store, bucket, result.ResultFiles[0], "id")
	}

	serial := run(t, "morsel-sort-serial", 1)
	parallel := run(t, "morsel-sort-parallel", 4)
	if len(serial) != len(parallel) {
		t.Fatalf("row counts differ: serial %d, parallel %d", len(serial), len(parallel))
	}
	for i := range serial {
		if serial[i] != parallel[i] {
			t.Fatalf("row %d: parallel id %d != serial id %d (sorted output must be identical)", i, parallel[i], serial[i])
		}
	}
}

// TestExecuteFragment_MorselParallel_AggPressureCollapse forces the shared
// pool over the SpillCheap threshold mid-consume (high-cardinality groups,
// tiny budget). The parallel consume must collapse to the serial spill path
// — observable via the executor's collapse counter — and still produce
// exactly one output row per distinct group with correct sums.
func TestExecuteFragment_MorselParallel_AggPressureCollapse(t *testing.T) {
	const numFiles = 16
	const rowsPerFile = 2048
	const totalRows = numFiles * rowsPerFile

	bucket := "morsel-agg-collapse"
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	// groups == totalRows → every row is its own group; group state grows
	// linearly with input and blows through the tiny budget fast.
	keys, want := putGroupedFiles(t, store, bucket, numFiles, rowsPerFile, totalRows, 0)

	executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
	// 40% SpillCheap threshold of 256 KB = ~100 KB — the warmup batch's
	// group state alone approaches it, so collapse fires within the first
	// few parallel batches.
	executor.SetSharedPoolBudget(256 * 1024)
	executor.SetMorselWorkers(4)
	executor.cpuTokens = newCPUTokens(8)

	task := aggFragmentTask(bucket, keys, 0)
	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(context.Background(), task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if got := executor.morselCollapses.Load(); got < 1 {
		t.Fatalf("expected at least one pressure collapse, counter = %d", got)
	}
	if held := executor.cpuTokens.InUse(); held != 0 {
		t.Fatalf("cpu tokens leaked after collapse: %d", held)
	}
	if result.NumRows != int64(totalRows) {
		t.Fatalf("NumRows = %d, want %d distinct groups", result.NumRows, totalRows)
	}
	got := readGroupTotals(t, store, bucket, result.ResultFiles[0])
	if len(got) != totalRows {
		t.Fatalf("output groups = %d, want %d", len(got), totalRows)
	}
	for g, s := range want {
		if got[g] != s {
			t.Fatalf("group %d: total %v, want %v (state lost across collapse/merge)", g, got[g], s)
		}
	}
}

// TestExecuteFragment_MorselParallel_AggEmptyInput covers the breaker
// warmup-nil path: zero input batches must still finalize the aggregate
// (empty output) without engaging clones.
func TestExecuteFragment_MorselParallel_AggEmptyInput(t *testing.T) {
	bucket := "morsel-agg-empty"
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	data := makeGroupedWshf(t, nil)
	key := "in/agg/empty.wshf"
	if _, err := store.Put(context.Background(), bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	executor := NewExecutor(store, NewLRUCache(1024*1024), nil)
	executor.SetMorselWorkers(4)
	executor.cpuTokens = newCPUTokens(8)

	task := aggFragmentTask(bucket, []string{key}, 0)
	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(context.Background(), task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != 0 {
		t.Fatalf("NumRows = %d, want 0", result.NumRows)
	}
	if held := executor.cpuTokens.InUse(); held != 0 {
		t.Fatalf("cpu tokens leaked: %d", held)
	}
}

// TestExecuteFragment_MorselParallel_AggLargeBatchParity is the AggParity
// shape with source batches ~3× DefaultBatchSize: the dispenser splits them
// into zero-copy views (HashAggregate copies rows out during Consume, so
// views are safe into breaker clones), and group sums must still match
// serial exactly. This is the SF100 parquet row-group shape on the breaker
// path.
func TestExecuteFragment_MorselParallel_AggLargeBatchParity(t *testing.T) {
	// Shrink the dispenser budget so these small test parents exceed the
	// bytes gate and the view-splitting machinery engages end-to-end.
	origBudget := morselDispenserBudgetBytes
	morselDispenserBudgetBytes = 64 << 10
	defer func() { morselDispenserBudgetBytes = origBudget }()


	const numFiles = 4
	const rowsPerFile = 6000 // > DefaultBatchSize → dispenser splits into views
	const groups = 50
	const minV = 1000

	run := func(t *testing.T, bucket string, morselWorkers int) map[int64]float64 {
		store := objstore.NewMemStore()
		if err := store.MakeBucket(context.Background(), bucket); err != nil {
			t.Fatalf("MakeBucket: %v", err)
		}
		keys, _ := putGroupedFiles(t, store, bucket, numFiles, rowsPerFile, groups, minV)

		executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
		executor.SetMorselWorkers(morselWorkers)
		executor.cpuTokens = newCPUTokens(8)
		task := aggFragmentTask(bucket, keys, minV)
		result := &distributed.ResultNotification{TaskID: task.ID}
		if err := executor.executeStage(context.Background(), task, result); err != nil {
			t.Fatalf("executeStage (morsel=%d): %v", morselWorkers, err)
		}
		if held := executor.cpuTokens.InUse(); held != 0 {
			t.Fatalf("cpu tokens leaked: %d", held)
		}
		return readGroupTotals(t, store, bucket, result.ResultFiles[0])
	}

	serial := run(t, "morsel-agg-big-serial", 1)
	parallel := run(t, "morsel-agg-big-parallel", 4)
	if len(serial) != groups || len(parallel) != groups {
		t.Fatalf("groups: serial %d, parallel %d, want %d", len(serial), len(parallel), groups)
	}
	for g, s := range serial {
		if parallel[g] != s {
			t.Errorf("group %d: parallel total %v != serial %v", g, parallel[g], s)
		}
	}
}

// TestExecuteFragment_MorselParallel_SortLargeBatchParity: Sort RETAINS
// consumed batches and charges them via the Sel-blind MemBytes, so the
// dispenser must NOT split for Sort sinks (each retained view would charge
// the full parent). Large batches flow through the byte-bounded dispenser
// unsplit; sorted output must be identical to serial.
func TestExecuteFragment_MorselParallel_SortLargeBatchParity(t *testing.T) {
	const numFiles = 4
	const rowsPerFile = 6000

	run := func(t *testing.T, bucket string, morselWorkers int) []int64 {
		store := objstore.NewMemStore()
		if err := store.MakeBucket(context.Background(), bucket); err != nil {
			t.Fatalf("MakeBucket: %v", err)
		}
		keys := make([]string, numFiles)
		for f := 0; f < numFiles; f++ {
			rows := make([][2]int64, rowsPerFile)
			for i := range rows {
				id := int64(f*rowsPerFile + i)
				rows[i] = [2]int64{id, id * 7 % 9973}
			}
			data := makeScanWshf(t, rows)
			key := "in/sort/t" + strconv.Itoa(f) + ".wshf"
			if _, err := store.Put(context.Background(), bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
				t.Fatalf("Put: %v", err)
			}
			keys[f] = key
		}

		executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
		executor.SetMorselWorkers(morselWorkers)
		executor.cpuTokens = newCPUTokens(8)
		task := distributed.Task{
			ID:           "frag-morsel-sort-big",
			QueryID:      "q-morsel-sort-big",
			StageID:      "sort-0",
			Type:         distributed.TaskTypeStage,
			DataBucket:   bucket,
			ResultBucket: bucket,
			ResultPrefix: "out/sort-0/",
			Operators: []distributed.OpSpec{
				{
					Type:        distributed.OpScan,
					InputAlias:  "t",
					InputFiles:  keys,
					InputBucket: bucket,
					Columns:     []string{"id", "val"},
				},
				{
					Type: distributed.OpSort,
					SortKeySpecs: []distributed.SortKeySpec{
						{Column: "val", Desc: true},
						{Column: "id", Desc: false},
					},
				},
				{
					Type: distributed.OpUnpartitionedSink,
				},
			},
		}
		result := &distributed.ResultNotification{TaskID: task.ID}
		if err := executor.executeStage(context.Background(), task, result); err != nil {
			t.Fatalf("executeStage (morsel=%d): %v", morselWorkers, err)
		}
		if result.NumRows != int64(numFiles*rowsPerFile) {
			t.Fatalf("NumRows = %d, want %d", result.NumRows, numFiles*rowsPerFile)
		}
		return readMemStoreInts(t, store, bucket, result.ResultFiles[0], "id")
	}

	serial := run(t, "morsel-sort-big-serial", 1)
	parallel := run(t, "morsel-sort-big-parallel", 4)
	if len(serial) != len(parallel) {
		t.Fatalf("row counts differ: serial %d, parallel %d", len(serial), len(parallel))
	}
	for i := range serial {
		if serial[i] != parallel[i] {
			t.Fatalf("row %d: parallel id %d != serial id %d", i, parallel[i], serial[i])
		}
	}
}
