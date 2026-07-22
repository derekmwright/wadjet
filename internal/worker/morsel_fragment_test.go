package worker

import (
	"bytes"
	"context"
	"runtime"
	"sort"
	"strconv"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// nonCloneableOp is a UnaryOperator that deliberately does NOT implement
// exec.Cloneable — fragments containing one must stay serial.
type nonCloneableOp struct{}

func (nonCloneableOp) Init(context.Context) error { return nil }
func (nonCloneableOp) Execute(_ context.Context, b *batch.RecordBatch) (*batch.RecordBatch, error) {
	return b, nil
}
func (nonCloneableOp) Close() error { return nil }

// pinWidthYield pins the work-conserving-width mode for a test and restores
// it at cleanup. The fixed-width subtests below pin false: they encode the
// legacy WADJET_MORSEL_YIELD=0 semantics (width frozen at start-time token
// availability).
func pinWidthYield(tb testing.TB, on bool) {
	tb.Helper()
	prev := morselWidthYield
	morselWidthYield = on
	tb.Cleanup(func() { morselWidthYield = prev })
}

func TestMorselFragmentWorkers_Gates(t *testing.T) {
	pinWidthYield(t, false)
	newExec := func(policy, tokens int) *Executor {
		e := NewExecutor(objstore.NewMemStore(), NewLRUCache(1024*1024), nil)
		e.SetMorselWorkers(policy)
		if e.cpuTokens != nil {
			e.cpuTokens = newCPUTokens(tokens)
		}
		return e
	}
	cloneable := []exec.UnaryOperator{exec.NewFilter(func(*batch.RecordBatch, int) bool { return true })}
	bigTask := distributed.Task{EstimatedBytes: morselMinFragmentBytes}
	smallTask := distributed.Task{EstimatedBytes: morselMinFragmentBytes - 1}

	t.Run("zero value stays serial", func(t *testing.T) {
		e := NewExecutor(objstore.NewMemStore(), NewLRUCache(1024*1024), nil)
		if k, _, _ := e.morselFragmentWorkers(bigTask, cloneable); k != 1 {
			t.Fatalf("k = %d, want 1", k)
		}
	})
	t.Run("explicit 1 stays serial", func(t *testing.T) {
		e := newExec(1, 8)
		if k, _, _ := e.morselFragmentWorkers(bigTask, cloneable); k != 1 {
			t.Fatalf("k = %d, want 1", k)
		}
	})
	t.Run("fixed width acquires extras and releases", func(t *testing.T) {
		e := newExec(4, 8)
		k, _, release := e.morselFragmentWorkers(smallTask, cloneable) // fixed bypasses size gate
		if k != 4 {
			t.Fatalf("k = %d, want 4", k)
		}
		if got := e.cpuTokens.InUse(); got != 3 {
			t.Fatalf("tokens in use = %d, want 3 (first consumer is free)", got)
		}
		release()
		if got := e.cpuTokens.InUse(); got != 0 {
			t.Fatalf("tokens in use after release = %d, want 0", got)
		}
	})
	t.Run("token scarcity narrows width", func(t *testing.T) {
		e := newExec(4, 1)
		k, _, release := e.morselFragmentWorkers(bigTask, cloneable)
		if k != 2 {
			t.Fatalf("k = %d, want 2 (1 free + 1 token)", k)
		}
		release()
	})
	t.Run("token exhaustion degrades to serial", func(t *testing.T) {
		e := newExec(4, 0)
		k, _, release := e.morselFragmentWorkers(bigTask, cloneable)
		if k != 1 {
			t.Fatalf("k = %d, want 1", k)
		}
		release()
		if got := e.cpuTokens.InUse(); got != 0 {
			t.Fatalf("tokens leaked: %d", got)
		}
	})
	t.Run("non-cloneable op stays serial without taking tokens", func(t *testing.T) {
		e := newExec(4, 8)
		ops := append([]exec.UnaryOperator{nonCloneableOp{}}, cloneable...)
		k, _, _ := e.morselFragmentWorkers(bigTask, ops)
		if k != 1 {
			t.Fatalf("k = %d, want 1", k)
		}
		if got := e.cpuTokens.InUse(); got != 0 {
			t.Fatalf("tokens in use = %d, want 0", got)
		}
	})
	t.Run("auto respects size gate", func(t *testing.T) {
		e := newExec(-1, 8)
		if k, _, _ := e.morselFragmentWorkers(smallTask, cloneable); k != 1 {
			t.Fatalf("small fragment k = %d, want 1", k)
		}
		k, _, release := e.morselFragmentWorkers(bigTask, cloneable)
		want := runtime.GOMAXPROCS(0)
		if want > morselFragmentParallelismCap {
			want = morselFragmentParallelismCap
		}
		if want > 9 { // tokens(8) + 1 free
			want = 9
		}
		if k != want {
			t.Fatalf("big fragment k = %d, want %d", k, want)
		}
		release()
	})
	t.Run("fixed width capped", func(t *testing.T) {
		e := newExec(64, 64)
		k, _, release := e.morselFragmentWorkers(bigTask, cloneable)
		if k != morselFragmentParallelismCap {
			t.Fatalf("k = %d, want cap %d", k, morselFragmentParallelismCap)
		}
		release()
	})
}

// TestMorselFragmentWorkers_YieldMode: work-conserving mode takes no tokens
// up front and returns the full policy target even on an exhausted pool —
// active width is metered per-morsel by the gate instead of frozen at
// start-time availability.
func TestMorselFragmentWorkers_YieldMode(t *testing.T) {
	pinWidthYield(t, true)
	e := NewExecutor(objstore.NewMemStore(), NewLRUCache(1024*1024), nil)
	e.SetMorselWorkers(4)
	e.cpuTokens = newCPUTokens(0) // exhausted pool must not narrow k
	cloneable := []exec.UnaryOperator{exec.NewFilter(func(*batch.RecordBatch, int) bool { return true })}
	task := distributed.Task{EstimatedBytes: morselMinFragmentBytes}

	k, gate, release := e.morselFragmentWorkers(task, cloneable)
	if k != 4 {
		t.Fatalf("k = %d, want policy target 4", k)
	}
	if gate == nil {
		t.Fatal("gate = nil, want a width gate in yield mode")
	}
	if got := e.cpuTokens.InUse(); got != 0 {
		t.Fatalf("tokens taken up front = %d, want 0", got)
	}
	release()
}

// TestExecuteFragment_MorselParallel_FilterParity runs the same
// scan→filter→unpartitioned-sink fragment serial and morsel-parallel and
// asserts the outputs are row-identical (as sets — parallel consumers
// reorder). Multi-file input so the producer emits many batches; the filter
// predicates are compiled closures with lazily-resolved column caches, so
// running this under -race also exercises the warmup-before-clone rule.
func TestExecuteFragment_MorselParallel_FilterParity(t *testing.T) {
	ctx := context.Background()
	const numFiles = 8
	const rowsPerFile = 1000

	run := func(t *testing.T, bucket string, morselWorkers int) (int64, []int64) {
		store := objstore.NewMemStore()
		if err := store.MakeBucket(ctx, bucket); err != nil {
			t.Fatalf("MakeBucket: %v", err)
		}
		keys := make([]string, numFiles)
		for f := 0; f < numFiles; f++ {
			rows := make([][2]int64, rowsPerFile)
			for i := range rows {
				id := int64(f*rowsPerFile + i)
				rows[i] = [2]int64{id, id * 10}
			}
			data := makeScanWshf(t, rows)
			key := "in/scan/t" + strconv.Itoa(f) + ".wshf"
			if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
				t.Fatalf("Put: %v", err)
			}
			keys[f] = key
		}

		executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
		executor.SetMorselWorkers(morselWorkers)
		if executor.cpuTokens != nil {
			executor.cpuTokens = newCPUTokens(8) // deterministic width on small CI boxes
		}

		task := distributed.Task{
			ID:           "frag-morsel-parity",
			QueryID:      "q-morsel-parity",
			StageID:      "scan-1",
			Type:         distributed.TaskTypeStage,
			DataBucket:   bucket,
			ResultBucket: bucket,
			ResultPrefix: "out/scan-1/",
			Operators: []distributed.OpSpec{
				{
					Type:        distributed.OpScan,
					InputAlias:  "t",
					InputFiles:  keys,
					InputBucket: bucket,
					Columns:     []string{"id", "val"},
				},
				{
					Type:       distributed.OpFilter,
					Predicates: []string{"id > 500", "val < 60000"},
				},
				{
					Type: distributed.OpUnpartitionedSink,
				},
			},
		}
		result := &distributed.ResultNotification{TaskID: task.ID}
		if err := executor.executeStage(ctx, task, result); err != nil {
			t.Fatalf("executeStage (morsel=%d): %v", morselWorkers, err)
		}
		if executor.cpuTokens != nil {
			if held := executor.cpuTokens.InUse(); held != 0 {
				t.Fatalf("cpu tokens leaked after fragment: %d", held)
			}
		}
		if len(result.ResultFiles) != 1 {
			t.Fatalf("expected 1 output file, got %d", len(result.ResultFiles))
		}
		ids := readMemStoreInts(t, store, bucket, result.ResultFiles[0], "id")
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		return result.NumRows, ids
	}

	serialRows, serialIDs := run(t, "morsel-parity-serial", 1)
	parallelRows, parallelIDs := run(t, "morsel-parity-parallel", 4)

	// ids 501..5999 pass both predicates (val = id*10 < 60000).
	const wantRows = 5499
	if serialRows != wantRows {
		t.Fatalf("serial NumRows = %d, want %d", serialRows, wantRows)
	}
	if parallelRows != serialRows {
		t.Fatalf("parallel NumRows = %d, serial = %d", parallelRows, serialRows)
	}
	if len(parallelIDs) != len(serialIDs) {
		t.Fatalf("parallel output rows = %d, serial = %d", len(parallelIDs), len(serialIDs))
	}
	for i := range serialIDs {
		if serialIDs[i] != parallelIDs[i] {
			t.Fatalf("row %d: parallel id %d != serial id %d", i, parallelIDs[i], serialIDs[i])
		}
	}
}

// TestExecuteFragment_MorselParallel_JoinSpillFlush is the morsel-parallel
// variant of TestExecuteFragment_HashJoinProbe_FlushesSpilledPartitions:
// cloned probes share the grace join's spillState, all clones route
// spilled-partition probe rows to the shared (mutex'd) writers, and the
// post-Wait drain must produce each spilled match EXACTLY once — fewer means
// a clone's routed rows were dropped, more means the per-clone drain
// double-processed the shared partitions.
func TestExecuteFragment_MorselParallel_JoinSpillFlush(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-morsel-hj-flush"

	const numBuildFiles = 4
	const rowsPerFile = 2048
	const buildN = numBuildFiles * rowsPerFile

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	buildKeys := make([]string, numBuildFiles)
	for f := 0; f < numBuildFiles; f++ {
		rows := make([][2]int64, rowsPerFile)
		for i := range rows {
			id := int64(f*rowsPerFile + i)
			rows[i] = [2]int64{id, id * 10}
		}
		data := makeBuildWshf(t, rows)
		key := "in/hj/build-" + strconv.Itoa(f) + ".wshf"
		if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		buildKeys[f] = key
	}
	// Probe side spans multiple files so the parallel consumers actually see
	// concurrent batches.
	const numProbeFiles = 4
	const probePerFile = 2048
	const probeN = numProbeFiles * probePerFile
	probeKeys := make([]string, numProbeFiles)
	for f := 0; f < numProbeFiles; f++ {
		probeRows := make([]struct {
			ID   int64
			Name string
		}, probePerFile)
		for i := range probeRows {
			n := f*probePerFile + i
			probeRows[i] = struct {
				ID   int64
				Name string
			}{ID: int64(n % buildN), Name: "row-" + strconv.Itoa(n)}
		}
		data := makeProbeWshf(t, probeRows)
		key := "in/hj/probe-" + strconv.Itoa(f) + ".wshf"
		if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		probeKeys[f] = key
	}

	executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
	// Tight shared budget forces the build to spill a partition; probe rows
	// hashing there route to disk and re-enter via the flush drain.
	executor.SetSharedPoolBudget(500 * 1024)
	executor.SetMorselWorkers(4)
	executor.cpuTokens = newCPUTokens(8)

	task := distributed.Task{
		ID:           "frag-morsel-hj-flush",
		QueryID:      "q-morsel-hj-flush",
		StageID:      "join-0",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/join-0/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpShuffleSource,
				InputAlias:  "probe",
				InputFiles:  probeKeys,
				InputBucket: bucket,
			},
			{
				Type:        distributed.OpBroadcastProbe,
				JoinType:    "inner",
				LeftKeys:    []string{"id"},
				RightKeys:   []string{"id"},
				BuildAlias:  "build",
				BuildFiles:  buildKeys,
				BuildBucket: bucket,
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
	if result.NumRows != int64(probeN) {
		t.Fatalf("NumRows = %d, want exactly %d (fewer = spilled probe rows dropped; more = per-clone drain double-processed shared partitions)",
			result.NumRows, probeN)
	}
	if held := executor.cpuTokens.InUse(); held != 0 {
		t.Fatalf("cpu tokens leaked after fragment: %d", held)
	}
}

// TestExecuteFragment_MorselParallel_EmptyInput covers the warmup-nil path:
// input files exist but decode to zero batches. The parallel runner must
// still run the flush drain and finalize the sink with NumRows 0.
func TestExecuteFragment_MorselParallel_EmptyInput(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-morsel-empty"

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	data := makeScanWshf(t, nil)
	key := "in/scan/empty.wshf"
	if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	executor := NewExecutor(store, NewLRUCache(1024*1024), nil)
	executor.SetMorselWorkers(4)
	executor.cpuTokens = newCPUTokens(8)

	task := distributed.Task{
		ID:           "frag-morsel-empty",
		QueryID:      "q-morsel-empty",
		StageID:      "scan-1",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/scan-1/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpScan,
				InputAlias:  "t",
				InputFiles:  []string{key},
				InputBucket: bucket,
				Columns:     []string{"id", "val"},
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
	if result.NumRows != 0 {
		t.Fatalf("NumRows = %d, want 0", result.NumRows)
	}
	if held := executor.cpuTokens.InUse(); held != 0 {
		t.Fatalf("cpu tokens leaked: %d", held)
	}
}

// runMorselFilterFragment executes a scan→filter→unpartitioned-sink fragment
// over numFiles WSHF files of rowsPerFile rows each and returns (NumRows,
// sorted ids). rowsPerFile > batch.DefaultBatchSize makes the source emit
// oversized batches, engaging the dispenser's zero-copy view splitting.
func runMorselFilterFragment(t *testing.T, bucket string, morselWorkers, numFiles, rowsPerFile int) (int64, []int64) {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	keys := make([]string, numFiles)
	for f := 0; f < numFiles; f++ {
		rows := make([][2]int64, rowsPerFile)
		for i := range rows {
			id := int64(f*rowsPerFile + i)
			rows[i] = [2]int64{id, id * 10}
		}
		data := makeScanWshf(t, rows)
		key := "in/scan/t" + strconv.Itoa(f) + ".wshf"
		if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatalf("Put: %v", err)
		}
		keys[f] = key
	}

	executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
	executor.SetMorselWorkers(morselWorkers)
	executor.cpuTokens = newCPUTokens(8)

	task := distributed.Task{
		ID:           "frag-morsel-views",
		QueryID:      "q-morsel-views",
		StageID:      "scan-1",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/scan-1/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpScan,
				InputAlias:  "t",
				InputFiles:  keys,
				InputBucket: bucket,
				Columns:     []string{"id", "val"},
			},
			{
				Type:       distributed.OpFilter,
				Predicates: []string{"id > 500", "val < 60000"},
			},
			{
				Type: distributed.OpUnpartitionedSink,
			},
		},
	}
	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage (morsel=%d): %v", morselWorkers, err)
	}
	if held := executor.cpuTokens.InUse(); held != 0 {
		t.Fatalf("cpu tokens leaked after fragment: %d", held)
	}
	if len(result.ResultFiles) != 1 {
		t.Fatalf("expected 1 output file, got %d", len(result.ResultFiles))
	}
	ids := readMemStoreInts(t, store, bucket, result.ResultFiles[0], "id")
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return result.NumRows, ids
}

// TestExecuteFragment_MorselParallel_LargeBatchViewParity feeds the parallel
// linear path batches ~5× DefaultBatchSize so the dispenser splits them into
// zero-copy views, and asserts row-identical output vs serial. This is the
// SF100 parquet shape (a source batch = one decoded row group, not 2048
// rows) that the 2026-07-03 A/B failed on.
func TestExecuteFragment_MorselParallel_LargeBatchViewParity(t *testing.T) {
	// Shrink the dispenser budget so these small test parents exceed the
	// bytes gate and the view-splitting machinery engages end-to-end.
	origBudget := morselDispenserBudgetBytes
	morselDispenserBudgetBytes = 64 << 10
	defer func() { morselDispenserBudgetBytes = origBudget }()


	const numFiles = 4
	const rowsPerFile = 10000 // > DefaultBatchSize → splitting engages

	serialRows, serialIDs := runMorselFilterFragment(t, "morsel-views-serial", 1, numFiles, rowsPerFile)
	parallelRows, parallelIDs := runMorselFilterFragment(t, "morsel-views-parallel", 4, numFiles, rowsPerFile)

	// ids 501..5999 pass both predicates (val = id*10 < 60000).
	const wantRows = 5499
	if serialRows != wantRows {
		t.Fatalf("serial NumRows = %d, want %d", serialRows, wantRows)
	}
	if parallelRows != serialRows {
		t.Fatalf("parallel NumRows = %d, serial = %d", parallelRows, serialRows)
	}
	for i := range serialIDs {
		if serialIDs[i] != parallelIDs[i] {
			t.Fatalf("row %d: parallel id %d != serial id %d", i, parallelIDs[i], serialIDs[i])
		}
	}
}

// TestExecuteFragment_MorselParallel_LinearPressureCollapse forces the heap
// backpressure signal on and asserts the parallel linear fragment collapses
// to serial (morselCollapses increments) while still producing exactly the
// serial output. Regression for the SF100 2026-07-03 failure: Q17/Q18
// grace-join LINEAR fragments blew the worker heap with morselCollapses = 0
// because only the breaker path had a pressure-collapse rule.
func TestExecuteFragment_MorselParallel_LinearPressureCollapse(t *testing.T) {
	// Shrink the dispenser budget so these small test parents exceed the
	// bytes gate and the view-splitting machinery engages end-to-end.
	origBudget := morselDispenserBudgetBytes
	morselDispenserBudgetBytes = 64 << 10
	defer func() { morselDispenserBudgetBytes = origBudget }()


	orig := heapPressureActive
	heapPressureActive = func() bool { return true }
	defer func() { heapPressureActive = orig }()

	const numFiles = 4
	const rowsPerFile = 10000

	serialRows, serialIDs := runMorselFilterFragment(t, "morsel-collapse-serial", 1, numFiles, rowsPerFile)

	// Run the parallel arm manually so we can read the collapse counter.
	ctx := context.Background()
	const bucket = "morsel-collapse-parallel"
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	keys := make([]string, numFiles)
	for f := 0; f < numFiles; f++ {
		rows := make([][2]int64, rowsPerFile)
		for i := range rows {
			id := int64(f*rowsPerFile + i)
			rows[i] = [2]int64{id, id * 10}
		}
		data := makeScanWshf(t, rows)
		key := "in/scan/t" + strconv.Itoa(f) + ".wshf"
		if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatalf("Put: %v", err)
		}
		keys[f] = key
	}
	executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
	executor.SetMorselWorkers(4)
	executor.cpuTokens = newCPUTokens(8)
	task := distributed.Task{
		ID:           "frag-morsel-collapse",
		QueryID:      "q-morsel-collapse",
		StageID:      "scan-1",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/scan-1/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpScan,
				InputAlias:  "t",
				InputFiles:  keys,
				InputBucket: bucket,
				Columns:     []string{"id", "val"},
			},
			{
				Type:       distributed.OpFilter,
				Predicates: []string{"id > 500", "val < 60000"},
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
	if got := executor.morselCollapses.Load(); got != 1 {
		t.Fatalf("morselCollapses = %d, want 1 (linear path must collapse under heap pressure)", got)
	}
	if result.NumRows != serialRows {
		t.Fatalf("collapsed-parallel NumRows = %d, serial = %d", result.NumRows, serialRows)
	}
	ids := readMemStoreInts(t, store, bucket, result.ResultFiles[0], "id")
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for i := range serialIDs {
		if serialIDs[i] != ids[i] {
			t.Fatalf("row %d: collapsed id %d != serial id %d", i, ids[i], serialIDs[i])
		}
	}
	if held := executor.cpuTokens.InUse(); held != 0 {
		t.Fatalf("cpu tokens leaked after collapse: %d", held)
	}
}

// TestExecuteFragment_MorselParallel_LargeBatchJoinSpillFlush is the
// JoinSpillFlush property with probe batches ~3× DefaultBatchSize: the
// dispenser splits them into zero-copy views, cloned probes reassign each
// view's private Sel (grace routing + inMemSel), spilled probe rows route
// through the shared writers, and every match must still appear exactly
// once. This is the Q17/Q18 grace-join linear shape that failed the SF100
// 2026-07-03 A/B.
func TestExecuteFragment_MorselParallel_LargeBatchJoinSpillFlush(t *testing.T) {
	// Shrink the dispenser budget so these small test parents exceed the
	// bytes gate and the view-splitting machinery engages end-to-end.
	origBudget := morselDispenserBudgetBytes
	morselDispenserBudgetBytes = 64 << 10
	defer func() { morselDispenserBudgetBytes = origBudget }()


	ctx := context.Background()
	const bucket = "test-morsel-hj-flush-big"

	const numBuildFiles = 4
	const rowsPerFile = 2048
	const buildN = numBuildFiles * rowsPerFile

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	buildKeys := make([]string, numBuildFiles)
	for f := 0; f < numBuildFiles; f++ {
		rows := make([][2]int64, rowsPerFile)
		for i := range rows {
			id := int64(f*rowsPerFile + i)
			rows[i] = [2]int64{id, id * 10}
		}
		data := makeBuildWshf(t, rows)
		key := "in/hj/build-" + strconv.Itoa(f) + ".wshf"
		if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		buildKeys[f] = key
	}
	const numProbeFiles = 4
	const probePerFile = 6000 // > DefaultBatchSize → dispenser splits into views
	const probeN = numProbeFiles * probePerFile
	probeKeys := make([]string, numProbeFiles)
	for f := 0; f < numProbeFiles; f++ {
		probeRows := make([]struct {
			ID   int64
			Name string
		}, probePerFile)
		for i := range probeRows {
			n := f*probePerFile + i
			probeRows[i] = struct {
				ID   int64
				Name string
			}{ID: int64(n % buildN), Name: "row-" + strconv.Itoa(n)}
		}
		data := makeProbeWshf(t, probeRows)
		key := "in/hj/probe-" + strconv.Itoa(f) + ".wshf"
		if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		probeKeys[f] = key
	}

	executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
	executor.SetSharedPoolBudget(500 * 1024)
	executor.SetMorselWorkers(4)
	executor.cpuTokens = newCPUTokens(8)

	task := distributed.Task{
		ID:           "frag-morsel-hj-flush-big",
		QueryID:      "q-morsel-hj-flush-big",
		StageID:      "join-0",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/join-0/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpShuffleSource,
				InputAlias:  "probe",
				InputFiles:  probeKeys,
				InputBucket: bucket,
			},
			{
				Type:        distributed.OpBroadcastProbe,
				JoinType:    "inner",
				LeftKeys:    []string{"id"},
				RightKeys:   []string{"id"},
				BuildAlias:  "build",
				BuildFiles:  buildKeys,
				BuildBucket: bucket,
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
	if result.NumRows != int64(probeN) {
		t.Fatalf("NumRows = %d, want exactly %d (fewer = spilled/view probe rows dropped; more = double-processed)",
			result.NumRows, probeN)
	}
	if held := executor.cpuTokens.InUse(); held != 0 {
		t.Fatalf("cpu tokens leaked after fragment: %d", held)
	}
}
