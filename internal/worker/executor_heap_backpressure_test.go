package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// TestExecuteShuffle_PausesUnderHeapPressure verifies that the executeShuffle
// per-batch loop calls memory.PauseOnHeapBackpressure between batches. When
// heap pressure is spoofed on, each iteration sleeps HeapBackpressurePauseDuration
// (50ms), so a shuffle of N input files takes at least ~N × 50ms wall time.
//
// Mirrors the fragment-runner backpressure pattern landed in 2026-05-17
// (ac7a517), addressing the Q17 SF100 mc=3 stall where 3 concurrent shuffle
// tasks pushed the worker heap past GOMEMLIMIT with no per-batch yielding.
func TestExecuteShuffle_PausesUnderHeapPressure(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-shuffle-bp"
	const numFiles = 4
	const rowsPerFile = 50

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}

	// Write numFiles small parquet files so the shuffle source iterates
	// multiple times. Each src.Next() consumes one file at a time, so the
	// outer loop runs at least numFiles iterations with a PauseOnHeapBackpressure
	// call before each Next.
	inputFiles := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		rows := make([]map[string]any, rowsPerFile)
		for j := 0; j < rowsPerFile; j++ {
			rows[j] = map[string]any{
				"k": int64(i*rowsPerFile + j),
				"v": fmt.Sprintf("value-%d-%d", i, j),
			}
		}
		path := fmt.Sprintf("tables/src/part-%d.parquet", i)
		writeParquetFile(t, store, bucket, path, rows)
		inputFiles[i] = path
	}

	cache := NewLRUCache(64 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	executor.SetMemoryBudget(0, t.TempDir())

	task := distributed.Task{
		ID:            "bp-shuffle",
		QueryID:       "q-bp",
		StageID:       "shuffle-bp",
		Type:          distributed.TaskTypeShuffle,
		Files:         inputFiles,
		ShuffleKeys:   []string{"k"},
		NumPartitions: 2,
		DataBucket:    bucket,
		ResultBucket:  bucket,
		ResultPrefix:  "shuffle/q-bp/",
	}

	// Baseline run WITHOUT pressure — establishes that the loop is fast when
	// HeapBackpressureActive returns false.
	memory.ClearTestHeapPressure()
	r1 := executor.Execute(ctx, task, "w1")
	if !r1.Success {
		t.Fatalf("baseline run failed: %s", r1.Error)
	}
	baselineWall := r1.Duration
	t.Logf("baseline (no pressure) wall=%v", baselineWall)

	// Pressure-on run — the new PauseOnHeapBackpressure call should fire
	// before every src.Next(), adding ~50 ms per file.
	memory.SetTestHeapPressure(true)
	defer memory.ClearTestHeapPressure()

	task.ID = "bp-shuffle-2"
	task.ResultPrefix = "shuffle/q-bp-2/"
	r2 := executor.Execute(ctx, task, "w1")
	if !r2.Success {
		t.Fatalf("pressure run failed: %s", r2.Error)
	}
	pressureWall := r2.Duration
	t.Logf("pressure wall=%v (baseline %v)", pressureWall, baselineWall)

	// Expected lower bound: at least (numFiles - 1) pause iterations fired.
	// Subtracting one allows for the loop-exit iteration that may not pause
	// (early break path) and for one batch's pause being absorbed by other
	// work. Each pause is HeapBackpressurePauseDuration (50ms).
	minExpected := time.Duration(numFiles-1) * memory.HeapBackpressurePauseDuration
	if pressureWall < minExpected {
		t.Errorf("expected pressure run >= %v (at least %d × %v); got %v",
			minExpected, numFiles-1, memory.HeapBackpressurePauseDuration, pressureWall)
	}

	// Sanity: the pressure run should be visibly slower than baseline. If
	// baseline already happens to be slow (CI noise), the assertion above
	// already catches the absolute floor. This is a softer cross-check.
	if pressureWall <= baselineWall {
		t.Errorf("expected pressure run > baseline; pressure=%v baseline=%v",
			pressureWall, baselineWall)
	}

	// Row count must still match — backpressure only delays, never drops.
	if r2.NumRows != numFiles*rowsPerFile {
		t.Errorf("row count: got %d, want %d", r2.NumRows, numFiles*rowsPerFile)
	}
}

// TestExecuteGatherStage_PausesUnderHeapPressure verifies that the
// executeGatherStage per-batch loop calls memory.PauseOnHeapBackpressure
// between batches. Mirrors the shuffle test above for the gather code path
// (executor_stage.go).
//
// Uses embedded NATS so the gather reply sink has a real subject to publish
// to and we can confirm the terminal marker arrives (correctness check
// alongside the timing assertion).
func TestExecuteGatherStage_PausesUnderHeapPressure(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-gather-bp"
	const numFiles = 4
	const rowsPerFile = 50

	// Embedded NATS — required because executeGatherStage publishes the
	// gathered batches to e.nc.
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	en, err := distributed.NewEmbeddedNATS(cfg, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	if err != nil {
		t.Fatalf("embed NATS: %v", err)
	}
	t.Cleanup(en.Shutdown)
	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	const subject = "test.gather.bp"
	var (
		mu        sync.Mutex
		terminals int
		batches   int
	)
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		var msg distributed.GatherBatchMsg
		if err := distributed.Unmarshal(m.Data, &msg); err != nil {
			return
		}
		mu.Lock()
		if msg.Terminal {
			terminals++
		} else {
			batches++
		}
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { sub.Unsubscribe() })

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}

	// Write numFiles small parquet files. executeGatherStage opens each
	// file's source and streams batches via src.Next() — that loop is
	// where the new PauseOnHeapBackpressure call sits.
	inputFiles := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		rows := make([]map[string]any, rowsPerFile)
		for j := 0; j < rowsPerFile; j++ {
			rows[j] = map[string]any{
				"k": int64(i*rowsPerFile + j),
				"v": fmt.Sprintf("value-%d-%d", i, j),
			}
		}
		path := fmt.Sprintf("tables/gather-src/part-%d.parquet", i)
		writeParquetFile(t, store, bucket, path, rows)
		inputFiles[i] = path
	}

	executor := NewExecutor(store, NewLRUCache(64*1024*1024), nil)
	executor.SetNATSConn(nc)
	executor.SetMemoryBudget(0, t.TempDir())

	makeTask := func(id string) distributed.Task {
		return distributed.Task{
			ID:           id,
			QueryID:      "q-gather-bp",
			StageID:      "gather",
			Type:         distributed.TaskTypeGather,
			ReplySubject: subject,
			Inputs:       map[string][]string{"upstream": inputFiles},
			DataBucket:   bucket,
			ResultBucket: bucket,
		}
	}

	// Baseline — no spoofed pressure.
	memory.ClearTestHeapPressure()
	r1 := executor.Execute(ctx, makeTask("bp-gather-1"), "w1")
	if !r1.Success {
		t.Fatalf("baseline run failed: %s", r1.Error)
	}
	baselineWall := r1.Duration

	// Pressure-on run.
	memory.SetTestHeapPressure(true)
	defer memory.ClearTestHeapPressure()
	r2 := executor.Execute(ctx, makeTask("bp-gather-2"), "w1")
	if !r2.Success {
		t.Fatalf("pressure run failed: %s", r2.Error)
	}
	pressureWall := r2.Duration
	t.Logf("gather pressure wall=%v (baseline %v)", pressureWall, baselineWall)

	minExpected := time.Duration(numFiles-1) * memory.HeapBackpressurePauseDuration
	if pressureWall < minExpected {
		t.Errorf("expected gather pressure run >= %v (at least %d × %v); got %v",
			minExpected, numFiles-1, memory.HeapBackpressurePauseDuration, pressureWall)
	}
	if pressureWall <= baselineWall {
		t.Errorf("expected gather pressure run > baseline; pressure=%v baseline=%v",
			pressureWall, baselineWall)
	}

	// Terminal markers and row counts confirm backpressure delayed but didn't
	// drop any batches. Two runs, each publishes a terminal at the end.
	mu.Lock()
	gotBatches := batches
	gotTerminals := terminals
	mu.Unlock()
	if gotTerminals < 2 {
		t.Errorf("expected at least 2 terminals (one per run); got %d", gotTerminals)
	}
	if gotBatches == 0 {
		t.Errorf("expected non-zero batch deliveries; got %d", gotBatches)
	}
}
