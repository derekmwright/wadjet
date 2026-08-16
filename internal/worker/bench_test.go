package worker

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// --- ResultStore micro-benchmarks ---

func BenchmarkResultStorePut(b *testing.B) {
	rs := NewResultStore(1024 * 1024 * 1024) // 1 GB
	data := make([]byte, 64*1024)            // 64 KB chunks
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("q1/stage/task-%d.parquet", i)
		rs.Put("q1", path, data)
	}
}

func BenchmarkResultStoreGet(b *testing.B) {
	rs := NewResultStore(1024 * 1024 * 1024)
	data := make([]byte, 64*1024)
	// Pre-populate
	for i := 0; i < 1000; i++ {
		path := fmt.Sprintf("q1/stage/task-%d.parquet", i)
		rs.Put("q1", path, data)
	}
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("q1/stage/task-%d.parquet", i%1000)
		rs.Get(path)
	}
}

func BenchmarkResultStoreGetMiss(b *testing.B) {
	rs := NewResultStore(1024 * 1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rs.Get("nonexistent/path.parquet")
	}
}

func BenchmarkResultStorePutGetRoundTrip(b *testing.B) {
	rs := NewResultStore(1024 * 1024 * 1024)
	data := make([]byte, 64*1024)
	b.SetBytes(int64(len(data)) * 2) // put + get
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("q%d/stage/task.parquet", i)
		rs.Put("q1", path, data)
		rs.Get(path)
	}
}

func BenchmarkResultStoreCleanupQuery(b *testing.B) {
	for _, numResults := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("results=%d", numResults), func(b *testing.B) {
			data := make([]byte, 1024)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				rs := NewResultStore(1024 * 1024 * 1024)
				for j := 0; j < numResults; j++ {
					rs.Put("q1", fmt.Sprintf("path-%d", j), data)
				}
				b.StartTimer()
				rs.CleanupQuery("q1")
			}
		})
	}
}

// --- Executor pipeline benchmarks: S3 path vs ResultStore vs inline ---

func benchSetup(b *testing.B, numRows int) (*objstore.MemStore, []map[string]any) {
	b.Helper()
	store := objstore.NewMemStore()
	ctx := context.Background()
	if err := store.MakeBucket(ctx, "bench"); err != nil {
		b.Fatal(err)
	}

	rows := make([]map[string]any, numRows)
	for i := range rows {
		rows[i] = map[string]any{
			"user_id": fmt.Sprintf("u%d", i%100),
			"amount":  float64(i * 10),
			"region":  fmt.Sprintf("r%d", i%5),
		}
	}

	// Write input parquet
	schema := schemaFromRows(rows)
	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, parquet.Schema{Columns: schema}, parquet.DefaultWriterConfig())
	if err != nil {
		b.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		b.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		b.Fatal(err)
	}
	data := buf.Bytes()
	if _, err := store.Put(ctx, "bench", "input/data.parquet", bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		b.Fatal(err)
	}

	return store, rows
}

func BenchmarkExecutorSort_S3Path(b *testing.B) {
	for _, numRows := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			store, _ := benchSetup(b, numRows)
			cache := NewLRUCache(64 * 1024 * 1024)
			executor := NewExecutor(store, cache, nil)
			ctx := context.Background()

			task := distributed.Task{
				ID:           "bench-sort",
				QueryID:      "q-bench",
				StageID:      "s1",
				Type:         distributed.TaskType("sort"),
				InputFiles:   []string{"input/data.parquet"},
				SortKeys:     []distributed.SortKeySpec{{Column: "amount", Desc: true}},
				ResultBucket: "bench",
				ResultPrefix: "output/",
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result := executor.Execute(ctx, task, "w1")
				if !result.Success {
					b.Fatalf("sort failed: %s", result.Error)
				}
			}
		})
	}
}

func BenchmarkExecutorSort_ResultStore(b *testing.B) {
	for _, numRows := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			store, _ := benchSetup(b, numRows)
			cache := NewLRUCache(64 * 1024 * 1024)
			executor := NewExecutor(store, cache, nil)
			rs := NewResultStore(256 * 1024 * 1024) // 256 MB
			executor.SetResultStore(rs)
			ctx := context.Background()

			task := distributed.Task{
				ID:           "bench-sort",
				QueryID:      "q-bench",
				StageID:      "s1",
				Type:         distributed.TaskType("sort"),
				InputFiles:   []string{"input/data.parquet"},
				SortKeys:     []distributed.SortKeySpec{{Column: "amount", Desc: true}},
				ResultBucket: "bench",
				ResultPrefix: "output/",
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// Clean result store each iteration so Put succeeds
				rs.CleanupQuery("q-bench")
				result := executor.Execute(ctx, task, "w1")
				if !result.Success {
					b.Fatalf("sort failed: %s", result.Error)
				}
			}
		})
	}
}

func BenchmarkExecutorAggregate_S3Path(b *testing.B) {
	for _, numRows := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			store, _ := benchSetup(b, numRows)
			cache := NewLRUCache(64 * 1024 * 1024)
			executor := NewExecutor(store, cache, nil)
			ctx := context.Background()

			task := distributed.Task{
				ID:          "bench-agg",
				QueryID:     "q-bench",
				StageID:     "s1",
				Type:        distributed.TaskType("aggregate"),
				InputFiles:  []string{"input/data.parquet"},
				GroupByCols: []string{"region"},
				Aggregates: []distributed.AggSpec{
					{Func: "sum", InputCol: "amount", OutputCol: "total"},
					{Func: "count", InputCol: "amount", OutputCol: "cnt"},
				},
				ResultBucket: "bench",
				ResultPrefix: "output/",
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result := executor.Execute(ctx, task, "w1")
				if !result.Success {
					b.Fatalf("aggregate failed: %s", result.Error)
				}
			}
		})
	}
}

func BenchmarkExecutorAggregate_ResultStore(b *testing.B) {
	for _, numRows := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			store, _ := benchSetup(b, numRows)
			cache := NewLRUCache(64 * 1024 * 1024)
			executor := NewExecutor(store, cache, nil)
			rs := NewResultStore(256 * 1024 * 1024)
			executor.SetResultStore(rs)
			ctx := context.Background()

			task := distributed.Task{
				ID:          "bench-agg",
				QueryID:     "q-bench",
				StageID:     "s1",
				Type:        distributed.TaskType("aggregate"),
				InputFiles:  []string{"input/data.parquet"},
				GroupByCols: []string{"region"},
				Aggregates: []distributed.AggSpec{
					{Func: "sum", InputCol: "amount", OutputCol: "total"},
					{Func: "count", InputCol: "amount", OutputCol: "cnt"},
				},
				ResultBucket: "bench",
				ResultPrefix: "output/",
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rs.CleanupQuery("q-bench")
				result := executor.Execute(ctx, task, "w1")
				if !result.Success {
					b.Fatalf("aggregate failed: %s", result.Error)
				}
			}
		})
	}
}

// --- Two-stage pipeline: measures the actual S3 round-trip avoidance ---

func BenchmarkTwoStagePipeline_S3(b *testing.B) {
	for _, numRows := range []int{100, 1000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			store, _ := benchSetup(b, numRows)
			cache := NewLRUCache(64 * 1024 * 1024)
			executor := NewExecutor(store, cache, nil)
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// Stage 1: aggregate
				task1 := distributed.Task{
					ID:          fmt.Sprintf("s1-%d", i),
					QueryID:     fmt.Sprintf("q-%d", i),
					StageID:     "s1",
					Type:        distributed.TaskType("aggregate"),
					InputFiles:  []string{"input/data.parquet"},
					GroupByCols: []string{"region"},
					Aggregates: []distributed.AggSpec{
						{Func: "sum", InputCol: "amount", OutputCol: "total"},
					},
					ResultBucket: "bench",
					ResultPrefix: "stage1/",
				}
				r1 := executor.Execute(ctx, task1, "w1")
				if !r1.Success {
					b.Fatalf("stage 1 failed: %s", r1.Error)
				}

				// Stage 2: sort the aggregated results
				var inputFiles []string
				if r1.ResultPath != "" {
					inputFiles = []string{r1.ResultPath}
				} else {
					b.Skip("result was inlined, S3 path not exercised")
				}

				task2 := distributed.Task{
					ID:           fmt.Sprintf("s2-%d", i),
					QueryID:      fmt.Sprintf("q-%d", i),
					StageID:      "s2",
					Type:         distributed.TaskType("sort"),
					InputFiles:   inputFiles,
					SortKeys:     []distributed.SortKeySpec{{Column: "total", Desc: true}},
					ResultBucket: "bench",
					ResultPrefix: "stage2/",
				}
				r2 := executor.Execute(ctx, task2, "w1")
				if !r2.Success {
					b.Fatalf("stage 2 failed: %s", r2.Error)
				}
			}
		})
	}
}

func BenchmarkTwoStagePipeline_ResultStore(b *testing.B) {
	for _, numRows := range []int{100, 1000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			store, _ := benchSetup(b, numRows)
			cache := NewLRUCache(64 * 1024 * 1024)
			executor := NewExecutor(store, cache, nil)
			rs := NewResultStore(256 * 1024 * 1024)
			executor.SetResultStore(rs)
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				queryID := fmt.Sprintf("q-%d", i)

				// Stage 1: aggregate
				task1 := distributed.Task{
					ID:          fmt.Sprintf("s1-%d", i),
					QueryID:     queryID,
					StageID:     "s1",
					Type:        distributed.TaskType("aggregate"),
					InputFiles:  []string{"input/data.parquet"},
					GroupByCols: []string{"region"},
					Aggregates: []distributed.AggSpec{
						{Func: "sum", InputCol: "amount", OutputCol: "total"},
					},
					ResultBucket: "bench",
					ResultPrefix: "stage1/",
				}
				r1 := executor.Execute(ctx, task1, "w1")
				if !r1.Success {
					b.Fatalf("stage 1 failed: %s", r1.Error)
				}

				// Stage 2: sort the aggregated results
				var inputFiles []string
				if r1.ResultPath != "" {
					inputFiles = []string{r1.ResultPath}
				} else {
					b.Skip("result was inlined, result store not exercised")
				}

				task2 := distributed.Task{
					ID:           fmt.Sprintf("s2-%d", i),
					QueryID:      queryID,
					StageID:      "s2",
					Type:         distributed.TaskType("sort"),
					InputFiles:   inputFiles,
					SortKeys:     []distributed.SortKeySpec{{Column: "total", Desc: true}},
					ResultBucket: "bench",
					ResultPrefix: "stage2/",
				}
				r2 := executor.Execute(ctx, task2, "w1")
				if !r2.Success {
					b.Fatalf("stage 2 failed: %s", r2.Error)
				}

				rs.CleanupQuery(queryID)
			}
		})
	}
}

// --- Spill overhead ---

func BenchmarkExecutorSort_NoSpill(b *testing.B) {
	store, _ := benchSetup(b, 1000)
	cache := NewLRUCache(64 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	// No memory budget = no spill manager created
	ctx := context.Background()

	task := distributed.Task{
		ID:           "bench-sort",
		QueryID:      "q-bench",
		StageID:      "s1",
		Type:         distributed.TaskType("sort"),
		InputFiles:   []string{"input/data.parquet"},
		SortKeys:     []distributed.SortKeySpec{{Column: "amount", Desc: true}},
		ResultBucket: "bench",
		ResultPrefix: "output/",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result := executor.Execute(ctx, task, "w1")
		if !result.Success {
			b.Fatalf("sort failed: %s", result.Error)
		}
	}
}

func BenchmarkExecutorSort_WithBudget(b *testing.B) {
	store, _ := benchSetup(b, 1000)
	cache := NewLRUCache(64 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	// Large budget — SpillManager exists but never triggers
	executor.SetMemoryBudget(1024*1024*1024, b.TempDir()) // 1 GB
	ctx := context.Background()

	task := distributed.Task{
		ID:           "bench-sort",
		QueryID:      "q-bench",
		StageID:      "s1",
		Type:         distributed.TaskType("sort"),
		InputFiles:   []string{"input/data.parquet"},
		SortKeys:     []distributed.SortKeySpec{{Column: "amount", Desc: true}},
		ResultBucket: "bench",
		ResultPrefix: "output/",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result := executor.Execute(ctx, task, "w1")
		if !result.Success {
			b.Fatalf("sort failed: %s", result.Error)
		}
	}
}

func BenchmarkExecutorSort_SpillForced(b *testing.B) {
	store, _ := benchSetup(b, 1000)
	cache := NewLRUCache(64 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	// Tiny budget — forces spill
	executor.SetMemoryBudget(256, b.TempDir())
	ctx := context.Background()

	task := distributed.Task{
		ID:           "bench-sort",
		QueryID:      "q-bench",
		StageID:      "s1",
		Type:         distributed.TaskType("sort"),
		InputFiles:   []string{"input/data.parquet"},
		SortKeys:     []distributed.SortKeySpec{{Column: "amount", Desc: true}},
		ResultBucket: "bench",
		ResultPrefix: "output/",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result := executor.Execute(ctx, task, "w1")
		if !result.Success {
			b.Fatalf("sort failed: %s", result.Error)
		}
	}
}
