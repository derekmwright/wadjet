package worker

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// makeGroupedWshf writes rows with (group int64, value int64) schema.
func makeGroupedWshf(t *testing.T, rows [][2]int64) []byte {
	t.Helper()
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeInt64, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatalf("writeHeader: %v", err)
	}
	b := batch.NewRecordBatch(schema, len(rows))
	for i, r := range rows {
		b.Columns[0].Int64Data[i] = r[0]
		b.Columns[0].Nulls.SetValid(i)
		b.Columns[1].Int64Data[i] = r[1]
		b.Columns[1].Nulls.SetValid(i)
	}
	if err := sw.writeChunk(b.Columns, nil, len(rows)); err != nil {
		t.Fatalf("writeChunk: %v", err)
	}
	data := buf.Bytes()
	data[4] = byte(sw.numChunks)
	return data
}

func TestExecuteStageAggregate_SumByGroup(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-agg"

	// 3 groups: g=1 sum=30, g=2 sum=50, g=3 sum=60
	data := makeGroupedWshf(t, [][2]int64{
		{1, 10}, {1, 20}, {2, 50}, {3, 30}, {3, 30},
	})

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	key := "in/agg.wshf"
	store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream")

	executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)

	task := distributed.Task{
		ID:          "agg-t0",
		QueryID:     "q",
		StageID:     "agg-0",
		Type:        distributed.TaskTypeStage,
		StageType:   "aggregate",
		GroupByCols: []string{"g"},
		Aggregates: []distributed.AggSpec{
			{Func: "sum", InputCol: "v", OutputCol: "total"},
		},
		Inputs: map[string][]string{
			"src": {key},
		},
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/agg/",
	}

	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != 3 {
		t.Fatalf("expected 3 groups, got %d rows", result.NumRows)
	}
	if len(result.ResultFiles) != 1 {
		t.Fatalf("expected 1 output file, got %d", len(result.ResultFiles))
	}
	if !strings.HasSuffix(result.ResultFiles[0], ".wshf") {
		t.Errorf("output should be .wshf, got %s", result.ResultFiles[0])
	}
}

func TestExecuteStageSort_Ascending(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-sort"

	// Unsorted rows; expect ascending by v after sort.
	data := makeGroupedWshf(t, [][2]int64{
		{1, 30}, {2, 10}, {3, 20}, {4, 40},
	})

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	key := "in/sort.wshf"
	store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream")

	executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)

	task := distributed.Task{
		ID:        "sort-t0",
		QueryID:   "q",
		StageID:   "sort-0",
		Type:      distributed.TaskTypeStage,
		StageType: "sort",
		SortKeys:  []distributed.SortKeySpec{{Column: "v", Desc: false}},
		Limit:     2, // top-2: should emit rows with v=10, 20
		Inputs: map[string][]string{
			"src": {key},
		},
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/sort/",
	}

	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != 2 {
		t.Fatalf("expected 2 rows after limit, got %d", result.NumRows)
	}
}
