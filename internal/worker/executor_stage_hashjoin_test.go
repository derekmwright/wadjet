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

// makeBuildWshf writes a build-side .wshf payload with schema (id int64, val int64).
func makeBuildWshf(t *testing.T, rows [][2]int64) []byte {
	t.Helper()
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "val", Type: parquet.TypeInt64, Nullable: true},
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

// makeProbeWshf writes a probe-side .wshf payload with schema (id int64, name string).
func makeProbeWshf(t *testing.T, rows []struct {
	ID   int64
	Name string
}) []byte {
	t.Helper()
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
	}
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatalf("writeHeader: %v", err)
	}
	b := batch.NewRecordBatch(schema, len(rows))
	for i, r := range rows {
		b.Columns[0].Int64Data[i] = r.ID
		b.Columns[0].Nulls.SetValid(i)
		b.Columns[1].BytesData.Set(i, []byte(r.Name))
		b.Columns[1].Nulls.SetValid(i)
	}
	if err := sw.writeChunk(b.Columns, nil, len(rows)); err != nil {
		t.Fatalf("writeChunk: %v", err)
	}
	data := buf.Bytes()
	data[4] = byte(sw.numChunks)
	return data
}

// TestExecuteStageHashJoin_InnerJoin builds a small build and probe side,
// dispatches a TaskTypeStage hash_join through executeStage, and verifies
// the unpartitioned WSHF output carries the joined rows.
func TestExecuteStageHashJoin_InnerJoin(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-stage"

	buildData := makeBuildWshf(t, [][2]int64{{1, 100}, {2, 200}, {3, 300}})
	probeData := makeProbeWshf(t, []struct {
		ID   int64
		Name string
	}{{1, "alice"}, {2, "bob"}, {4, "dave"}})

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	buildKey := "stage-inputs/build/task-0.wshf"
	probeKey := "stage-inputs/probe/task-0.wshf"
	if _, err := store.Put(ctx, bucket, buildKey, bytes.NewReader(buildData), int64(len(buildData)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, bucket, probeKey, bytes.NewReader(probeData), int64(len(probeData)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}

	cache := NewLRUCache(4 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)

	task := distributed.Task{
		ID:              "hj-test",
		QueryID:         "q-test",
		StageID:         "join-0",
		Type:            distributed.TaskTypeStage,
		StageType:       "hash_join",
		JoinType:        "inner",
		JoinLeftKeys:    []string{"id"},
		JoinRightKeys:   []string{"id"},
		BuildTableAlias: "build",
		Inputs: map[string][]string{
			"build": {buildKey},
			"probe": {probeKey},
		},
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "stage-output/join-0/",
	}

	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}

	if result.NumRows != 2 {
		t.Fatalf("expected 2 joined rows (ids 1+2), got %d", result.NumRows)
	}
	if len(result.ResultFiles) != 1 {
		t.Fatalf("expected 1 output file, got %d: %v", len(result.ResultFiles), result.ResultFiles)
	}
	if !strings.HasPrefix(result.ResultFiles[0], "stage-output/join-0/") ||
		!strings.HasSuffix(result.ResultFiles[0], ".wshf") {
		t.Errorf("unexpected output key: %s", result.ResultFiles[0])
	}
}

// TestExecuteStageHashJoin_PartitionedOutput verifies that when
// ShuffleKeys+NumPartitions are set, the join output is hash-partitioned
// and each non-empty partition is uploaded separately.
func TestExecuteStageHashJoin_PartitionedOutput(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-stage-part"

	buildData := makeBuildWshf(t, [][2]int64{{1, 10}, {2, 20}, {3, 30}, {4, 40}})
	probeData := makeProbeWshf(t, []struct {
		ID   int64
		Name string
	}{{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}})

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	buildKey := "in/build/t0.wshf"
	probeKey := "in/probe/t0.wshf"
	store.Put(ctx, bucket, buildKey, bytes.NewReader(buildData), int64(len(buildData)), "application/octet-stream")
	store.Put(ctx, bucket, probeKey, bytes.NewReader(probeData), int64(len(probeData)), "application/octet-stream")

	cache := NewLRUCache(4 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)

	task := distributed.Task{
		ID:              "hj-part",
		QueryID:         "q",
		StageID:         "join-1",
		Type:            distributed.TaskTypeStage,
		StageType:       "hash_join",
		JoinType:        "inner",
		JoinLeftKeys:    []string{"id"},
		JoinRightKeys:   []string{"id"},
		BuildTableAlias: "build",
		Inputs: map[string][]string{
			"build": {buildKey},
			"probe": {probeKey},
		},
		// Partitioned output: hash joined rows on "id" into 4 partitions.
		ShuffleKeys:   []string{"id"},
		NumPartitions: 4,
		DataBucket:    bucket,
		ResultBucket:  bucket,
		ResultPrefix:  "out/join-1/",
	}

	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != 4 {
		t.Fatalf("expected 4 joined rows, got %d", result.NumRows)
	}
	// 4 joined rows hashed into 4 partitions — at least 1 partition has data,
	// output keys should all match partition=NNNN/ prefix.
	if len(result.ResultFiles) == 0 {
		t.Fatalf("expected at least 1 partition file, got 0")
	}
	for _, k := range result.ResultFiles {
		if !strings.Contains(k, "partition=") || !strings.HasSuffix(k, ".wshf") {
			t.Errorf("output key %q does not match partition=NNNN/*.wshf", k)
		}
	}
}

// TestExecuteStageHashJoin_OutputFilterPrunesColumns verifies that
// task.Columns is wired into probe.OutputFilter so the join's WSHF output
// only contains the requested columns, not the full union of build+probe
// schemas. Regression for the column-pruning audit (2026-05-03).
func TestExecuteStageHashJoin_OutputFilterPrunesColumns(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-stage-outfilter"

	buildData := makeBuildWshf(t, [][2]int64{{1, 100}, {2, 200}, {3, 300}})
	probeData := makeProbeWshf(t, []struct {
		ID   int64
		Name string
	}{{1, "alice"}, {2, "bob"}, {4, "dave"}})

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	buildKey := "in/build/t.wshf"
	probeKey := "in/probe/t.wshf"
	if _, err := store.Put(ctx, bucket, buildKey, bytes.NewReader(buildData), int64(len(buildData)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, bucket, probeKey, bytes.NewReader(probeData), int64(len(probeData)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}

	cache := NewLRUCache(4 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)

	// Without OutputFilter the join would emit the full union {id, val, name}.
	// With Columns=["name"] only the probe-side string column should remain.
	task := distributed.Task{
		ID:              "hj-outfilter",
		QueryID:         "q",
		StageID:         "join-out",
		Type:            distributed.TaskTypeStage,
		StageType:       "hash_join",
		JoinType:        "inner",
		JoinLeftKeys:    []string{"id"},
		JoinRightKeys:   []string{"id"},
		BuildTableAlias: "build",
		Columns:         []string{"name"},
		Inputs: map[string][]string{
			"build": {buildKey},
			"probe": {probeKey},
		},
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/join-out/",
	}

	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != 2 {
		t.Fatalf("expected 2 joined rows (ids 1+2), got %d", result.NumRows)
	}
	if len(result.ResultFiles) != 1 {
		t.Fatalf("expected 1 output file, got %d", len(result.ResultFiles))
	}

	got, _, err := store.Get(ctx, bucket, result.ResultFiles[0])
	if err != nil {
		t.Fatalf("get result file: %v", err)
	}
	defer got.Close()
	var raw bytes.Buffer
	if _, err := raw.ReadFrom(got); err != nil {
		t.Fatalf("read result file: %v", err)
	}
	payload, err := DecompressShuffleData(raw.Bytes())
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	rdr, err := newShuffleChunkReader(payload)
	if err != nil {
		t.Fatalf("newShuffleChunkReader: %v", err)
	}
	if len(rdr.schema) != 1 {
		names := make([]string, len(rdr.schema))
		for i, c := range rdr.schema {
			names[i] = c.Name
		}
		t.Fatalf("expected 1 output column with OutputFilter=[name], got %d: %v", len(rdr.schema), names)
	}
	if rdr.schema[0].Name != "name" {
		t.Errorf("expected output column 'name', got %q", rdr.schema[0].Name)
	}
}

// TestExecuteStageHashJoin_OutputFilterEmptyAllowsAllColumns verifies that
// when task.Columns is empty (e.g. tasks dispatched before the audit fix
// landed, or stages without a tighter projection), the probe behaves as
// before: emit all build+probe columns.
func TestExecuteStageHashJoin_OutputFilterEmptyAllowsAllColumns(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-stage-noflt"

	buildData := makeBuildWshf(t, [][2]int64{{1, 10}})
	probeData := makeProbeWshf(t, []struct {
		ID   int64
		Name string
	}{{1, "x"}})

	store := objstore.NewMemStore()
	_ = store.MakeBucket(ctx, bucket)
	store.Put(ctx, bucket, "b.wshf", bytes.NewReader(buildData), int64(len(buildData)), "application/octet-stream")
	store.Put(ctx, bucket, "p.wshf", bytes.NewReader(probeData), int64(len(probeData)), "application/octet-stream")

	cache := NewLRUCache(4 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)

	task := distributed.Task{
		ID:              "hj-nofilter",
		QueryID:         "q",
		StageID:         "join-nf",
		Type:            distributed.TaskTypeStage,
		StageType:       "hash_join",
		JoinType:        "inner",
		JoinLeftKeys:    []string{"id"},
		JoinRightKeys:   []string{"id"},
		BuildTableAlias: "build",
		// Columns: nil — preserve legacy "emit all" semantics.
		Inputs: map[string][]string{
			"build": {"b.wshf"},
			"probe": {"p.wshf"},
		},
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/join-nf/",
	}

	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	got, _, err := store.Get(ctx, bucket, result.ResultFiles[0])
	if err != nil {
		t.Fatalf("get result file: %v", err)
	}
	defer got.Close()
	var raw bytes.Buffer
	raw.ReadFrom(got)
	payload, err := DecompressShuffleData(raw.Bytes())
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	rdr, err := newShuffleChunkReader(payload)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	if len(rdr.schema) < 2 {
		t.Errorf("expected ≥2 output columns when Columns is nil (full union), got %d", len(rdr.schema))
	}
}
