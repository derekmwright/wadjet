package worker

import (
	"bytes"
	"context"
	"hash/fnv"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestExecuteFragment_ScanFilterAggregateUnpartitioned exercises the
// scan-aggregate fragment shape that today's dispatchScanAggregateStage
// emits via buildScanAggregateFragment:
//
//	[OpScan, OpFilter, OpHashAggregate(partial), OpUnpartitionedSink]
//
// Verifies (a) the parquet/wshf source reads through OpScan with column
// projection, (b) the OpFilter discards rows that don't match the WHERE,
// (c) the partial HashAggregate (MergeMode=false) runs to completion,
// (d) output is one .wshf with GROUP BY + agg-output columns.
//
// Test data: 5 rows with (g, v). Filter keeps rows where v >= 20. Sums
// per group: g=1 sum(20), g=2 sum(50), g=3 sum(60).
func TestExecuteFragment_ScanFilterAggregateUnpartitioned(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-fragment-scan-filt-agg"

	data := makeGroupedWshf(t, [][2]int64{
		{1, 10}, {1, 20}, // g=1 → after filter v>=20: 20
		{2, 50}, // g=2 → 50
		{3, 30}, {3, 30}, // g=3 → 60
	})

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	key := "in/sfa/t0.wshf"
	if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	cache := NewLRUCache(4 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)

	task := distributed.Task{
		ID:           "frag-sfa-0",
		QueryID:      "q-frag-sfa",
		StageID:      "sfa-0",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/sfa/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpScan,
				InputAlias:  "src",
				InputFiles:  []string{key},
				InputBucket: bucket,
				Columns:     []string{"g", "v"},
			},
			{
				Type:       distributed.OpFilter,
				Predicates: []string{"v >= 20"},
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

	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != 3 {
		t.Fatalf("NumRows = %d, want 3 groups", result.NumRows)
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
	got := map[int64]float64{}
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
			got[b.Columns[gIdx].Int64Data[row]] = b.Columns[totalIdx].Float64Data[row]
		}
	}
	want := map[int64]float64{1: 20, 2: 50, 3: 60}
	for g, v := range want {
		if got[g] != v {
			t.Errorf("group %d: got total=%v, want %v", g, got[g], v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("group count: got %d, want %d (%v)", len(got), len(want), got)
	}
}

// TestExecuteFragment_ScanExchangeSender exercises the fragment dispatch path
// end-to-end with the simplest non-trivial pipeline: a scan source feeding an
// exchange-sender sink. Output partition files must match the same shape and
// hash distribution as the legacy per-StageType path
// (TestExecuteStageScan_PartitionedStreamingOutput).
//
// Why this matters: this is the shape PR #85 was trying to express via a
// dedicated planner pass + bespoke coord propagation. With the fragment
// model, "scan + exchange-sender" is just a 2-element Operators[] — no
// special-case logic in the dispatch layer.
func TestExecuteFragment_ScanExchangeSender(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-fragment-scan-exch"
	const numParts = 4
	const numRows = 1000

	rows := make([][2]int64, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = [2]int64{int64(i), int64(i) * 10}
	}
	scanData := makeScanWshf(t, rows)

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	scanKey := "in/scan/t0.wshf"
	if _, err := store.Put(ctx, bucket, scanKey, bytes.NewReader(scanData), int64(len(scanData)), "application/octet-stream"); err != nil {
		t.Fatalf("Put scan input: %v", err)
	}

	cache := NewLRUCache(4 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)

	task := distributed.Task{
		ID:           "frag-scan-exch",
		QueryID:      "q-frag",
		StageID:      "scan-1",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/scan-1/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpScan,
				InputAlias:  "t",
				InputFiles:  []string{scanKey},
				InputBucket: bucket,
				Columns:     []string{"id", "val"},
			},
			{
				Type:          distributed.OpExchangeSender,
				ShuffleKeys:   []string{"id"},
				NumPartitions: numParts,
			},
		},
	}

	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != int64(numRows) {
		t.Fatalf("NumRows = %d, want %d", result.NumRows, numRows)
	}
	if len(result.ResultFiles) == 0 {
		t.Fatalf("expected at least 1 partition file, got 0")
	}

	keySchema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
	var scratch [8]byte
	totalRows := 0
	seen := make(map[int64]bool, numRows)
	for _, key := range result.ResultFiles {
		idx := strings.Index(key, "partition=")
		if idx < 0 {
			t.Fatalf("missing partition= in key %q", key)
		}
		partition, err := strconv.Atoi(key[idx+len("partition=") : idx+len("partition=")+4])
		if err != nil {
			t.Fatalf("parsing partition from %q: %v", key, err)
		}
		ids := readMemStoreInts(t, store, bucket, key, "id")
		for _, id := range ids {
			if seen[id] {
				t.Errorf("id %d appeared in multiple partitions", id)
			}
			seen[id] = true
			keyBatch := batch.NewRecordBatch(keySchema, 1)
			keyBatch.Columns[0].Int64Data[0] = id
			keyBatch.Columns[0].Nulls.SetValid(0)
			h := fnv.New64a()
			hashVectorValue(h, keyBatch.Columns[0], 0, scratch[:])
			got := int(h.Sum64() % uint64(numParts))
			if got != partition {
				t.Errorf("id=%d landed in partition=%d, hashes to %d", id, partition, got)
			}
		}
		totalRows += len(ids)
	}
	if totalRows != numRows {
		t.Errorf("total rows across partitions = %d, want %d", totalRows, numRows)
	}
}

// TestExecuteFragment_ScanHashAggregateUnpartitioned exercises the
// pipeline-breaker code path: scan source → HashAggregate (partial) →
// unpartitioned sink. Three groups in input → three rows in output, each
// summing the per-group values. Validates the consume/drain split that
// HashAggregate forces (it consumes all input, then emits results).
func TestExecuteFragment_ScanHashAggregateUnpartitioned(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-fragment-agg"

	// 3 groups: g=1 sum=30, g=2 sum=50, g=3 sum=60
	data := makeGroupedWshf(t, [][2]int64{
		{1, 10}, {1, 20}, {2, 50}, {3, 30}, {3, 30},
	})

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	key := "in/agg/t0.wshf"
	if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	cache := NewLRUCache(4 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)

	task := distributed.Task{
		ID:           "frag-agg-0",
		QueryID:      "q-frag-agg",
		StageID:      "agg-0",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/agg/",
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
				Type: distributed.OpUnpartitionedSink,
			},
		},
	}

	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != 3 {
		t.Fatalf("NumRows = %d, want 3 groups", result.NumRows)
	}
	if len(result.ResultFiles) != 1 {
		t.Fatalf("expected 1 unpartitioned output, got %d", len(result.ResultFiles))
	}

	// Read output and verify (group, total) pairs.
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
	got := map[int64]float64{}
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
			got[b.Columns[gIdx].Int64Data[row]] = b.Columns[totalIdx].Float64Data[row]
		}
	}
	want := map[int64]float64{1: 30, 2: 50, 3: 60}
	for g, v := range want {
		if got[g] != v {
			t.Errorf("group %d: got total=%v, want %v", g, got[g], v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("group count: got %d, want %d (%v)", len(got), len(want), got)
	}
}

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

// TestExecuteFragment_ScanFilterUnpartitioned exercises a 3-op fragment:
// scan source → filter (l_orderkey > 500) → unpartitioned sink. Verifies
// the unary-op chain runs and the unpartitioned sink path uploads the
// filtered output as a single .wshf.
// TestExecuteFragment_HashJoinProbe_FlushesSpilledPartitions is the unit
// regression test for the Q05 SF100 build-cache 0-rows bug. Before the fix,
// runFragmentLinear had no flush phase: when the build's only data partition
// got evicted under memory pressure, every probe row got routed to disk and
// the join silently produced 0 rows. The legacy executeStageHashJoin path
// drove probes through exec.Pipeline which already includes flushSpilledOps;
// the fragment runner did not.
//
// This test forces the build's hash partition to spill (tiny shared budget +
// PartitionOnArrival via OpBroadcastProbe) and asserts every matching
// probe→build pair appears in the joined output.
func TestExecuteFragment_HashJoinProbe_FlushesSpilledPartitions(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-fragment-hj-flush"

	// Build side spans multiple files so the source emits multiple batches.
	// The shared budget is sized so the first batch fits but the second
	// triggers spillUntilCanReserve, which evicts the largest in-memory
	// partition. Subsequent batches scattered to that partition write
	// directly to disk; probe rows hashing there route through the spill
	// flush path. PartitionOnArrival=true is forced by OpBroadcastProbe.
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
	// Probe side: every probe row matches a build row.
	const probeN = 4096
	probeRows := make([]struct {
		ID   int64
		Name string
	}, probeN)
	for i := range probeRows {
		probeRows[i] = struct {
			ID   int64
			Name string
		}{ID: int64(i % buildN), Name: "row-" + strconv.Itoa(i)}
	}
	probeData := makeProbeWshf(t, probeRows)

	probeKey := "in/hj/probe.wshf"
	if _, err := store.Put(ctx, bucket, probeKey, bytes.NewReader(probeData), int64(len(probeData)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}

	cache := NewLRUCache(4 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	// Budget sized so reactive spill in buildPartitioned fires between
	// batches. Each 2048-row batch tracks ~94 KB. A 200 KB budget puts the
	// 60% spill-cheap threshold (120 KB) between batch 1 and batch 2 — the
	// post-batch ShouldSpillFor check triggers a partition eviction before
	// the third batch lands. Probe rows hashing to the spilled partition
	// route through the flush phase. Without that phase, those probe rows
	// are silently dropped.
	executor.SetSharedPoolBudget(500 * 1024)

	task := distributed.Task{
		ID:           "frag-hj-flush",
		QueryID:      "q-frag-hj-flush",
		StageID:      "join-0",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/join-0/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpShuffleSource,
				InputAlias:  "probe",
				InputFiles:  []string{probeKey},
				InputBucket: bucket,
			},
			{
				// OpBroadcastProbe forces PartitionOnArrival=true on the
				// HashJoin (see buildFragmentJoinProbe). The tight shared
				// budget triggers a spill once the second build batch
				// arrives, putting probe-routing rows on disk.
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
	// Every probe row matches a build row. Without the flush phase, probes
	// hashing to spilled partitions are silently dropped; the test passes
	// only when every probe row is accounted for in the joined output.
	if result.NumRows != int64(probeN) {
		t.Fatalf("NumRows = %d, want %d (flush of spilled HashJoin partitions failed)",
			result.NumRows, probeN)
	}
}

func TestExecuteFragment_ScanFilterUnpartitioned(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-fragment-scan-filt"
	const numRows = 1000

	rows := make([][2]int64, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = [2]int64{int64(i), int64(i) * 10}
	}
	scanData := makeScanWshf(t, rows)

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	scanKey := "in/scan/t0.wshf"
	if _, err := store.Put(ctx, bucket, scanKey, bytes.NewReader(scanData), int64(len(scanData)), "application/octet-stream"); err != nil {
		t.Fatalf("Put scan input: %v", err)
	}

	cache := NewLRUCache(4 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)

	task := distributed.Task{
		ID:           "frag-scan-filt",
		QueryID:      "q-frag-2",
		StageID:      "scan-1",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/scan-1/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpScan,
				InputAlias:  "t",
				InputFiles:  []string{scanKey},
				InputBucket: bucket,
				Columns:     []string{"id", "val"},
			},
			{
				Type:       distributed.OpFilter,
				Predicates: []string{"id > 500"},
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
	const wantRows = 499 // ids 501..999
	if result.NumRows != wantRows {
		t.Fatalf("NumRows = %d, want %d", result.NumRows, wantRows)
	}
	if len(result.ResultFiles) != 1 {
		t.Fatalf("expected 1 unpartitioned output, got %d", len(result.ResultFiles))
	}
	got := readMemStoreInts(t, store, bucket, result.ResultFiles[0], "id")
	if len(got) != wantRows {
		t.Fatalf("partition row count = %d, want %d", len(got), wantRows)
	}
	for _, v := range got {
		if v <= 500 {
			t.Errorf("filter passed row id=%d (should have been filtered)", v)
		}
	}
}
