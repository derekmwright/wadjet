// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/wshf"
)

// TestExecuteShuffle_HappyPath verifies that a shuffle task reads source Parquet
// files, hash-partitions rows across NumPartitions output files, uploads each
// non-empty partition to MemStore, and reports the correct total row count.
func TestExecuteShuffle_HappyPath(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-shuffle-exec"
	const numParts = 4
	const numRows = 100

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}

	// Write a Parquet file with 100 rows of (k INT64, v STRING).
	rows := make([]map[string]any, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = map[string]any{
			"k": int64(i),
			"v": fmt.Sprintf("value-%d", i),
		}
	}
	writeParquetFile(t, store, bucket, "tables/src/part-0.parquet", rows)

	cache := NewLRUCache(64 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil /* js — not needed for shuffle */)
	executor.SetMemoryBudget(0, t.TempDir())

	task := distributed.Task{
		ID:            "shuf-1",
		QueryID:       "q-shuf",
		StageID:       "shuffle-0",
		Type:          distributed.TaskTypeShuffle,
		Files:         []string{"tables/src/part-0.parquet"},
		ColumnTypes:   declaredFromRows(rows),
		ShuffleKeys:   []string{"k"},
		NumPartitions: numParts,
		DataBucket:    bucket,
		ResultBucket:  bucket,
		ResultPrefix:  "shuffle/q-shuf/s0/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("shuffle task failed: %s", result.Error)
	}
	if result.NumRows != numRows {
		t.Errorf("expected %d rows, got %d", numRows, result.NumRows)
	}
	if len(result.ResultFiles) == 0 {
		t.Fatal("expected at least one partition file in ResultFiles")
	}
	if result.SizeBytes == 0 {
		t.Errorf("result.SizeBytes = 0; expected nonzero after writing 100 rows")
	}
	t.Logf("shuffle: %d rows → %d partition files, %d bytes",
		result.NumRows, len(result.ResultFiles), result.SizeBytes)

	// Per-partition accounting: vectors sized to NumPartitions, rows sum to
	// the task total, bytes sum to SizeBytes, and a partition has bytes iff
	// it has rows.
	if len(result.PartitionRows) != numParts || len(result.PartitionBytes) != numParts {
		t.Fatalf("partition vectors len = %d/%d, want %d",
			len(result.PartitionRows), len(result.PartitionBytes), numParts)
	}
	var vecRows, vecBytes int64
	for p := 0; p < numParts; p++ {
		vecRows += result.PartitionRows[p]
		vecBytes += result.PartitionBytes[p]
		if (result.PartitionRows[p] == 0) != (result.PartitionBytes[p] == 0) {
			t.Errorf("partition %d: rows=%d bytes=%d — must be zero/nonzero together",
				p, result.PartitionRows[p], result.PartitionBytes[p])
		}
	}
	if vecRows != numRows {
		t.Errorf("sum(PartitionRows) = %d, want %d", vecRows, numRows)
	}
	if vecBytes != result.SizeBytes {
		t.Errorf("sum(PartitionBytes) = %d, want SizeBytes %d", vecBytes, result.SizeBytes)
	}

	// Verify each ResultFile key follows the expected naming convention and exists.
	for _, key := range result.ResultFiles {
		if !strings.Contains(key, "partition=") {
			t.Errorf("result file key %q does not contain partition= prefix", key)
		}
		if !strings.HasSuffix(key, ".wshf") {
			t.Errorf("result file key %q does not end with .wshf", key)
		}
		rc, _, err := store.Get(ctx, bucket, key)
		if err != nil {
			t.Fatalf("result file %q not found in store: %v", key, err)
		}
		rc.Close()
	}

	// Read back all output files and sum rows to verify no row is lost.
	totalBack := int64(0)
	for _, key := range result.ResultFiles {
		rc, _, err := store.Get(ctx, bucket, key)
		if err != nil {
			t.Fatalf("reading back %q: %v", key, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("reading back %q data: %v", key, err)
		}
		data, err = DecompressShuffleData(data)
		if err != nil {
			t.Fatalf("decompressing %q: %v", key, err)
		}
		r, err := wshf.NewChunkReader(data)
		if err != nil {
			t.Fatalf("parsing %q: %v", key, err)
		}
		for {
			b, err := r.Next()
			if err != nil {
				t.Fatalf("chunk reader Next from %q: %v", key, err)
			}
			if b == nil {
				break
			}
			totalBack += int64(b.ActiveLen())
		}
	}
	if totalBack != numRows {
		t.Errorf("read-back total rows = %d, want %d", totalBack, numRows)
	}
}

// TestExecuteShuffle_ColumnProjection verifies that Columns projection is respected:
// only the projected columns appear in the output.
func TestExecuteShuffle_ColumnProjection(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-shuffle-proj"

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}

	rows := []map[string]any{
		{"k": int64(1), "v": "a", "extra": "x"},
		{"k": int64(2), "v": "b", "extra": "y"},
		{"k": int64(3), "v": "c", "extra": "z"},
	}
	writeParquetFile(t, store, bucket, "tables/src/proj-0.parquet", rows)

	cache := NewLRUCache(64 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	executor.SetMemoryBudget(0, t.TempDir())

	task := distributed.Task{
		ID:            "shuf-proj",
		QueryID:       "q-proj",
		StageID:       "shuffle-0",
		Type:          distributed.TaskTypeShuffle,
		Files:         []string{"tables/src/proj-0.parquet"},
		ColumnTypes:   declaredFromRows(rows),
		Columns:       []string{"k", "v"},
		ShuffleKeys:   []string{"k"},
		NumPartitions: 2,
		DataBucket:    bucket,
		ResultBucket:  bucket,
		ResultPrefix:  "shuffle/q-proj/s0/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("shuffle (projection) failed: %s", result.Error)
	}
	if result.NumRows != 3 {
		t.Errorf("expected 3 rows, got %d", result.NumRows)
	}

	// Confirm projected columns only (k, v) appear — not "extra".
	for _, key := range result.ResultFiles {
		rc, _, err := store.Get(ctx, bucket, key)
		if err != nil {
			t.Fatalf("reading back %q: %v", key, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("reading data %q: %v", key, err)
		}
		if len(data) == 0 {
			continue
		}
		data, err = DecompressShuffleData(data)
		if err != nil {
			t.Fatalf("decompressing %q: %v", key, err)
		}
		r, err := wshf.NewChunkReader(data)
		if err != nil {
			t.Fatalf("parsing %q: %v", key, err)
		}
		for _, col := range r.Schema() {
			if col.Name == "extra" {
				t.Errorf("unexpected column %q in projected output file %q", col.Name, key)
			}
		}
	}
}

// TestExecuteShuffle_ValidationErrors checks that missing required fields cause
// early failures with descriptive errors.
func TestExecuteShuffle_ValidationErrors(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)

	base := distributed.Task{
		ID:            "shuf-err",
		QueryID:       "q-err",
		Type:          distributed.TaskTypeShuffle,
		Files:         []string{"tables/src/f.parquet"},
		ShuffleKeys:   []string{"k"},
		NumPartitions: 4,
		DataBucket:    "b",
		ResultBucket:  "b",
		ResultPrefix:  "p/",
	}

	cases := []struct {
		name    string
		mutate  func(*distributed.Task)
		wantErr string
	}{
		{
			name:    "no_shuffle_keys",
			mutate:  func(t *distributed.Task) { t.ShuffleKeys = nil },
			wantErr: "ShuffleKeys",
		},
		{
			name:    "zero_num_partitions",
			mutate:  func(t *distributed.Task) { t.NumPartitions = 0 },
			wantErr: "NumPartitions",
		},
		{
			name:    "no_files",
			mutate:  func(t *distributed.Task) { t.Files = nil },
			wantErr: "Files",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := base
			tc.mutate(&task)
			result := executor.Execute(ctx, task, "w1")
			if result.Success {
				t.Fatal("expected failure, got success")
			}
			if !strings.Contains(result.Error, tc.wantErr) {
				t.Errorf("error %q does not mention %q", result.Error, tc.wantErr)
			}
		})
	}
}

// TestExecuteShuffle_SchemaRoundTrip verifies that the output schema matches
// the input schema (preserving column names and types).
func TestExecuteShuffle_SchemaRoundTrip(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-shuffle-schema"

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}

	rows := []map[string]any{
		{"id": int64(1), "score": float64(9.5)},
		{"id": int64(2), "score": float64(7.2)},
	}
	writeParquetFile(t, store, bucket, "tables/src/schema-0.parquet", rows)

	cache := NewLRUCache(64 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	executor.SetMemoryBudget(0, t.TempDir())

	task := distributed.Task{
		ID:            "shuf-schema",
		QueryID:       "q-schema",
		StageID:       "shuffle-0",
		Type:          distributed.TaskTypeShuffle,
		Files:         []string{"tables/src/schema-0.parquet"},
		ColumnTypes:   declaredFromRows(rows),
		ShuffleKeys:   []string{"id"},
		NumPartitions: 2,
		DataBucket:    bucket,
		ResultBucket:  bucket,
		ResultPrefix:  "shuffle/q-schema/s0/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("shuffle failed: %s", result.Error)
	}
	if result.NumRows != 2 {
		t.Errorf("expected 2 rows, got %d", result.NumRows)
	}

	wantSchema := map[string]parquet.TypeID{
		"id":    parquet.TypeInt64,
		"score": parquet.TypeFloat64,
	}

	for _, key := range result.ResultFiles {
		rc, _, err := store.Get(ctx, bucket, key)
		if err != nil {
			t.Fatalf("reading %q: %v", key, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("reading data %q: %v", key, err)
		}
		if len(data) == 0 {
			continue
		}
		data, err = DecompressShuffleData(data)
		if err != nil {
			t.Fatalf("decompressing %q: %v", key, err)
		}
		r, err := wshf.NewChunkReader(data)
		if err != nil {
			t.Fatalf("parsing %q: %v", key, err)
		}
		for _, col := range r.Schema() {
			wantType, ok := wantSchema[col.Name]
			if !ok {
				t.Errorf("unexpected column %q in output", col.Name)
				continue
			}
			if col.Type != wantType {
				t.Errorf("column %q: type = %v, want %v", col.Name, col.Type, wantType)
			}
		}
	}
}

// deferredDFFixture is the shuffle task behind the deferred-filter tests:
// 5000 rows (k = 0..4999, in files of rowsPerFile) shuffled on k, with a
// Deferred spec whose merged artifact admits k = 0..9.
//
// File layout matters to what a test can see. A single file reaches the
// shuffle loop as ONE batch, so the deferred install has exactly one chance
// (the promotion check after the first src.Next) and only the row-level op
// can drop rows (the [0,9] range intersects the file). Files of 2048 rows
// give between-batch points for a mid-task arrival, but once installed the
// range prunes the later files whole, so the row-level op is visible only
// in the batch the install lands on.
type deferredDFFixture struct {
	store    *objstore.MemStore
	executor *Executor
	task     distributed.Task
	spec     distributed.DynamicFilterSpec
}

const deferredDFRows = 5000

func newDeferredDFFixture(t *testing.T, rowsPerFile int) *deferredDFFixture {
	t.Helper()
	ctx := context.Background()
	const bucket = "test-shuffle-deferred-df"
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	rows := make([]map[string]any, deferredDFRows)
	for i := 0; i < deferredDFRows; i++ {
		rows[i] = map[string]any{"k": int64(i), "v": fmt.Sprintf("value-%d", i)}
	}
	var files []string
	for lo := 0; lo < deferredDFRows; lo += rowsPerFile {
		hi := min(lo+rowsPerFile, deferredDFRows)
		path := fmt.Sprintf("tables/src/part-%d.parquet", len(files))
		writeParquetFile(t, store, bucket, path, rows[lo:hi])
		files = append(files, path)
	}
	mergedKey := "queries/q-shuf-df/dynfilter-merged/scan-1/f1.wdf"
	stageRangedArtifact(t, store, bucket, mergedKey, 0, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

	executor := NewExecutor(store, NewLRUCache(64*1024*1024), nil)
	executor.SetMemoryBudget(0, t.TempDir())
	spec := distributed.DynamicFilterSpec{
		FilterID: "f1", TargetColumn: "k", KeyType: "int64",
		BloomBucket: bucket, BloomKey: mergedKey, Deferred: true,
	}
	return &deferredDFFixture{
		store:    store,
		executor: executor,
		spec:     spec,
		task: distributed.Task{
			ID:             "shuf-df-1",
			QueryID:        "q-shuf-df",
			StageID:        "shuffle-0",
			Type:           distributed.TaskTypeShuffle,
			Files:          files,
			ColumnTypes:    declaredFromRows(rows),
			ShuffleKeys:    []string{"k"},
			NumPartitions:  2,
			DataBucket:     bucket,
			ResultBucket:   bucket,
			ResultPrefix:   "shuffle/q-shuf-df/s0/",
			DynamicFilters: []distributed.DynamicFilterSpec{spec},
		},
	}
}

// resolveStagedPoll runs the executor's real singleflight poller against the
// staged artifact and waits for it to TERMINATE, so the outcome is decided
// before the task starts instead of racing the task's own scan (#1715: the
// poll goroutine and the scan are unordered — install is a non-blocking
// check per batch — so on one core the scan's three batches could finish
// before the poll goroutine ran). The loop deletes its map entry as it
// exits; the resolved slot is put back so the task's registration finds it,
// the state a task meets when its poll resolves before the first batch.
func (f *deferredDFFixture) resolveStagedPoll(t *testing.T) *pendingBloom {
	t.Helper()
	p := f.executor.pollDeferredBloom(f.spec)
	select {
	case <-p.done:
	case <-time.After(30 * time.Second):
		t.Fatal("deferred poll did not terminate against a staged artifact")
	}
	if _, _, ok := p.resolved(); !ok {
		t.Fatal("deferred poll terminated without a bloom against a staged artifact")
	}
	f.holdSlot(p)
	return p
}

// holdSlot installs p as the poll slot the task's registration will share.
func (f *deferredDFFixture) holdSlot(p *pendingBloom) {
	key := f.spec.BloomBucket + "/" + f.spec.BloomKey
	f.executor.dfAttachMu.Lock()
	if f.executor.dfAttachPolls == nil {
		f.executor.dfAttachPolls = make(map[string]*pendingBloom)
	}
	f.executor.dfAttachPolls[key] = p
	f.executor.dfAttachMu.Unlock()
}

// readShuffleKV decodes every shuffle output file into k -> v, failing on a
// duplicated key (the input holds each k once).
func readShuffleKV(t *testing.T, store objstore.Store, bucket string, files []string) map[int64]string {
	t.Helper()
	out := make(map[int64]string)
	for _, key := range files {
		rc, _, err := store.Get(context.Background(), bucket, key)
		if err != nil {
			t.Fatalf("reading %q: %v", key, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("reading data %q: %v", key, err)
		}
		if len(data) == 0 {
			continue
		}
		if data, err = DecompressShuffleData(data); err != nil {
			t.Fatalf("decompressing %q: %v", key, err)
		}
		r, err := wshf.NewChunkReader(data)
		if err != nil {
			t.Fatalf("parsing %q: %v", key, err)
		}
		ki, vi := -1, -1
		for i, col := range r.Schema() {
			switch col.Name {
			case "k":
				ki = i
			case "v":
				vi = i
			}
		}
		if ki < 0 || vi < 0 {
			t.Fatalf("%q: schema lacks k or v: %v", key, r.Schema())
		}
		for {
			b, err := r.Next()
			if err != nil {
				t.Fatalf("chunk of %q: %v", key, err)
			}
			if b == nil {
				break
			}
			for i := 0; i < b.ActiveLen(); i++ {
				row := i
				if b.Sel != nil {
					row = int(b.Sel[i])
				}
				k := b.Columns[ki].Int64Data[row]
				v, _ := b.Columns[vi].GetString(row)
				if _, dup := out[k]; dup {
					t.Fatalf("key %d shuffled twice", k)
				}
				out[k] = v
			}
		}
	}
	return out
}

// requireBuildKeysKept asserts every in-bloom row (k = 0..9) reached the
// shuffle output with its value: a dynamic filter only drops.
func requireBuildKeysKept(t *testing.T, got map[int64]string) {
	t.Helper()
	for k := int64(0); k < 10; k++ {
		if v, ok := got[k]; !ok || v != fmt.Sprintf("value-%d", k) {
			t.Fatalf("in-bloom row k=%d lost or changed: got (%q, %v)", k, v, ok)
		}
	}
}

// afterFirstBatch is a ProgressReporter that runs f once, when the shuffle
// loop reports its first consumed batch: a seam that orders an event
// strictly after batch 1 and strictly before batch 2's promotion check.
type afterFirstBatch struct {
	once sync.Once
	f    func()
}

func (a *afterFirstBatch) AddRows(int64)  { a.once.Do(a.f) }
func (a *afterFirstBatch) AddBytes(int64) {}

// TestExecuteShuffle_DeferredDynamicFilter: a Deferred (attach-on-arrival)
// spec whose artifact is already staged must engage on the shuffle path —
// before this test's fix, handleShuffleTask skipped Deferred specs
// entirely (no poller, no ops), leaving SF100 probe-side shuffle scans
// unfiltered at every layer. The poll is resolved before the task starts
// (resolveStagedPoll), so the install is ordered before the first batch at
// every core count; the filter must then drop the out-of-bloom rows. One
// file (one batch, range intersecting it) so the drop can only come from
// the row-level op: an install that never applies the op fails here.
func TestExecuteShuffle_DeferredDynamicFilter(t *testing.T) {
	f := newDeferredDFFixture(t, deferredDFRows)
	f.resolveStagedPoll(t)

	result := f.executor.Execute(context.Background(), f.task, "w1")
	if !result.Success {
		t.Fatalf("shuffle task failed: %s", result.Error)
	}
	// 10 keys pass the bloom; with the install ahead of batch 1 only bloom
	// false positives ride along. Anything close to the full input means
	// the deferred filter was never installed or never applied.
	if result.NumRows >= deferredDFRows/2 {
		t.Fatalf("deferred filter never engaged: %d of %d rows shuffled", result.NumRows, deferredDFRows)
	}
	if result.NumRows < 10 {
		t.Fatalf("in-bloom rows lost: %d rows shuffled, want >= 10", result.NumRows)
	}
	got := readShuffleKV(t, f.store, f.task.ResultBucket, result.ResultFiles)
	if int64(len(got)) != result.NumRows {
		t.Fatalf("output holds %d rows, result reports %d", len(got), result.NumRows)
	}
	requireBuildKeysKept(t, got)
	t.Logf("deferred filter engaged: %d of %d rows shuffled", result.NumRows, deferredDFRows)
}

// TestExecuteShuffle_DeferredDynamicFilterArrivalTiming pins the drop-only
// contract across the three arrival schedules a deferred filter can meet,
// each forced by a seam rather than by timing: resolved before the first
// batch, resolved between batch 1 and batch 2, and never resolved while the
// task runs (the #1715 schedule: the shuffle outran the poll). The task
// must succeed in all three, and the join this shuffle feeds (an inner
// join against the build keys 0..9) must see the same rows.
func TestExecuteShuffle_DeferredDynamicFilterArrivalTiming(t *testing.T) {
	joinAnswer := func(got map[int64]string) string {
		var out []string
		for k := int64(0); k < 10; k++ { // build side: k = 0..9
			if v, ok := got[k]; ok {
				out = append(out, fmt.Sprintf("%d|%s", k, v))
			}
		}
		return strings.Join(out, ",")
	}
	run := func(schedule string) (int64, string) {
		f := newDeferredDFFixture(t, 2048)
		resolved := f.resolveStagedPoll(t) // the real poller decodes the artifact
		ctx := context.Background()
		switch schedule {
		case "before-first-batch":
			// the resolved slot is already held
		case "mid-task":
			held := &pendingBloom{done: make(chan struct{})}
			f.holdSlot(held)
			ctx = exec.WithProgressReporter(ctx, &afterFirstBatch{f: func() {
				held.bloom, held.mask, held.ok = resolved.bloom, resolved.mask, true
				held.hasRange, held.min, held.max = resolved.hasRange, resolved.min, resolved.max
				held.source = resolved.source
				close(held.done)
			}})
		case "never":
			held := &pendingBloom{done: make(chan struct{})}
			f.holdSlot(held)
			defer close(held.done)
		default:
			t.Fatalf("unknown schedule %q", schedule)
		}
		result := f.executor.Execute(ctx, f.task, "w1")
		if !result.Success {
			t.Fatalf("%s: shuffle task failed: %s", schedule, result.Error)
		}
		got := readShuffleKV(t, f.store, f.task.ResultBucket, result.ResultFiles)
		if int64(len(got)) != result.NumRows {
			t.Fatalf("%s: output holds %d rows, result reports %d", schedule, len(got), result.NumRows)
		}
		requireBuildKeysKept(t, got)
		t.Logf("%s: %d of %d rows shuffled, join answer %s", schedule, result.NumRows, deferredDFRows, joinAnswer(got))
		return result.NumRows, joinAnswer(got)
	}

	nBefore, before := run("before-first-batch")
	nMid, mid := run("mid-task")
	nNever, never := run("never")

	want := "0|value-0,1|value-1,2|value-2,3|value-3,4|value-4,5|value-5,6|value-6,7|value-7,8|value-8,9|value-9"
	for _, c := range []struct{ name, got string }{{"before-first-batch", before}, {"mid-task", mid}, {"never", never}} {
		if c.got != want {
			t.Fatalf("%s: join answer %s, want %s", c.name, c.got, want)
		}
	}
	// The schedules must actually differ in what they shuffle, or the test
	// proves nothing about timing: never = the full input; mid-task = the
	// whole first batch unfiltered plus a filtered second batch (the install
	// lands on it, so its rows pass the row-level op) and a pruned third;
	// before = the first batch already row-filtered.
	if nNever != deferredDFRows {
		t.Fatalf("never: %d rows shuffled, want all %d", nNever, deferredDFRows)
	}
	if nMid < 2048 || nMid >= 2*2048 {
		t.Fatalf("mid-task: %d rows shuffled, want the unfiltered first batch (2048) plus a row-filtered second", nMid)
	}
	if nBefore >= 2048 {
		t.Fatalf("before-first-batch: %d rows shuffled, want the first batch row-filtered (< 2048)", nBefore)
	}
}
