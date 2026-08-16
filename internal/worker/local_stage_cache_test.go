package worker

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestLocalStageCache_AdoptGetCleanup(t *testing.T) {
	root := t.TempDir()
	c := NewLocalStageCache(root)

	src := filepath.Join(t.TempDir(), "src.wshf")
	if err := os.WriteFile(src, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write src: %v", err)
	}

	dst := c.Adopt("q1", "out/s/x.wshf", src)
	if dst == "" {
		t.Fatalf("Adopt returned empty path")
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatalf("source should have been renamed away, stat err=%v", err)
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("destination must exist: %v", err)
	}

	if got := c.Get("q1", "out/s/x.wshf"); got != dst {
		t.Errorf("Get returned %q, want %q", got, dst)
	}
	if got := c.Get("q2", "out/s/x.wshf"); got != "" {
		t.Errorf("queryID isolation broken: got %q for q2, want empty", got)
	}
	if got := c.Get("q1", "out/s/other.wshf"); got != "" {
		t.Errorf("key isolation broken: got %q for unknown key, want empty", got)
	}

	if n := c.CleanupQuery("q1"); n != 1 {
		t.Errorf("CleanupQuery removed %d, want 1", n)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Errorf("file should be gone after cleanup, stat err=%v", err)
	}
	if got := c.Get("q1", "out/s/x.wshf"); got != "" {
		t.Errorf("Get after cleanup returned %q, want empty", got)
	}
	// Idempotent.
	if n := c.CleanupQuery("q1"); n != 0 {
		t.Errorf("second CleanupQuery removed %d, want 0", n)
	}
}

// TestLocalStageCache_AdoptAfterCleanupDeclined is the regression test for
// the SF100 run-20260610-203304 leak: a straggler task of a terminated query
// reached Adopt AFTER the query's CleanupQuery had already run, registering a
// file into a per-query directory that no future cleanup message would ever
// visit. Adopt must decline for tombstoned queries so the caller keeps
// ownership and deletes the file via its normal no-adopt path.
func TestLocalStageCache_AdoptAfterCleanupDeclined(t *testing.T) {
	root := t.TempDir()
	c := NewLocalStageCache(root)

	// Query terminates (cleanup runs first, e.g. via the failure path)…
	c.CleanupQuery("dead-query")

	// …then a straggler producer tries to adopt its output.
	src := filepath.Join(t.TempDir(), "late.wshf")
	if err := os.WriteFile(src, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if dst := c.Adopt("dead-query", "out/s/late.wshf", src); dst != "" {
		t.Fatalf("Adopt after CleanupQuery must decline, got %q", dst)
	}
	// Caller retains ownership: the source file is untouched.
	if _, err := os.Stat(src); err != nil {
		t.Fatalf("declined Adopt must leave srcPath in place: %v", err)
	}
	// Nothing registered, no orphan directory recreated.
	if got := c.Get("dead-query", "out/s/late.wshf"); got != "" {
		t.Fatalf("Get returned %q for declined adopt, want empty", got)
	}
	if c.Count() != 0 {
		t.Fatalf("cache should be empty, has %d entries", c.Count())
	}

	// Other queries are unaffected.
	src2 := filepath.Join(t.TempDir(), "live.wshf")
	if err := os.WriteFile(src2, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write src2: %v", err)
	}
	if dst := c.Adopt("live-query", "out/s/live.wshf", src2); dst == "" {
		t.Fatal("Adopt for a live query must still succeed")
	}
}

// TestLocalStageCache_AsyncPurge: with the janitor enabled, CleanupQuery
// must return immediately with entries dropped (Get misses, Adopt
// tombstoned) while the files disappear shortly after in the background.
func TestLocalStageCache_AsyncPurge(t *testing.T) {
	root := t.TempDir()
	c := NewLocalStageCache(root)
	c.SetAsyncPurge(true)

	var dsts []string
	for i := 0; i < 3; i++ {
		src := filepath.Join(t.TempDir(), "src.wshf")
		if err := os.WriteFile(src, []byte("payload"), 0o600); err != nil {
			t.Fatal(err)
		}
		key := filepath.Join("queries/qa/s", string(rune('a'+i))+".wshf")
		dst := c.Adopt("qa", key, src)
		if dst == "" {
			t.Fatalf("Adopt %d failed", i)
		}
		dsts = append(dsts, dst)
	}

	if n := c.CleanupQuery("qa"); n != 3 {
		t.Fatalf("CleanupQuery = %d, want 3", n)
	}
	// Entries are gone synchronously.
	if got := c.Get("qa", "queries/qa/s/a.wshf"); got != "" {
		t.Fatalf("Get after async cleanup returned %q", got)
	}
	// Late Adopt still declined (tombstone unaffected by purge mode).
	src := filepath.Join(t.TempDir(), "late.wshf")
	if err := os.WriteFile(src, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := c.Adopt("qa", "queries/qa/s/late.wshf", src); got != "" {
		t.Fatal("late Adopt accepted on tombstoned query")
	}
	// Files disappear in the background.
	waitFor(t, "janitor to delete trashed files", func() bool {
		for _, d := range dsts {
			if _, err := os.Stat(d); !os.IsNotExist(err) {
				return false
			}
		}
		return true
	})
	// Idempotent second cleanup.
	if n := c.CleanupQuery("qa"); n != 0 {
		t.Fatalf("second CleanupQuery = %d, want 0", n)
	}
}

// TestLocalStageCache_AsyncPurgeNoAdopts: cleanup of a query with no
// on-disk directory must not spin up janitor work or fail.
func TestLocalStageCache_AsyncPurgeNoAdopts(t *testing.T) {
	c := NewLocalStageCache(t.TempDir())
	c.SetAsyncPurge(true)
	if n := c.CleanupQuery("ghost"); n != 0 {
		t.Fatalf("CleanupQuery(ghost) = %d, want 0", n)
	}
}

func TestLocalStageCache_AdoptMissingFile(t *testing.T) {
	c := NewLocalStageCache(t.TempDir())
	if got := c.Adopt("q", "k", "/no/such/file"); got != "" {
		t.Errorf("Adopt of missing file returned %q, want empty", got)
	}
	if c.Count() != 0 {
		t.Errorf("Count after failed Adopt = %d, want 0", c.Count())
	}
}

func TestLocalStageCache_NilSafe(t *testing.T) {
	var c *LocalStageCache
	if got := c.Get("q", "k"); got != "" {
		t.Errorf("Get on nil returned %q, want empty", got)
	}
	if got := c.Adopt("q", "k", "/tmp/x"); got != "" {
		t.Errorf("Adopt on nil returned %q, want empty", got)
	}
	if n := c.CleanupQuery("q"); n != 0 {
		t.Errorf("CleanupQuery on nil returned %d, want 0", n)
	}
	if n := c.Count(); n != 0 {
		t.Errorf("Count on nil returned %d, want 0", n)
	}
}

// TestLocalStageFastPath_ProducerToConsumerSkipsS3 wires the producer/consumer
// integration end-to-end with the LocalStageCache enabled but KV disabled, so
// stage outputs flow MemStore + local cache. After production we delete the
// MemStore copy: a subsequent consumer read MUST succeed via the local cache
// (otherwise it would hit S3 and fail). This proves the tier-0 fast path is
// taking effect.
func TestLocalStageFastPath_ProducerToConsumerSkipsS3(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	const bucket = "test-bucket"
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}

	spillRoot := t.TempDir()
	executor := NewExecutor(store, NewLRUCache(1024*1024), nil)
	executor.SetMemoryBudget(0, spillRoot)
	executor.SetLocalStageCache(NewLocalStageCache(filepath.Join(spillRoot, "stage-cache")))
	executor.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))

	schema := []parquet.Column{{Name: "v", Type: parquet.TypeInt64, Nullable: true}}
	b := batch.NewRecordBatch(schema, 4)
	for i := 0; i < 4; i++ {
		b.Columns[0].Int64Data[i] = int64(i + 100)
		b.Columns[0].Nulls.SetValid(i)
	}

	task := distributed.Task{
		ID:           "tlc",
		QueryID:      "q-fast",
		StageID:      "s",
		Type:         distributed.TaskTypeStage,
		StageType:    "hash_join",
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/s/",
	}
	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.writeStageOutput(ctx, task, []*batch.RecordBatch{b}, result); err != nil {
		t.Fatalf("writeStageOutput: %v", err)
	}
	if len(result.ResultFiles) != 1 {
		t.Fatalf("expected 1 result file, got %d", len(result.ResultFiles))
	}
	key := result.ResultFiles[0]

	if executor.localCache.Get(task.QueryID, key) == "" {
		t.Fatalf("producer did not register %q in LocalStageCache", key)
	}

	// Force the consumer to depend on the local cache: delete the durable
	// S3 copy. A miss in the cache would manifest as a Get error here.
	if err := store.Delete(ctx, bucket, key); err != nil {
		t.Fatalf("delete from MemStore: %v", err)
	}

	src := newCachedFileStreamSource(executor, task.QueryID, bucket, []string{key})
	if err := src.Init(ctx); err != nil {
		t.Fatalf("source Init: %v", err)
	}
	defer src.Close()

	got, err := src.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got == nil {
		t.Fatalf("Next returned nil batch on cache hit")
	}
	if got.ActiveLen() != 4 {
		t.Errorf("got %d rows, want 4", got.ActiveLen())
	}

	// CleanupQuery should drop the cache entry and unlink the file.
	cachedPath := executor.localCache.Get(task.QueryID, key)
	executor.localCache.CleanupQuery(task.QueryID)
	if executor.localCache.Get(task.QueryID, key) != "" {
		t.Errorf("Get after cleanup returned non-empty")
	}
	if _, err := os.Stat(cachedPath); !os.IsNotExist(err) {
		t.Errorf("cached file should be unlinked after CleanupQuery, stat err=%v", err)
	}
}

// TestLocalStageFastPath_DifferentQueryIDFallsThrough verifies that a consumer
// reading the same key under a different queryID misses the cache and falls
// through to S3. Same setup as above but the consumer asks for a queryID that
// has no cached entries — MemStore must still hold the durable copy.
func TestLocalStageFastPath_DifferentQueryIDFallsThrough(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	const bucket = "test-bucket"
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}

	spillRoot := t.TempDir()
	executor := NewExecutor(store, NewLRUCache(1024*1024), nil)
	executor.SetMemoryBudget(0, spillRoot)
	executor.SetLocalStageCache(NewLocalStageCache(filepath.Join(spillRoot, "stage-cache")))
	executor.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))

	schema := []parquet.Column{{Name: "v", Type: parquet.TypeInt64, Nullable: true}}
	b := batch.NewRecordBatch(schema, 2)
	for i := 0; i < 2; i++ {
		b.Columns[0].Int64Data[i] = int64(i + 7)
		b.Columns[0].Nulls.SetValid(i)
	}

	task := distributed.Task{
		ID:           "tlc2",
		QueryID:      "q-A",
		StageID:      "s",
		Type:         distributed.TaskTypeStage,
		StageType:    "hash_join",
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/s/",
	}
	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.writeStageOutput(ctx, task, []*batch.RecordBatch{b}, result); err != nil {
		t.Fatalf("writeStageOutput: %v", err)
	}
	key := result.ResultFiles[0]

	// Consumer reads with a different queryID — must miss the cache and
	// succeed by reading the durable S3 copy that MemStore still holds.
	src := newCachedFileStreamSource(executor, "q-B", bucket, []string{key})
	if err := src.Init(ctx); err != nil {
		t.Fatalf("source Init: %v", err)
	}
	defer src.Close()

	got, err := src.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got == nil || got.ActiveLen() != 2 {
		t.Fatalf("expected 2-row batch via S3 fallback, got %v rows=%d", got, got.ActiveLen())
	}
}
