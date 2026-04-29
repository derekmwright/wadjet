package worker

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// kvFastPathFixture stages an executor with both MemStore (S3) and an
// in-process NATS JetStream KV bucket. Returns the components a test needs
// to drive writeStageOutput and inspect both stores.
type kvFastPathFixture struct {
	ctx      context.Context
	executor *Executor
	store    *objstore.MemStore
	kv       jetstream.KeyValue
	bucket   string
}

func newKVFastPathFixture(t *testing.T) *kvFastPathFixture {
	t.Helper()
	ctx := context.Background()

	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	en, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatalf("NATS: %v", err)
	}
	t.Cleanup(en.Shutdown)
	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("js: %v", err)
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   "wadjet_results_data",
		TTL:      5 * time.Minute,
		MaxBytes: 64 * 1024 * 1024,
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("kv: %v", err)
	}

	store := objstore.NewMemStore()
	const bucket = "test-bucket"
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	executor := NewExecutor(store, NewLRUCache(1024*1024), nil)
	executor.SetResultKV(kv)

	return &kvFastPathFixture{
		ctx:      ctx,
		executor: executor,
		store:    store,
		kv:       kv,
		bucket:   bucket,
	}
}

func (f *kvFastPathFixture) writeSmallStageOutput(t *testing.T) string {
	t.Helper()
	schema := []parquet.Column{{Name: "v", Type: parquet.TypeInt64, Nullable: true}}
	b := batch.NewRecordBatch(schema, 3)
	for i := 0; i < 3; i++ {
		b.Columns[0].Int64Data[i] = int64(i + 1)
		b.Columns[0].Nulls.SetValid(i)
	}

	task := distributed.Task{
		ID:           "tkv",
		QueryID:      "q",
		StageID:      "s",
		Type:         distributed.TaskTypeStage,
		StageType:    "hash_join",
		DataBucket:   f.bucket,
		ResultBucket: f.bucket,
		ResultPrefix: "out/s/",
	}
	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := f.executor.writeStageOutput(f.ctx, task, []*batch.RecordBatch{b}, result); err != nil {
		t.Fatalf("writeStageOutput: %v", err)
	}
	if len(result.ResultFiles) != 1 {
		t.Fatalf("expected 1 result file, got %d: %v", len(result.ResultFiles), result.ResultFiles)
	}
	return result.ResultFiles[0]
}

// TestKVCache_SmallOutputDurableInS3AndCachedInKV documents the post-2026-04-28
// invariant: small stage outputs are written durably to S3 *and* cached in KV
// for fast reads. The pre-fix behavior — KV-only with no S3 upload — failed
// Q02 SF10 at 10m57s when the 5-minute KV TTL expired mid-query.
func TestKVCache_SmallOutputDurableInS3AndCachedInKV(t *testing.T) {
	f := newKVFastPathFixture(t)
	key := f.writeSmallStageOutput(t)

	// KV must hold the data under the dot-sanitized key (fast-read cache).
	kvKey := natsKVKey(key)
	if entry, err := f.kv.Get(f.ctx, kvKey); err != nil {
		t.Fatalf("KV cache miss for key %q (sanitized=%q): %v", key, kvKey, err)
	} else if len(entry.Value()) == 0 {
		t.Fatalf("KV cache value empty for key %q", kvKey)
	}

	// S3 must also hold the data (durable store of record).
	rc, _, err := f.store.Get(f.ctx, f.bucket, key)
	if err != nil {
		t.Fatalf("S3 must hold the durable copy for key %q: %v", key, err)
	}
	rc.Close()
}

// TestKVCache_FallsBackToS3WhenKVMisses is the regression test for Q02 SF10
// 2026-04-28: a downstream consumer encounters a missing KV entry (TTL
// expiry, eviction, or never-written) and must still find the data in S3.
//
// Before the fix this would fail with `nats: key not found` + `object not
// found` — the producer had skipped S3 in the KV-only fast path. After the
// fix, S3 is always durable and the fallback succeeds.
func TestKVCache_FallsBackToS3WhenKVMisses(t *testing.T) {
	f := newKVFastPathFixture(t)
	key := f.writeSmallStageOutput(t)

	// Simulate KV expiry / eviction by purging the entry directly. The
	// downstream stream_source.openNextFile path will then hit `nats: key
	// not found` and must fall through to S3.
	kvKey := natsKVKey(key)
	if err := f.kv.Purge(f.ctx, kvKey); err != nil {
		t.Fatalf("purging KV entry: %v", err)
	}
	if _, err := f.kv.Get(f.ctx, kvKey); err == nil {
		t.Fatalf("expected KV miss after purge, got hit")
	}

	// The S3 read must succeed — this is the regression assertion.
	rc, info, err := f.store.Get(f.ctx, f.bucket, key)
	if err != nil {
		t.Fatalf("S3 fallback failed for key %q: %v", key, err)
	}
	defer rc.Close()
	if info.Size == 0 {
		t.Fatalf("S3 fallback returned empty body for key %q", key)
	}
}
