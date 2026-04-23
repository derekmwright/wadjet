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

// TestKVFastPath_SmallOutputSkipsS3 stages the executor with both MemStore
// (for S3 fallback) and NATS KV. A stage task whose output fits under
// natsKVResultThreshold should put the payload into KV and NOT write to
// the object store — proving the perf-A fast-path activates.
func TestKVFastPath_SmallOutputSkipsS3(t *testing.T) {
	ctx := context.Background()

	// Embedded NATS + KV bucket (same config the coordinator uses).
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

	// Executor with MemStore + KV wired.
	store := objstore.NewMemStore()
	const bucket = "test-bucket"
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	executor := NewExecutor(store, NewLRUCache(1024*1024), nil)
	executor.SetResultKV(kv)

	// Build a small batch (fits easily under the 4 MB threshold).
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
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/s/",
	}
	result := &distributed.ResultNotification{TaskID: task.ID}

	if err := executor.writeStageOutput(ctx, task, []*batch.RecordBatch{b}, result); err != nil {
		t.Fatalf("writeStageOutput: %v", err)
	}

	if len(result.ResultFiles) != 1 {
		t.Fatalf("expected 1 result file, got %d: %v", len(result.ResultFiles), result.ResultFiles)
	}
	key := result.ResultFiles[0]

	// KV must hold the data under the dot-sanitized key.
	kvKey := natsKVKey(key)
	if entry, err := kv.Get(ctx, kvKey); err != nil {
		t.Fatalf("KV miss for key %q (sanitized=%q): %v", key, kvKey, err)
	} else if len(entry.Value()) == 0 {
		t.Fatalf("KV value empty for key %q", kvKey)
	}

	// S3 (MemStore) must NOT hold the data — fast-path's whole purpose is
	// to skip the S3 round-trip.
	if _, _, err := store.Get(ctx, bucket, key); err == nil {
		t.Fatalf("S3 should NOT hold the key when KV fast-path takes effect: %q", key)
	}
}
