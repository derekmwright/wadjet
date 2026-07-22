package worker

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// gatedStore blocks Put until the gate closes — deterministic control over
// the async-upload in-flight window.
type gatedStore struct {
	*objstore.MemStore
	gate chan struct{}
}

func (g *gatedStore) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) (string, error) {
	select {
	case <-g.gate:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return g.MemStore.Put(ctx, bucket, key, r, size, contentType)
}

func writeTempFile(t *testing.T, data []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "part.wshf")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestUploadManagerLandsFiles(t *testing.T) {
	store := newTestStore(t, "b")
	m := newUploadManager(store, nil, nil)
	payload := makeWshfBytes(t, []int64{1, 2, 3})
	m.StartTask("q1", "t1", "w1", []uploadJob{
		{bucket: "b", key: "queries/q1/s/a.wshf", srcPath: writeTempFile(t, payload), tmpDir: t.TempDir()},
		{bucket: "b", key: "queries/q1/s/c.wshf", srcPath: writeTempFile(t, payload), compress: true, tmpDir: t.TempDir()},
	})
	waitFor(t, "uploads to land", func() bool {
		c, _, _ := m.UploadStats()
		return c == 2
	})
	for _, key := range []string{"queries/q1/s/a.wshf", "queries/q1/s/c.wshf"} {
		rc, _, err := store.Get(context.Background(), "b", key)
		if err != nil {
			t.Fatalf("uploaded object %s missing: %v", key, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		// The compressed variant may be WSHC; either way it must round-trip
		// through the standard reader path — just check non-empty here (the
		// consumer-path tests cover decoding).
		if len(got) == 0 {
			t.Fatalf("uploaded object %s empty", key)
		}
	}
	// Source files are cache-owned: the manager must not delete them.
	if _, _, failed := m.UploadStats(); failed != 0 {
		t.Fatalf("unexpected failed uploads")
	}
	// Byte ledger: both files' wire bytes counted; the uncompressed job
	// alone contributes its full payload size.
	if done, cancelled := m.UploadByteStats(); done < int64(len(payload)) || cancelled != 0 {
		t.Fatalf("byte stats = (done %d, cancelled %d), want done >= %d, cancelled 0", done, cancelled, len(payload))
	}
}

func TestUploadManagerCancelQuery(t *testing.T) {
	mem := objstore.NewMemStore()
	if err := mem.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	gs := &gatedStore{MemStore: mem, gate: make(chan struct{})}
	m := newUploadManager(gs, nil, nil)
	m.StartTask("q1", "t1", "w1", []uploadJob{
		{bucket: "b", key: "queries/q1/s/a.wshf", srcPath: writeTempFile(t, []byte("WSHFxxxx")), tmpDir: t.TempDir()},
	})
	// The job is blocked inside Put; cancelling the query must abort it
	// without the gate ever opening.
	time.Sleep(50 * time.Millisecond)
	m.CancelQuery("q1")
	waitFor(t, "upload cancellation", func() bool {
		_, cancelled, _ := m.UploadStats()
		return cancelled == 1
	})
	if _, _, err := mem.Get(context.Background(), "b", "queries/q1/s/a.wshf"); err == nil {
		t.Fatal("cancelled upload landed anyway")
	}
	if done, cancelled := m.UploadByteStats(); done != 0 || cancelled != 8 {
		t.Fatalf("byte stats = (done %d, cancelled %d), want (0, 8)", done, cancelled)
	}
}

// TestAsyncUnpartitionedWrite drives the Phase-B producer path end to end
// at the executor level: completion is immediate (cache adopted, KV+peers
// servable), the durable copy lands in the background.
func TestAsyncUnpartitionedWrite(t *testing.T) {
	store := newTestStore(t, "b")
	e := NewExecutor(store, NewLRUCache(1<<20), nil)
	e.SetMemoryBudget(0, t.TempDir())
	e.SetLocalStageCache(NewLocalStageCache(filepath.Join(t.TempDir(), "sc")))

	task := distributed.Task{
		ID: "t1", QueryID: "st-agg-1-qz", Type: distributed.TaskTypeStage,
		ResultBucket: "b", ResultPrefix: "queries/qz/stage-1/",
		AsyncUpload: true, FetchToken: "tok",
	}
	payload := makeWshfBytes(t, []int64{5, 6})
	var result distributed.ResultNotification
	result.WorkerID = "w1"
	batches, err := readShuffleBatchesForTest(payload)
	if err != nil {
		t.Fatalf("decoding test payload: %v", err)
	}
	schema := batches[0].Schema
	if err := e.writeUnpartitionedWSHF(context.Background(), task, batches, schema, &result); err != nil {
		t.Fatalf("writeUnpartitionedWSHF: %v", err)
	}
	key := "queries/qz/stage-1/t1.wshf"
	if len(result.ResultFiles) != 1 || result.ResultFiles[0] != key {
		t.Fatalf("result files = %v", result.ResultFiles)
	}
	// Peer-servable immediately: the cache holds the file under the root.
	if e.localCache.Get("anything", key) == "" {
		t.Fatal("output not in LocalStageCache at completion time")
	}
	// Durable copy lands in the background.
	waitFor(t, "background upload", func() bool {
		_, _, err := store.Get(context.Background(), "b", key)
		return err == nil
	})
}

// readShuffleBatchesForTest decodes a WSHF payload via the production
// chunk reader.
func readShuffleBatchesForTest(payload []byte) ([]*batch.RecordBatch, error) {
	r, err := newShuffleChunkReader(payload)
	if err != nil {
		return nil, err
	}
	var out []*batch.RecordBatch
	for {
		b, err := r.Next()
		if err != nil {
			return nil, err
		}
		if b == nil {
			return out, nil
		}
		out = append(out, b)
	}
}
