package worker

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
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
	}, distributed.UploadEager)
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
	}, distributed.UploadEager)
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

// TestUploadManagerLazyReleaseQuery: lazy jobs stay queued (nothing lands),
// then ReleaseQuery starts them and the durable copies land. Later lazy
// jobs on the released root start immediately.
func TestUploadManagerLazyReleaseQuery(t *testing.T) {
	store := newTestStore(t, "b")
	m := newUploadManager(store, nil, nil)
	m.StartTask("q1", "t1", "w1", []uploadJob{
		{bucket: "b", key: "queries/q1/s/a.wshf", srcPath: writeTempFile(t, []byte("WSHFaaaa")), size: 8},
	}, distributed.UploadLazy)

	time.Sleep(50 * time.Millisecond)
	if done, _, _ := m.UploadStats(); done != 0 {
		t.Fatalf("lazy upload ran without a release (done=%d)", done)
	}
	if _, _, err := store.Get(context.Background(), "b", "queries/q1/s/a.wshf"); err == nil {
		t.Fatal("lazy upload landed without a release")
	}

	m.ReleaseQuery("q1")
	waitFor(t, "released upload to land", func() bool {
		done, _, _ := m.UploadStats()
		return done == 1
	})
	if _, _, err := store.Get(context.Background(), "b", "queries/q1/s/a.wshf"); err != nil {
		t.Fatalf("released upload missing: %v", err)
	}

	// Root is released: subsequent lazy jobs start without another signal.
	m.StartTask("q1", "t2", "w1", []uploadJob{
		{bucket: "b", key: "queries/q1/s/b.wshf", srcPath: writeTempFile(t, []byte("WSHFbbbb")), size: 8},
	}, distributed.UploadLazy)
	waitFor(t, "post-release lazy upload to land", func() bool {
		done, _, _ := m.UploadStats()
		return done == 2
	})
}

// TestUploadManagerLazyElidedOnCancel: a query that finishes with lazy jobs
// still queued never PUTs them — they land on the elided side of the ledger.
func TestUploadManagerLazyElidedOnCancel(t *testing.T) {
	store := newTestStore(t, "b")
	m := newUploadManager(store, nil, nil)
	m.StartTask("q1", "t1", "w1", []uploadJob{
		{bucket: "b", key: "queries/q1/s/a.wshf", srcPath: writeTempFile(t, []byte("WSHFaaaa")), size: 8},
		{bucket: "b", key: "queries/q1/s/b.wshf", srcPath: writeTempFile(t, []byte("WSHFbbbb")), size: 8},
	}, distributed.UploadLazy)

	m.CancelQuery("q1")
	files, bytes := m.UploadElidedStats()
	if files != 2 || bytes != 16 {
		t.Fatalf("elided = (%d files, %d bytes), want (2, 16)", files, bytes)
	}
	done, cancelled, failed := m.UploadStats()
	if done != 0 || cancelled != 0 || failed != 0 {
		t.Fatalf("stats = (done %d, cancelled %d, failed %d), want all 0 — queued jobs were never started", done, cancelled, failed)
	}
	if _, _, err := store.Get(context.Background(), "b", "queries/q1/s/a.wshf"); err == nil {
		t.Fatal("elided upload landed anyway")
	}
	// A release after the query is terminal is a no-op.
	m.ReleaseQuery("q1")
	time.Sleep(20 * time.Millisecond)
	if done, _, _ := m.UploadStats(); done != 0 {
		t.Fatal("post-terminal release started uploads")
	}
}

// TestUploadManagerStartTaskAfterCancelTombstoned: a straggler task's
// StartTask arriving AFTER its query's CancelQuery must not resurrect the
// root with a fresh un-cancelled context — before the tombstone, its jobs
// ran against already-purged scratch files and burned the full retry ladder
// into an abandoned-upload error storm (q22-R2 stall window, 2026-08-10).
func TestUploadManagerStartTaskAfterCancelTombstoned(t *testing.T) {
	store := newTestStore(t, "b")
	m := newUploadManager(store, nil, nil)
	m.CancelQuery("q1")
	// Simulate the purge having already unlinked the adopted file.
	gone := filepath.Join(t.TempDir(), "purged.wshf")
	m.StartTask("q1", "t1", "w1", []uploadJob{
		{bucket: "b", key: "queries/q1/s/a.wshf", srcPath: gone, size: 8},
		{bucket: "b", key: "queries/q1/s/b.wshf", srcPath: gone, size: 8},
	}, distributed.UploadEager)
	done, cancelled, failed := m.UploadStats()
	if done != 0 || cancelled != 2 || failed != 0 {
		t.Fatalf("stats = (done %d, cancelled %d, failed %d), want (0, 2, 0)", done, cancelled, failed)
	}
	if _, cancelledBytes := m.UploadByteStats(); cancelledBytes != 16 {
		t.Fatalf("cancelledBytes = %d, want 16", cancelledBytes)
	}
	m.mu.Lock()
	_, resurrected := m.queries["q1"]
	m.mu.Unlock()
	if resurrected {
		t.Fatal("StartTask resurrected a cancelled root's upload scope")
	}
}

// TestUploadManagerMissingSourceFailsFast: a live root whose source file is
// gone must abandon on the FIRST attempt — the source is cache-owned, so
// ENOENT is permanent and the retry backoff ladder (3.5s/file) is pure delay.
func TestUploadManagerMissingSourceFailsFast(t *testing.T) {
	store := newTestStore(t, "b")
	m := newUploadManager(store, nil, nil)
	start := time.Now()
	m.StartTask("q1", "t1", "w1", []uploadJob{
		{bucket: "b", key: "queries/q1/s/a.wshf", srcPath: filepath.Join(t.TempDir(), "gone.wshf"), size: 8},
	}, distributed.UploadEager)
	waitFor(t, "missing-source job to abandon", func() bool {
		_, _, failed := m.UploadStats()
		return failed == 1
	})
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("abandon took %v — retry ladder ran despite permanent ENOENT", elapsed)
	}
}

// TestUploadManagerOffElidesImmediately: off-policy jobs never start, never
// track, and count elided at StartTask.
func TestUploadManagerOffElidesImmediately(t *testing.T) {
	store := newTestStore(t, "b")
	m := newUploadManager(store, nil, nil)
	m.StartTask("q1", "t1", "w1", []uploadJob{
		{bucket: "b", key: "queries/q1/s/a.wshf", srcPath: writeTempFile(t, []byte("WSHFaaaa")), size: 8},
	}, distributed.UploadOff)
	files, bytes := m.UploadElidedStats()
	if files != 1 || bytes != 8 {
		t.Fatalf("elided = (%d files, %d bytes), want (1, 8)", files, bytes)
	}
	if _, _, err := store.Get(context.Background(), "b", "queries/q1/s/a.wshf"); err == nil {
		t.Fatal("off-policy upload landed")
	}
	// Neither release nor flush can resurrect an off job.
	m.ReleaseQuery("q1")
	if err := m.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if done, _, _ := m.UploadStats(); done != 0 {
		t.Fatal("off-policy job uploaded after release/flush")
	}
}

// TestUploadManagerFlushReleasesLazy: worker drain (Flush) must start queued
// lazy uploads and wait for them — after drain the peer server is gone, so
// the durable copies are all consumers have left.
func TestUploadManagerFlushReleasesLazy(t *testing.T) {
	store := newTestStore(t, "b")
	m := newUploadManager(store, nil, nil)
	m.StartTask("q1", "t1", "w1", []uploadJob{
		{bucket: "b", key: "queries/q1/s/a.wshf", srcPath: writeTempFile(t, []byte("WSHFaaaa")), size: 8},
	}, distributed.UploadLazy)
	if err := m.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if _, _, err := store.Get(context.Background(), "b", "queries/q1/s/a.wshf"); err != nil {
		t.Fatalf("lazy upload missing after drain flush: %v", err)
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

// TestAsyncUnpartitionedWriteOffPolicy mirrors TestAsyncUnpartitionedWrite
// under UploadPolicy=off: completion is identical (files recorded, cache
// adopted, peers servable) but the durable copy never lands and the bytes
// count as elided.
func TestAsyncUnpartitionedWriteOffPolicy(t *testing.T) {
	store := newTestStore(t, "b")
	e := NewExecutor(store, NewLRUCache(1<<20), nil)
	e.SetMemoryBudget(0, t.TempDir())
	e.SetLocalStageCache(NewLocalStageCache(filepath.Join(t.TempDir(), "sc")))

	task := distributed.Task{
		ID: "t1", QueryID: "st-agg-1-qz", Type: distributed.TaskTypeStage,
		ResultBucket: "b", ResultPrefix: "queries/qz/stage-1/",
		AsyncUpload: true, FetchToken: "tok",
		UploadPolicy: distributed.UploadOff,
	}
	payload := makeWshfBytes(t, []int64{5, 6})
	var result distributed.ResultNotification
	result.WorkerID = "w1"
	batches, err := readShuffleBatchesForTest(payload)
	if err != nil {
		t.Fatalf("decoding test payload: %v", err)
	}
	if err := e.writeUnpartitionedWSHF(context.Background(), task, batches, batches[0].Schema, &result); err != nil {
		t.Fatalf("writeUnpartitionedWSHF: %v", err)
	}
	key := "queries/qz/stage-1/t1.wshf"
	if len(result.ResultFiles) != 1 || result.ResultFiles[0] != key {
		t.Fatalf("result files = %v", result.ResultFiles)
	}
	if e.localCache.Get("anything", key) == "" {
		t.Fatal("output not in LocalStageCache at completion time")
	}
	files, bytes := e.uploads.UploadElidedStats()
	if files != 1 || bytes == 0 {
		t.Fatalf("elided = (%d files, %d bytes), want the output elided", files, bytes)
	}
	time.Sleep(50 * time.Millisecond)
	if _, _, err := store.Get(context.Background(), "b", key); err == nil {
		t.Fatal("off-policy stage output landed in the object store")
	}
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
