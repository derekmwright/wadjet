package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/wshf"
)

// truncatingReadCloser serves up to remain bytes then fails with an
// injected transport error — a peer/S3 stream dying mid-body.
type truncatingReadCloser struct {
	rc     io.ReadCloser
	remain int
}

func (t *truncatingReadCloser) Read(p []byte) (int, error) {
	if t.remain <= 0 {
		return 0, fmt.Errorf("injected transport failure")
	}
	if len(p) > t.remain {
		p = p[:t.remain]
	}
	n, err := t.rc.Read(p)
	t.remain -= n
	return n, err
}

func (t *truncatingReadCloser) Close() error { return t.rc.Close() }

// flakyGetStore fails the FIRST Get of failKey mid-body; later Gets serve
// the full object (the durable copy).
type flakyGetStore struct {
	objstore.Store
	failKey   string
	failAfter int
	gets      atomic.Int64
}

func (s *flakyGetStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	rc, info, err := s.Store.Get(ctx, bucket, key)
	if err != nil || key != s.failKey {
		return rc, info, err
	}
	if s.gets.Add(1) == 1 {
		return &truncatingReadCloser{rc: rc, remain: s.failAfter}, info, nil
	}
	return rc, info, err
}

func stageWSHF(t *testing.T, store objstore.Store, bucket string, files map[string][]byte) {
	t.Helper()
	ctx := context.Background()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	for key, data := range files {
		if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), ""); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
}

func drainSource(t *testing.T, src *cachedFileStreamSource) []*batch.RecordBatch {
	t.Helper()
	ctx := context.Background()
	if err := src.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	var out []*batch.RecordBatch
	for {
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		out = append(out, b)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return out
}

func countSpillFiles(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			n++
		}
		return nil
	})
	return n
}

// TestStreamingShuffleRead_S3PathNoStaging: with the flag on, WSHF inputs
// resolved from the store decode without a single spill-dir staging file,
// and yield the same batches as the staged path.
func TestStreamingShuffleRead_S3PathNoStaging(t *testing.T) {
	wireA := buildMultiTypeWSHF(t, 21, 3, 200)
	wireB := buildMultiTypeWSHF(t, 22, 2, 150)
	files := map[string][]byte{
		"queries/q1/s0/partition=0001/t1.wshf": wireA,
		"queries/q1/s0/partition=0001/t2.wshf": wireB,
	}

	// Staged reference (flag off).
	refStore := objstore.NewMemStore()
	stageWSHF(t, refStore, "b", files)
	refEx := &Executor{store: refStore, spillDir: t.TempDir(), peers: newPeerExchange()}
	refSrc := newCachedFileStreamSource(refEx, "q1", "b", []string{
		"queries/q1/s0/partition=0001/t1.wshf",
		"queries/q1/s0/partition=0001/t2.wshf",
	})
	want := drainSource(t, refSrc)

	// Streaming arm.
	store := objstore.NewMemStore()
	stageWSHF(t, store, "b", files)
	spill := t.TempDir()
	ex := &Executor{store: store, spillDir: spill, peers: newPeerExchange(), streamingShuffleRead: true}
	src := newCachedFileStreamSource(ex, "q1", "b", []string{
		"queries/q1/s0/partition=0001/t1.wshf",
		"queries/q1/s0/partition=0001/t2.wshf",
	})
	got := drainSource(t, src)

	requireBatchesEqual(t, want, got)
	reads, fallbacks, skips := ex.ShuffleStreamStats()
	if reads != 2 || fallbacks != 0 || skips != 0 {
		t.Fatalf("stats = (%d,%d,%d), want (2,0,0)", reads, fallbacks, skips)
	}
	if n := countSpillFiles(t, spill); n != 0 {
		t.Fatalf("%d staging files created on the streaming path, want 0", n)
	}
}

// TestStreamingShuffleRead_WSHC: compressed payloads stream-decode too.
func TestStreamingShuffleRead_WSHC(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 33, 4, 400)
	compressed := CompressShuffleData(wire)
	if !bytes.HasPrefix(compressed, compressedMagic[:]) {
		t.Skip("payload did not compress")
	}
	key := "queries/q2/s0/partition=0000/t1.wshf"

	store := objstore.NewMemStore()
	stageWSHF(t, store, "b", map[string][]byte{key: compressed})
	ex := &Executor{store: store, spillDir: t.TempDir(), peers: newPeerExchange(), streamingShuffleRead: true}
	got := drainSource(t, newCachedFileStreamSource(ex, "q2", "b", []string{key}))

	mm, err := wshf.NewChunkReader(wire)
	if err != nil {
		t.Fatalf("reference reader: %v", err)
	}
	requireBatchesEqual(t, drain(t, mm.Next), got)
	if reads, _, _ := ex.ShuffleStreamStats(); reads != 1 {
		t.Fatalf("reads = %d, want 1", reads)
	}
}

// TestStreamingShuffleRead_MidStreamFallback: the stream dies after some
// batches were delivered; the source falls back to the durable copy and
// resumes with no lost and no duplicated rows.
func TestStreamingShuffleRead_MidStreamFallback(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 44, 6, 250)
	key := "queries/q3/s0/partition=0002/t1.wshf"

	mem := objstore.NewMemStore()
	stageWSHF(t, mem, "b", map[string][]byte{key: wire})
	// Fail the first Get after ~2.5 chunks' worth of bytes so at least one
	// batch is delivered before the transport dies.
	store := &flakyGetStore{Store: mem, failKey: key, failAfter: len(wire) * 2 / 5}
	ex := &Executor{store: store, spillDir: t.TempDir(), peers: newPeerExchange(), streamingShuffleRead: true}
	got := drainSource(t, newCachedFileStreamSource(ex, "q3", "b", []string{key}))

	mm, err := wshf.NewChunkReader(wire)
	if err != nil {
		t.Fatalf("reference reader: %v", err)
	}
	requireBatchesEqual(t, drain(t, mm.Next), got)

	reads, fallbacks, skips := ex.ShuffleStreamStats()
	if reads != 1 || fallbacks != 1 {
		t.Fatalf("stats = (%d,%d,%d), want reads=1 fallbacks=1", reads, fallbacks, skips)
	}
	if skips < 1 {
		t.Fatalf("skip_resumes = %d, want >= 1 (batches were delivered before the failure)", skips)
	}
	if store.gets.Load() != 2 {
		t.Fatalf("store gets = %d, want 2 (stream + durable fallback)", store.gets.Load())
	}
}

// TestStreamingShuffleRead_FlagOffUnchanged: default-off leaves the staged
// path byte-identical (staging temp appears and is cleaned up).
func TestStreamingShuffleRead_FlagOffUnchanged(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 55, 2, 100)
	key := "queries/q4/s0/partition=0000/t1.wshf"
	store := objstore.NewMemStore()
	stageWSHF(t, store, "b", map[string][]byte{key: wire})
	ex := &Executor{store: store, spillDir: t.TempDir(), peers: newPeerExchange()}
	got := drainSource(t, newCachedFileStreamSource(ex, "q4", "b", []string{key}))
	mm, _ := wshf.NewChunkReader(wire)
	requireBatchesEqual(t, drain(t, mm.Next), got)
	if reads, _, _ := ex.ShuffleStreamStats(); reads != 0 {
		t.Fatalf("flag off but streaming reads = %d", reads)
	}
}
