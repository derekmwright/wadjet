package worker

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/citc-tech/wadjet/internal/dataplane"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// startProducerPeer builds a producer-side executor holding one adopted
// shuffle file and serves it via a real PeerServer. Returns the peer address.
func startProducerPeer(t *testing.T, queryID, key, token string, wshf []byte) string {
	t.Helper()
	producer := NewExecutor(objstore.NewMemStore(), NewLRUCache(1<<20), nil)
	producer.SetMemoryBudget(0, t.TempDir())
	producer.SetLocalStageCache(NewLocalStageCache(filepath.Join(t.TempDir(), "stage-cache")))

	src := filepath.Join(t.TempDir(), "producer-part.wshf")
	if err := os.WriteFile(src, wshf, 0o644); err != nil {
		t.Fatal(err)
	}
	if adopted := producer.localCache.Adopt(queryID, key, src); adopted == "" {
		t.Fatal("LocalStageCache.Adopt failed")
	}
	// The producing task's spec carried the fetch token — record it the
	// same way Execute does. Note the stage-scoped QueryID: the root is
	// derived from the scratch prefix, mirroring production.
	producer.peers.registerTask(&distributed.Task{
		QueryID:      "st-stage-1-" + queryID,
		ResultPrefix: "queries/" + queryID + "/stage-1/",
		FetchToken:   token,
	})

	srv := dataplane.NewPeerServer(dataplane.PeerServerConfig{Addr: "127.0.0.1:0"}, producer, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("starting peer server: %v", err)
	}
	t.Cleanup(srv.Stop)
	return srv.AdvertiseAddr()
}

// newConsumer builds a consumer-side executor over its own (empty unless
// populated) MemStore with the peer client wired.
func newConsumer(t *testing.T, bucket string) (*Executor, *objstore.MemStore) {
	t.Helper()
	store := newTestStore(t, bucket)
	consumer := NewExecutor(store, NewLRUCache(1<<20), nil)
	consumer.SetMemoryBudget(0, t.TempDir())
	client := dataplane.NewPeerClient(nil)
	t.Cleanup(client.Close)
	consumer.SetPeerClient(client)
	return consumer, store
}

func putObject(t *testing.T, store *objstore.MemStore, bucket, key string, data []byte) {
	t.Helper()
	if _, err := store.Put(context.Background(), bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatalf("Put %s: %v", key, err)
	}
}

func drainRows(t *testing.T, s *cachedFileStreamSource) int {
	t.Helper()
	defer s.Close()
	ctx := context.Background()
	if err := s.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	rows := 0
	for {
		b, err := s.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			return rows
		}
		rows += b.ActiveLen()
	}
}

// TestPeerTierServesWithoutS3 proves the Tier-1.5 read: the consumer's
// object store does NOT hold the file, only the producer's local disk does —
// rows must arrive via the peer fetch alone.
func TestPeerTierServesWithoutS3(t *testing.T) {
	const (
		queryID = "q-peer"
		key     = "queries/q-peer/stage-1/partition=0001/task-1.wshf"
		token   = "tok-1"
		bucket  = "scratch"
	)
	addr := startProducerPeer(t, queryID, key, token, makeWshfBytes(t, []int64{1, 2, 3, 4, 5}))

	consumer, _ := newConsumer(t, bucket) // bucket exists but has no objects
	// Consumer task has a DIFFERENT stage-scoped QueryID than the producer —
	// the root derived from the key is what has to line up.
	consumer.peers.registerTask(&distributed.Task{
		QueryID:        "st-join-2-" + queryID,
		FetchToken:     token,
		InputLocations: map[string]string{key: addr},
	})

	src := newCachedFileStreamSource(consumer, "st-join-2-"+queryID, bucket, []string{key})
	if got := drainRows(t, src); got != 5 {
		t.Fatalf("rows = %d, want 5", got)
	}
	if consumer.PeerFetchHits() != 1 {
		t.Fatalf("peer hits = %d, want 1", consumer.PeerFetchHits())
	}
	if consumer.PeerFetchFallthroughs() != 0 {
		t.Fatalf("fallthroughs = %d, want 0", consumer.PeerFetchFallthroughs())
	}
}

// TestPeerTierFallsThroughToS3 proves the failure contract: a dead peer
// hint costs one failed fetch, then the durable S3 copy serves the read.
func TestPeerTierFallsThroughToS3(t *testing.T) {
	const (
		queryID = "q-fall"
		key     = "queries/q-fall/stage-1/partition=0002/task-2.wshf"
		bucket  = "scratch"
	)
	wshf := makeWshfBytes(t, []int64{7, 8, 9})

	consumer, store := newConsumer(t, bucket)
	putObject(t, store, bucket, key, wshf)
	consumer.peers.registerTask(&distributed.Task{
		QueryID:        queryID,
		FetchToken:     "tok",
		InputLocations: map[string]string{key: "127.0.0.1:1"}, // nothing listens
	})

	src := newCachedFileStreamSource(consumer, queryID, bucket, []string{key})
	if got := drainRows(t, src); got != 3 {
		t.Fatalf("rows = %d, want 3", got)
	}
	if consumer.PeerFetchFallthroughs() != 1 {
		t.Fatalf("fallthroughs = %d, want 1", consumer.PeerFetchFallthroughs())
	}
	if consumer.PeerFetchHits() != 0 {
		t.Fatalf("peer hits = %d, want 0", consumer.PeerFetchHits())
	}
}

// TestPeerTierBadTokenFallsThrough: the producer rejects a stale token;
// the consumer still gets rows from S3.
func TestPeerTierBadTokenFallsThrough(t *testing.T) {
	const (
		queryID = "q-tok"
		key     = "queries/q-tok/stage-1/partition=0003/task-3.wshf"
		bucket  = "scratch"
	)
	wshf := makeWshfBytes(t, []int64{11, 12})
	addr := startProducerPeer(t, queryID, key, "producer-token", wshf)

	consumer, store := newConsumer(t, bucket)
	putObject(t, store, bucket, key, wshf)
	consumer.peers.registerTask(&distributed.Task{
		QueryID:        queryID,
		FetchToken:     "different-token",
		InputLocations: map[string]string{key: addr},
	})

	src := newCachedFileStreamSource(consumer, queryID, bucket, []string{key})
	if got := drainRows(t, src); got != 2 {
		t.Fatalf("rows = %d, want 2", got)
	}
	if consumer.PeerFetchFallthroughs() != 1 {
		t.Fatalf("fallthroughs = %d, want 1", consumer.PeerFetchFallthroughs())
	}
}

func TestPeerExchangeRegistryLifecycle(t *testing.T) {
	e := NewExecutor(objstore.NewMemStore(), NewLRUCache(1<<20), nil)
	e.SetLocalStageCache(NewLocalStageCache(filepath.Join(t.TempDir(), "sc")))
	task := &distributed.Task{
		QueryID:        "q1",
		FetchToken:     "tok",
		InputLocations: map[string]string{"queries/q1/s/f.wshf": "10.0.0.1:9095"},
	}
	e.peers.registerTask(task)

	if addr, tok := e.peers.hintFor("queries/q1/s/f.wshf"); addr != "10.0.0.1:9095" || tok != "tok" {
		t.Fatalf("hintFor = (%q, %q)", addr, tok)
	}
	// Serving validation: right token but key not in the local cache.
	if _, err := e.ResolveShuffleFile("q1", "queries/q1/s/f.wshf", "tok"); !errors.Is(err, dataplane.ErrPeerNotFound) {
		t.Fatalf("want ErrPeerNotFound, got %v", err)
	}
	if _, err := e.ResolveShuffleFile("q1", "queries/q1/s/f.wshf", "bad"); !errors.Is(err, dataplane.ErrPeerDenied) {
		t.Fatalf("want ErrPeerDenied for bad token, got %v", err)
	}
	if _, err := e.ResolveShuffleFile("q1", "any", ""); !errors.Is(err, dataplane.ErrPeerDenied) {
		t.Fatalf("want ErrPeerDenied for empty token, got %v", err)
	}

	e.peers.CleanupQuery("q1")
	if addr, tok := e.peers.hintFor("queries/q1/s/f.wshf"); addr != "" || tok != "" {
		t.Fatalf("hints survived CleanupQuery: (%q, %q)", addr, tok)
	}
	if _, err := e.ResolveShuffleFile("q1", "queries/q1/s/f.wshf", "tok"); !errors.Is(err, dataplane.ErrPeerDenied) {
		t.Fatalf("want ErrPeerDenied after cleanup, got %v", err)
	}
}
