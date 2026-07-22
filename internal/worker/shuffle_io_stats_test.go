package worker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// TestShuffleIOStats_S3Tier: WSHF inputs resolved from the durable store
// land on the s3 side of the per-tier ledger, with wire-exact byte counts,
// on both the streaming and the staged read path.
func TestShuffleIOStats_S3Tier(t *testing.T) {
	for _, streaming := range []bool{true, false} {
		wireA := buildMultiTypeWSHF(t, 21, 3, 200)
		wireB := buildMultiTypeWSHF(t, 22, 2, 150)
		files := map[string][]byte{
			"queries/q1/s0/partition=0001/t1.wshf": wireA,
			"queries/q1/s0/partition=0001/t2.wshf": wireB,
		}
		store := objstore.NewMemStore()
		stageWSHF(t, store, "b", files)
		ex := &Executor{store: store, spillDir: t.TempDir(), peers: newPeerExchange(), streamingShuffleRead: streaming}
		drainSource(t, newCachedFileStreamSource(ex, "q1", "b", []string{
			"queries/q1/s0/partition=0001/t1.wshf",
			"queries/q1/s0/partition=0001/t2.wshf",
		}))

		got := ex.ShuffleIOStats()
		wantBytes := int64(len(wireA) + len(wireB))
		if got.S3Files != 2 || got.S3Bytes != wantBytes {
			t.Fatalf("streaming=%v: s3 tier = (%d files, %d bytes), want (2, %d)",
				streaming, got.S3Files, got.S3Bytes, wantBytes)
		}
		if got.LocalFiles != 0 || got.PeerFiles != 0 || got.KVFiles != 0 {
			t.Fatalf("streaming=%v: non-s3 tiers counted: %+v", streaming, got)
		}
	}
}

// TestShuffleIOStats_LocalTier: a same-worker LocalStageCache hit lands on
// the local side of the ledger with the file's full size, and nothing on s3.
func TestShuffleIOStats_LocalTier(t *testing.T) {
	const (
		queryID = "q-lst"
		key     = "queries/q-lst/stage-1/partition=0001/task-1.wshf"
		bucket  = "scratch"
	)
	wshf := makeWshfBytes(t, []int64{1, 2, 3})
	ex, _ := newConsumer(t, bucket) // store stays empty: local tier must serve
	ex.SetLocalStageCache(NewLocalStageCache(filepath.Join(t.TempDir(), "stage-cache")))
	src := filepath.Join(t.TempDir(), "part.wshf")
	if err := os.WriteFile(src, wshf, 0o644); err != nil {
		t.Fatal(err)
	}
	if adopted := ex.localCache.Adopt(queryID, key, src); adopted == "" {
		t.Fatal("LocalStageCache.Adopt failed")
	}

	if got := drainRows(t, newCachedFileStreamSource(ex, "st-join-2-"+queryID, bucket, []string{key})); got != 3 {
		t.Fatalf("rows = %d, want 3", got)
	}
	got := ex.ShuffleIOStats()
	if got.LocalFiles != 1 || got.LocalBytes != int64(len(wshf)) {
		t.Fatalf("local tier = (%d files, %d bytes), want (1, %d)", got.LocalFiles, got.LocalBytes, len(wshf))
	}
	if got.S3Files != 0 || got.PeerFiles != 0 || got.KVFiles != 0 {
		t.Fatalf("non-local tiers counted: %+v", got)
	}
}

// TestShuffleIOStats_PeerTier: a Tier-1.5 peer fetch lands on the peer side
// of the ledger with the full wire size.
func TestShuffleIOStats_PeerTier(t *testing.T) {
	const (
		queryID = "q-pst"
		key     = "queries/q-pst/stage-1/partition=0001/task-1.wshf"
		token   = "tok-1"
		bucket  = "scratch"
	)
	wshf := makeWshfBytes(t, []int64{1, 2, 3, 4, 5})
	addr := startProducerPeer(t, queryID, key, token, wshf)

	consumer, _ := newConsumer(t, bucket)
	consumer.peers.registerTask(&distributed.Task{
		QueryID:        "st-join-2-" + queryID,
		FetchToken:     token,
		InputLocations: map[string]string{key: addr},
	})

	if got := drainRows(t, newCachedFileStreamSource(consumer, "st-join-2-"+queryID, bucket, []string{key})); got != 5 {
		t.Fatalf("rows = %d, want 5", got)
	}
	got := consumer.ShuffleIOStats()
	if got.PeerFiles != 1 || got.PeerBytes != int64(len(wshf)) {
		t.Fatalf("peer tier = (%d files, %d bytes), want (1, %d)", got.PeerFiles, got.PeerBytes, len(wshf))
	}
	if got.S3Files != 0 || got.LocalFiles != 0 || got.KVFiles != 0 {
		t.Fatalf("non-peer tiers counted: %+v", got)
	}
}
