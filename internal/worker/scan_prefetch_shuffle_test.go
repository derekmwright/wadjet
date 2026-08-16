package worker

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// TestShufflePrefetch_PeerHintedFilesServeAhead: with streaming shuffle
// read on, a multi-file shuffle input with peer hints is downloaded ahead
// by the prefetcher (via the peer tier) and consumed from the staged
// temps. The consumer's object store holds NOTHING — rows can only have
// arrived via peer transport — and the tier-level peer counter stays 0
// because the prefetcher, not the tiered path, did the fetching.
func TestShufflePrefetch_PeerHintedFilesServeAhead(t *testing.T) {
	const (
		queryID = "q-pf"
		token   = "tok-pf"
		bucket  = "scratch"
	)
	keys := []string{
		"queries/q-pf/stage-1/partition=0001/task-1.wshf",
		"queries/q-pf/stage-1/partition=0001/task-2.wshf",
		"queries/q-pf/stage-1/partition=0001/task-3.wshf",
	}
	vals := [][]int64{{1, 2, 3}, {4, 5}, {6, 7, 8, 9}}
	locations := make(map[string]string, len(keys))
	for i, key := range keys {
		locations[key] = startProducerPeer(t, queryID, key, token, makeWshfBytes(t, vals[i]))
	}

	consumer, _ := newConsumer(t, bucket) // store stays empty
	consumer.SetStreamingShuffleRead(true)
	consumer.peers.registerTask(&distributed.Task{
		QueryID:        "st-agg-2-" + queryID,
		FetchToken:     token,
		InputLocations: locations,
	})

	src := newCachedFileStreamSource(consumer, "st-agg-2-"+queryID, bucket, keys)
	if got := drainRows(t, src); got != 9 {
		t.Fatalf("rows = %d, want 9", got)
	}
	if hits := consumer.PeerFetchHits(); hits != 0 {
		t.Fatalf("tier-level peer hits = %d, want 0 (prefetcher should have fetched ahead)", hits)
	}
}

// TestShufflePrefetch_WSHCTranscodedOnLanding: a compressed producer file
// prefetches through the streaming s2 decoder and lands as plain WSHF the
// standard mmap chunk reader consumes.
func TestShufflePrefetch_WSHCTranscodedOnLanding(t *testing.T) {
	const (
		queryID = "q-pfc"
		token   = "tok-pfc"
		bucket  = "scratch"
	)
	// Compressible payload: repeated values.
	vals := make([]int64, 4096)
	wire := makeWshfBytes(t, vals)
	compressed := CompressShuffleData(wire)
	if string(compressed[:4]) != string(compressedMagic[:]) {
		t.Skip("payload did not compress")
	}
	keys := []string{
		"queries/q-pfc/stage-1/partition=0000/task-1.wshf",
		"queries/q-pfc/stage-1/partition=0000/task-2.wshf",
	}
	locations := map[string]string{
		keys[0]: startProducerPeer(t, queryID, keys[0], token, compressed),
		keys[1]: startProducerPeer(t, queryID, keys[1], token, makeWshfBytes(t, []int64{1})),
	}

	consumer, _ := newConsumer(t, bucket)
	consumer.SetStreamingShuffleRead(true)
	consumer.peers.registerTask(&distributed.Task{
		QueryID:        "st-agg-2-" + queryID,
		FetchToken:     token,
		InputLocations: locations,
	})

	src := newCachedFileStreamSource(consumer, "st-agg-2-"+queryID, bucket, keys)
	if got := drainRows(t, src); got != 4097 {
		t.Fatalf("rows = %d, want 4097", got)
	}
}

// TestShufflePrefetch_NoHintFallsToStreamingS3: shuffle keys without a
// peer hint are never blind-fetched by the prefetcher; the tiered path
// resolves them from the store — with the flag on, via streaming decode.
func TestShufflePrefetch_NoHintFallsToStreamingS3(t *testing.T) {
	const bucket = "scratch"
	keys := []string{
		"queries/q-nh/stage-1/partition=0000/task-1.wshf",
		"queries/q-nh/stage-1/partition=0000/task-2.wshf",
	}
	consumer, store := newConsumer(t, bucket)
	consumer.SetStreamingShuffleRead(true)
	putObject(t, store, bucket, keys[0], makeWshfBytes(t, []int64{1, 2}))
	putObject(t, store, bucket, keys[1], makeWshfBytes(t, []int64{3, 4, 5}))

	src := newCachedFileStreamSource(consumer, "q-nh", bucket, keys)
	if got := drainRows(t, src); got != 5 {
		t.Fatalf("rows = %d, want 5", got)
	}
	reads, fallbacks, _ := consumer.ShuffleStreamStats()
	if reads != 2 || fallbacks != 0 {
		t.Fatalf("streaming stats = (%d,%d), want (2,0) — unhinted keys should stream from the store", reads, fallbacks)
	}
}
