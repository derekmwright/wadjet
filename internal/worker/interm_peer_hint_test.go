package worker

import (
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// TestIntermMergeInputTakesPeerTier is the consumer half of the hint fix:
// once the coordinator hints a gather-merge input, the merge task reads it
// off the producing worker's NVMe and never touches the durable tier — and
// with the producer gone it falls through to exactly the wait it used to
// start in.
func TestIntermMergeInputTakesPeerTier(t *testing.T) {
	const (
		queryID = "q-interm"
		key     = "queries/q-interm/final_aggregate-7/interm-0.wshf"
		token   = "tok-interm"
		bucket  = "scratch"
	)
	// The merge task's own QueryID is stage-scoped and differs from the
	// producer's; the root derived from the key is what lines up.
	const consumerQueryID = "st-final_aggregate-7-interm-" + queryID
	wshf := makeWshfBytes(t, []int64{10, 20, 30, 40})

	t.Run("peer serves it", func(t *testing.T) {
		addr := startProducerPeer(t, queryID, key, token, wshf)
		consumer, _ := newConsumer(t, bucket) // durable copy does NOT exist
		consumer.peers.registerTask(&distributed.Task{
			QueryID:        consumerQueryID,
			ResultPrefix:   "queries/" + queryID + "/final_aggregate-7/",
			FetchToken:     token,
			InputLocations: map[string]string{key: addr},
		})
		src := newCachedFileStreamSource(consumer, consumerQueryID, bucket, []string{key})
		if got := drainRows(t, src); got != 4 {
			t.Fatalf("rows = %d, want 4", got)
		}
		if src.acq.peerHits != 1 || src.acq.peerMisses != 0 {
			t.Fatalf("peer_hits=%d peer_misses=%d, want 1/0", src.acq.peerHits, src.acq.peerMisses)
		}
		if src.acq.durableWaits != 0 {
			t.Fatalf("durable_waits = %d; a peer-served input must never reach the wait", src.acq.durableWaits)
		}
	})

	t.Run("peer gone falls through to the durable wait", func(t *testing.T) {
		consumer, store := newConsumer(t, bucket)
		consumer.peers.registerTask(&distributed.Task{
			QueryID:        consumerQueryID,
			ResultPrefix:   "queries/" + queryID + "/final_aggregate-7/",
			FetchToken:     token,
			InputLocations: map[string]string{key: "127.0.0.1:1"}, // nothing listens
		})
		go func() {
			time.Sleep(80 * time.Millisecond)
			putObject(t, store, bucket, key, wshf)
		}()
		src := newCachedFileStreamSource(consumer, consumerQueryID, bucket, []string{key})
		if got := drainRows(t, src); got != 4 {
			t.Fatalf("rows = %d, want 4", got)
		}
		if src.acq.peerMisses != 1 {
			t.Fatalf("peer_misses = %d, want 1", src.acq.peerMisses)
		}
		if src.acq.durableWaits != 1 || src.acq.durableWaitPolls == 0 {
			t.Fatalf("durable_waits=%d polls=%d, want the bounded wait to have run",
				src.acq.durableWaits, src.acq.durableWaitPolls)
		}
	})
}
