package worker

import (
	"bytes"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// TestResultStoreNeverWedgesWhenACleanupBroadcastIsMissed is #818.
//
// The store had a hard capacity and NO eviction: Put refused new entries
// once UsedBytes would exceed --result-store (512 MiB by default), and
// CleanupQuery — driven only by the coordinator's terminal broadcast — was
// the only removal path. A worker that missed one (a cancel that never
// published CompleteSubject, #817; a restarted coordinator; a dropped
// message) held those bytes for its PROCESS lifetime, and once accumulated
// misses reached the capacity every later stage output silently fell back
// to S3 for the rest of that lifetime. A memory leak and a permanent,
// unsignalled performance cliff on the feature this type's own comment
// calls the primary optimization for standalone mode.
func TestResultStoreNeverWedgesWhenACleanupBroadcastIsMissed(t *testing.T) {
	rs := NewResultStore(4096)
	rs.ttl = 50 * time.Millisecond
	payload := bytes.Repeat([]byte("x"), 1024)

	// Four queries fill the store. No cleanup broadcast ever arrives.
	for i := 0; i < 4; i++ {
		q := string(rune('a' + i))
		if !rs.Put(q, "queries/"+q+"/stage-0/out.wshf", payload) {
			t.Fatalf("put %d refused while the store still had room", i)
		}
	}
	if rs.UsedBytes() != 4096 {
		t.Fatalf("UsedBytes = %d, want 4096", rs.UsedBytes())
	}
	if rs.Put("full", "queries/full/out.wshf", payload) {
		t.Fatal("a full store accepted a put")
	}

	// Past the TTL, the next put reclaims and succeeds. Without eviction
	// this line fails forever.
	time.Sleep(2 * rs.ttl)
	if !rs.Put("later", "queries/later/stage-0/out.wshf", payload) {
		t.Fatalf("the store is still wedged %v after every entry expired: "+
			"UsedBytes=%d of %d, and the only removal path is a broadcast that never came (#818)",
			2*rs.ttl, rs.UsedBytes(), rs.MaxBytes())
	}
	if rs.Evicted() == 0 {
		t.Fatal("entries were reclaimed but Evicted() is 0; an operator has no signal that " +
			"terminal broadcasts are being missed")
	}
	if got := rs.UsedBytes(); got != 1024 {
		t.Fatalf("UsedBytes after the prune = %d, want 1024 (only the new entry)", got)
	}
}

// TestResultStoreCleanupIsIdempotentAndExact guards the accounting the
// prune path now shares with CleanupQuery.
func TestResultStoreCleanupIsIdempotentAndExact(t *testing.T) {
	rs := NewResultStore(1 << 20)
	payload := bytes.Repeat([]byte("y"), 100)
	for i := 0; i < 3; i++ {
		rs.Put("q1", "queries/q1/out-"+string(rune('0'+i))+".wshf", payload)
	}
	rs.Put("q2", "queries/q2/out.wshf", payload)
	if got := rs.UsedBytes(); got != 400 {
		t.Fatalf("UsedBytes = %d, want 400", got)
	}
	rs.CleanupQuery("q1")
	rs.CleanupQuery("q1") // idempotent
	if got := rs.UsedBytes(); got != 100 {
		t.Fatalf("UsedBytes after cleanup = %d, want 100", got)
	}
	rs.CleanupQuery("q2")
	if got := rs.UsedBytes(); got != 0 {
		t.Fatalf("UsedBytes after cleaning every query = %d, want 0", got)
	}
	if got := rs.Count(); got != 0 {
		t.Fatalf("Count = %d, want 0", got)
	}
}

// TestCancelBroadcastFreesTheResultStore is the other half of #818: the
// worker's COMPLETE handler dropped ResultStore entries and its CANCEL
// handler did not. The gap is reachable exactly through
// Coordinator.CancelQuery, which publishes cancel and not complete (#817).
func TestCancelBroadcastFreesTheResultStore(t *testing.T) {
	ctx, en, store := setupWorkerNATS(t)

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	w := New(Config{
		WorkerID: "resultstore-cancel", MaxConcurrent: 1,
		CacheBytes: 1 << 20, ResultStoreBytes: 1 << 20, SpillDir: t.TempDir(),
	}, store, nc, js, nil)
	if err := w.Start(ctx); err != nil {
		t.Fatalf("worker start: %v", err)
	}
	t.Cleanup(w.Stop)

	rs := w.ResultStore()
	if rs == nil {
		t.Skip("this build has no in-memory result store")
	}
	payload := bytes.Repeat([]byte("z"), 2048)
	if !rs.Put("q-cancelled", "queries/q-cancelled/stage-0/out.wshf", payload) {
		t.Fatal("seeding the result store failed")
	}
	if rs.UsedBytes() == 0 {
		t.Fatal("seed did not register")
	}

	if err := nc.Publish(distributed.CancelSubject("q-cancelled"), []byte("q-cancelled")); err != nil {
		t.Fatalf("publish cancel: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if rs.UsedBytes() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("a cancelled query's ResultStore entries are still resident (%d bytes) after "+
		"its cancel broadcast; the store has a hard capacity and no other removal path (#818)",
		rs.UsedBytes())
}
