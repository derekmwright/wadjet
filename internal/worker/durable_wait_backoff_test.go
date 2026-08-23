package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// waitForLateObject drives one source whose only input lands in the store
// after landAt, returning the wall the read took.
func waitForLateObject(t *testing.T, queryID, key, bucket string, landAt time.Duration) (time.Duration, *cachedFileStreamSource) {
	t.Helper()
	consumer, store := newConsumer(t, bucket) // store starts empty
	consumer.peers.registerTask(&distributed.Task{
		QueryID:      "st-final_aggregate-7-interm-" + queryID,
		ResultPrefix: "queries/" + queryID + "/final_aggregate-7/",
		FetchToken:   "tok",
	})
	wshf := makeWshfBytes(t, []int64{1, 2, 3, 4})
	go func() {
		time.Sleep(landAt)
		putObject(t, store, bucket, key, wshf)
	}()
	src := newCachedFileStreamSource(consumer, "st-final_aggregate-7-interm-"+queryID, bucket, []string{key})
	start := time.Now()
	if got := drainRows(t, src); got != 4 {
		t.Fatalf("rows = %d, want 4", got)
	}
	return time.Since(start), src
}

// TestDurableWaitBackoffFindsEarlyLandings is the window-2 §7.1 regression:
// the durable wait used to re-poll on a flat 500ms clock, so a copy that
// landed in 150ms still cost 500ms of critical path — and the gather-merge
// tail of every aggregate query is exactly that shape. The ramp resolves
// the landing in several sub-quantum probes; the kill switch restores the
// old single-probe quantum, which is what makes this a real before/after
// and not a tautology.
func TestDurableWaitBackoffFindsEarlyLandings(t *testing.T) {
	const (
		bucket = "scratch"
		landAt = 150 * time.Millisecond
	)

	t.Run("backoff on", func(t *testing.T) {
		prev := durableWaitBackoff.Set(true)
		t.Cleanup(func() { durableWaitBackoff.Set(prev) })

		const queryID = "q-backoff-on"
		key := "queries/" + queryID + "/final_aggregate-7/merge-0.wshf"
		_, src := waitForLateObject(t, queryID, key, bucket, landAt)
		if src.acq.durableWaits != 1 {
			t.Fatalf("durable_waits = %d, want 1", src.acq.durableWaits)
		}
		// The PROBE COUNT is the mechanism, and it is what separates the two
		// arms: the ramp re-probes inside one flat quantum (25/75/175ms
		// cumulative), the flat clock probes exactly once. The wall this
		// took to run is a scheduling outcome — asserting "wait <= 300ms"
		// here failed at 312.8ms whenever other suites ran concurrently
		// (#421), while proving nothing the counts do not.
		if src.acq.durableWaitPolls < 2 {
			t.Fatalf("durable_wait_polls = %d; the ramp must probe more than once inside %v",
				src.acq.durableWaitPolls, landAt)
		}
		if src.acq.durableWaitPolls > 8 {
			t.Fatalf("durable_wait_polls = %d; the ramp saturates at %v, so an object landing at %v "+
				"cannot need that many probes", src.acq.durableWaitPolls, durableWaitPoll, landAt)
		}
		if !src.acq.notable() {
			t.Fatal("a task that waited on a durable object must escalate its phases line")
		}
	})

	t.Run("backoff off keeps the flat quantum", func(t *testing.T) {
		prev := durableWaitBackoff.Set(false)
		t.Cleanup(func() { durableWaitBackoff.Set(prev) })

		const queryID = "q-backoff-off"
		key := "queries/" + queryID + "/final_aggregate-7/merge-0.wshf"
		elapsed, src := waitForLateObject(t, queryID, key, bucket, landAt)
		if elapsed < durableWaitPoll {
			t.Fatalf("wait = %v with the switch off; the flat cadence cannot resolve before %v",
				elapsed, durableWaitPoll)
		}
		if src.acq.durableWaitPolls != 1 {
			t.Fatalf("durable_wait_polls = %d, want exactly 1 on the flat clock", src.acq.durableWaitPolls)
		}
	})
}

// TestDurableWaitRespectsCancellation pins the two bounds the ramp must not
// change: ctx cancellation returns the ctx error promptly, and an expired
// budget returns a non-nil cause (the caller wraps it into
// missingInputError, and a nil cause there prints "<nil>").
func TestDurableWaitRespectsCancellation(t *testing.T) {
	const (
		bucket  = "scratch"
		queryID = "q-cancel"
		key     = "queries/q-cancel/final_aggregate-7/merge-0.wshf"
	)
	consumer, _ := newConsumer(t, bucket)
	src := newCachedFileStreamSource(consumer, "st-x-"+queryID, bucket, []string{key})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := src.awaitDurableObject(ctx, key); err != context.Canceled {
		t.Fatalf("cancelled wait returned %v, want context.Canceled", err)
	}

	origTotal := durableWaitTotal
	durableWaitTotal = 0
	t.Cleanup(func() { durableWaitTotal = origTotal })
	_, err := src.awaitDurableObject(context.Background(), key)
	if err == nil {
		t.Fatal("expired budget returned a nil cause")
	}
	if !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("expired budget cause = %v, want not-found", err)
	}
}

// TestNextDurableWaitPollRamp pins the ramp's shape independent of timing:
// it doubles, it saturates at durableWaitPoll, and jitter stays inside
// +/-25% of the nominal delay so the mean cadence is unchanged.
func TestNextDurableWaitPollRamp(t *testing.T) {
	prev := durableWaitBackoff.Set(true)
	t.Cleanup(func() { durableWaitBackoff.Set(prev) })

	cur := durableWaitPollMin
	for i, want := range []time.Duration{50 * time.Millisecond, 100 * time.Millisecond,
		200 * time.Millisecond, 400 * time.Millisecond, 500 * time.Millisecond, 500 * time.Millisecond} {
		sleep, next := nextDurableWaitPoll(cur)
		lo := cur - cur/4
		hi := cur + cur/4
		if sleep < lo || sleep > hi {
			t.Fatalf("step %d: sleep %v outside [%v, %v]", i, sleep, lo, hi)
		}
		if next != want {
			t.Fatalf("step %d: next = %v, want %v", i, next, want)
		}
		cur = next
	}

	durableWaitBackoff.Set(false)
	if sleep, next := nextDurableWaitPoll(durableWaitPollMin); sleep != durableWaitPoll || next != durableWaitPoll {
		t.Fatalf("switch off: (%v, %v), want the flat %v", sleep, next, durableWaitPoll)
	}
}
