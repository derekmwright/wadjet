// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1706, line 1706o: a fragment's hash-join build that FAILS leaves nothing
// on the shared pool. buildFragmentJoinProbe returned (nil, error) before it
// installed the cleanup closure, and the broadcast cache never receives a join
// from a failed build, so nobody closed it: every charge the build took before
// it failed stayed on the worker's pool for the worker's lifetime.
//
// The build reads a good run (charged on arrival) and then a corrupt one (the
// decode fails). A live peer holds a charge and a forced charge on the same
// pool throughout; after the failure the pool must be back at exactly the
// peer's balance, under every purpose.
func TestFragmentJoinBuildFailureReleasesTheSharedPool(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-build-failure"
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	rows := make([][2]int64, 20000)
	for i := range rows {
		rows[i] = [2]int64{int64(i), int64(i % 97)}
	}
	good := makeBuildWshf(t, rows)
	const goodKey, badKey = "in/bf/build-0.wshf", "in/bf/build-1.wshf"
	if _, err := store.Put(ctx, bucket, goodKey, bytes.NewReader(good), int64(len(good)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	// The second run: a valid header and a truncated body, so the failure
	// comes from decoding it, after the first run's rows were absorbed.
	bad := append([]byte(nil), good[:len(good)/2]...)
	if _, err := store.Put(ctx, bucket, badKey, bytes.NewReader(bad), int64(len(bad)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	for _, route := range []struct {
		name  string
		typ   distributed.OpType
		panic bool // fail by a panic in the build tail instead of a decode error
	}{
		{"hash-shuffle", distributed.OpHashJoinProbe, false},
		{"broadcast-cache", distributed.OpBroadcastProbe, false},
		{"hash-shuffle-panic", distributed.OpHashJoinProbe, true},
		{"broadcast-cache-panic", distributed.OpBroadcastProbe, true},
	} {
		t.Run(route.name, func(t *testing.T) {
			ex := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
			ex.SetMemoryBudget(64<<20, t.TempDir())
			tr := ex.sharedTracker
			if tr == nil {
				t.Fatal("no shared pool")
			}
			// The live peer: another statement's ordinary and forced charges.
			const peer, peerForced = 3 << 20, 1 << 20
			if err := tr.Reserve(peer); err != nil {
				t.Fatal(err)
			}
			tr.ForceReserveFor(peerForced, memory.ForceScanDecodedBatch)
			defer func() {
				tr.Release(peer)
				tr.ReleaseForced(peerForced, memory.ForceScanDecodedBatch)
			}()
			var before [memory.NumForcePurposes]int64
			for p := memory.ForcePurpose(0); p < memory.NumForcePurposes; p++ {
				before[p] = tr.ForcedFor(p)
			}
			usedBefore, peakBefore := tr.Used(), tr.Peak()

			files := []string{goodKey, badKey}
			if route.panic {
				// A panic raised inside the build (here: in its tail, after
				// every arrival was charged) unwinds through the fragment
				// builder; the broadcast cache recovers it into an error, the
				// direct path lets it reach the task boundary.
				files = []string{goodKey}
				defer exec.SetPhaseHook(func(site exec.PhaseSite, _ any) {
					if site == exec.PhaseJoinBuildTail {
						panic("injected build-tail panic")
					}
				})()
			}
			spec := distributed.OpSpec{
				Type:        route.typ,
				JoinType:    "inner",
				LeftKeys:    []string{"id"},
				RightKeys:   []string{"id"},
				BuildAlias:  "build",
				BuildFiles:  files,
				BuildBucket: bucket,
				BuildSchema: []distributed.ColumnSpec{
					{Name: "id", Type: int(parquet.TypeInt64)},
					{Name: "val", Type: int(parquet.TypeInt64)},
				},
			}
			task := distributed.Task{ID: "t-bf-" + route.name, QueryID: "q-bf-" + route.name, DataBucket: bucket, ResultBucket: bucket}
			ops, cleanup, err := func() (ops []exec.UnaryOperator, cleanup func(), err error) {
				defer func() {
					if r := recover(); r != nil {
						err = fmt.Errorf("panicked: %v", r)
					}
				}()
				return ex.buildFragmentJoinProbe(ctx, task, spec)
			}()
			if err == nil {
				if cleanup != nil {
					cleanup()
				}
				t.Fatalf("the build over a corrupt run succeeded (%d ops): this test proves nothing", len(ops))
			}
			t.Logf("build failed as arranged: %s", strings.SplitN(err.Error(), "\n", 2)[0])
			if tr.Peak() <= peakBefore {
				t.Fatalf("the failed build charged nothing before it failed (peak %d, before %d): this test proves nothing",
					tr.Peak(), peakBefore)
			}
			if got := tr.Used(); got != usedBefore {
				t.Errorf("after the failed build the pool holds %d bytes; the live peer holds %d (%d stranded)",
					got, usedBefore, got-usedBefore)
			}
			for p := memory.ForcePurpose(0); p < memory.NumForcePurposes; p++ {
				if got := tr.ForcedFor(p); got != before[p] {
					t.Errorf("after the failed build the forced census reports %d for %q; the peer's baseline is %d",
						got, p, before[p])
				}
			}
			if n := ex.broadcastCache.Count(); n != 0 {
				t.Errorf("the broadcast cache holds %d entries after a failed build", n)
			}
		})
	}
}
