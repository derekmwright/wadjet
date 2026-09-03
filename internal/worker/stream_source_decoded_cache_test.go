package worker

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestDecodedCache_StreamSourceEngagement drives the full worker scan path
// (cachedFileStreamSource → decode-ahead iterator → ReadRowGroupNativeCached)
// against an executor with the decoded-chunk cache attached, three times
// over the same base-table keys: pass 1 registers ghosts, pass 2 admits,
// pass 3 serves hits — with identical row sums every pass. This is the
// wiring proof (identity set from bucket/key, cache reaches the iterator);
// value-level correctness is covered in scan's decoded_cache_test.go.
//
// The admission PAUSE is disarmed here, and that is the whole of #564's
// decoded-cache member. SetDecodedCache installs a pressure hook
// (executor.go, `rawHeapPressureActive() || pageCachePressureActive()`) that
// DecodedChunkCache.Offer consults BEFORE it registers a ghost
// (scan/decoded_cache.go — `c.pressurePaused.Add(1); return`, above
// `c.ghostRegistered.Add(1)`). Both gauges are process-wide, and the heap one
// caches its verdict for 100ms across the whole process, so whatever else the
// test binary or the machine is doing decides them. On a loaded parallel sweep
// this test therefore read `ghosts=0 admitted=0` and reported the WIRING as
// broken when the wiring was fine and an unrelated global gauge was hot.
//
// Wiring and admission-pause are two mechanisms, so they get two assertions.
// This test pins the pressure hook off and asserts PressurePaused stayed 0 —
// so the ghost counts are a statement about the scan path and nothing else,
// and a future change that reintroduced a machine-dependent skip would fail
// here loudly instead of flaking. TestDecodedCache_AdmissionPausesUnderPressure
// below covers the pause itself, with the gauge under the test's control.
func TestDecodedCache_StreamSourceEngagement(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	keys, wantSum := writeMultiGroupFixture(t, store, "b", 3, 3, 32)

	e := &Executor{store: store, spillDir: t.TempDir()}
	e.SetScanDecodeAhead(true, 0)
	cache := scan.NewDecodedChunkCache(64 << 20)
	e.SetDecodedCache(cache)
	// After SetDecodedCache, which is what installs the process-wide hook.
	cache.SetPressureFunc(func() bool { return false })

	scanOnce := func(pass int) {
		src := newCachedFileStreamSource(e, "", "b", keys)
		if err := src.Init(ctx); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		sum, rows := drainValSum(t, ctx, src)
		if err := src.Close(); err != nil {
			t.Fatalf("pass %d close: %v", pass, err)
		}
		if sum != wantSum || rows != 3*3*32 {
			t.Fatalf("pass %d: sum=%d rows=%d, want sum=%d rows=%d", pass, sum, rows, wantSum, 3*3*32)
		}
	}

	scanOnce(1)
	st := cache.Stats()
	if st.PressurePaused != 0 {
		t.Fatalf("after pass 1: PressurePaused=%d with the hook pinned off — this test's counts are "+
			"supposed to depend on the scan path alone", st.PressurePaused)
	}
	if st.GhostRegistered == 0 || st.Admitted != 0 {
		t.Fatalf("after pass 1: ghosts=%d admitted=%d, want >0 / 0 (stats: %+v)",
			st.GhostRegistered, st.Admitted, st)
	}

	scanOnce(2)
	st = cache.Stats()
	if st.Admitted == 0 || st.SizeBytes == 0 {
		t.Fatalf("after pass 2: admitted=%d size=%d, want both >0", st.Admitted, st.SizeBytes)
	}

	scanOnce(3)
	st = cache.Stats()
	if st.Hits == 0 {
		t.Fatalf("after pass 3: hits=%d, want >0 (stats: %+v)", st.Hits, st)
	}

	// Serial (kill-switch) path engages the cache too.
	e.SetScanDecodeAhead(false, 0)
	scanOnce(4)
	if got := cache.Stats().Hits; got <= st.Hits {
		t.Fatalf("serial pass 4 added no hits: %d -> %d", st.Hits, got)
	}
}

// TestDecodedCache_AdmissionPausesUnderPressure is the other half of the
// mechanism TestDecodedCache_StreamSourceEngagement pins off: while pressure is
// live the cache registers no ghosts and clones nothing, and it resumes when
// pressure clears. The gauge is the test's, not the process's, so this asserts
// the POLICY rather than the machine's mood — which is the distinction #564 is
// about (a counter that is a function of how many cores were free cannot tell
// "the mechanism disengaged" from "the machine was busy").
func TestDecodedCache_AdmissionPausesUnderPressure(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	keys, wantSum := writeMultiGroupFixture(t, store, "b", 3, 3, 32)

	e := &Executor{store: store, spillDir: t.TempDir()}
	e.SetScanDecodeAhead(true, 0)
	cache := scan.NewDecodedChunkCache(64 << 20)
	e.SetDecodedCache(cache)

	var pressed atomic.Bool
	cache.SetPressureFunc(pressed.Load)

	scanOnce := func(pass int) {
		t.Helper()
		src := newCachedFileStreamSource(e, "", "b", keys)
		if err := src.Init(ctx); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		sum, rows := drainValSum(t, ctx, src)
		if err := src.Close(); err != nil {
			t.Fatalf("pass %d close: %v", pass, err)
		}
		// The rows are the same either way: the pause governs RESIDENCY, never
		// the answer.
		if sum != wantSum || rows != 3*3*32 {
			t.Fatalf("pass %d: sum=%d rows=%d, want sum=%d rows=%d", pass, sum, rows, wantSum, 3*3*32)
		}
	}

	pressed.Store(true)
	scanOnce(1)
	scanOnce(2)
	st := cache.Stats()
	if st.PressurePaused == 0 {
		t.Fatalf("under pressure: PressurePaused=0, so the pause never ran and the rest of this proves nothing")
	}
	if st.GhostRegistered != 0 || st.Admitted != 0 || st.SizeBytes != 0 {
		t.Fatalf("under pressure: ghosts=%d admitted=%d size=%d, want 0/0/0 — a pressured heap must not be "+
			"re-inflated by new clones", st.GhostRegistered, st.Admitted, st.SizeBytes)
	}

	pressed.Store(false)
	scanOnce(3)
	if got := cache.Stats(); got.GhostRegistered == 0 {
		t.Fatalf("after pressure cleared: ghosts=%d, want >0 — the pause must not be permanent (stats: %+v)",
			got.GhostRegistered, got)
	}
	scanOnce(4)
	if got := cache.Stats(); got.Admitted == 0 || got.SizeBytes == 0 {
		t.Fatalf("after pressure cleared: admitted=%d size=%d, want both >0 (stats: %+v)",
			got.Admitted, got.SizeBytes, got)
	}
}

// TestDecodedCache_QueryScratchIneligible: queries/-prefixed parquet keys
// must never engage the cache (scratch objects are deleted at query end).
func TestDecodedCache_QueryScratchIneligible(t *testing.T) {
	e := &Executor{store: objstore.NewMemStore(), spillDir: t.TempDir()}
	cache := scan.NewDecodedChunkCache(64 << 20)
	e.SetDecodedCache(cache)
	src := newCachedFileStreamSource(e, "", "b", nil)

	if id := src.decodedCacheIdentity("queries/q1/stage0/part.parquet", 100); id != "" {
		t.Fatalf("query scratch got identity %q", id)
	}
	if id := src.decodedCacheIdentity("tables/t/part.wshf", 100); id != "" {
		t.Fatalf("non-parquet got identity %q", id)
	}
	if id := src.decodedCacheIdentity("tables/t/part.parquet", 100); id != "b/tables/t/part.parquet#100" {
		t.Fatalf("base table identity = %q", id)
	}

	// With the cache detached, identity must be empty (cache fully inert).
	e.SetDecodedCache(nil)
	if id := src.decodedCacheIdentity("tables/t/part.parquet", 100); id != "" {
		t.Fatalf("identity with nil cache = %q", id)
	}
}
