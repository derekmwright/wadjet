package worker

import (
	"context"
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
func TestDecodedCache_StreamSourceEngagement(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	keys, wantSum := writeMultiGroupFixture(t, store, "b", 3, 3, 32)

	e := &Executor{store: store, spillDir: t.TempDir()}
	e.SetScanDecodeAhead(true, 0)
	cache := scan.NewDecodedChunkCache(64 << 20)
	e.SetDecodedCache(cache)

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
	if st.GhostRegistered == 0 || st.Admitted != 0 {
		t.Fatalf("after pass 1: ghosts=%d admitted=%d, want >0 / 0", st.GhostRegistered, st.Admitted)
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
