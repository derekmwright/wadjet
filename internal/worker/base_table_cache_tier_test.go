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

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// getCountingStore counts Get calls so the test can prove the second scan
// pass never reached S3.
type getCountingStore struct {
	objstore.Store
	gets atomic.Int64
}

func (s *getCountingStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	s.gets.Add(1)
	return s.Store.Get(ctx, bucket, key)
}

// TestBaseTableCacheTier_SecondPassServesWithoutGET: with the base-table
// cache enabled, the first scan pass populates the cache (via the miss
// tee) and the second pass must be served entirely from local disk — the
// prefetcher skips resident files (HasCachedPath probe) and openNextFile
// mmaps the cache files in place, which also means Close must NOT unlink
// them (the cache owns its files; docs/design/base-table-nvme-cache.md §7).
func TestBaseTableCacheTier_SecondPassServesWithoutGET(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}}
	inner := &getCountingStore{Store: objstore.NewMemStore()}
	ctx := context.Background()
	if err := inner.MakeBucket(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	var files []string
	const rowsPerFile = 100
	for f := 0; f < 3; f++ {
		var buf bytes.Buffer
		w, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, rowsPerFile)
		for i := range rows {
			rows[i] = map[string]any{"id": int64(f*rowsPerFile + i)}
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("tables/t/chunk_%d.parquet", f)
		if _, err := inner.Put(ctx, "b", path, bytes.NewReader(buf.Bytes()), int64(buf.Len()), ""); err != nil {
			t.Fatal(err)
		}
		files = append(files, path)
	}

	cacheDir := t.TempDir()
	btc, err := objstore.NewBaseTableCache(inner, cacheDir, 1<<30, nil)
	if err != nil {
		t.Fatal(err)
	}
	ex := &Executor{store: btc, spillDir: t.TempDir()}

	scanAll := func() int {
		t.Helper()
		src := newCachedFileStreamSource(ex, "", "b", files)
		if err := src.Init(ctx); err != nil {
			t.Fatal(err)
		}
		rows := 0
		for {
			b, err := src.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			rows += b.Len
		}
		if err := src.Close(); err != nil {
			t.Fatal(err)
		}
		if src.mmapData != nil || src.localPath != "" {
			t.Fatal("mmap/localPath still held after Close")
		}
		return rows
	}

	if rows := scanAll(); rows != 3*rowsPerFile {
		t.Fatalf("cold pass rows = %d, want %d", rows, 3*rowsPerFile)
	}
	coldGets := inner.gets.Load()
	if coldGets == 0 {
		t.Fatal("cold pass issued no inner Gets")
	}
	if s := btc.Stats(); s.Entries != 3 {
		t.Fatalf("cold pass cached %d entries, want 3 (stats %+v)", s.Entries, s)
	}

	if rows := scanAll(); rows != 3*rowsPerFile {
		t.Fatalf("warm pass rows = %d, want %d", rows, 3*rowsPerFile)
	}
	if got := inner.gets.Load(); got != coldGets {
		t.Fatalf("warm pass reached the inner store: gets %d -> %d", coldGets, got)
	}
	if s := btc.Stats(); s.Hits < 3 {
		t.Fatalf("warm pass hits = %d, want >= 3 (stats %+v)", s.Hits, s)
	}

	// The warm pass mmap'd the cache's files in place — they must survive
	// the source Close (only LRU eviction may unlink them).
	survivors := 0
	if err := filepath.Walk(filepath.Join(cacheDir, "b"), func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			survivors++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if survivors != 3 {
		t.Fatalf("%d cache files survive after warm pass, want 3", survivors)
	}
}

// memPeerFetcher serves whole objects from a second store, standing in for
// the rendezvous owner's NVMe copy.
type memPeerFetcher struct {
	src     objstore.Store
	fetches atomic.Int64
}

func (p *memPeerFetcher) FetchBaseTable(ctx context.Context, bucket, key string) (io.ReadCloser, bool) {
	rc, _, err := p.src.Get(ctx, bucket, key)
	if err != nil {
		return nil, false
	}
	p.fetches.Add(1)
	return rc, true
}

// TestBaseTableCacheTier_PeerPopulateSkipsPrefetchCopy: a probe file this
// worker does not hold is fetched from its owner by the cache's peer tier
// INSIDE the prefetcher's Get, which spools it onto the cache volume and
// admits it. Before the 2026-08-22 fix the prefetcher then copied that
// admitted file a second time into the spill dir (+fsync) and the consumer
// mmapped the copy; now the prefetcher notices the cache became resident
// and skips, so the open takes the base-cache tier in place — no second
// copy, no spill temp, and the inner store is never consulted.
func TestBaseTableCacheTier_PeerPopulateSkipsPrefetchCopy(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	ctx := context.Background()
	owner := objstore.NewMemStore() // the peer's copy
	inner := &getCountingStore{Store: objstore.NewMemStore()}
	for _, st := range []objstore.Store{owner, inner} {
		if err := st.MakeBucket(ctx, "b"); err != nil {
			t.Fatal(err)
		}
	}
	var files []string
	const rowsPerFile = 50
	for f := 0; f < 3; f++ {
		var buf bytes.Buffer
		w, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, rowsPerFile)
		for i := range rows {
			rows[i] = map[string]any{"id": int64(f*rowsPerFile + i)}
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("tables/t/chunk_%d.parquet", f)
		for _, st := range []objstore.Store{owner, inner} {
			if _, err := st.Put(ctx, "b", path, bytes.NewReader(buf.Bytes()), int64(buf.Len()), ""); err != nil {
				t.Fatal(err)
			}
		}
		files = append(files, path)
	}

	btc, err := objstore.NewBaseTableCache(inner, t.TempDir(), 1<<30, nil)
	if err != nil {
		t.Fatal(err)
	}
	peer := &memPeerFetcher{src: owner}
	btc.SetPeerFetcher(peer)
	spill := t.TempDir()
	ex := &Executor{store: btc, spillDir: spill}

	src := newCachedFileStreamSource(ex, "", "b", files)
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	rows := 0
	for {
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		rows += b.Len
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if rows != 3*rowsPerFile {
		t.Fatalf("rows = %d, want %d", rows, 3*rowsPerFile)
	}
	if got := peer.fetches.Load(); got != 3 {
		t.Fatalf("peer fetches = %d, want 3 (one per file)", got)
	}
	if got := inner.gets.Load(); got != 0 {
		t.Fatalf("inner store reached %d times; the peer tier should have served every miss", got)
	}
	if n := src.acq.files[acqPrefetch]; n != 0 {
		t.Fatalf("%d files opened from a prefetch copy; peer-populated files must open from the cache in place (stats %+v)", n, src.acq.files)
	}
	if n := src.acq.files[acqBaseCache]; n != 3 {
		t.Fatalf("base-cache opens = %d, want 3 (stats %+v)", n, src.acq.files)
	}
	// No spill temp was ever written for these files.
	entries, err := os.ReadDir(spill)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			t.Fatalf("spill dir holds %s: the prefetcher copied a cache-resident file", e.Name())
		}
	}
	if s := btc.Stats(); s.PeerHits != 3 || s.PeerFetchNanos <= 0 {
		t.Fatalf("peer ledger: %+v (want 3 hits with nonzero fetch time)", s)
	}
}

// TestBaseTableCacheTier_PeerPopulateCopiesPrefetchWhenSkipOff is the same
// shape as TestBaseTableCacheTier_PeerPopulateSkipsPrefetchCopy but with
// prefetchCacheSkip OFF, proving the switch restores the pre-2026-08-22
// behavior: the prefetcher copies a peer-populated file into the spill dir
// a second time instead of noticing the cache became resident, so the
// consumer opens the spill-temp copy (acqPrefetch) rather than the
// base-cache tier in place (acqBaseCache).
func TestBaseTableCacheTier_PeerPopulateCopiesPrefetchWhenSkipOff(t *testing.T) {
	prev := prefetchCacheSkip.Set(false)
	t.Cleanup(func() { prefetchCacheSkip.Set(prev) })

	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	ctx := context.Background()
	owner := objstore.NewMemStore() // the peer's copy
	inner := &getCountingStore{Store: objstore.NewMemStore()}
	for _, st := range []objstore.Store{owner, inner} {
		if err := st.MakeBucket(ctx, "b"); err != nil {
			t.Fatal(err)
		}
	}
	var files []string
	const rowsPerFile = 50
	for f := 0; f < 3; f++ {
		var buf bytes.Buffer
		w, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, rowsPerFile)
		for i := range rows {
			rows[i] = map[string]any{"id": int64(f*rowsPerFile + i)}
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("tables/t/chunk_%d.parquet", f)
		for _, st := range []objstore.Store{owner, inner} {
			if _, err := st.Put(ctx, "b", path, bytes.NewReader(buf.Bytes()), int64(buf.Len()), ""); err != nil {
				t.Fatal(err)
			}
		}
		files = append(files, path)
	}

	btc, err := objstore.NewBaseTableCache(inner, t.TempDir(), 1<<30, nil)
	if err != nil {
		t.Fatal(err)
	}
	peer := &memPeerFetcher{src: owner}
	btc.SetPeerFetcher(peer)
	ex := &Executor{store: btc, spillDir: t.TempDir()}

	src := newCachedFileStreamSource(ex, "", "b", files)
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	rows := 0
	for {
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		rows += b.Len
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if rows != 3*rowsPerFile {
		t.Fatalf("rows = %d, want %d", rows, 3*rowsPerFile)
	}
	// Old behavior: every file opens from the prefetch spill copy, never
	// the base-cache tier in place. Close may reap the spill temps, so
	// only the per-tier open counts are asserted (not the spill dir's
	// contents).
	if n := src.acq.files[acqPrefetch]; n != 3 {
		t.Fatalf("prefetch opens = %d, want 3 (switch off must restore the old copy-into-spill behavior; stats %+v)", n, src.acq.files)
	}
	if n := src.acq.files[acqBaseCache]; n != 0 {
		t.Fatalf("base-cache opens = %d, want 0 (stats %+v)", n, src.acq.files)
	}
}
