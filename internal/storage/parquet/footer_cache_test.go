package parquet

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// footerFixture builds a parquet file with cols columns and rows rows.
func footerFixture(t testing.TB, cols, rows int) ([]byte, Schema) {
	t.Helper()
	schema := Schema{Columns: make([]Column, cols)}
	for i := range schema.Columns {
		schema.Columns[i] = Column{Name: fmt.Sprintf("c%d", i), Type: TypeInt64}
	}
	rowMaps := make([]map[string]any, rows)
	for r := range rowMaps {
		m := make(map[string]any, cols)
		for c := 0; c < cols; c++ {
			m[fmt.Sprintf("c%d", c)] = int64(r*cols + c)
		}
		rowMaps[r] = m
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rowMaps); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), schema
}

// withCleanFooterCache gives a test the process cache to itself and restores
// the enable flag afterwards.
func withCleanFooterCache(t testing.TB, enabled bool) {
	t.Helper()
	prev := SetFooterCacheEnabled(enabled)
	ResetFooterCache()
	t.Cleanup(func() {
		SetFooterCacheEnabled(prev)
		ResetFooterCache()
	})
}

func TestFooterCacheHitAndMiss(t *testing.T) {
	withCleanFooterCache(t, true)
	data, schema := footerFixture(t, 8, 64)

	fr1, err := OpenFileReaderFromBytesCached(data, "store/bucket/a.parquet#1@1")
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if s := FooterCacheStatsSnapshot(); s.Misses != 1 || s.Hits != 0 || s.Inserts != 1 {
		t.Fatalf("after first open: %+v", s)
	}

	fr2, err := OpenFileReaderFromBytesCached(data, "store/bucket/a.parquet#1@1")
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	if s := FooterCacheStatsSnapshot(); s.Hits != 1 || s.Inserts != 1 {
		t.Fatalf("after second open: %+v", s)
	}

	// The hit must share the decoded metadata, not a re-decode.
	if fr1.Meta() != fr2.Meta() {
		t.Fatal("cache hit returned a different *FileMetaData")
	}
	// ...and still describe the file correctly.
	for _, fr := range []*FileReader{fr1, fr2} {
		if got := len(fr.Schema().Columns); got != len(schema.Columns) {
			t.Fatalf("columns = %d, want %d", got, len(schema.Columns))
		}
		if fr.NumRows() != 64 {
			t.Fatalf("rows = %d, want 64", fr.NumRows())
		}
	}

	// A different identity must not hit.
	if _, err := OpenFileReaderFromBytesCached(data, "store/bucket/b.parquet#1@1"); err != nil {
		t.Fatalf("third open: %v", err)
	}
	if s := FooterCacheStatsSnapshot(); s.Inserts != 2 {
		t.Fatalf("distinct identity should insert: %+v", s)
	}
}

// TestFooterCacheKeyDiscriminates is the anti-staleness test: the same object
// path with a different size or a different CreatedAt stamp (a recreated
// path) must never serve the earlier footer.
func TestFooterCacheKeyDiscriminates(t *testing.T) {
	withCleanFooterCache(t, true)
	narrow, _ := footerFixture(t, 2, 8)
	wide, _ := footerFixture(t, 9, 8)

	// Same path, different size component.
	frNarrow, err := OpenFileReaderFromBytesCached(narrow,
		"mem:1/test/tables/t/chunk_001.parquet#100@5")
	if err != nil {
		t.Fatal(err)
	}
	frWide, err := OpenFileReaderFromBytesCached(wide,
		"mem:1/test/tables/t/chunk_001.parquet#200@5")
	if err != nil {
		t.Fatal(err)
	}
	if len(frNarrow.Schema().Columns) != 2 || len(frWide.Schema().Columns) != 9 {
		t.Fatalf("size component failed to discriminate: %d / %d",
			len(frNarrow.Schema().Columns), len(frWide.Schema().Columns))
	}

	// Same path AND same size, different CreatedAt — the recreated-path case.
	frRecreated, err := OpenFileReaderFromBytesCached(wide,
		"mem:1/test/tables/t/chunk_001.parquet#100@6")
	if err != nil {
		t.Fatal(err)
	}
	if len(frRecreated.Schema().Columns) != 9 {
		t.Fatalf("createdAt component failed to discriminate: got %d columns, want 9",
			len(frRecreated.Schema().Columns))
	}

	// Same path, same size, same CreatedAt, different store instance.
	frOtherStore, err := OpenFileReaderFromBytesCached(wide,
		"mem:2/test/tables/t/chunk_001.parquet#100@5")
	if err != nil {
		t.Fatal(err)
	}
	if len(frOtherStore.Schema().Columns) != 9 {
		t.Fatalf("storeID component failed to discriminate: got %d columns, want 9",
			len(frOtherStore.Schema().Columns))
	}
}

func TestFooterCacheEmptyIdentityBypasses(t *testing.T) {
	withCleanFooterCache(t, true)
	data, _ := footerFixture(t, 4, 8)

	for i := 0; i < 3; i++ {
		if _, err := OpenFileReaderFromBytesCached(data, ""); err != nil {
			t.Fatal(err)
		}
	}
	s := FooterCacheStatsSnapshot()
	if s.Hits != 0 || s.Misses != 0 || s.Inserts != 0 || s.Entries != 0 {
		t.Fatalf("empty identity must not touch the cache: %+v", s)
	}
}

func TestFooterCacheDisabled(t *testing.T) {
	withCleanFooterCache(t, false)
	data, _ := footerFixture(t, 4, 8)

	fr1, err := OpenFileReaderFromBytesCached(data, "s/b/k.parquet#1@1")
	if err != nil {
		t.Fatal(err)
	}
	fr2, err := OpenFileReaderFromBytesCached(data, "s/b/k.parquet#1@1")
	if err != nil {
		t.Fatal(err)
	}
	if fr1.Meta() == fr2.Meta() {
		t.Fatal("cache disabled but metadata was shared")
	}
	if s := FooterCacheStatsSnapshot(); s.Entries != 0 || s.Inserts != 0 {
		t.Fatalf("cache disabled but populated: %+v", s)
	}
	if LookupFooter("s/b/k.parquet#1@1") != nil {
		t.Fatal("LookupFooter must be inert while disabled")
	}
	// Results must still be right with the cache off.
	if fr2.NumRows() != 8 || len(fr2.Schema().Columns) != 4 {
		t.Fatalf("disabled path decoded wrong: rows=%d cols=%d", fr2.NumRows(), len(fr2.Schema().Columns))
	}
}

func TestFooterCacheEviction(t *testing.T) {
	data, _ := footerFixture(t, 4, 8)
	entry, err := decodeFooter(newBytesReaderAt(data), int64(len(data)), false)
	if err != nil {
		t.Fatal(err)
	}
	if entry.bytes <= 0 {
		t.Fatalf("entry size estimate must be positive, got %d", entry.bytes)
	}

	// Cap at ~3 entries; maxEntry is cap/8 so give the cap room to admit.
	c := newFooterCache(entry.bytes * 3)
	c.maxEntry = entry.bytes * 2
	for i := 0; i < 10; i++ {
		e, err := decodeFooter(newBytesReaderAt(data), int64(len(data)), false)
		if err != nil {
			t.Fatal(err)
		}
		c.put(fmt.Sprintf("k%d", i), e)
	}
	if c.bytes > c.capBytes {
		t.Fatalf("cache over cap: %d > %d", c.bytes, c.capBytes)
	}
	if c.evictions.Load() == 0 {
		t.Fatal("expected evictions")
	}
	// LRU order: the newest keys survive, the oldest are gone.
	if c.get("k9") == nil {
		t.Fatal("most recent entry evicted")
	}
	if c.get("k0") != nil {
		t.Fatal("oldest entry should have been evicted")
	}

	// An entry larger than maxEntry is rejected rather than admitted.
	c.maxEntry = 1
	before := c.inserts.Load()
	e, err := decodeFooter(newBytesReaderAt(data), int64(len(data)), false)
	if err != nil {
		t.Fatal(err)
	}
	c.put("oversized", e)
	if c.inserts.Load() != before || c.rejected.Load() == 0 {
		t.Fatalf("oversized entry admitted: inserts=%d rejected=%d",
			c.inserts.Load(), c.rejected.Load())
	}
}

// TestFooterCacheConcurrent exercises the cache under -race with many
// goroutines reading and inserting overlapping identities, and checks that
// every reader sees a footer describing its own file.
func TestFooterCacheConcurrent(t *testing.T) {
	withCleanFooterCache(t, true)

	const files = 8
	datas := make([][]byte, files)
	wantCols := make([]int, files)
	for i := range datas {
		cols := 2 + i
		datas[i], _ = footerFixture(t, cols, 16)
		wantCols[i] = cols
	}

	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for n := 0; n < 50; n++ {
				i := (g + n) % files
				id := fmt.Sprintf("mem:1/test/tables/t/f%d.parquet#%d@7", i, len(datas[i]))
				fr, err := OpenFileReaderFromBytesCached(datas[i], id)
				if err != nil {
					t.Errorf("open %d: %v", i, err)
					return
				}
				if got := len(fr.Schema().Columns); got != wantCols[i] {
					t.Errorf("file %d: got %d columns, want %d", i, got, wantCols[i])
					return
				}
				if fr.NumRows() != 16 {
					t.Errorf("file %d: got %d rows, want 16", i, fr.NumRows())
					return
				}
				// Read through the shared metadata the way a scan does.
				for rg := 0; rg < fr.NumRowGroups(); rg++ {
					if fr.RowGroupMeta(rg) == nil {
						t.Errorf("file %d: nil row group %d", i, rg)
						return
					}
					_ = fr.RowGroupStats(rg)
				}
				// LookupFooter may return a DIFFERENT *FileMetaData than
				// fr: without singleflight two racing misses both decode
				// and only one insert wins. What must always hold is that
				// whatever is cached under this identity describes THIS
				// file — that is the property a stale key would break.
				if lf := LookupFooter(id); lf != nil {
					if len(lf.Schema().Columns) != wantCols[i] || lf.NumRows() != 16 {
						t.Errorf("file %d: cached footer describes a different file (%d cols, %d rows)",
							i, len(lf.Schema().Columns), lf.NumRows())
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestFooterCacheSharedMetadataMatchesUncached pins the equivalence the cache
// depends on: a reader built over a cached footer decodes byte-identically to
// one built by the uncached opener.
func TestFooterCacheSharedMetadataMatchesUncached(t *testing.T) {
	withCleanFooterCache(t, true)
	data, _ := footerFixture(t, 6, 200)

	ref, err := OpenFileReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	// Populate, then read through a second reader over the cached footer.
	if _, err := OpenFileReaderFromBytesCached(data, "mem:1/test/x.parquet#9@9"); err != nil {
		t.Fatal(err)
	}
	got, err := OpenFileReaderFromBytesCached(data, "mem:1/test/x.parquet#9@9")
	if err != nil {
		t.Fatal(err)
	}
	if got.NumRows() != ref.NumRows() || got.NumRowGroups() != ref.NumRowGroups() {
		t.Fatalf("shape mismatch: %d/%d vs %d/%d",
			got.NumRows(), got.NumRowGroups(), ref.NumRows(), ref.NumRowGroups())
	}
	for rg := 0; rg < ref.NumRowGroups(); rg++ {
		rs, rr := got.RowGroupStats(rg), ref.RowGroupStats(rg)
		if rs.NumRows != rr.NumRows || len(rs.Columns) != len(rr.Columns) {
			t.Fatalf("rg %d stats mismatch", rg)
		}
		for name, want := range rr.Columns {
			gotCS, ok := rs.Columns[name]
			if !ok || gotCS.MinValue != want.MinValue || gotCS.MaxValue != want.MaxValue {
				t.Fatalf("rg %d column %q stats mismatch: %+v vs %+v", rg, name, gotCS, want)
			}
		}
		for col := 0; col < len(ref.Leaves()); col++ {
			pr, pref := got.ColumnPages(rg, col), ref.ColumnPages(rg, col)
			if (pr == nil) != (pref == nil) {
				t.Fatalf("rg %d col %d page reader mismatch", rg, col)
			}
		}
	}
}

// TestFooterEntrySizeEstimate reports the decoded footprint of a wide file —
// the number the byte cap is sized against.
func TestFooterEntrySizeEstimate(t *testing.T) {
	for _, cols := range []int{16, 105} {
		data, _ := footerFixture(t, cols, 64)
		e, err := decodeFooter(newBytesReaderAt(data), int64(len(data)), false)
		if err != nil {
			t.Fatal(err)
		}
		if e.bytes <= 0 {
			t.Fatalf("%d columns: non-positive estimate %d", cols, e.bytes)
		}
		t.Logf("%d-column footer: estimated %d bytes decoded (file %d bytes)",
			cols, e.bytes, len(data))
	}
}

// BenchmarkFooterDecode measures the footer-decode work a query pays per file
// with the cache disabled versus enabled. The cached arm is what both
// buildRGUnits and fileSlot.ensureLoaded now pay after the first decode.
func BenchmarkFooterDecode(b *testing.B) {
	for _, cols := range []int{16, 105} {
		data, _ := footerFixture(b, cols, 512)
		id := fmt.Sprintf("mem:1/bench/tables/t/f.parquet#%d@1", len(data))

		b.Run(fmt.Sprintf("cols=%d/cache=off", cols), func(b *testing.B) {
			withCleanFooterCache(b, false)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fr, err := OpenFileReaderFromBytesCached(data, id)
				if err != nil {
					b.Fatal(err)
				}
				_ = fr.NumRowGroups()
			}
		})

		b.Run(fmt.Sprintf("cols=%d/cache=on", cols), func(b *testing.B) {
			withCleanFooterCache(b, true)
			if _, err := OpenFileReaderFromBytesCached(data, id); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fr, err := OpenFileReaderFromBytesCached(data, id)
				if err != nil {
					b.Fatal(err)
				}
				_ = fr.NumRowGroups()
			}
		})
	}
}
