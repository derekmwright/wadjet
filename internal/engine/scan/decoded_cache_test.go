package scan

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

// decodedCacheFixtureSchema is the multi-type schema used by the cache e2e
// tests: fixed-width, bytes (with nulls) and bool columns, covering the
// clone/copy arms the TPC-H suite exercises.
var decodedCacheFixtureSchema = []pqt.Column{
	{Name: "id", Type: pqt.TypeInt64},
	{Name: "name", Type: pqt.TypeString, Nullable: true},
	{Name: "val", Type: pqt.TypeFloat64},
	{Name: "flag", Type: pqt.TypeBool},
}

// decodedCacheFixtureFile builds an in-memory parquet file with numGroups
// row groups of rowsPerGroup rows; every 7th name is null.
func decodedCacheFixtureFile(t *testing.T, numGroups, rowsPerGroup int) []byte {
	t.Helper()
	cfg := pqt.DefaultWriterConfig()
	cfg.RowGroupSize = rowsPerGroup

	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, pqt.Schema{Columns: decodedCacheFixtureSchema}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for g := 0; g < numGroups; g++ {
		rows := make([]map[string]any, rowsPerGroup)
		for i := range rows {
			id := int64(g*100000 + i)
			row := map[string]any{
				"id":   id,
				"val":  float64(id) * 1.5,
				"flag": i%2 == 0,
			}
			if i%7 != 0 {
				row["name"] = fmt.Sprintf("row-%d-%d", g, i)
			}
			rows[i] = row
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatalf("write group %d: %v", g, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func openFixtureReader(t *testing.T, data []byte, identity string) *pqt.FileReader {
	t.Helper()
	reader, err := pqt.NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	fr := reader.FileReader()
	if identity != "" {
		fr.SetCacheIdentity(identity)
	}
	return fr
}

// assertBatchesEqual compares two batches value-for-value via the
// view-aware accessor (covers nulls).
func assertBatchesEqual(t *testing.T, want, got *batch.RecordBatch) {
	t.Helper()
	if want.Len != got.Len {
		t.Fatalf("row count %d != %d", got.Len, want.Len)
	}
	if len(want.Columns) != len(got.Columns) {
		t.Fatalf("column count %d != %d", len(got.Columns), len(want.Columns))
	}
	for c := range want.Columns {
		for r := 0; r < want.Len; r++ {
			w := want.Columns[c].GetValue(r)
			g := got.Columns[c].GetValue(r)
			if fmt.Sprint(w) != fmt.Sprint(g) {
				t.Fatalf("col %d row %d: got %v, want %v", c, r, g, w)
			}
		}
	}
}

// TestDecodedChunkCache_SecondTouchAdmitAndHit walks the ghost → admit →
// hit lifecycle through ReadRowGroupNativeCached and verifies the hit
// batch is value-identical to an uncached decode.
func TestDecodedChunkCache_SecondTouchAdmitAndHit(t *testing.T) {
	data := decodedCacheFixtureFile(t, 2, 200)
	fr := openFixtureReader(t, data, "bucket/tables/t/a.parquet#1")
	cache := NewDecodedChunkCache(64 << 20)

	uncached, err := ReadRowGroupNative(fr, 0, decodedCacheFixtureSchema, nil)
	if err != nil {
		t.Fatal(err)
	}

	// First cached read: every column registers a ghost, nothing stored.
	b1, err := ReadRowGroupNativeCached(fr, 0, decodedCacheFixtureSchema, nil, cache)
	if err != nil {
		t.Fatal(err)
	}
	assertBatchesEqual(t, uncached, b1)
	st := cache.Stats()
	if st.Admitted != 0 || st.GhostRegistered != int64(len(decodedCacheFixtureSchema)) {
		t.Fatalf("after 1st read: admitted=%d ghosts=%d, want 0/%d",
			st.Admitted, st.GhostRegistered, len(decodedCacheFixtureSchema))
	}
	if st.SizeBytes != 0 {
		t.Fatalf("after 1st read: size=%d, want 0", st.SizeBytes)
	}

	// Second read: ghosts promote, clones stored. Still a decode (miss).
	b2, err := ReadRowGroupNativeCached(fr, 0, decodedCacheFixtureSchema, nil, cache)
	if err != nil {
		t.Fatal(err)
	}
	assertBatchesEqual(t, uncached, b2)
	st = cache.Stats()
	if st.Admitted != int64(len(decodedCacheFixtureSchema)) {
		t.Fatalf("after 2nd read: admitted=%d, want %d", st.Admitted, len(decodedCacheFixtureSchema))
	}
	if st.SizeBytes <= 0 || st.Hits != 0 {
		t.Fatalf("after 2nd read: size=%d hits=%d, want >0 / 0", st.SizeBytes, st.Hits)
	}

	// Third read: pure hits, value-identical batch.
	b3, err := ReadRowGroupNativeCached(fr, 0, decodedCacheFixtureSchema, nil, cache)
	if err != nil {
		t.Fatal(err)
	}
	assertBatchesEqual(t, uncached, b3)
	st = cache.Stats()
	if st.Hits != int64(len(decodedCacheFixtureSchema)) {
		t.Fatalf("after 3rd read: hits=%d, want %d", st.Hits, len(decodedCacheFixtureSchema))
	}

	// A different row group is a fresh key set — no false hits.
	bg2, err := ReadRowGroupNativeCached(fr, 1, decodedCacheFixtureSchema, nil, cache)
	if err != nil {
		t.Fatal(err)
	}
	if bg2.Columns[0].Int64Data[0] != 100000 {
		t.Fatalf("row group 1 first id = %d, want 100000", bg2.Columns[0].Int64Data[0])
	}
}

// TestDecodedChunkCache_HitIsolation verifies the copy discipline: mutating
// a batch served from cache must not corrupt later reads (the cache owns
// private clones; consumers own private copies).
func TestDecodedChunkCache_HitIsolation(t *testing.T) {
	data := decodedCacheFixtureFile(t, 1, 100)
	fr := openFixtureReader(t, data, "bucket/tables/t/b.parquet#1")
	cache := NewDecodedChunkCache(64 << 20)

	uncached, err := ReadRowGroupNative(fr, 0, decodedCacheFixtureSchema, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ { // ghost, then admit
		if _, err := ReadRowGroupNativeCached(fr, 0, decodedCacheFixtureSchema, nil, cache); err != nil {
			t.Fatal(err)
		}
	}

	hit, err := ReadRowGroupNativeCached(fr, 0, decodedCacheFixtureSchema, nil, cache)
	if err != nil {
		t.Fatal(err)
	}
	if cache.Stats().Hits == 0 {
		t.Fatal("expected a cache hit before mutating")
	}
	// Trash the served batch in place, the way downstream operators may.
	hit.Columns[0].Int64Data[0] = -999
	hit.Columns[1].BytesData.Data = append(hit.Columns[1].BytesData.Data, "garbage"...)
	hit.Columns[2].Float64Data[0] = -1
	hit.Sel = []uint32{0}

	again, err := ReadRowGroupNativeCached(fr, 0, decodedCacheFixtureSchema, nil, cache)
	if err != nil {
		t.Fatal(err)
	}
	assertBatchesEqual(t, uncached, again)
}

// TestDecodedChunkCache_NoIdentityNoEngagement: a reader without a
// CacheIdentity must leave the cache untouched (and produce identical
// results).
func TestDecodedChunkCache_NoIdentityNoEngagement(t *testing.T) {
	data := decodedCacheFixtureFile(t, 1, 50)
	fr := openFixtureReader(t, data, "")
	cache := NewDecodedChunkCache(64 << 20)

	uncached, err := ReadRowGroupNative(fr, 0, decodedCacheFixtureSchema, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		b, err := ReadRowGroupNativeCached(fr, 0, decodedCacheFixtureSchema, nil, cache)
		if err != nil {
			t.Fatal(err)
		}
		assertBatchesEqual(t, uncached, b)
	}
	st := cache.Stats()
	if st.Hits != 0 || st.Misses != 0 || st.GhostRegistered != 0 || st.SizeBytes != 0 {
		t.Fatalf("cache engaged without identity: %+v", st)
	}
}

// TestDecodedChunkCache_EvictionCap: admissions beyond the byte budget
// evict coldest-first and the size invariant holds.
func TestDecodedChunkCache_EvictionCap(t *testing.T) {
	src := batch.NewVector(batch.TypeInt64, 1024)
	for i := range src.Int64Data {
		src.Int64Data[i] = int64(i)
	}
	entryBytes := src.MemBytes()
	capBytes := entryBytes * 16 // maxEntry = 2×entry, room for ~16 entries
	cache := NewDecodedChunkCache(capBytes)

	admit := func(i int) {
		key := decodedChunkKey{identity: "x#1", rgIdx: i, colIdx: 0, catalogType: pqt.TypeInt64}
		cache.Offer(key, src, src.Len) // ghost
		cache.Offer(key, src, src.Len) // admit
	}
	for i := 0; i < 40; i++ {
		admit(i)
	}
	st := cache.Stats()
	if st.SizeBytes > capBytes {
		t.Fatalf("size %d exceeds cap %d", st.SizeBytes, capBytes)
	}
	if st.Evictions == 0 {
		t.Fatal("expected evictions past cap")
	}
	if st.Admitted != 40 {
		t.Fatalf("admitted=%d, want 40", st.Admitted)
	}
}

// TestDecodedChunkCache_ReliefSpillSome: the AccountedOperator surface
// frees at least target bytes by evicting and reports a coherent footprint.
func TestDecodedChunkCache_ReliefSpillSome(t *testing.T) {
	src := batch.NewVector(batch.TypeInt64, 4096)
	cache := NewDecodedChunkCache(64 << 20)
	for i := 0; i < 6; i++ {
		key := decodedChunkKey{identity: "x#1", rgIdx: i, colIdx: 0, catalogType: pqt.TypeInt64}
		cache.Offer(key, src, src.Len)
		cache.Offer(key, src, src.Len)
	}
	before := cache.Size()
	if before == 0 {
		t.Fatal("nothing admitted")
	}
	fp := cache.Inspect()
	if fp.SpillableBytes != before || fp.OwnedBytes != before {
		t.Fatalf("footprint %+v, want owned=spillable=%d", fp, before)
	}
	if got := cache.EstimateRelief(before / 2); got != before/2 {
		t.Fatalf("EstimateRelief=%d, want %d", got, before/2)
	}
	freed, err := cache.SpillSome(before / 2)
	if err != nil {
		t.Fatal(err)
	}
	if freed < before/2 {
		t.Fatalf("freed %d < target %d", freed, before/2)
	}
	if cache.Size() != before-freed {
		t.Fatalf("size %d, want %d", cache.Size(), before-freed)
	}
	// Draining everything leaves a zero footprint.
	if _, err := cache.SpillSome(1 << 62); err != nil {
		t.Fatal(err)
	}
	if cache.Size() != 0 || cache.Inspect().State != memory.OpRegistered {
		t.Fatalf("drained cache: size=%d state=%v", cache.Size(), cache.Inspect().State)
	}
}

// TestDecodedChunkCache_TooLargeRejected: entries above cap/8 never admit.
func TestDecodedChunkCache_TooLargeRejected(t *testing.T) {
	src := batch.NewVector(batch.TypeInt64, 4096) // 32 KiB data
	cache := NewDecodedChunkCache(64 << 10)       // maxEntry = 8 KiB
	key := decodedChunkKey{identity: "x#1", rgIdx: 0, colIdx: 0, catalogType: pqt.TypeInt64}
	cache.Offer(key, src, src.Len)
	cache.Offer(key, src, src.Len)
	st := cache.Stats()
	if st.Admitted != 0 || st.RejectedTooLarge == 0 || st.SizeBytes != 0 {
		t.Fatalf("oversize entry admitted: %+v", st)
	}
}

// TestDecodedChunkCache_ConcurrentReaders hammers hit/miss/admit paths from
// concurrent goroutines (run under -race).
func TestDecodedChunkCache_ConcurrentReaders(t *testing.T) {
	data := decodedCacheFixtureFile(t, 4, 100)
	fr := openFixtureReader(t, data, "bucket/tables/t/c.parquet#1")
	cache := NewDecodedChunkCache(64 << 20)

	want := make([]*batch.RecordBatch, 4)
	for g := 0; g < 4; g++ {
		b, err := ReadRowGroupNative(fr, g, decodedCacheFixtureSchema, nil)
		if err != nil {
			t.Fatal(err)
		}
		want[g] = b
	}

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iter := 0; iter < 8; iter++ {
				for g := 0; g < 4; g++ {
					b, err := ReadRowGroupNativeCached(fr, g, decodedCacheFixtureSchema, nil, cache)
					if err != nil {
						errs <- err
						return
					}
					if b.Len != want[g].Len || b.Columns[0].Int64Data[0] != want[g].Columns[0].Int64Data[0] {
						errs <- fmt.Errorf("group %d: wrong batch (len=%d first=%d)", g, b.Len, b.Columns[0].Int64Data[0])
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// Final full-value verification against uncached decodes.
	for g := 0; g < 4; g++ {
		b, err := ReadRowGroupNativeCached(fr, g, decodedCacheFixtureSchema, nil, cache)
		if err != nil {
			t.Fatal(err)
		}
		assertBatchesEqual(t, want[g], b)
	}
}

// BenchmarkDecodedChunkRead compares a zstd decompress + decode of one row
// group (the SF100 data class) against a cache hit serving the same batch —
// the per-read cost the cache removes vs the copy cost it adds.
func BenchmarkDecodedChunkRead(b *testing.B) {
	cfg := pqt.DefaultWriterConfig()
	cfg.RowGroupSize = 64 * 1024
	cfg.Compression = pqt.CompressionZstd
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, pqt.Schema{Columns: decodedCacheFixtureSchema}, cfg)
	if err != nil {
		b.Fatal(err)
	}
	rows := make([]map[string]any, 64*1024)
	for i := range rows {
		rows[i] = map[string]any{
			"id": int64(i), "val": float64(i) * 1.5, "flag": i%2 == 0,
		}
		if i%7 != 0 {
			rows[i]["name"] = fmt.Sprintf("row-with-a-medium-width-value-%d", i)
		}
	}
	if err := w.WriteRows(rows); err != nil {
		b.Fatal(err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	data := buf.Bytes()
	reader, err := pqt.NewReaderFromBytes(data)
	if err != nil {
		b.Fatal(err)
	}
	fr := reader.FileReader()
	fr.SetCacheIdentity("bench/x.parquet#1")

	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := ReadRowGroupNative(fr, 0, decodedCacheFixtureSchema, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("cache_hit", func(b *testing.B) {
		cache := NewDecodedChunkCache(256 << 20)
		for i := 0; i < 2; i++ { // ghost, then admit
			if _, err := ReadRowGroupNativeCached(fr, 0, decodedCacheFixtureSchema, nil, cache); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := ReadRowGroupNativeCached(fr, 0, decodedCacheFixtureSchema, nil, cache); err != nil {
				b.Fatal(err)
			}
		}
		if cache.Stats().Hits == 0 {
			b.Fatal("bench loop never hit")
		}
	})
}

// TestCopyChunkVector_DecimalAndNulls covers the hand-built arms the parquet
// fixture writer can't produce: DECIMAL round-trip and null preservation.
func TestCopyChunkVector_DecimalAndNulls(t *testing.T) {
	src := batch.NewVectorWithScale(batch.TypeDecimal, 8, 2)
	for i := range src.DecimalData.Data {
		src.DecimalData.Data[i] = batch.Int128From(int64(i * 125))
	}
	src.Nulls.SetNull(3)

	clone := cloneChunkVector(src, 8, 2)
	if clone == nil {
		t.Fatal("decimal clone failed")
	}
	dst := batch.NewVectorWithScale(batch.TypeDecimal, 8, 2)
	if !copyChunkVector(dst, clone, 8) {
		t.Fatal("decimal copy failed")
	}
	for i := 0; i < 8; i++ {
		if dst.Nulls.IsNull(i) != src.Nulls.IsNull(i) {
			t.Fatalf("row %d null mismatch", i)
		}
		if dst.DecimalData.Data[i] != src.DecimalData.Data[i] {
			t.Fatalf("row %d decimal mismatch", i)
		}
	}

	// View vectors are refused (clone would alias the base).
	view := &batch.Vector{Type: batch.TypeInt64, Len: 8, Base: src, Indices: make([]uint32, 8)}
	if cloneChunkVector(view, 8, 0) != nil {
		t.Fatal("view vector must not clone")
	}
}
