package physical

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// footerScanTable builds a MemStore-backed table of nFiles parquet parts,
// rowsPerFile rows each, with "val" carrying the originating file index so a
// wrong footer served from cache would show up as wrong DATA, not just a
// wrong count.
func footerScanTable(t testing.TB, nFiles, rowsPerFile int) (*catalog.Catalog, *objstore.MemStore, parquet.Schema) {
	t.Helper()
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}}
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateTable(ctx, "parts", schema, nil); err != nil {
		t.Fatal(err)
	}
	for f := 0; f < nFiles; f++ {
		addFooterScanFile(t, cat, store, schema, f, rowsPerFile)
	}
	return cat, store, schema
}

// addFooterScanFile writes one more part into the table's manifest.
func addFooterScanFile(t testing.TB, cat *catalog.Catalog, store *objstore.MemStore,
	schema parquet.Schema, fileIdx, rows int) {
	t.Helper()
	ctx := context.Background()
	rowMaps := make([]map[string]any, rows)
	for r := range rowMaps {
		rowMaps[r] = map[string]any{
			"id":  int64(fileIdx*1000 + r),
			"val": fmt.Sprintf("f%d", fileIdx),
		}
	}
	data := writeFooterScanParquet(t, schema, rowMaps)
	path := fmt.Sprintf("tables/parts/chunk_%04d.parquet", fileIdx)
	if _, err := store.Put(ctx, cat.Bucket(), path, bytes.NewReader(data),
		int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	entry := catalog.FileEntry{
		Path:      path,
		SizeBytes: int64(len(data)),
		NumRows:   int64(rows),
		CreatedAt: time.Now(),
	}
	if err := cat.AddFiles(ctx, "parts", map[string]string{}, "tables/parts/",
		[]catalog.FileEntry{entry}); err != nil {
		t.Fatal(err)
	}
}

// writeFooterScanParquet is writeTestParquetMultiRG for a testing.TB (this
// file's fixtures are shared with a benchmark).
func writeFooterScanParquet(t testing.TB, schema parquet.Schema, rows []map[string]any) []byte {
	t.Helper()
	cfg := parquet.DefaultWriterConfig()
	cfg.Compression = parquet.CompressionNone
	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// scanFooterTable runs a full scan and returns "<id>:<val>" for every row.
func scanFooterTable(t *testing.T, cat *catalog.Catalog) []string {
	t.Helper()
	src := &catalogScanSource{
		catalog:      cat,
		tableName:    "parts",
		requiredCols: []string{"id", "val"},
	}
	var out []string
	for _, b := range drainSource(t, src) {
		idCol, valCol := b.Columns[0], b.Columns[1]
		for i := 0; i < b.Len; i++ {
			row := i
			if b.Sel != nil {
				if i >= len(b.Sel) {
					break
				}
				row = int(b.Sel[i])
			}
			out = append(out, fmt.Sprintf("%d:%s", idCol.Int64Data[row], valCol.BytesData.StringValue(row)))
		}
	}
	sort.Strings(out)
	return out
}

// TestFooterCacheScanResultsIdentical is the correctness gate: the same scan
// must return exactly the same rows with the footer cache enabled and
// disabled. A wrong footer would misreport row groups, row counts, column
// chunk offsets, or the schema — all of which show up here as differing rows.
func TestFooterCacheScanResultsIdentical(t *testing.T) {
	cat, _, _ := footerScanTable(t, 6, 40)

	prev := parquet.SetFooterCacheEnabled(false)
	parquet.ResetFooterCache()
	t.Cleanup(func() {
		parquet.SetFooterCacheEnabled(prev)
		parquet.ResetFooterCache()
	})

	off := scanFooterTable(t, cat)
	if len(off) != 6*40 {
		t.Fatalf("cache off: got %d rows, want 240", len(off))
	}

	parquet.SetFooterCacheEnabled(true)
	parquet.ResetFooterCache()
	onCold := scanFooterTable(t, cat) // populates the cache
	onWarm := scanFooterTable(t, cat) // served from it

	for _, arm := range []struct {
		name string
		got  []string
	}{{"cache-on-cold", onCold}, {"cache-on-warm", onWarm}} {
		if len(arm.got) != len(off) {
			t.Fatalf("%s: got %d rows, want %d", arm.name, len(arm.got), len(off))
		}
		for i := range off {
			if arm.got[i] != off[i] {
				t.Fatalf("%s: row %d = %q, want %q", arm.name, i, arm.got[i], off[i])
			}
		}
	}
}

// TestFooterCacheSecondScanDecodesNothing proves the redundant decodes are
// actually gone: a warm scan over the same files must not miss the cache
// once — not in buildRGUnits, not in fileSlot.ensureLoaded.
func TestFooterCacheSecondScanDecodesNothing(t *testing.T) {
	const files = 5
	cat, _, _ := footerScanTable(t, files, 20)

	prev := parquet.SetFooterCacheEnabled(true)
	parquet.ResetFooterCache()
	t.Cleanup(func() {
		parquet.SetFooterCacheEnabled(prev)
		parquet.ResetFooterCache()
	})

	scanFooterTable(t, cat)
	cold := parquet.FooterCacheStatsSnapshot()
	if cold.Inserts != files {
		t.Fatalf("cold scan inserted %d footers, want %d", cold.Inserts, files)
	}
	// Cold scan: buildRGUnits probes (miss) then decodes; ensureLoaded hits.
	if cold.Hits != files {
		t.Fatalf("cold scan hits = %d, want %d (ensureLoaded must reuse buildRGUnits' decode)",
			cold.Hits, files)
	}

	scanFooterTable(t, cat)
	warm := parquet.FooterCacheStatsSnapshot()
	if warm.Misses != cold.Misses {
		t.Fatalf("warm scan took %d cache misses, want 0", warm.Misses-cold.Misses)
	}
	if warm.Inserts != cold.Inserts {
		t.Fatalf("warm scan decoded %d footers, want 0", warm.Inserts-cold.Inserts)
	}
	if got := warm.Hits - cold.Hits; got != 2*files {
		t.Fatalf("warm scan hits = %d, want %d (one probe + one load per file)", got, 2*files)
	}
}

// TestFooterCacheSeesNewFiles guards the failure mode the cache must NOT
// have: it caches per-file footers, never the table's file LIST. A table
// whose file set grew after an earlier scan must scan the new files too.
func TestFooterCacheSeesNewFiles(t *testing.T) {
	cat, store, schema := footerScanTable(t, 2, 25)

	prev := parquet.SetFooterCacheEnabled(true)
	parquet.ResetFooterCache()
	t.Cleanup(func() {
		parquet.SetFooterCacheEnabled(prev)
		parquet.ResetFooterCache()
	})

	if got := scanFooterTable(t, cat); len(got) != 50 {
		t.Fatalf("initial scan: got %d rows, want 50", len(got))
	}

	// Two more parts land (fresh paths, as ingest and compaction produce).
	addFooterScanFile(t, cat, store, schema, 2, 25)
	addFooterScanFile(t, cat, store, schema, 3, 25)

	got := scanFooterTable(t, cat)
	if len(got) != 100 {
		t.Fatalf("after adding 2 files: got %d rows, want 100", len(got))
	}
	seen := map[string]int{}
	for _, row := range got {
		seen[row[len(row)-2:]]++
	}
	for f := 0; f < 4; f++ {
		if n := seen[fmt.Sprintf("f%d", f)]; n != 25 {
			t.Fatalf("file %d contributed %d rows, want 25", f, n)
		}
	}
}

// TestFooterCacheIdentityEligibility pins the identity builder's fail-closed
// gates — the places a bad key would be minted.
func TestFooterCacheIdentityEligibility(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	created := time.Unix(0, 12345)
	base := catalog.FileEntry{Path: "tables/t/chunk_0001.parquet", CreatedAt: created}

	id := footerCacheIdentity(cat, base, 4096)
	if id == "" {
		t.Fatal("eligible file produced an empty identity")
	}
	wantSuffix := "/test/tables/t/chunk_0001.parquet#4096@12345"
	if len(id) <= len(wantSuffix) || id[len(id)-len(wantSuffix):] != wantSuffix {
		t.Fatalf("identity %q does not end in %q", id, wantSuffix)
	}

	// Each of these must fail closed.
	for name, got := range map[string]string{
		"nil catalog":  footerCacheIdentity(nil, base, 4096),
		"zero size":    footerCacheIdentity(cat, base, 0),
		"negative":     footerCacheIdentity(cat, base, -1),
		"non-parquet":  footerCacheIdentity(cat, catalog.FileEntry{Path: "tables/t/part.wshf", CreatedAt: created}, 4096),
		"query scrtch": footerCacheIdentity(cat, catalog.FileEntry{Path: "queries/q1/s0/p.parquet", CreatedAt: created}, 4096),
	} {
		if got != "" {
			t.Fatalf("%s: expected empty identity, got %q", name, got)
		}
	}

	// Distinct MemStore instances must never share a namespace even with
	// identical bucket, key, size and timestamp — the in-process hazard.
	other := catalog.NewWithStore(objstore.NewMemStore(), "test")
	if err := other.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if otherID := footerCacheIdentity(other, base, 4096); otherID == id {
		t.Fatalf("two MemStores produced the same identity %q", id)
	}
}

// BenchmarkScanFooterDecode measures a full multi-file scan with the footer
// cache disabled versus enabled. The delta is the redundant decode: two
// footer decodes per file per query (buildRGUnits + ensureLoaded) become
// zero on a warm cache.
func BenchmarkScanFooterDecode(b *testing.B) {
	const files = 20
	cat, _, _ := footerScanTable(b, files, 200)
	ctx := context.Background()

	run := func(b *testing.B) {
		src := &catalogScanSource{
			catalog:      cat,
			tableName:    "parts",
			requiredCols: []string{"id", "val"},
		}
		if err := src.Init(ctx); err != nil {
			b.Fatal(err)
		}
		for {
			batch, err := src.Next(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if batch == nil {
				break
			}
		}
		if err := src.Close(); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("cache=off", func(b *testing.B) {
		prev := parquet.SetFooterCacheEnabled(false)
		parquet.ResetFooterCache()
		defer func() {
			parquet.SetFooterCacheEnabled(prev)
			parquet.ResetFooterCache()
		}()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			run(b)
		}
	})

	b.Run("cache=on", func(b *testing.B) {
		prev := parquet.SetFooterCacheEnabled(true)
		parquet.ResetFooterCache()
		defer func() {
			parquet.SetFooterCacheEnabled(prev)
			parquet.ResetFooterCache()
		}()
		run(b) // warm
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			run(b)
		}
	})
}
