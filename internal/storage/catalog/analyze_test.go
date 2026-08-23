package catalog

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestAnalyzeTableEndToEnd verifies the full flow: write parquet files
// via direct parquet.Writer (simulating S3-staged data with no ingest-
// path HLL), call AnalyzeTable, then read AggregateColumnStats and
// confirm the NDV estimate matches the actual distinct-value count
// within HLL's standard error.
//
// Hard requirement: ANALYZE produces HLLs byte-compatible with the
// ingest path so future re-ingestion can merge with existing sketches.
func TestAnalyzeTableEndToEnd(t *testing.T) {
	ctx := context.Background()

	// In-memory store + NATS-less catalog (KV is a noop fake).
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	cat := New(NewMemKV(), store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "cat", Type: parquet.TypeString, Nullable: false},
	}}
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Write 3 parquet files. Each has 10K rows. The id column is a
	// dense int with the file's offset, so across all files there are
	// 30K distinct ids. cat is one of 5 values, so 5 distinct values
	// total.
	const rowsPerFile = 10_000
	const numFiles = 3
	for fi := 0; fi < numFiles; fi++ {
		rows := make([]map[string]any, rowsPerFile)
		for i := 0; i < rowsPerFile; i++ {
			rows[i] = map[string]any{
				"id":  int64(fi*rowsPerFile + i),
				"cat": fmt.Sprintf("c%d", i%5),
			}
		}
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := pw.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := pw.Close(); err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("tables/events/chunk_%04d.parquet", fi)
		data := buf.Bytes()
		if _, err := store.Put(ctx, "test", key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		entry := FileEntry{Path: key, SizeBytes: int64(len(data)), NumRows: int64(rowsPerFile), CreatedAt: time.Now()}
		if err := cat.AddFiles(ctx, "events", nil, "tables/events/", []FileEntry{entry}); err != nil {
			t.Fatal(err)
		}
	}

	// Pre-ANALYZE: NDV should be 0 (no HLL collected).
	stats, err := cat.AggregateColumnStats(ctx, "events")
	if err != nil {
		t.Fatalf("AggregateColumnStats: %v", err)
	}
	if cs, ok := stats["id"]; ok && cs.NDV != 0 {
		t.Errorf("pre-ANALYZE: id NDV = %d, want 0", cs.NDV)
	}

	// Run ANALYZE.
	count, err := cat.AnalyzeTable(ctx, "events")
	if err != nil {
		t.Fatalf("AnalyzeTable: %v", err)
	}
	if count != numFiles {
		t.Errorf("AnalyzeTable analyzed %d files, want %d", count, numFiles)
	}

	// Post-ANALYZE: NDV should be close to the truth.
	stats, err = cat.AggregateColumnStats(ctx, "events")
	if err != nil {
		t.Fatalf("AggregateColumnStats: %v", err)
	}
	idStats, ok := stats["id"]
	if !ok {
		t.Fatal("missing id stats")
	}
	if idStats.NDV == 0 {
		t.Fatal("post-ANALYZE: id NDV = 0, want ~30K")
	}
	const truthID = 30_000
	errPctID := abs(idStats.NDV-truthID) * 100 / truthID
	if errPctID > 3 {
		t.Errorf("id NDV err %d%% exceeds 3%% tolerance (NDV=%d, truth=%d)", errPctID, idStats.NDV, truthID)
	}
	t.Logf("id NDV: %d (truth %d, err %d%%)", idStats.NDV, truthID, errPctID)

	catStats, ok := stats["cat"]
	if !ok {
		t.Fatal("missing cat stats")
	}
	const truthCat = 5
	if catStats.NDV < 4 || catStats.NDV > 6 {
		t.Errorf("cat NDV = %d, want ~%d", catStats.NDV, truthCat)
	}
	t.Logf("cat NDV: %d (truth %d)", catStats.NDV, truthCat)
}

// TestAnalyzeTableIdempotent re-runs ANALYZE and verifies the NDV
// estimate is stable (same input → same HLL).
func TestAnalyzeTableIdempotent(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	store.MakeBucket(ctx, "test")
	cat := New(NewMemKV(), store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{Columns: []parquet.Column{{Name: "k", Type: parquet.TypeInt32}}}
	cat.CreateTable(ctx, "t", schema, nil)

	rows := make([]map[string]any, 5_000)
	for i := range rows {
		rows[i] = map[string]any{"k": int32(i)}
	}
	var buf bytes.Buffer
	pw, _ := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
	pw.WriteRows(rows)
	pw.Close()
	data := buf.Bytes()
	store.Put(ctx, "test", "tables/t/a.parquet", bytes.NewReader(data), int64(len(data)), "application/octet-stream")
	cat.AddFiles(ctx, "t", nil, "tables/t/", []FileEntry{{Path: "tables/t/a.parquet", SizeBytes: int64(len(data)), NumRows: int64(len(rows))}})

	if _, err := cat.AnalyzeTable(ctx, "t"); err != nil {
		t.Fatal(err)
	}
	s1, _ := cat.AggregateColumnStats(ctx, "t")
	if _, err := cat.AnalyzeTable(ctx, "t"); err != nil {
		t.Fatal(err)
	}
	s2, _ := cat.AggregateColumnStats(ctx, "t")
	if s1["k"].NDV != s2["k"].NDV {
		t.Errorf("non-idempotent: %d vs %d", s1["k"].NDV, s2["k"].NDV)
	}
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// TestAnalyzeTableHistogramEndToEnd verifies the full histogram path:
// ANALYZE collects reservoir samples per file, AggregateColumnStats
// merges them into a table-level histogram, and the histogram returns
// accurate selectivity for range queries.
func TestAnalyzeTableHistogramEndToEnd(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	cat := New(NewMemKV(), store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "n", Type: parquet.TypeInt64, Nullable: false},
	}}
	cat.CreateTable(ctx, "nums", schema, nil)

	// Write 3 files. n ranges from 0 to 30000 across all files.
	const rowsPerFile = 10_000
	for fi := 0; fi < 3; fi++ {
		rows := make([]map[string]any, rowsPerFile)
		for i := 0; i < rowsPerFile; i++ {
			rows[i] = map[string]any{"n": int64(fi*rowsPerFile + i)}
		}
		var buf bytes.Buffer
		pw, _ := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		pw.WriteRows(rows)
		pw.Close()
		key := fmt.Sprintf("tables/nums/c_%d.parquet", fi)
		data := buf.Bytes()
		store.Put(ctx, "test", key, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
		cat.AddFiles(ctx, "nums", nil, "tables/nums/", []FileEntry{{Path: key, SizeBytes: int64(len(data)), NumRows: rowsPerFile}})
	}

	if _, err := cat.AnalyzeTable(ctx, "nums"); err != nil {
		t.Fatalf("AnalyzeTable: %v", err)
	}

	stats, _ := cat.AggregateColumnStats(ctx, "nums")
	nStats, ok := stats["n"]
	if !ok {
		t.Fatal("missing n stats")
	}
	if nStats.Histogram == nil {
		t.Fatal("no histogram built")
	}

	// First quarter: [0, 7500] → ~25%
	q1 := nStats.Histogram.SelectivityRange(int64(0), int64(7500))
	t.Logf("[0, 7500] sel=%.4f (expect ~0.25)", q1)
	if q1 < 0.20 || q1 > 0.30 {
		t.Errorf("first quarter sel %.4f outside expected", q1)
	}
	// Second half: [15000, 30000] → ~50%
	h2 := nStats.Histogram.SelectivityRange(int64(15000), int64(30000))
	t.Logf("[15000, 30000] sel=%.4f (expect ~0.5)", h2)
	if h2 < 0.40 || h2 > 0.60 {
		t.Errorf("second half sel %.4f outside expected", h2)
	}
}

// TestAnalyzeTableWithZeroLengthUUIDValues: ANALYZE reads through
// parquet.Reader.ReadRowGroup — the ROW path — and that path refused a
// zero-length entry in a UUID column ("UUID is 16 bytes per value but row 2
// holds 10") on files wadjet had written itself, while the native columnar
// reader read them. ANALYZE is background work, so the divergence surfaced as
// a table that could be queried but never analyzed.
//
// A zero-length entry is an absence now, on both paths, and ANALYZE counts it
// as one rather than as a distinct value.
func TestAnalyzeTableWithZeroLengthUUIDValues(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	cat := New(NewMemKV(), store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "u", Type: parquet.TypeUUID, Nullable: true},
		{Name: "a", Type: parquet.TypeIPv6, Nullable: true},
	}}
	if err := cat.CreateTable(ctx, "sessions", schema, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	rows := make([]map[string]any, 0, 300)
	for i := 0; i < 100; i++ {
		rows = append(rows,
			map[string]any{"id": int64(i*3 + 0),
				"u": fmt.Sprintf("00000000-0000-4000-8000-%012x", i),
				"a": fmt.Sprintf("2001:db8::%x", i)},
			// The legacy shape: a zero-length BYTE_ARRAY entry, not NULL.
			map[string]any{"id": int64(i*3 + 1), "u": []byte{}, "a": []byte{}},
			map[string]any{"id": int64(i*3 + 2), "u": nil, "a": nil},
		)
	}
	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	const key = "tables/sessions/chunk_0000.parquet"
	if _, err := store.Put(ctx, "test", key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddFiles(ctx, "sessions", nil, "tables/sessions/", []FileEntry{
		{Path: key, SizeBytes: int64(len(data)), NumRows: int64(len(rows)), CreatedAt: time.Now()},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := cat.AnalyzeTable(ctx, "sessions"); err != nil {
		t.Fatalf("AnalyzeTable over a file with zero-length UUID values: %v", err)
	}

	stats, err := cat.AggregateColumnStats(ctx, "sessions")
	if err != nil {
		t.Fatalf("AggregateColumnStats: %v", err)
	}
	cs, ok := stats["u"]
	if !ok {
		t.Fatal("no stats for the UUID column")
	}
	// 100 real values; the 200 absences must not be counted as one more.
	if cs.NDV < 90 || cs.NDV > 110 {
		t.Errorf("UUID NDV = %d, want ~100", cs.NDV)
	}
}

// TestAnalyzeTableHistogramWideDecimal: a DECIMAL column whose precision
// exceeds 18 digits is boxed by the row reader as parquet.Decimal128 (#419),
// and the sampler's type switch had no arm for it — so ReservoirSampler.Add
// dropped every value, ANALYZE stored an empty sample, and the column got no
// histogram at all while a DECIMAL(18,2) beside it got one. Silent: the
// planner just fell back to its hardcoded fractions.
//
// wadjet's own writer stores DECIMAL as INT64, so the values here all FIT in
// 64 bits; what makes them Decimal128 is the declared precision, which is
// what the reader boxes by.
func TestAnalyzeTableHistogramWideDecimal(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	cat := New(NewMemKV(), store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	const scale = 10
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: scale, Nullable: false},
	}}
	if err := cat.CreateTable(ctx, "wide", schema, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	const rowsPerFile = 5_000
	const numFiles = 3
	for fi := 0; fi < numFiles; fi++ {
		rows := make([]map[string]any, rowsPerFile)
		for i := 0; i < rowsPerFile; i++ {
			rows[i] = map[string]any{"d": int64(fi*rowsPerFile + i)}
		}
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := pw.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := pw.Close(); err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("tables/wide/c_%d.parquet", fi)
		data := buf.Bytes()
		if _, err := store.Put(ctx, "test", key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		if err := cat.AddFiles(ctx, "wide", nil, "tables/wide/", []FileEntry{
			{Path: key, SizeBytes: int64(len(data)), NumRows: rowsPerFile},
		}); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := cat.AnalyzeTable(ctx, "wide"); err != nil {
		t.Fatalf("AnalyzeTable: %v", err)
	}
	stats, err := cat.AggregateColumnStats(ctx, "wide")
	if err != nil {
		t.Fatalf("AggregateColumnStats: %v", err)
	}
	dStats, ok := stats["d"]
	if !ok {
		t.Fatal("missing d stats")
	}
	if dStats.Histogram == nil {
		t.Fatal("no histogram built for DECIMAL(38,10)")
	}
	if dStats.Histogram.TotalValues != rowsPerFile*numFiles {
		t.Errorf("histogram TotalValues = %d, want %d",
			dStats.Histogram.TotalValues, rowsPerFile*numFiles)
	}

	// Boundaries are the column's UNSCALED values, which is the same unit
	// the reader hands back; the query side asks in that unit too, through
	// the same Decimal128 box.
	unscaled := func(n int64) parquet.Decimal128 {
		pow := int64(1)
		for i := 0; i < scale; i++ {
			pow *= 10
		}
		return parquet.Decimal128From(n * pow)
	}
	q1 := dStats.Histogram.SelectivityRange(unscaled(0), unscaled(3750))
	t.Logf("[0, 3750] sel=%.4f (expect ~0.25)", q1)
	if q1 < 0.20 || q1 > 0.30 {
		t.Errorf("first quarter sel %.4f outside expected", q1)
	}
	h2 := dStats.Histogram.SelectivityRange(unscaled(7500), unscaled(15000))
	t.Logf("[7500, 15000] sel=%.4f (expect ~0.5)", h2)
	if h2 < 0.40 || h2 > 0.60 {
		t.Errorf("second half sel %.4f outside expected", h2)
	}
}
