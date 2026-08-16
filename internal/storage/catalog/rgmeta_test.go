package catalog

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestRGMetaRoundTrip verifies the WRGM wire format preserves every value
// type exactly — the whole point of the binary encoding is that pruning
// stats never suffer JSON's int64→float64 / non-UTF-8 degradation.
func TestRGMetaRoundTrip(t *testing.T) {
	files := []FileRGMeta{
		{
			Path: "tables/t/chunk_0001.parquet",
			Groups: []parquet.RowGroupStats{
				{
					NumRows: 128 * 1024,
					Columns: map[string]parquet.ColumnStats{
						"l_orderkey": {HasStats: true, MinValue: int64(-42), MaxValue: int64(1<<60 + 7), NullCount: 3},
						"l_price":    {HasStats: true, MinValue: float64(-0.5), MaxValue: float64(1e300), NullCount: 0},
						"l_comment":  {HasStats: true, MinValue: "", MaxValue: string([]byte{0xff, 0xfe, 'x'}), NullCount: 9},
						"l_flag":     {HasStats: true, MinValue: false, MaxValue: true, NullCount: 0},
						"l_nostats":  {HasStats: false},
						"l_nullmin":  {HasStats: true, MinValue: nil, MaxValue: int64(10), NullCount: 1},
					},
				},
				{NumRows: 7, Columns: map[string]parquet.ColumnStats{}},
			},
		},
		{Path: "tables/t/empty.parquet", Groups: nil},
		{
			Path: "tables/t/chunk_0002.parquet",
			Groups: []parquet.RowGroupStats{
				{NumRows: 1, Columns: map[string]parquet.ColumnStats{
					"id": {HasStats: true, MinValue: int64(5), MaxValue: int64(5), NullCount: 0},
				}},
			},
		},
	}

	data := EncodeTableRGMeta(files)
	if len(data) == 0 {
		t.Fatal("encode returned empty blob")
	}
	got, err := DecodeTableRGMeta(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != len(files) {
		t.Fatalf("decoded %d files, want %d", len(got), len(files))
	}
	for _, f := range files {
		gg, ok := got[f.Path]
		if !ok {
			t.Fatalf("missing path %s", f.Path)
		}
		if len(gg) != len(f.Groups) {
			t.Fatalf("%s: %d groups, want %d", f.Path, len(gg), len(f.Groups))
		}
		for i, want := range f.Groups {
			if gg[i].NumRows != want.NumRows {
				t.Errorf("%s rg %d: NumRows %d, want %d", f.Path, i, gg[i].NumRows, want.NumRows)
			}
			if len(gg[i].Columns) != len(want.Columns) {
				t.Fatalf("%s rg %d: %d cols, want %d", f.Path, i, len(gg[i].Columns), len(want.Columns))
			}
			for name, wcs := range want.Columns {
				gcs, ok := gg[i].Columns[name]
				if !ok {
					t.Fatalf("%s rg %d: missing col %s", f.Path, i, name)
				}
				if !reflect.DeepEqual(gcs, wcs) {
					t.Errorf("%s rg %d col %s: got %#v, want %#v", f.Path, i, name, gcs, wcs)
				}
			}
		}
	}

	// Empty input → no blob.
	if b := EncodeTableRGMeta(nil); b != nil {
		t.Errorf("EncodeTableRGMeta(nil) = %d bytes, want nil", len(b))
	}
}

// TestRGMetaEncodeUnsupportedType verifies values outside statsToNative's
// closed type set are dropped (nil) rather than approximated — the pruner
// must never act on a lossy stand-in.
func TestRGMetaEncodeUnsupportedType(t *testing.T) {
	files := []FileRGMeta{{
		Path: "f.parquet",
		Groups: []parquet.RowGroupStats{{
			NumRows: 10,
			Columns: map[string]parquet.ColumnStats{
				"weird": {HasStats: true, MinValue: []byte{1, 2}, MaxValue: int32(7), NullCount: 0},
			},
		}},
	}}
	got, err := DecodeTableRGMeta(bytes.NewReader(EncodeTableRGMeta(files)))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	cs := got["f.parquet"][0].Columns["weird"]
	if cs.MinValue != nil || cs.MaxValue != nil {
		t.Errorf("unsupported types must encode as nil, got min=%#v max=%#v", cs.MinValue, cs.MaxValue)
	}
}

// TestRGMetaDecodeErrors verifies corrupt blobs error out instead of
// producing garbage stats (callers treat errors as "no blob" and fall
// back to footer reads).
func TestRGMetaDecodeErrors(t *testing.T) {
	valid := EncodeTableRGMeta([]FileRGMeta{{
		Path: "f.parquet",
		Groups: []parquet.RowGroupStats{{
			NumRows: 10,
			Columns: map[string]parquet.ColumnStats{"c": {HasStats: true, MinValue: int64(1), MaxValue: int64(2)}},
		}},
	}})

	cases := map[string][]byte{
		"empty":     {},
		"bad magic": append([]byte("XXXX"), valid[4:]...),
		"bad version": func() []byte {
			b := append([]byte(nil), valid...)
			b[4] = 99
			return b
		}(),
		"truncated": valid[:len(valid)-3],
		"bad tag": func() []byte {
			b := append([]byte(nil), valid...)
			b[len(b)-9] = 200 // the max value's tag byte (1 tag + 8 int64 payload)
			return b
		}(),
	}
	for name, data := range cases {
		if _, err := DecodeTableRGMeta(bytes.NewReader(data)); err == nil {
			t.Errorf("%s: expected decode error", name)
		}
	}
}

// TestTableRGMetaLifecycle exercises the catalog surface: no blob before
// ANALYZE, full coverage matching real footer stats after, memoization
// keyed by manifest version, and fallback-to-nil on a missing blob after
// invalidation.
func TestTableRGMetaLifecycle(t *testing.T) {
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
		{Name: "name", Type: parquet.TypeString},
	}}
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	addFile := func(fi, base int) []byte {
		t.Helper()
		rows := make([]map[string]any, 100)
		for i := range rows {
			rows[i] = map[string]any{"id": int64(base + i), "name": fmt.Sprintf("n%d", i)}
		}
		var buf bytes.Buffer
		cfg := parquet.DefaultWriterConfig()
		cfg.RowGroupSize = 40 // 3 row groups per file (40+40+20)
		pw, err := parquet.NewWriter(&buf, schema, cfg)
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
		entry := FileEntry{Path: key, SizeBytes: int64(len(data)), NumRows: 100, CreatedAt: time.Now()}
		if err := cat.AddFiles(ctx, "events", nil, "tables/events/", []FileEntry{entry}); err != nil {
			t.Fatal(err)
		}
		return data
	}

	fileData := addFile(0, 0)
	addFile(1, 100)

	// Before ANALYZE: no blob, nil map, no error.
	if m, err := cat.TableRGMeta(ctx, "events"); err != nil || m != nil {
		t.Fatalf("pre-ANALYZE TableRGMeta = %v, %v; want nil, nil", m, err)
	}

	if _, err := cat.AnalyzeTable(ctx, "events"); err != nil {
		t.Fatalf("AnalyzeTable: %v", err)
	}

	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil || manifest == nil || manifest.RGMetaKey == "" {
		t.Fatalf("post-ANALYZE manifest RGMetaKey = %q (err %v), want set", manifestKey(manifest), err)
	}

	m, err := cat.TableRGMeta(ctx, "events")
	if err != nil || len(m) != 2 {
		t.Fatalf("post-ANALYZE TableRGMeta: %d files (err %v), want 2", len(m), err)
	}

	// Blob stats must match the file's real footer, bit-for-bit types.
	fr, err := parquet.NewReaderFromBytes(fileData)
	if err != nil {
		t.Fatal(err)
	}
	native := fr.FileReader()
	groups := m["tables/events/chunk_0000.parquet"]
	if len(groups) != native.NumRowGroups() {
		t.Fatalf("blob has %d groups, footer has %d", len(groups), native.NumRowGroups())
	}
	for rg := range groups {
		want := native.RowGroupStats(rg)
		if groups[rg].NumRows != want.NumRows {
			t.Errorf("rg %d NumRows = %d, want %d", rg, groups[rg].NumRows, want.NumRows)
		}
		for col, wcs := range want.Columns {
			if !reflect.DeepEqual(groups[rg].Columns[col], wcs) {
				t.Errorf("rg %d col %s: blob %#v != footer %#v", rg, col, groups[rg].Columns[col], wcs)
			}
		}
	}

	// Memoization: delete the blob object; a second call must still
	// serve the cached decode (same manifest version → no store fetch).
	if err := store.Delete(ctx, "test", manifest.RGMetaKey); err != nil {
		t.Fatal(err)
	}
	if m2, err := cat.TableRGMeta(ctx, "events"); err != nil || len(m2) != 2 {
		t.Fatalf("cached TableRGMeta after blob delete: %d files (err %v), want 2 from cache", len(m2), err)
	}

	// Invalidation: adding a file bumps the manifest version; the cache
	// entry no longer matches, the re-fetch finds no blob, and the call
	// degrades to nil (scan falls back to footer reads).
	addFile(2, 200)
	if m3, err := cat.TableRGMeta(ctx, "events"); err != nil || m3 != nil {
		t.Fatalf("TableRGMeta after invalidation with deleted blob = %v, %v; want nil, nil", m3, err)
	}
}

func manifestKey(m *PartitionManifest) string {
	if m == nil {
		return "<nil manifest>"
	}
	return m.RGMetaKey
}
