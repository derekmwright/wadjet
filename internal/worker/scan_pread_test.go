package worker

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestScanPread_ParityWithMmapPath scans the same parquet inputs through
// every local tier twice — pread mode (default) and the
// WADJET_SCAN_PREAD=0 mmap path — and requires identical results. Covers
// both the S3-streamed staging (cold pass) and the base-table cache hit
// (steady pass), and asserts the pread fd is released on Close.
func TestScanPread_ParityWithMmapPath(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
	}}
	inner := objstore.NewMemStore()
	ctx := context.Background()
	if err := inner.MakeBucket(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	var files []string
	const rowsPerFile = 500
	for f := 0; f < 3; f++ {
		var buf bytes.Buffer
		cfg := parquet.DefaultWriterConfig()
		cfg.RowGroupSize = 128 // several row groups per file
		w, err := parquet.NewWriter(&buf, schema, cfg)
		if err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, rowsPerFile)
		for i := range rows {
			rows[i] = map[string]any{"id": int64(f*rowsPerFile + i)}
			if i%3 != 0 {
				rows[i]["name"] = fmt.Sprintf("f%d-row-%04d", f, i)
			}
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

	// One scan pass; returns every (id, name) in delivery order.
	scanAll := func(ex *Executor) []string {
		t.Helper()
		src := newCachedFileStreamSource(ex, "", "b", files)
		if err := src.Init(ctx); err != nil {
			t.Fatal(err)
		}
		var out []string
		for {
			b, err := src.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			for i := 0; i < b.Len; i++ {
				name := ""
				if !b.Columns[1].Nulls.IsNull(i) {
					name = string(b.Columns[1].BytesData.Value(i))
				}
				out = append(out, fmt.Sprintf("%d|%s", b.Columns[0].Int64Data[i], name))
			}
		}
		if err := src.Close(); err != nil {
			t.Fatal(err)
		}
		if src.file != nil {
			t.Fatal("pread fd still held after Close")
		}
		if src.mmapData != nil || src.localPath != "" {
			t.Fatal("mmap/localPath still held after Close")
		}
		return out
	}

	// Cold (stream-to-spill) + steady (cache-hit) passes per mode, each
	// mode with its own cache dir so both populate from the same store.
	// Backing per tier is part of the contract: with the hot extension
	// (default) BOTH passes stage via pread; under WADJET_SCAN_PREAD_HOT=0
	// just-written stream temps decode via mmap (pread counters must not
	// move on the cold pass) while cache hits still pread; with
	// WADJET_SCAN_PREAD=0 nothing preads.
	runMode := func(enabled, hot bool) (cold, steady []string) {
		t.Helper()
		prevP, prevH := scanPreadEnabled, scanPreadHotEnabled
		scanPreadEnabled, scanPreadHotEnabled = enabled, hot
		defer func() { scanPreadEnabled, scanPreadHotEnabled = prevP, prevH }()
		btc, err := objstore.NewBaseTableCache(inner, t.TempDir(), 1<<30, nil)
		if err != nil {
			t.Fatal(err)
		}
		ex := &Executor{store: btc, spillDir: t.TempDir()}
		c0, _, _ := parquet.PreadStats()
		cold = scanAll(ex)
		if s := btc.Stats(); s.Entries != len(files) {
			t.Fatalf("cold pass cached %d entries, want %d", s.Entries, len(files))
		}
		c1, _, _ := parquet.PreadStats()
		if enabled && hot && c1 == c0 {
			t.Fatal("cold (just-written) pass staged nothing via pread with the hot extension on")
		}
		if (!enabled || !hot) && c1 != c0 {
			t.Fatalf("cold (just-written) pass staged %d chunks via pread; want 0 (enabled=%v hot=%v)", c1-c0, enabled, hot)
		}
		steady = scanAll(ex)
		c2, _, _ := parquet.PreadStats()
		if enabled && c2 == c1 {
			t.Fatal("steady (cache-hit) pass staged nothing via pread with the lever on")
		}
		if !enabled && c2 != c1 {
			t.Fatalf("steady pass staged %d chunks via pread with the kill switch set", c2-c1)
		}
		return cold, steady
	}

	hotCold, hotSteady := runMode(true, true)
	preadCold, preadSteady := runMode(true, false)
	mmapCold, mmapSteady := runMode(false, false)

	want := 3 * rowsPerFile
	if len(preadCold) != want {
		t.Fatalf("pread cold rows = %d, want %d", len(preadCold), want)
	}
	for name, pair := range map[string][2][]string{
		"cold":         {preadCold, mmapCold},
		"steady":       {preadSteady, mmapSteady},
		"pread-passes": {preadCold, preadSteady},
		"hot-cold":     {hotCold, mmapCold},
		"hot-steady":   {hotSteady, mmapSteady},
	} {
		a, b := pair[0], pair[1]
		if len(a) != len(b) {
			t.Fatalf("%s: row counts differ (%d vs %d)", name, len(a), len(b))
		}
		for i := range a {
			if a[i] != b[i] {
				t.Fatalf("%s: row %d differs: %q vs %q", name, i, a[i], b[i])
			}
		}
	}
}
