package compaction

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	gp "github.com/parquet-go/parquet-go"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Compaction REPLACES its inputs. Every wrong value the reader hands it
// becomes the durable one, and the file that could still prove itself wrong —
// by its own page checksum, by its own row counts — is deleted.
//
// Both of the reader defects this arc closes were exactly that shape. A page
// with a flipped bit read as a value (#891); a chunk that stopped before its
// declared rows read as NULLs (#892). Either one, compacted, is silent
// permanent data loss. These cells assert the refusal reaches the compactor:
// it fails, the manifest is unchanged, and the input files are still there.

func corruptInputSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "x", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
	}}
}

// checksummedParquet writes a file with parquet-go, which emits a page
// checksum for every page. Wadjet's own writer emits none, so a
// checksum-carrying input can only come from another writer.
func checksummedParquet(t *testing.T, n int) []byte {
	t.Helper()
	type rec struct {
		X int64  `parquet:"x,plain"`
		S string `parquet:"s,plain"`
	}
	var b bytes.Buffer
	w := gp.NewGenericWriter[rec](&b,
		gp.DataPageVersion(1), gp.PageBufferSize(512), gp.Compression(&gp.Uncompressed))
	rows := make([]rec, n)
	for i := range rows {
		rows[i] = rec{int64(i), fmt.Sprintf("v-%04d", i)}
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// flipFirstDataPageBit corrupts one byte of the first data page's body.
func flipFirstDataPageBit(t *testing.T, data []byte) []byte {
	t.Helper()
	md, err := parquet.ReadFileMetaData(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	cm := md.RowGroups[0].Columns[0].MetaData
	start := cm.DataPageOffset
	if cm.DictionaryPageOffset > 0 && cm.DictionaryPageOffset < start {
		start = cm.DictionaryPageOffset
	}
	for off := int(start); off < int(start+cm.TotalCompressedSize); {
		ph, n, err := parquet.DecodePageHeader(data[off:])
		if err != nil {
			t.Fatal(err)
		}
		if ph.Type == parquet.PageDataV1 && ph.CompressedPageSize > 0 {
			if !ph.CRCSet {
				t.Fatal("fixture page carries no checksum")
			}
			out := append([]byte(nil), data...)
			out[off+n] ^= 1
			return out
		}
		off += n + int(ph.CompressedPageSize)
	}
	t.Fatal("no data page found")
	return nil
}

// cutFirstChunkAfterFirstPage makes a column chunk end at its first complete
// page while its metadata still promises every row.
func cutFirstChunkAfterFirstPage(t *testing.T, data []byte) []byte {
	t.Helper()
	md, err := parquet.ReadFileMetaData(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	cm := md.RowGroups[0].Columns[0].MetaData
	start := cm.DataPageOffset
	if cm.DictionaryPageOffset > 0 && cm.DictionaryPageOffset < start {
		start = cm.DictionaryPageOffset
	}
	ph, n, err := parquet.DecodePageHeader(data[start:])
	if err != nil {
		t.Fatal(err)
	}
	if int64(n)+int64(ph.CompressedPageSize) >= cm.TotalCompressedSize {
		t.Fatal("chunk has only one page; the cut would be a no-op")
	}
	cm.TotalCompressedSize = int64(n) + int64(ph.CompressedPageSize)

	footerLen := binary.LittleEndian.Uint32(data[len(data)-8:])
	out := append([]byte(nil), data[:len(data)-8-int(footerLen)]...)
	footer := parquet.EncodeFileMetaData(md)
	out = append(out, footer...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(footer)))
	return append(out, "PAR1"...)
}

// wadjetParquet writes a multi-page file with wadjet's own writer.
func wadjetParquet(t *testing.T, n int) []byte {
	t.Helper()
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"x": int64(i), "s": fmt.Sprintf("v-%04d", i)}
	}
	var b bytes.Buffer
	w, err := parquet.NewWriter(&b, corruptInputSchema(),
		parquet.WriterConfig{PageBufferSize: 64, Compression: parquet.CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// TestCompactionRefusesACorruptInputAndPersistsNothing drives the real
// compactor over a partition whose files include one the reader can prove
// wrong. It must fail, and the manifest must come out exactly as it went in.
func TestCompactionRefusesACorruptInputAndPersistsNothing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		build   func(t *testing.T) []byte
		corrupt func(t *testing.T, data []byte) []byte
		want    string
	}{
		{
			name:    "flipped_bit_under_a_page_checksum",
			build:   func(t *testing.T) []byte { return checksummedParquet(t, 400) },
			corrupt: flipFirstDataPageBit,
			want:    "fails its own checksum",
		},
		{
			name:    "chunk_ends_before_its_declared_rows",
			build:   func(t *testing.T) []byte { return wadjetParquet(t, 400) },
			corrupt: cutFirstChunkAfterFirstPage,
			want:    "the chunk ends before its declared rows",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cat, store := setupTestCatalog(t)
			schema := corruptInputSchema()
			const table = "corrupt_in"
			if err := cat.CreateTable(ctx, table, schema, nil); err != nil {
				t.Fatal(err)
			}

			// Two healthy files and one corrupt one, all in one partition,
			// enough of them to clear the compaction floors.
			var paths []string
			put := func(i int, data []byte) {
				path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", table, i)
				if _, err := store.Put(ctx, "test-bucket", path,
					bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
					t.Fatal(err)
				}
				if err := cat.AddFiles(ctx, table, nil, "", []catalog.FileEntry{
					{Path: path, SizeBytes: int64(len(data)), NumRows: 400, CreatedAt: time.Now().UTC()},
				}); err != nil {
					t.Fatal(err)
				}
				paths = append(paths, path)
			}
			clean := tc.build(t)
			put(0, clean)
			put(1, clean)
			put(2, tc.corrupt(t, tc.build(t)))

			before := manifestPaths(t, cat, table)
			if len(before) != 3 {
				t.Fatalf("manifest has %d files, want 3", len(before))
			}

			cfg := DefaultConfig()
			cfg.MinFiles = 2
			cfg.MaxFileSizeBytes = 1 << 30
			cfg.DeleteGrace = -1
			c := New(cat, nil, cfg)

			res, err := c.CompactTable(ctx, table)
			failed := err != nil
			if res != nil && len(res.Failed) > 0 {
				failed = true
				joined := fmt.Sprint(res.Failed)
				if !strings.Contains(joined, tc.want) {
					t.Fatalf("compaction failed, but not for the corruption: %v", joined)
				}
			} else if err != nil && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("compaction failed, but not for the corruption: %v", err)
			}
			if !failed {
				t.Fatalf("compaction merged a file the reader can prove wrong: %+v", res)
			}
			if res != nil && res.FilesCreated > 0 {
				t.Fatalf("compaction wrote %d output files from a corrupt input", res.FilesCreated)
			}

			// The inputs are still in the manifest and still in the store:
			// nothing was replaced by a file the corruption had been read
			// into.
			after := manifestPaths(t, cat, table)
			if !equalStrings(after, before) {
				t.Fatalf("manifest changed: %v -> %v", before, after)
			}
			for _, p := range paths {
				if _, _, err := store.Get(ctx, "test-bucket", p); err != nil {
					t.Fatalf("input %s was deleted: %v", p, err)
				}
			}
		})
	}
}
