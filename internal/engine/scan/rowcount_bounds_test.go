package scan

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"strings"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A row group's num_rows is the length every destination vector is allocated
// for. Holding it only to "not negative" and "not more than the file's own
// total" left the file's own total unbounded, and num_rows = 2^40 on a
// two-row file reached batch.NewRecordBatch with 128 GiB of bitmap: "fatal
// error: runtime: out of memory", which no recover catches and which in a
// worker process is the worker. 2^30 was worse in kind — it was ACCEPTED, and
// decoded after allocating gibibytes.

func twoRowFile(t *testing.T) []byte {
	t.Helper()
	schema := pqt.Schema{Columns: []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64, Nullable: true},
		{Name: "name", Type: pqt.TypeString, Nullable: true},
	}}
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, schema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := w.WriteRows([]map[string]any{
		{"id": int64(1), "name": "a"},
		{"id": int64(2), "name": "b"},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return buf.Bytes()
}

// rewriteFooter decodes a file's footer, hands it to edit, and re-encodes it.
// The pages are untouched; only the claims about them change.
func rewriteFooter(t *testing.T, raw []byte, edit func(*pqt.FileMetaData)) []byte {
	t.Helper()
	footerLen := int(binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]))
	start := len(raw) - 8 - footerLen
	md, err := pqt.DecodeFileMetaData(raw[start : start+footerLen])
	if err != nil {
		t.Fatalf("decoding footer: %v", err)
	}
	edit(md)
	return withFooter(raw[:start], pqt.EncodeFileMetaData(md))
}

// readSchemaBothPaths drives a file through the native columnar reader and the row
// reader, returning the first error each produced. No recover: an
// out-of-memory fatal is not recoverable anywhere, which is the point.
func readSchemaBothPaths(t *testing.T, raw []byte, schema []pqt.Column) (nativeErr, rowErr error) {
	t.Helper()
	fr, err := pqt.OpenFileReaderFromBytes(raw)
	if err != nil {
		nativeErr = err
	} else {
		_, nativeErr = ReadFileBatchesNative(fr, schema, nil)
	}
	r, err := pqt.NewReaderFromBytes(raw)
	if err != nil {
		rowErr = err
	} else {
		_, rowErr = r.ReadRowsAs(schema, nil)
	}
	return nativeErr, rowErr
}

func TestInflatedRowCountsAreRefusedOnBothPaths(t *testing.T) {
	good := twoRowFile(t)
	schema := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64, Nullable: true},
		{Name: "name", Type: pqt.TypeString, Nullable: true},
	}
	if n, r := readSchemaBothPaths(t, good, schema); n != nil || r != nil {
		t.Fatalf("the honest fixture does not read: native=%v row=%v", n, r)
	}

	cases := []struct {
		name string
		edit func(*pqt.FileMetaData)
	}{
		{"file total 2^40", func(md *pqt.FileMetaData) { md.NumRows = 1 << 40 }},
		{"file total 2^30", func(md *pqt.FileMetaData) { md.NumRows = 1 << 30 }},
		{"file and row group 2^40", func(md *pqt.FileMetaData) {
			md.NumRows = 1 << 40
			md.RowGroups[0].NumRows = 1 << 40
		}},
		{"file and row group 2^30", func(md *pqt.FileMetaData) {
			md.NumRows = 1 << 30
			md.RowGroups[0].NumRows = 1 << 30
		}},
		{"row group only, 2^30", func(md *pqt.FileMetaData) { md.RowGroups[0].NumRows = 1 << 30 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := rewriteFooter(t, good, tc.edit)

			// TotalAlloc is cumulative, so the delta is every byte the two
			// reads asked for. A refusal costs the footer and the metadata;
			// the bug cost 128 GiB. Anything between is a partial fix.
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			nativeErr, rowErr := readSchemaBothPaths(t, raw, schema)
			runtime.ReadMemStats(&after)

			if nativeErr == nil {
				t.Error("the native path read the file")
			} else if !strings.Contains(nativeErr.Error(), "rows") {
				t.Errorf("native error %q does not name the row count", nativeErr)
			}
			if rowErr == nil {
				t.Error("the row path read the file")
			} else if !strings.Contains(rowErr.Error(), "rows") {
				t.Errorf("row error %q does not name the row count", rowErr)
			}

			const budget = 1 << 20 // 1 MiB, against a ~1 KiB file
			if grew := after.TotalAlloc - before.TotalAlloc; grew > budget {
				t.Errorf("refusing the file allocated %d bytes, more than the %d-byte budget "+
					"for a %d-byte file", grew, budget, len(raw))
			}
		})
	}
}

// TestMillionRowFileStillReads is the other half: the bound has to be a bound
// on nonsense and not on size. One million rows is pyarrow's default row
// group size and eight of wadjet's, so it crosses the multi-row-group path
// the sum check runs over.
func TestMillionRowFileStillReads(t *testing.T) {
	const n = 1_000_000
	schema := pqt.Schema{Columns: []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}}
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, schema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	rows := make([]map[string]any, 0, 8192)
	for i := 0; i < n; i++ {
		rows = append(rows, map[string]any{"id": int64(i)})
		if len(rows) == cap(rows) {
			if err := w.WriteRows(rows); err != nil {
				t.Fatalf("write: %v", err)
			}
			rows = rows[:0]
		}
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	fr, err := pqt.OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("opening a million-row file: %v", err)
	}
	if fr.NumRowGroups() < 2 {
		t.Fatalf("the fixture has %d row groups; the multi-group path is untested", fr.NumRowGroups())
	}
	batches, err := ReadFileBatchesNative(fr, schema.Columns, nil)
	if err != nil {
		t.Fatalf("reading a million-row file: %v", err)
	}
	total := 0
	for _, b := range batches {
		if b != nil {
			total += b.Len
		}
	}
	if total != n {
		t.Errorf("read %d rows, want %d", total, n)
	}
}
