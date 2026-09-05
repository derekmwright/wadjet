package scan

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #892 through the columnar path. The row reader padded a short column with
// NULLs; the native scan left the destination vector's tail at whatever it
// was allocated as, which renders the same way. One reconciliation in the
// page reader covers both, and this is the half that every query takes.

// cutScanChunk rewrites a file's footer so one column chunk's declared byte
// length stops after keepBytes. Nothing else changes — the row group still
// declares its rows.
func cutScanChunk(tb testing.TB, data []byte, leaf string, keepBytes int64) []byte {
	tb.Helper()
	md, err := pqt.ReadFileMetaData(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		tb.Fatal(err)
	}
	found := false
	for rgi := range md.RowGroups {
		rg := &md.RowGroups[rgi]
		for ci := range rg.Columns {
			cm := rg.Columns[ci].MetaData
			if cm == nil || strings.Join(cm.PathInSchema, ".") != leaf {
				continue
			}
			cm.TotalCompressedSize = keepBytes
			found = true
		}
	}
	if !found {
		tb.Fatalf("no chunk for leaf %q", leaf)
	}
	footerLen := binary.LittleEndian.Uint32(data[len(data)-8:])
	out := append([]byte(nil), data[:len(data)-8-int(footerLen)]...)
	footer := pqt.EncodeFileMetaData(md)
	out = append(out, footer...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(footer)))
	return append(out, "PAR1"...)
}

// scanChunkPageEnds returns the cumulative byte length of the chunk after
// each complete page.
func scanChunkPageEnds(tb testing.TB, data []byte, leaf string) []int64 {
	tb.Helper()
	md, err := pqt.ReadFileMetaData(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		tb.Fatal(err)
	}
	for rgi := range md.RowGroups {
		rg := &md.RowGroups[rgi]
		for ci := range rg.Columns {
			cm := rg.Columns[ci].MetaData
			if cm == nil || strings.Join(cm.PathInSchema, ".") != leaf {
				continue
			}
			s := cm.DataPageOffset
			if cm.DictionaryPageOffset > 0 && cm.DictionaryPageOffset < s {
				s = cm.DictionaryPageOffset
			}
			end := s + cm.TotalCompressedSize
			var ends []int64
			for off := int(s); off < int(end); {
				ph, n, err := pqt.DecodePageHeader(data[off:])
				if err != nil {
					tb.Fatalf("page header at %d: %v", off, err)
				}
				off += n + int(ph.CompressedPageSize)
				ends = append(ends, int64(off)-s)
			}
			return ends
		}
	}
	tb.Fatalf("no chunk for leaf %q", leaf)
	return nil
}

func completenessScanFile(tb testing.TB, n int) []byte {
	tb.Helper()
	schema := pqt.Schema{Columns: []pqt.Column{
		{Name: "x", Type: pqt.TypeInt64},
		{Name: "s", Type: pqt.TypeString, Nullable: true},
	}}
	rows := make([]map[string]any, n)
	for i := range rows {
		m := map[string]any{"x": int64(i * 3)}
		if i%5 == 0 {
			m["s"] = nil
		} else {
			m["s"] = fmt.Sprintf("s-%04d", i)
		}
		rows[i] = m
	}
	var b bytes.Buffer
	w, err := pqt.NewWriter(&b, schema, pqt.WriterConfig{PageBufferSize: 64, Compression: pqt.CompressionNone})
	if err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	return b.Bytes()
}

// TestTheNativeScanRefusesAColumnThatEndsEarly: one cell per page boundary,
// for a required and an optional column, through the columnar decode.
func TestTheNativeScanRefusesAColumnThatEndsEarly(t *testing.T) {
	const n = 400
	data := completenessScanFile(t, n)
	schema := []pqt.Column{
		{Name: "x", Type: pqt.TypeInt64},
		{Name: "s", Type: pqt.TypeString, Nullable: true},
	}

	// Unmutated: the scan reads every row.
	clean, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for rg := 0; rg < clean.FileReader().NumRowGroups(); rg++ {
		rb, err := ReadRowGroupNative(clean.FileReader(), rg, schema, nil)
		if err != nil {
			t.Fatalf("unmutated: %v", err)
		}
		total += rb.Len
	}
	if total != n {
		t.Fatalf("unmutated scan read %d rows, want %d", total, n)
	}

	for _, leaf := range []string{"x", "s"} {
		t.Run(leaf, func(t *testing.T) {
			ends := scanChunkPageEnds(t, data, leaf)
			if len(ends) < 3 {
				t.Fatalf("leaf %s has %d pages; the cell needs several", leaf, len(ends))
			}
			for i, keep := range ends[:len(ends)-1] {
				t.Run(fmt.Sprintf("after_page_%d", i), func(t *testing.T) {
					mutated := cutScanChunk(t, data, leaf, keep)
					r, err := pqt.NewReader(bytes.NewReader(mutated), int64(len(mutated)))
					if err != nil {
						return // refused at open
					}
					fr := r.FileReader()
					var lastErr error
					for rg := 0; rg < fr.NumRowGroups(); rg++ {
						if _, err := ReadRowGroupNative(fr, rg, schema, nil); err != nil {
							lastErr = err
						}
					}
					if lastErr == nil {
						t.Fatalf("leaf %s cut after page %d: the native scan read it with no error", leaf, i)
					}
					if !strings.Contains(lastErr.Error(), "the chunk ends before its declared rows") {
						t.Fatalf("refused, but not as a short column: %v", lastErr)
					}
				})
			}
		})
	}
}

// TestASelDecodeOverAShortColumnIsRefused: the page-skip path accounts a
// skipped page's rows from its header, so a chunk that runs out of pages is
// short there too — a selection must not be able to launder a truncation into
// a clean read of zero-length slots.
func TestASelDecodeOverAShortColumnIsRefused(t *testing.T) {
	const n = 400
	data := completenessScanFile(t, n)
	schema := []pqt.Column{
		{Name: "x", Type: pqt.TypeInt64},
		{Name: "s", Type: pqt.TypeString, Nullable: true},
	}
	ends := scanChunkPageEnds(t, data, "s")
	if len(ends) < 3 {
		t.Fatalf("fixture has %d pages", len(ends))
	}
	mutated := cutScanChunk(t, data, "s", ends[len(ends)-2])

	r, err := pqt.NewReader(bytes.NewReader(mutated), int64(len(mutated)))
	if err != nil {
		t.Fatal(err)
	}
	fr := r.FileReader()
	sel := []uint32{0, 1, 2}
	var lastErr error
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		if _, err := readRowGroupNative(fr, rg, schema, nil, nil, sel, nil, nil); err != nil {
			lastErr = err
		}
	}
	if lastErr == nil {
		t.Fatal("a selection read a chunk that ends before its declared rows with no error")
	}
	if !strings.Contains(lastErr.Error(), "the chunk ends before its declared rows") {
		t.Fatalf("refused, but not as a short column: %v", lastErr)
	}
}
