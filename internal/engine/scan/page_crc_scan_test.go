package scan

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	gp "github.com/parquet-go/parquet-go"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The row reader is not the only door onto a page. The native columnar scan
// walks the same ColumnPageReader and is the path every query actually takes,
// so #891's refusal has to hold here too — a file whose checksum says its
// bytes are wrong must not become a query result.

type scanCRCPage struct {
	column   string
	kind     pqt.PageType
	headerAt int
	bodyAt   int
	bodyLen  int
}

func scanWalkPages(tb testing.TB, data []byte) []scanCRCPage {
	tb.Helper()
	md, err := pqt.ReadFileMetaData(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		tb.Fatalf("footer: %v", err)
	}
	var out []scanCRCPage
	for rgi := range md.RowGroups {
		rg := &md.RowGroups[rgi]
		for ci := range rg.Columns {
			cm := rg.Columns[ci].MetaData
			if cm == nil {
				continue
			}
			start := cm.DataPageOffset
			if cm.DictionaryPageOffset > 0 && cm.DictionaryPageOffset < start {
				start = cm.DictionaryPageOffset
			}
			end := start + cm.TotalCompressedSize
			for off := int(start); off < int(end); {
				ph, n, err := pqt.DecodePageHeader(data[off:])
				if err != nil {
					tb.Fatalf("page header at %d: %v", off, err)
				}
				if !ph.CRCSet {
					tb.Fatalf("fixture page at %d carries no checksum", off)
				}
				out = append(out, scanCRCPage{
					column:   strings.Join(cm.PathInSchema, "."),
					kind:     ph.Type,
					headerAt: off,
					bodyAt:   off + n,
					bodyLen:  int(ph.CompressedPageSize),
				})
				off += n + int(ph.CompressedPageSize)
			}
		}
	}
	return out
}

type crcRec struct {
	X int64  `parquet:"x,plain"`
	S string `parquet:"s,plain"`
}

// scanCRCFile writes a parquet-go file (which checksums every page) with
// several pages per column so a bit flip can be aimed at one of them.
func scanCRCFile(tb testing.TB, pageVersion, n int) []byte {
	tb.Helper()
	var b bytes.Buffer
	w := gp.NewGenericWriter[crcRec](&b,
		gp.DataPageVersion(pageVersion), gp.PageBufferSize(512), gp.Compression(&gp.Uncompressed))
	rows := make([]crcRec, n)
	for i := range rows {
		rows[i] = crcRec{int64(i), fmt.Sprintf("value-%04d", i)}
	}
	if _, err := w.Write(rows); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	return b.Bytes()
}

func crcScanSchema() []pqt.Column {
	return []pqt.Column{
		{Name: "x", Type: pqt.TypeInt64},
		{Name: "s", Type: pqt.TypeString},
	}
}

// TestTheNativeScanRefusesAPageThatFailsItsChecksum: one cell per page of a
// checksummed file, through the columnar path.
func TestTheNativeScanRefusesAPageThatFailsItsChecksum(t *testing.T) {
	for _, ver := range []int{1, 2} {
		t.Run(fmt.Sprintf("v%d", ver), func(t *testing.T) {
			data := scanCRCFile(t, ver, 600)
			schema := crcScanSchema()

			clean, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			total := 0
			for rg := 0; rg < clean.FileReader().NumRowGroups(); rg++ {
				rb, err := ReadRowGroupNative(clean.FileReader(), rg, schema, nil)
				if err != nil {
					t.Fatalf("unmutated scan: %v", err)
				}
				total += rb.Len
			}
			if total != 600 {
				t.Fatalf("unmutated scan read %d rows, want 600", total)
			}

			pages := scanWalkPages(t, data)
			if len(pages) < 2 {
				t.Fatalf("fixture has %d pages; the cell needs several", len(pages))
			}
			for i, p := range pages {
				if p.bodyLen == 0 {
					continue
				}
				t.Run(fmt.Sprintf("page%d_%v", i, p.kind), func(t *testing.T) {
					mutated := append([]byte(nil), data...)
					mutated[p.bodyAt+p.bodyLen/2] ^= 0x40

					r, err := pqt.NewReader(bytes.NewReader(mutated), int64(len(mutated)))
					if err != nil {
						return // refused at open: still a refusal
					}
					fr := r.FileReader()
					var lastErr error
					for rg := 0; rg < fr.NumRowGroups(); rg++ {
						if _, err := ReadRowGroupNative(fr, rg, schema, nil); err != nil {
							lastErr = err
						}
					}
					if lastErr == nil {
						t.Fatalf("column %s: the native scan read a %v at %d whose checksum fails, with no error",
							p.column, p.kind, p.headerAt)
					}
					if !strings.Contains(lastErr.Error(), "fails its own checksum") {
						t.Fatalf("column %s: refused, but not for the checksum: %v", p.column, lastErr)
					}
				})
			}
		})
	}
}

// TestASkippedPageIsNotHeldToItsChecksum is the boundary, claimed from both
// sides. The reader verifies every page it DECODES; a page the sel path skips
// contributes no value, so its bytes are never touched and never checksummed
// — which is what makes the skip a skip. The same file read in full refuses.
//
// Without both halves "we check the checksum" is either untrue (the skip
// silently exempts arbitrary pages) or the skip is not one.
func TestASkippedPageIsNotHeldToItsChecksum(t *testing.T) {
	data := scanCRCFile(t, 1, 2000)
	schema := crcScanSchema()

	// Find the LAST data page of the string column: rows near the end of the
	// row group, which a selection of early rows will skip.
	pages := scanWalkPages(t, data)
	var target scanCRCPage
	for _, p := range pages {
		if p.column == "s" && p.kind == pqt.PageDataV1 && p.bodyLen > 0 {
			target = p
		}
	}
	if target.bodyLen == 0 {
		t.Fatal("no string data page in fixture")
	}
	mutated := append([]byte(nil), data...)
	mutated[target.bodyAt+target.bodyLen/2] ^= 0x40

	r, err := pqt.NewReader(bytes.NewReader(mutated), int64(len(mutated)))
	if err != nil {
		t.Fatal(err)
	}
	fr := r.FileReader()
	if fr.NumRowGroups() != 1 {
		t.Skipf("fixture wrote %d row groups; the cell aims at one", fr.NumRowGroups())
	}

	// Read in full: refused.
	if _, err := ReadRowGroupNative(fr, 0, schema, nil); err == nil {
		t.Fatal("the full decode read a page whose checksum fails, with no error")
	} else if !strings.Contains(err.Error(), "fails its own checksum") {
		t.Fatalf("full decode refused, but not for the checksum: %v", err)
	}

	// Read under a selection that lands entirely before the corrupt page:
	// the page is skipped, its bytes are never decoded, and the rows the
	// query asked for are exactly right.
	sel := []uint32{0, 1, 2, 3}
	rb, err := readRowGroupNative(fr, 0, schema, nil, nil, sel, nil, nil)
	if err != nil {
		t.Fatalf("a selection that skips the corrupt page must still read: %v", err)
	}
	sv := rb.Columns[1]
	for _, row := range sel {
		if got := sv.GetValue(int(row)); got != fmt.Sprintf("value-%04d", row) {
			t.Fatalf("row %d: got %v, want value-%04d", row, got, row)
		}
	}
}
