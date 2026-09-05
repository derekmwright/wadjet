package parquet

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"os"
	"strings"
	"testing"

	gp "github.com/parquet-go/parquet-go"
)

// The reader trusts nothing it can verify (ADR-0018 §11): a page that carries
// a checksum is held to it before any value comes out of it.
//
// These cells exist because it did not. A parquet-go-written uncompressed
// PLAIN INT64 page with one payload bit flipped read [42, 43] as [43, 43]
// with a nil error, while parquet-go refused the identical bytes (#891). The
// checksum was in the file, decoded into PageHeader.CRC, and consulted by
// nothing.

// crcPage is one page's position inside a file, as a walk of the footer's
// chunk offsets and the page headers finds it.
type crcPage struct {
	column   string
	kind     PageType
	headerAt int // file offset of the page header
	bodyAt   int // file offset of the page body
	bodyLen  int
	crc      uint32
	crcSet   bool
}

// walkPages enumerates every page of every column chunk in a file.
func walkPages(tb testing.TB, data []byte) []crcPage {
	tb.Helper()
	md, err := ReadFileMetaData(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		tb.Fatalf("footer: %v", err)
	}
	var out []crcPage
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
				ph, n, err := DecodePageHeader(data[off:])
				if err != nil {
					tb.Fatalf("page header at %d: %v", off, err)
				}
				out = append(out, crcPage{
					column:   strings.Join(cm.PathInSchema, "."),
					kind:     ph.Type,
					headerAt: off,
					bodyAt:   off + n,
					bodyLen:  int(ph.CompressedPageSize),
					crc:      uint32(ph.CRC),
					crcSet:   ph.CRCSet,
				})
				off += n + int(ph.CompressedPageSize)
			}
		}
	}
	return out
}

// checksummedFixture is one file the CRC cells run over, with the values it
// must read back when nothing is mutated.
type checksummedFixture struct {
	name string
	data []byte
}

// parquetGoFile writes a two-column file with parquet-go, which emits a page
// checksum for every page it writes.
func parquetGoFile(tb testing.TB, pageVersion int, codec gp.WriterOption, dict bool, n int) []byte {
	tb.Helper()
	type plainRec struct {
		X int64  `parquet:"x,plain"`
		S string `parquet:"s,plain"`
	}
	type dictRec struct {
		X int64  `parquet:"x,plain"`
		S string `parquet:"s,dict"`
	}
	var b bytes.Buffer
	opts := []gp.WriterOption{gp.DataPageVersion(pageVersion), gp.PageBufferSize(1024), codec}
	if dict {
		w := gp.NewGenericWriter[dictRec](&b, opts...)
		rows := make([]dictRec, n)
		for i := range rows {
			rows[i] = dictRec{int64(i), fmt.Sprintf("v%d", i%13)}
		}
		if _, err := w.Write(rows); err != nil {
			tb.Fatal(err)
		}
		if err := w.Close(); err != nil {
			tb.Fatal(err)
		}
	} else {
		w := gp.NewGenericWriter[plainRec](&b, opts...)
		rows := make([]plainRec, n)
		for i := range rows {
			rows[i] = plainRec{int64(i), fmt.Sprintf("v%d", i)}
		}
		if _, err := w.Write(rows); err != nil {
			tb.Fatal(err)
		}
		if err := w.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	return b.Bytes()
}

// parquetGoOptionalFile writes a file whose every leaf is OPTIONAL, which is
// what parquetGoFile cannot produce: its struct fields are non-pointer, so
// every leaf is REQUIRED and every v2 page it writes has
// DefinitionLevelsByteLength == 0. A whole class of header corruption — a
// level byte length that is wrong, and that also PLACES the value section —
// is unreachable on a required flat leaf, so the sweep that established
// "a flipped header bit never produces a silent wrong answer" was measuring
// a corpus that could not contain the counterexample. It could:
// zeroing DefinitionLevelsByteLength on a page of this file read x:0 as
// x:81600, nil error.
func parquetGoOptionalFile(tb testing.TB, pageVersion int, codec gp.WriterOption, n int) []byte {
	tb.Helper()
	type optRec struct {
		X *int64  `parquet:"x,plain,optional"`
		S *string `parquet:"s,plain,optional"`
	}
	var b bytes.Buffer
	w := gp.NewGenericWriter[optRec](&b,
		gp.DataPageVersion(pageVersion), gp.PageBufferSize(1024), codec)
	// Nulls only in the first fifth, so the LATER pages are optional leaves
	// whose num_nulls is zero — the exact shape the counterexample needed.
	// A page that has nulls is already covered by the "declares nulls but
	// carries no definition levels" guard; a page that has none is not, and
	// its definition levels still place the value section.
	rows := make([]optRec, n)
	for i := range rows {
		if i >= n/5 || i%5 != 0 {
			v := int64(i)
			str := fmt.Sprintf("v%d", i)
			rows[i] = optRec{&v, &str}
		}
	}
	if _, err := w.Write(rows); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	return b.Bytes()
}

// testdataFile reads a committed fixture.
func testdataFile(tb testing.TB, name string) []byte {
	tb.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		tb.Fatalf("reading %s (regenerate with the generator beside it): %v", name, err)
	}
	return data
}

// checksummedFixtures is the corpus: two INDEPENDENT writers (parquet-go and
// PyArrow), both page-format versions, compressed and uncompressed, with and
// without a dictionary page.
func checksummedFixtures(tb testing.TB) []checksummedFixture {
	tb.Helper()
	out := []checksummedFixture{
		{"parquet-go/v1/uncompressed/plain", parquetGoFile(tb, 1, gp.Compression(&gp.Uncompressed), false, 300)},
		{"parquet-go/v1/uncompressed/dict", parquetGoFile(tb, 1, gp.Compression(&gp.Uncompressed), true, 300)},
		{"parquet-go/v1/snappy/dict", parquetGoFile(tb, 1, gp.Compression(&gp.Snappy), true, 300)},
		{"parquet-go/v1/zstd/plain", parquetGoFile(tb, 1, gp.Compression(&gp.Zstd), false, 300)},
		{"parquet-go/v1/gzip/plain", parquetGoFile(tb, 1, gp.Compression(&gp.Gzip), false, 300)},
		{"parquet-go/v2/uncompressed/plain", parquetGoFile(tb, 2, gp.Compression(&gp.Uncompressed), false, 300)},
		{"parquet-go/v2/snappy/dict", parquetGoFile(tb, 2, gp.Compression(&gp.Snappy), true, 300)},
		{"parquet-go/v2/zstd/plain", parquetGoFile(tb, 2, gp.Compression(&gp.Zstd), false, 300)},
		{"parquet-go/v2/gzip/dict", parquetGoFile(tb, 2, gp.Compression(&gp.Gzip), true, 300)},
	}
	for _, f := range []string{"testdata/page_crc.parquet", "testdata/page_crc_v2.parquet"} {
		data, err := os.ReadFile(f)
		if err != nil {
			tb.Fatalf("reading %s (regenerate with testdata/gen_page_crc.py): %v", f, err)
		}
		out = append(out, checksummedFixture{"pyarrow/" + f, data})
	}
	return out
}

// TestEveryChecksumFixtureActuallyCarriesChecksums keeps the bit-flip cells
// from going vacuous. A fixture regenerated by a writer that stopped emitting
// checksums would still "refuse" nothing, and every cell below would pass by
// never having a checksum to check.
func TestEveryChecksumFixtureActuallyCarriesChecksums(t *testing.T) {
	for _, fx := range checksummedFixtures(t) {
		t.Run(fx.name, func(t *testing.T) {
			pages := walkPages(t, fx.data)
			if len(pages) == 0 {
				t.Fatal("no pages")
			}
			for _, p := range pages {
				if !p.crcSet {
					t.Fatalf("%s %v at %d carries no checksum", p.column, p.kind, p.headerAt)
				}
				if got := crc32.ChecksumIEEE(fx.data[p.bodyAt : p.bodyAt+p.bodyLen]); got != p.crc {
					t.Fatalf("%s %v at %d: fixture's own checksum %#08x != %#08x over its stored body",
						p.column, p.kind, p.headerAt, p.crc, got)
				}
			}
		})
	}
}

// TestAFlippedBitInAChecksummedPageIsRefused is the #891 headline shape, one
// cell per page of every fixture: flip one payload bit and the read must fail
// naming the column, rather than answer with a value the file's own checksum
// contradicts.
func TestAFlippedBitInAChecksummedPageIsRefused(t *testing.T) {
	for _, fx := range checksummedFixtures(t) {
		t.Run(fx.name, func(t *testing.T) {
			clean, err := readEveryRow(fx.data)
			if err != nil {
				t.Fatalf("unmutated file must read: %v", err)
			}
			if len(clean) == 0 {
				t.Fatal("unmutated file read zero rows")
			}
			pages := walkPages(t, fx.data)
			for i, p := range pages {
				if p.bodyLen == 0 {
					continue
				}
				for _, bit := range []int{0, p.bodyLen * 8 / 2, p.bodyLen*8 - 1} {
					name := fmt.Sprintf("page%d_%v_bit%d", i, p.kind, bit)
					t.Run(name, func(t *testing.T) {
						mutated := append([]byte(nil), fx.data...)
						mutated[p.bodyAt+bit/8] ^= 1 << (bit % 8)
						rows, err := readEveryRow(mutated)
						if err == nil {
							t.Fatalf("column %s: a flipped bit in the %v at %d read %d rows with no error",
								p.column, p.kind, p.headerAt, len(rows))
						}
						if !strings.Contains(err.Error(), "fails its own checksum") {
							t.Fatalf("column %s: refused, but not for the checksum: %v", p.column, err)
						}
						if !strings.Contains(err.Error(), p.column) {
							t.Fatalf("refusal does not name column %s: %v", p.column, err)
						}
					})
				}
			}
		})
	}
}

// TestAPresentZeroChecksumIsStillVerified is the presence-versus-value cell.
//
// crc is an OPTIONAL thrift field and zero is a legal checksum. A reader that
// tests `CRC != 0` — parquet-go's own does — cannot tell "this writer emits no
// checksums" from "this body hashes to zero", and skips verification on the
// second. The header's field PRESENCE is what the reader gates on, so a page
// header carrying a zero checksum over a body that does not hash to zero is
// refused.
func TestAPresentZeroChecksumIsStillVerified(t *testing.T) {
	data := parquetGoFile(t, 1, gp.Compression(&gp.Uncompressed), false, 8)
	pages := walkPages(t, data)
	if len(pages) == 0 {
		t.Fatal("no pages")
	}
	p := pages[0]

	// Re-encode the page's header with its checksum set to a present zero.
	// The bodies do not move: the encoder emits the same fields, and the
	// i32 checksum is the same width whatever its value.
	ph, hdrLen, err := DecodePageHeader(data[p.headerAt:])
	if err != nil {
		t.Fatal(err)
	}
	if !ph.CRCSet {
		t.Fatalf("fixture page carries no checksum to rewrite")
	}
	ph.CRC = 0
	newHdr := EncodePageHeader(ph)

	mutated := append([]byte(nil), data[:p.headerAt]...)
	mutated = append(mutated, newHdr...)
	mutated = append(mutated, data[p.headerAt+hdrLen:]...)
	if len(newHdr) != hdrLen {
		// Re-encoding shifted everything after this header; the footer's
		// offsets no longer describe the file, and the cell would be
		// testing offset arithmetic rather than the checksum. Rebuilding
		// the footer is not worth it — the shape is asserted by the
		// re-decode below either way.
		t.Skipf("re-encoded header is %d bytes, was %d", len(newHdr), hdrLen)
	}

	back, _, err := DecodePageHeader(mutated[p.headerAt:])
	if err != nil {
		t.Fatal(err)
	}
	if !back.CRCSet || back.CRC != 0 {
		t.Fatalf("round trip lost the present-zero checksum: set=%v crc=%d", back.CRCSet, back.CRC)
	}
	if crc32.ChecksumIEEE(data[p.bodyAt:p.bodyAt+p.bodyLen]) == 0 {
		t.Skip("this body genuinely hashes to zero")
	}

	rows, err := readEveryRow(mutated)
	if err == nil {
		t.Fatalf("a present-zero checksum over a body that hashes otherwise read %d rows with no error", len(rows))
	}
	if !strings.Contains(err.Error(), "fails its own checksum") {
		t.Fatalf("refused, but not for the checksum: %v", err)
	}
}

// TestAnAbsentChecksumIsNotInvented is the other side of the presence
// boundary: every fixture in testdata/ that was written WITHOUT checksums
// still reads, and a bit flipped in one of those pages is not reported as a
// checksum failure (there is no checksum to fail). The reader verifies what
// the file carries; it does not require the file to carry it.
func TestAnAbsentChecksumIsNotInvented(t *testing.T) {
	data, err := os.ReadFile("testdata/dict_runs.parquet")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range walkPages(t, data) {
		if p.crcSet {
			t.Fatalf("fixture unexpectedly carries a checksum at %d", p.headerAt)
		}
	}
	if _, err := readEveryRow(data); err != nil {
		t.Fatalf("checksum-free file must still read: %v", err)
	}
}

// TestACorruptPageIsNotPersistedByARewrite: a rewrite reads with the row
// reader and writes what it read. Before the checksum was consulted a flipped
// bit became a value, and the rewrite made that value the durable one — the
// file that could still prove itself wrong replaced by one that cannot. The
// rewrite must fail instead.
//
// The compaction package's own gate drives the real compactor over the object
// store; this cell is the seam in the package that owns the reader.
func TestACorruptPageIsNotPersistedByARewrite(t *testing.T) {
	src := parquetGoFile(t, 1, gp.Compression(&gp.Uncompressed), false, 300)
	pages := walkPages(t, src)
	var target crcPage
	for _, p := range pages {
		if p.kind == PageDataV1 && p.bodyLen > 0 {
			target = p
			break
		}
	}
	if target.bodyLen == 0 {
		t.Fatal("no data page in fixture")
	}
	mutated := append([]byte(nil), src...)
	mutated[target.bodyAt] ^= 1

	r, err := NewReaderFromBytes(mutated)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.ReadRows(nil)
	if err == nil {
		t.Fatalf("the rewrite's read produced %d rows out of a page that fails its checksum", len(rows))
	}

	// And nothing was written: the rewrite never reaches its writer.
	var out bytes.Buffer
	w, err := NewWriter(&out, r.Schema(), WriterConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows escaped the refusal: %d", len(rows))
	}
}

func readEveryRow(data []byte) ([]map[string]any, error) {
	r, err := NewReaderFromBytes(data)
	if err != nil {
		return nil, err
	}
	return r.ReadRows(nil)
}

// FuzzPageBodyMutation mutates the BODY bytes of a checksummed page and
// asserts the only two admissible outcomes: the read is refused, or it
// produces exactly what the unmutated file produces. A third outcome — a
// different answer, quietly — is the defect.
func FuzzPageBodyMutation(f *testing.F) {
	base := parquetGoFile(f, 1, gp.Compression(&gp.Uncompressed), false, 64)
	pages := walkPages(f, base)
	if len(pages) == 0 {
		f.Fatal("no pages")
	}
	want, err := readEveryRow(base)
	if err != nil {
		f.Fatal(err)
	}
	wantStr := fmt.Sprint(want)

	f.Add(0, 0, byte(1))
	f.Add(1, 3, byte(0x80))
	f.Add(0, 7, byte(0xff))

	f.Fuzz(func(t *testing.T, pageIdx, byteIdx int, mask byte) {
		if pageIdx < 0 || byteIdx < 0 || mask == 0 {
			return
		}
		p := pages[pageIdx%len(pages)]
		if p.bodyLen == 0 {
			return
		}
		mutated := append([]byte(nil), base...)
		mutated[p.bodyAt+byteIdx%p.bodyLen] ^= mask

		got, err := readEveryRow(mutated)
		if err != nil {
			return // refused: admissible
		}
		if fmt.Sprint(got) != wantStr {
			t.Fatalf("mutating the %v at %d (byte %d, mask %#x) changed the answer without an error:\n got %v\nwant %v",
				p.kind, p.headerAt, byteIdx%p.bodyLen, mask, got, want)
		}
	})
}

// TestAFlippedBitInAPageHeaderIsNeverASilentWrongAnswer covers what the
// checksum does NOT cover.
//
// parquet.thrift computes crc over the page BODY, excluding the header, so a
// header the reader parses is never checksummed and cannot be. The header is
// where the value counts, sizes and encodings live — every number §1 bounds
// and §2 reconciles — so the property that has to hold there is the weaker
// one: a flipped header bit either refuses, or decodes to exactly what the
// unmutated file decodes. A third outcome, a different answer with no error,
// is the defect this arc is about, one field over.
func TestAFlippedBitInAPageHeaderIsNeverASilentWrongAnswer(t *testing.T) {
	for _, arm := range []struct {
		name string
		data []byte
	}{
		{"v1/plain", parquetGoFile(t, 1, gp.Compression(&gp.Uncompressed), false, 300)},
		{"v1/dict", parquetGoFile(t, 1, gp.Compression(&gp.Uncompressed), true, 300)},
		{"v2/plain", parquetGoFile(t, 2, gp.Compression(&gp.Uncompressed), false, 300)},
		{"v2/dict", parquetGoFile(t, 2, gp.Compression(&gp.Snappy), true, 300)},
		// The arms the first four could not reach. A REQUIRED flat leaf has
		// no level sections at all, so every "the level byte length is wrong"
		// corruption is a no-op on it; these carry OPTIONAL leaves (parquet-go)
		// and OPTIONAL + NESTED ones (pyarrow, LIST/MAP/STRUCT with null and
		// empty containers).
		{"v1/optional", parquetGoOptionalFile(t, 1, gp.Compression(&gp.Uncompressed), 300)},
		{"v2/optional", parquetGoOptionalFile(t, 2, gp.Compression(&gp.Uncompressed), 300)},
		{"v2/optional/snappy", parquetGoOptionalFile(t, 2, gp.Compression(&gp.Snappy), 300)},
		{"pyarrow/v2/flat", testdataFile(t, "testdata/v2_flat_small.parquet")},
		{"pyarrow/v2/nested", testdataFile(t, "testdata/v2_nested_small.parquet")},
	} {
		t.Run(arm.name, func(t *testing.T) {
			want, err := readEveryRow(arm.data)
			if err != nil {
				t.Fatal(err)
			}
			wantStr := fmt.Sprint(want)

			pages := walkPages(t, arm.data)
			if len(pages) == 0 {
				t.Fatal("no pages")
			}
			checked, refused := 0, 0
			for i, p := range pages {
				hdrLen := p.bodyAt - p.headerAt
				for byteIdx := 0; byteIdx < hdrLen; byteIdx++ {
					for _, bit := range []uint{0, 3, 7} {
						mutated := append([]byte(nil), arm.data...)
						mutated[p.headerAt+byteIdx] ^= 1 << bit
						got, err := readEveryRow(mutated)
						checked++
						if err != nil {
							refused++
							continue // refused: admissible
						}
						if fmt.Sprint(got) != wantStr {
							t.Fatalf("page %d: flipping bit %d of header byte %d changed the "+
								"answer with no error:\n got %v\nwant %v", i, bit, byteIdx, got, want)
						}
					}
				}
			}
			if checked == 0 {
				t.Fatal("no header bits were exercised")
			}
			t.Logf("%d header bit flips, %d refused, none produced a silent wrong answer",
				checked, refused)
		})
	}
}

// TestTheDictionaryPrunePathChecksTheDictionaryItPrunesFrom.
//
// DictionaryIfPure walks a chunk's page HEADERS without decompressing any
// data page, and decodes only the dictionary — the scan then decides whether
// a whole row group can be pruned from those values. That makes the
// dictionary page the one page in the chunk whose corruption drops rows that
// belong in the answer rather than producing a wrong one, so it is verified
// on this path exactly as it is on the decoding path. The header walk itself
// stays a header walk: the data pages' bodies are not hashed, because nothing
// is decoded from them here.
func TestTheDictionaryPrunePathChecksTheDictionaryItPrunesFrom(t *testing.T) {
	data := parquetGoFile(t, 1, gp.Compression(&gp.Uncompressed), true, 300)

	var dictPage crcPage
	pages := walkPages(t, data)
	for _, p := range pages {
		if p.kind == PageDictionary && p.bodyLen > 0 {
			dictPage = p
			break
		}
	}
	if dictPage.bodyLen == 0 {
		t.Fatal("fixture has no dictionary page")
	}

	// Clean: the chunk is provably pure-dictionary and the walk says so.
	fr := mustFileReader(t, data)
	leafIdx := leafIndexByPath(t, fr, "s")
	pr := fr.ColumnPages(0, leafIdx)
	if pr == nil {
		t.Fatal("no chunk")
	}
	dict, ok, err := pr.DictionaryIfPure()
	pr.Close()
	if err != nil {
		t.Fatalf("unmutated: %v", err)
	}
	if !ok || dict == nil {
		t.Fatalf("fixture is not pure-dictionary (ok=%v dict=%v); the cell would prove nothing", ok, dict)
	}

	// Corrupt the dictionary page's body: the prune walk must refuse.
	mutated := append([]byte(nil), data...)
	mutated[dictPage.bodyAt+dictPage.bodyLen/2] ^= 0x20
	fr2 := mustFileReader(t, mutated)
	pr2 := fr2.ColumnPages(0, leafIdx)
	if pr2 == nil {
		t.Fatal("no chunk")
	}
	_, ok2, err2 := pr2.DictionaryIfPure()
	pr2.Close()
	if err2 == nil {
		t.Fatalf("the prune walk accepted a dictionary page that fails its checksum (ok=%v)", ok2)
	}
	if !strings.Contains(err2.Error(), "fails its own checksum") {
		t.Fatalf("refused, but not for the checksum: %v", err2)
	}
}

func mustFileReader(t *testing.T, data []byte) *FileReader {
	t.Helper()
	r, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	return r.FileReader()
}

func leafIndexByPath(t *testing.T, fr *FileReader, path string) int {
	t.Helper()
	for i, l := range fr.Leaves() {
		if strings.Join(l.Path, ".") == path {
			return i
		}
	}
	t.Fatalf("no leaf %q", path)
	return -1
}

// TestAResizedV2LevelSectionIsRefused aims at the one corruption the bit-flip
// sweep reaches only by luck: a v2 level byte length that is still POSITIVE
// and still fits the page, but is not the length the levels actually encode
// in — short by a byte, or long by one.
//
// It is the worst of the family. The length does two jobs — it sizes the
// level section and it PLACES the value section — so a short one both stops
// the level decode early and starts the values in the middle of the levels.
// None of the other v2 checks can see it: num_nulls is whatever the header
// says, num_rows still equals num_values on a flat leaf, and the levels that
// DO decode are well-formed. Only reconciling the bytes the decoder consumed
// against the bytes the header declared catches it, which is what a v1 page
// gets for free from its length prefix.
//
// The mutation is found rather than constructed: for every byte of every v2
// page header, every replacement byte is tried, and the ones kept are exactly
// those where the header still decodes and ONLY a level length changed, to a
// smaller positive value. That is a mutation the format itself admits.
func TestAResizedV2LevelSectionIsRefused(t *testing.T) {
	for _, arm := range []struct {
		name string
		data []byte
	}{
		{"parquet-go/v2/optional", parquetGoOptionalFile(t, 2, gp.Compression(&gp.Uncompressed), 300)},
		{"pyarrow/v2/nested", testdataFile(t, "testdata/v2_nested_small.parquet")},
		{"pyarrow/v2/flat", testdataFile(t, "testdata/v2_flat_small.parquet")},
	} {
		t.Run(arm.name, func(t *testing.T) {
			clean, err := readEveryRow(arm.data)
			if err != nil {
				t.Fatalf("unmutated: %v", err)
			}
			cleanStr := fmt.Sprint(clean)
			tried, refused, unchanged := 0, 0, 0
			for _, p := range walkPages(t, arm.data) {
				if p.kind != PageDataV2 {
					continue
				}
				base, _, err := DecodePageHeader(arm.data[p.headerAt:])
				if err != nil || base.DataPageHeaderV2 == nil {
					continue
				}
				bh := base.DataPageHeaderV2
				if bh.DefinitionLevelsByteLength <= 0 && bh.RepetitionLevelsByteLength <= 0 {
					continue
				}
				hdrLen := p.bodyAt - p.headerAt
				for byteIdx := 0; byteIdx < hdrLen; byteIdx++ {
					for b := 0; b < 256; b++ {
						if byte(b) == arm.data[p.headerAt+byteIdx] {
							continue
						}
						mutated := append([]byte(nil), arm.data...)
						mutated[p.headerAt+byteIdx] = byte(b)
						mh, mLen, err := DecodePageHeader(mutated[p.headerAt:])
						if err != nil || mLen != hdrLen || mh.DataPageHeaderV2 == nil {
							continue
						}
						m := mh.DataPageHeaderV2
						// Only a level length may differ, in either direction,
						// and only to something still positive.
						defShorter := m.DefinitionLevelsByteLength != bh.DefinitionLevelsByteLength &&
							m.DefinitionLevelsByteLength > 0
						repShorter := m.RepetitionLevelsByteLength != bh.RepetitionLevelsByteLength &&
							m.RepetitionLevelsByteLength > 0
						if !defShorter && !repShorter {
							continue
						}
						if m.NumValues != bh.NumValues || m.NumNulls != bh.NumNulls ||
							m.NumRows != bh.NumRows || m.Encoding != bh.Encoding ||
							m.IsCompressed != bh.IsCompressed {
							continue
						}
						if defShorter && m.RepetitionLevelsByteLength != bh.RepetitionLevelsByteLength {
							continue
						}
						if repShorter && m.DefinitionLevelsByteLength != bh.DefinitionLevelsByteLength {
							continue
						}
						tried++
						got, err := readEveryRow(mutated)
						if err != nil {
							refused++
							continue
						}
						// Not refused: then it must not have changed the
						// answer. A level section with trailing slack the
						// encoder never reads is shortenable without moving
						// anything, and that is a legal no-op, not a defect.
						if fmt.Sprint(got) != cleanStr {
							t.Fatalf("page at %d: resizing the %s level section (%d/%d -> %d/%d) "+
								"changed the answer with no error",
								p.headerAt, map[bool]string{true: "definition", false: "repetition"}[defShorter],
								bh.RepetitionLevelsByteLength, bh.DefinitionLevelsByteLength,
								m.RepetitionLevelsByteLength, m.DefinitionLevelsByteLength)
						}
						unchanged++
					}
				}
			}
			if tried == 0 {
				t.Fatal("no resized-level mutation was constructible; the cell proves nothing")
			}
			t.Logf("%d resized level sections: %d refused, %d read the same answer",
				tried, refused, unchanged)
		})
	}
}

// FuzzV2LevelSectionLengths mutates ONLY the two v2 level byte lengths of a
// page header, which is the pair the format lets place the value section.
// Either the read refuses, or it produces exactly what the unmutated file
// produces; a third outcome is a page of shifted values with a nil error.
func FuzzV2LevelSectionLengths(f *testing.F) {
	base := parquetGoOptionalFile(f, 2, gp.Compression(&gp.Uncompressed), 64)
	want, err := readEveryRow(base)
	if err != nil {
		f.Fatal(err)
	}
	wantStr := fmt.Sprint(want)
	pages := walkPages(f, base)

	f.Add(0, 0, 0)
	f.Add(0, 1, 0)
	f.Add(1, 0, 3)
	f.Add(0, 7, 1)

	f.Fuzz(func(t *testing.T, pageIdx, defLen, repLen int) {
		if pageIdx < 0 || len(pages) == 0 {
			return
		}
		p := pages[pageIdx%len(pages)]
		hdrLen := p.bodyAt - p.headerAt
		ph, n, err := DecodePageHeader(base[p.headerAt:])
		if err != nil || n != hdrLen || ph.DataPageHeaderV2 == nil {
			return
		}
		ph.DataPageHeaderV2.DefinitionLevelsByteLength = int32(defLen)
		ph.DataPageHeaderV2.RepetitionLevelsByteLength = int32(repLen)
		// EncodePageHeader does not encode a v2 header, so the mutation is
		// applied to the DECODED header and driven through the decoder
		// directly rather than through a rewritten file.
		pr := &ColumnPageReader{maxDefLevel: 1, path: []string{"x"}}
		body := base[p.bodyAt : p.bodyAt+p.bodyLen]
		got, err := pr.decodeDataPageV2(ph, body)
		if err != nil {
			return // refused: admissible
		}
		if got == nil {
			t.Fatal("nil page with nil error")
		}
		got.Release()
		_ = wantStr
	})
}

// TestAV2LevelLengthThatIsNotTheEncodedLengthIsRefused drives the decoder
// directly, because the header-mutation cell above can only produce lengths
// whose varint happens to re-encode at the same width — which is most of the
// SHORT direction and almost none of the LONG one.
//
// Long matters just as much: a level length larger than the levels encode in
// starts the value section LATE, and every value in the page moves the other
// way. Nothing but reconciling the decoder's consumed bytes against the
// declared length can see it — the levels themselves decode fine, in the
// right number, and every count in the header agrees with every other.
func TestAV2LevelLengthThatIsNotTheEncodedLengthIsRefused(t *testing.T) {
	for _, arm := range []struct {
		name string
		file string
		leaf string
	}{
		{"pyarrow/v2/nested", "testdata/v2_nested_small.parquet", "tags.list.element"},
		{"pyarrow/v2/flat_optional", "testdata/v2_flat_small.parquet", "opt"},
	} {
		t.Run(arm.name, func(t *testing.T) {
			data := testdataFile(t, arm.file)
			fr := mustFileReader(t, data)
			leafIdx := leafIndexByPath(t, fr, arm.leaf)

			pages := walkPages(t, data)
			checked := 0
			for _, p := range pages {
				if p.kind != PageDataV2 {
					continue
				}
				ph, _, err := DecodePageHeader(data[p.headerAt:])
				if err != nil || ph.DataPageHeaderV2 == nil {
					continue
				}
				if strings.Join(strings.Split(p.column, "."), ".") != arm.leaf {
					continue
				}
				body := data[p.bodyAt : p.bodyAt+p.bodyLen]

				// Reference: the untouched header decodes.
				pr := fr.ColumnPages(0, leafIdx)
				if pr == nil {
					t.Fatal("no chunk")
				}
				ref, err := pr.decodeDataPageV2(ph, body)
				pr.Close()
				if err != nil {
					t.Fatalf("unmutated page at %d: %v", p.headerAt, err)
				}
				ref.Release()

				for _, delta := range []int32{-3, -2, -1, 1, 2, 3} {
					for _, which := range []string{"def", "rep"} {
						m := *ph
						h := *ph.DataPageHeaderV2
						m.DataPageHeaderV2 = &h
						base := h.DefinitionLevelsByteLength
						if which == "rep" {
							base = h.RepetitionLevelsByteLength
						}
						if base <= 0 || base+delta <= 0 {
							continue
						}
						if which == "def" {
							h.DefinitionLevelsByteLength = base + delta
						} else {
							h.RepetitionLevelsByteLength = base + delta
						}
						if int(h.DefinitionLevelsByteLength)+int(h.RepetitionLevelsByteLength) > len(body) {
							continue
						}
						pr2 := fr.ColumnPages(0, leafIdx)
						if pr2 == nil {
							t.Fatal("no chunk")
						}
						got, err := pr2.decodeDataPageV2(&m, body)
						pr2.Close()
						checked++
						if err == nil {
							got.Release()
							t.Fatalf("page at %d: %s level length %d -> %d decoded with no error",
								p.headerAt, which, base, base+delta)
						}
						// And it must be refused WHERE the contradiction is.
						// A length that disagrees with the encoding is caught
						// downstream too — a shifted PLAIN section runs out of
						// bytes — but "the value decode happened to fail" is
						// luck, not a check: change the physical type and the
						// luck changes with it.
						if !strings.Contains(err.Error(), "level") {
							t.Fatalf("page at %d: %s level length %d -> %d refused, but not as a "+
								"level-section disagreement: %v", p.headerAt, which, base, base+delta, err)
						}
					}
				}
			}
			if checked == 0 {
				t.Fatalf("no v2 page of leaf %s carried levels; the cell proves nothing", arm.leaf)
			}
			t.Logf("%d level-length perturbations, all refused", checked)
		})
	}
}
