package parquet

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	goparquet "github.com/parquet-go/parquet-go"
)

// TestReadFileMetaDataAgainstParquetGo is the critical correctness test:
// write a Parquet file with parquet-go, then read the footer with our
// custom Thrift decoder and verify the metadata matches exactly.
func TestReadFileMetaDataAgainstParquetGo(t *testing.T) {
	type Record struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
		Val  float64 `parquet:"val"`
	}

	// Write a small Parquet file to a buffer.
	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf)

	rows := []Record{
		{1, "alice", 1.1},
		{2, "bob", 2.2},
		{3, "charlie", 3.3},
		{4, "delta", 4.4},
		{5, "echo", 5.5},
	}
	n, err := writer.Write(rows)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(rows) {
		t.Fatalf("wrote %d rows, want %d", n, len(rows))
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()
	r := bytes.NewReader(data)

	// Read with our custom footer reader.
	md, err := ReadFileMetaData(r, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// Also read with parquet-go for reference comparison.
	pqFile, err := goparquet.OpenFile(r, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// Verify version.
	if md.Version != 1 && md.Version != 2 {
		t.Errorf("Version = %d, want 1 or 2", md.Version)
	}

	// Verify row count.
	if md.NumRows != int64(len(rows)) {
		t.Errorf("NumRows = %d, want %d", md.NumRows, len(rows))
	}
	if md.NumRows != pqFile.NumRows() {
		t.Errorf("NumRows mismatch: ours=%d, parquet-go=%d", md.NumRows, pqFile.NumRows())
	}

	// Verify row group count.
	pqRGs := pqFile.RowGroups()
	if len(md.RowGroups) != len(pqRGs) {
		t.Fatalf("RowGroup count: ours=%d, parquet-go=%d", len(md.RowGroups), len(pqRGs))
	}

	for i, rg := range md.RowGroups {
		pqRG := pqRGs[i]
		if rg.NumRows != pqRG.NumRows() {
			t.Errorf("RG[%d] NumRows: ours=%d, pq=%d", i, rg.NumRows, pqRG.NumRows())
		}

		// Verify column chunk count.
		pqChunks := pqRG.ColumnChunks()
		if len(rg.Columns) != len(pqChunks) {
			t.Errorf("RG[%d] column count: ours=%d, pq=%d", i, len(rg.Columns), len(pqChunks))
			continue
		}

		for j, cc := range rg.Columns {
			if cc.MetaData == nil {
				t.Errorf("RG[%d] Column[%d]: MetaData is nil", i, j)
				continue
			}
			cm := cc.MetaData
			if cm.NumValues <= 0 {
				t.Errorf("RG[%d] Column[%d]: NumValues = %d, want > 0", i, j, cm.NumValues)
			}
			if len(cm.PathInSchema) == 0 {
				t.Errorf("RG[%d] Column[%d]: PathInSchema empty", i, j)
			}
			if cm.TotalCompressedSize <= 0 {
				t.Errorf("RG[%d] Column[%d]: TotalCompressedSize = %d", i, j, cm.TotalCompressedSize)
			}
		}
	}

	// Verify schema structure.
	// parquet-go schema: root group + 3 leaf columns = 4 schema elements.
	pqSchema := pqFile.Schema()
	pqCols := pqSchema.Columns()
	leafCount := 0
	for _, se := range md.Schema {
		if se.Type != nil {
			leafCount++
		}
	}
	if leafCount != len(pqCols) {
		t.Errorf("leaf column count: ours=%d, parquet-go=%d", leafCount, len(pqCols))
	}

	// Verify column names and types.
	wantNames := []string{"id", "name", "val"}
	wantTypes := []PhysicalType{PhysicalInt64, PhysicalByteArray, PhysicalDouble}
	leafIdx := 0
	for _, se := range md.Schema {
		if se.Type == nil {
			continue // skip group nodes
		}
		if leafIdx < len(wantNames) {
			if se.Name != wantNames[leafIdx] {
				t.Errorf("leaf[%d] name = %q, want %q", leafIdx, se.Name, wantNames[leafIdx])
			}
			if *se.Type != wantTypes[leafIdx] {
				t.Errorf("leaf[%d] type = %v, want %v", leafIdx, *se.Type, wantTypes[leafIdx])
			}
		}
		leafIdx++
	}

	// Verify created_by is non-empty.
	if md.CreatedBy == "" {
		t.Error("CreatedBy should not be empty for parquet-go files")
	}
	t.Logf("CreatedBy: %s", md.CreatedBy)
	t.Logf("Version: %d, NumRows: %d, RowGroups: %d, Schema elements: %d",
		md.Version, md.NumRows, len(md.RowGroups), len(md.Schema))
}

// TestReadFileMetaDataMultiRowGroup verifies correct handling of files
// with multiple row groups and various column types.
func TestReadFileMetaDataMultiRowGroup(t *testing.T) {
	type Record struct {
		A int32  `parquet:"a"`
		B string `parquet:"b"`
	}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf,
		goparquet.MaxRowsPerRowGroup(3), // force multiple row groups
	)

	rows := []Record{
		{1, "one"}, {2, "two"}, {3, "three"},
		{4, "four"}, {5, "five"},
	}
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()
	r := bytes.NewReader(data)

	md, err := ReadFileMetaData(r, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	if md.NumRows != 5 {
		t.Errorf("NumRows = %d, want 5", md.NumRows)
	}
	if len(md.RowGroups) < 2 {
		t.Errorf("RowGroups = %d, want >= 2", len(md.RowGroups))
	}

	// Verify total rows across row groups sum to NumRows.
	var total int64
	for _, rg := range md.RowGroups {
		total += rg.NumRows
	}
	if total != md.NumRows {
		t.Errorf("sum of RG rows = %d, want %d", total, md.NumRows)
	}
}

// TestValidateHeader checks header magic validation.
func TestValidateHeader(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		data := append([]byte("PAR1"), make([]byte, 100)...)
		r := bytes.NewReader(data)
		if err := ValidateHeader(r); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		data := append([]byte("NOPE"), make([]byte, 100)...)
		r := bytes.NewReader(data)
		if err := ValidateHeader(r); err == nil {
			t.Error("expected error for invalid header")
		}
	})
}

// TestReadFileMetaDataTooSmall verifies rejection of tiny files.
func TestReadFileMetaDataTooSmall(t *testing.T) {
	r := bytes.NewReader([]byte("PAR1"))
	_, err := ReadFileMetaData(r, 4)
	if err == nil {
		t.Error("expected error for file too small")
	}
}

// eofGreedyReaderAt wraps a *bytes.Reader and returns io.EOF together with
// the requested data whenever the read reaches the end of the underlying
// data — exactly what minio-go's *minio.Object.ReadAt does for S3 objects
// when the read range touches the last byte. The io.ReaderAt contract
// allows this:
//
//	If ReadAt is reading from an input source with a known end, ReadAt
//	may return either err == EOF or err == nil after reading the final
//	bytes.
//
// Regression coverage for the SF100 schema-autodetect failure where this
// reader (via minio-go) caused ReadFileMetaData / ValidateHeader to fail
// with "parquet: reading trailer: EOF" even though every requested byte
// had been delivered.
type eofGreedyReaderAt struct {
	data []byte
}

func (e *eofGreedyReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(e.data)) {
		return 0, io.EOF
	}
	n := copy(p, e.data[off:])
	if int(off)+n >= len(e.data) {
		return n, io.EOF
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// TestReadFileMetaDataTolerantOfEOFGreedyReader is a regression test for
// the SF100 schema-autodetect failure: minio-go returns (n, io.EOF) when
// reading the parquet trailer at offset (size-8), and the previous
// ReadFileMetaData treated that as fatal even though the bytes had been
// successfully delivered.
func TestReadFileMetaDataTolerantOfEOFGreedyReader(t *testing.T) {
	// Build a minimal valid parquet file in-memory using the writer.
	var buf bytes.Buffer
	w, err := NewWriter(&buf, Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64, Nullable: true},
	}}, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows([]map[string]any{{"id": int64(1)}, {"id": int64(2)}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()

	// Sanity: a normal bytes.Reader works.
	if _, err := ReadFileMetaData(bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("baseline (bytes.Reader) failed: %v", err)
	}

	// Now exercise the EOF-greedy path.
	greedy := &eofGreedyReaderAt{data: data}
	md, err := ReadFileMetaData(greedy, int64(len(data)))
	if err != nil {
		t.Fatalf("EOF-greedy reader: %v (this is the SF100 bug)", err)
	}
	if md == nil {
		t.Fatal("EOF-greedy reader: nil metadata")
	}

	// ValidateHeader must also tolerate (n, EOF) — the header is at offset 0
	// but minio-go can still return EOF on small files where 4 bytes IS the
	// entire object's first read.
	if err := ValidateHeader(greedy); err != nil {
		t.Fatalf("ValidateHeader on EOF-greedy reader: %v", err)
	}
}

// TestReadFileMetaDataBadMagic verifies rejection of wrong trailing magic.
func TestReadFileMetaDataBadMagic(t *testing.T) {
	data := make([]byte, 20)
	copy(data[:4], "PAR1")
	binary.LittleEndian.PutUint32(data[len(data)-8:], 4)     // footer length
	copy(data[len(data)-4:], "NOPE")                          // wrong magic
	r := bytes.NewReader(data)
	_, err := ReadFileMetaData(r, int64(len(data)))
	if err == nil {
		t.Error("expected error for bad trailing magic")
	}
}

// TestValidateFileMetaDataBoundsRowCounts covers the three bounds the
// validator applies to a footer's row counts. Each of them is the last thing
// between a thrift varint and an allocation sized from it.
func TestValidateFileMetaDataBoundsRowCounts(t *testing.T) {
	// A two-row file, near enough to the real thing: 1 KiB.
	const fileSize = 1024
	rgs := func(counts ...int64) []RowGroup {
		out := make([]RowGroup, len(counts))
		for i, c := range counts {
			out[i] = RowGroup{NumRows: c}
		}
		return out
	}
	cases := []struct {
		name string
		md   FileMetaData
		size int64
		want string
	}{
		{"honest two-row file", FileMetaData{NumRows: 2, RowGroups: rgs(2)}, fileSize, ""},
		{"honest empty file", FileMetaData{NumRows: 0}, fileSize, ""},
		{"negative file total", FileMetaData{NumRows: -1}, fileSize, "footer declares -1 rows"},
		{"negative row group", FileMetaData{NumRows: 0, RowGroups: rgs(-4)}, fileSize,
			"row group 0 declares -4 rows"},
		// The reviewer's two: the file says it holds 2^40 (or 2^30) rows and
		// the row group still says two. batch.NewRecordBatch(schema, 1<<40)
		// was 128 GiB of bitmap and "fatal error: runtime: out of memory".
		{"file total of 2^40 over a two-row group",
			FileMetaData{NumRows: 1 << 40, RowGroups: rgs(2)}, fileSize, "a 1024-byte file can carry"},
		{"file total of 2^30 over a two-row group",
			FileMetaData{NumRows: 1 << 30, RowGroups: rgs(2)}, fileSize, "a 1024-byte file can carry"},
		// And with the row group inflated to match, so the sum agrees.
		{"both inflated to 2^40",
			FileMetaData{NumRows: 1 << 40, RowGroups: rgs(1 << 40)}, fileSize, "a 1024-byte file can carry"},
		{"both inflated to 2^30",
			FileMetaData{NumRows: 1 << 30, RowGroups: rgs(1 << 30)}, fileSize, "a 1024-byte file can carry"},
		// Past the per-row-group ceiling even for a file large enough to
		// carry that many rows by bytes.
		{"row group past MaxRowsPerRowGroup",
			FileMetaData{NumRows: MaxRowsPerRowGroup + 1, RowGroups: rgs(MaxRowsPerRowGroup + 1)},
			1 << 30, "past the 67108864 one row group may hold"},
		{"MaxRowsPerRowGroup exactly, with the bytes to back it",
			FileMetaData{NumRows: MaxRowsPerRowGroup, RowGroups: rgs(MaxRowsPerRowGroup)}, 1 << 30, ""},
		// The exact check: one flipped varint anywhere shows up as a sum
		// that no longer adds.
		{"row groups do not sum to the file total",
			FileMetaData{NumRows: 6, RowGroups: rgs(2, 2)}, fileSize, "row groups sum to 4"},
		{"rows with no row groups at all",
			FileMetaData{NumRows: 3}, fileSize, "row groups sum to 0"},
		{"several honest row groups", FileMetaData{NumRows: 9, RowGroups: rgs(2, 3, 4)}, fileSize, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			md := tc.md
			err := ValidateFileMetaData(&md, tc.size)
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("refused an honest footer: %v", err)
			case tc.want != "" && err == nil:
				t.Fatal("accepted")
			case tc.want != "" && !bytes.Contains([]byte(err.Error()), []byte(tc.want)):
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
	if err := ValidateFileMetaData(nil, fileSize); err == nil {
		t.Error("a nil footer was accepted")
	}
}

// TestRowCeilingHasHeadroomOverRealWriters pins the constant against what was
// measured rather than against itself: the densest files pyarrow will produce
// (one all-null or one constant column, zstd, dictionary on, maximal pages)
// ran 210-464 rows per byte at 1e6, 1e7 and 1e8 rows.
func TestRowCeilingHasHeadroomOverRealWriters(t *testing.T) {
	const densestObserved = 464
	if got := rowCeiling(1); got <= densestObserved {
		t.Errorf("rowCeiling admits %d rows per byte; the densest real writer output was %d",
			got, densestObserved)
	}
	if got := rowCeiling(0); got != 0 {
		t.Errorf("rowCeiling(0) = %d, want 0", got)
	}
	if got := rowCeiling(-1); got != 0 {
		t.Errorf("rowCeiling(-1) = %d, want 0", got)
	}
	// And it does not overflow into a negative for a nonsense size.
	if got := rowCeiling(1<<62 + 1); got <= 0 {
		t.Errorf("rowCeiling(2^62) = %d", got)
	}
}

func TestCheckRowGroupRowCount(t *testing.T) {
	if err := CheckRowGroupRowCount(0, 0); err != nil {
		t.Errorf("zero rows: %v", err)
	}
	if err := CheckRowGroupRowCount(0, MaxRowsPerRowGroup); err != nil {
		t.Errorf("the ceiling itself: %v", err)
	}
	if err := CheckRowGroupRowCount(3, -1); err == nil {
		t.Error("a negative row count was accepted")
	}
	if err := CheckRowGroupRowCount(3, MaxRowsPerRowGroup+1); err == nil {
		t.Error("a row count past the ceiling was accepted")
	}
}

// TestRealWritersDoNotOverstateChunkSizes is the measurement ValidateChunkLayout
// rests on, kept as a test so the "writers round it up" belief cannot come
// back without evidence. Every column chunk in a real file ends exactly where
// the next one begins.
//
// The corpus here is what CI has: the pyarrow fixtures in testdata (written by
// the gen_*.py scripts next to them), parquet-go, and wadjet's own writer. The
// same check over 44 files spanning four codecs, format 1.0 and 2.6, and the
// page index found worst gap zero and worst overlap zero.
func TestRealWritersDoNotOverstateChunkSizes(t *testing.T) {
	files := map[string][]byte{}
	paths, err := filepath.Glob(filepath.Join("testdata", "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		files[p] = raw
	}

	// wadjet's own writer, several row groups and a mix of widths.
	var wbuf bytes.Buffer
	cfg := DefaultWriterConfig()
	cfg.RowGroupSize = 37
	w, err := NewWriter(&wbuf, Schema{Columns: []Column{
		{Name: "i", Type: TypeInt64},
		{Name: "s", Type: TypeString, Nullable: true},
		{Name: "f", Type: TypeFloat64, Nullable: true},
	}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 200)
	for i := range rows {
		rows[i] = map[string]any{"i": int64(i), "s": strings.Repeat("x", i%17), "f": float64(i) / 3}
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	files["wadjet-writer"] = wbuf.Bytes()

	// parquet-go, which lays out dictionary pages ahead of data pages.
	type rec struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name,dict"`
	}
	var gbuf bytes.Buffer
	gw := goparquet.NewGenericWriter[rec](&gbuf, goparquet.MaxRowsPerRowGroup(64))
	grows := make([]rec, 200)
	for i := range grows {
		grows[i] = rec{int64(i), string(rune('a' + i%26))}
	}
	if _, err := gw.Write(grows); err != nil {
		t.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}
	files["parquet-go"] = gbuf.Bytes()

	for name, raw := range files {
		t.Run(name, func(t *testing.T) {
			md, err := ReadFileMetaData(bytes.NewReader(raw), int64(len(raw)))
			if err != nil {
				t.Fatalf("reading the footer: %v", err)
			}
			type span struct{ start, end int64 }
			var spans []span
			for i := range md.RowGroups {
				for j := range md.RowGroups[i].Columns {
					cm := md.RowGroups[i].Columns[j].MetaData
					if cm == nil || cm.TotalCompressedSize <= 0 {
						continue
					}
					s := cm.DataPageOffset
					if cm.DictionaryPageOffset > 0 && cm.DictionaryPageOffset < s {
						s = cm.DictionaryPageOffset
					}
					spans = append(spans, span{s, s + cm.TotalCompressedSize})
				}
			}
			if len(spans) == 0 {
				t.Skip("no column chunks")
			}
			sort.Slice(spans, func(a, b int) bool { return spans[a].start < spans[b].start })
			for k := 0; k+1 < len(spans); k++ {
				if gap := spans[k+1].start - spans[k].end; gap != 0 {
					t.Errorf("chunk ending at %d is %d bytes from the next, which starts at %d",
						spans[k].end, gap, spans[k+1].start)
				}
			}
			footerLen := int64(binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]))
			dataEnd := int64(len(raw)) - trailerSize - footerLen
			if last := spans[len(spans)-1].end; last > dataEnd {
				t.Errorf("the last chunk ends at %d, past the %d bytes before the footer", last, dataEnd)
			}
		})
	}
}

// TestChunkLayoutAcceptsAFooterListedOutOfOrder covers the fallback. The
// ordered walk is the common case and allocates nothing; a footer whose
// chunks are not listed in file order re-walks with a sort, and the answer
// must be the same either way.
func TestChunkLayoutAcceptsAFooterListedOutOfOrder(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, Schema{Columns: []Column{
		{Name: "a", Type: TypeInt64},
		{Name: "b", Type: TypeInt64},
	}}, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 64)
	for i := range rows {
		rows[i] = map[string]any{"a": int64(i), "b": int64(i * 2)}
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()

	decode := func(raw []byte) (*FileMetaData, int64) {
		t.Helper()
		footerLen := int64(binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]))
		start := int64(len(raw)) - trailerSize - footerLen
		md, err := DecodeFileMetaData(raw[start : start+footerLen])
		if err != nil {
			t.Fatalf("decoding footer: %v", err)
		}
		return md, start
	}

	md, dataEnd := decode(raw)
	if len(md.RowGroups[0].Columns) != 2 {
		t.Fatalf("fixture has %d columns", len(md.RowGroups[0].Columns))
	}
	// In order: accepted, and by the walk that allocates nothing.
	if ok, err := validateChunkLayoutInOrder(md, dataEnd); !ok || err != nil {
		t.Fatalf("the in-order walk answered (%v, %v)", ok, err)
	}

	// Listed backwards: the ordered walk gives up, the sorted one accepts.
	md.RowGroups[0].Columns[0], md.RowGroups[0].Columns[1] =
		md.RowGroups[0].Columns[1], md.RowGroups[0].Columns[0]
	if ok, err := validateChunkLayoutInOrder(md, dataEnd); ok || err != nil {
		t.Errorf("the in-order walk answered (%v, %v) for a backwards footer", ok, err)
	}
	if err := ValidateChunkLayout(md, dataEnd); err != nil {
		t.Errorf("a backwards-listed footer was refused: %v", err)
	}

	// Backwards AND overlapping: still refused.
	md.RowGroups[0].Columns[1].MetaData.TotalCompressedSize += 32
	if err := ValidateChunkLayout(md, dataEnd); err == nil {
		t.Error("a backwards-listed footer with overlapping chunks was accepted")
	}
}
