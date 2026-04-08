package parquet

import (
	"bytes"
	"encoding/binary"
	"io"
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
