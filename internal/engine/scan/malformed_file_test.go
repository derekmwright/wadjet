package scan

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A parquet file's footer and page headers are claims about bytes the file
// does not have to keep consistent with them. Two of those claims used to be
// believed without checking:
//
//   - the LOGICAL annotation on a leaf, against that leaf's own PHYSICAL
//     type. An INT64 leaf annotated LogicalInteger{BitWidth: 32} is
//     recovered as an INT32 column, and the native scan then asked
//     Values.Int32() for 1000 int32s out of a page of int64s. The accessor
//     answers nil for a physical-type mismatch, and the copy did src[:n] on
//     it: a slice-bounds panic inside the scan errgroup, which in a worker
//     is the worker. The row path took the same file the other way — every
//     unpack loop is bounded by len(src), so nil ran zero iterations and
//     1000 rows came back as 1000 NULLs with err == nil.
//   - a page header's value COUNT, against the page body's length. Every
//     PLAIN decoder sliced data[:n*width] before anything could check it.
//
// Neither test needs a recover harness: the native panic was raised in an
// errgroup goroutine, which no `recover` in this package would have caught —
// the test binary died with it.

// annotatedInt64File writes a real 1000-row INT64 column and then re-encodes
// the footer so the leaf claims a 32-bit INTEGER logical type. Everything
// else in the file, page bodies included, is untouched and valid.
func annotatedInt64File(t *testing.T) []byte {
	t.Helper()
	schema := pqt.Schema{Columns: []pqt.Column{{Name: "c", Type: pqt.TypeInt64}}}
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, schema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	rows := make([]map[string]any, 1000)
	for i := range rows {
		rows[i] = map[string]any{"c": int64(i)}
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	raw := buf.Bytes()

	footerLen := int(binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]))
	start := len(raw) - 8 - footerLen
	md, err := pqt.DecodeFileMetaData(raw[start : start+footerLen])
	if err != nil {
		t.Fatalf("decoding footer: %v", err)
	}
	found := false
	for i := range md.Schema {
		if md.Schema[i].Name == "c" {
			md.Schema[i].LogicalType = &pqt.LogicalType{Type: pqt.LogicalInteger, BitWidth: 32, IsSigned: true}
			found = true
		}
	}
	if !found {
		t.Fatal("column c not in the footer schema")
	}
	return withFooter(raw[:start], pqt.EncodeFileMetaData(md))
}

// inflatedNumValuesFile hand-builds a one-column parquet file whose single
// PLAIN data page carries 1000 int64s and whose page header says it carries
// `declared`. Hand-built rather than written-then-patched: the value count is
// a varint, so patching it in place either changes every offset after it or
// constrains the test to counts that happen to encode in the same width.
func inflatedNumValuesFile(t *testing.T, declared int32) []byte {
	t.Helper()
	const rows = 1000
	body := make([]byte, rows*8)
	for i := 0; i < rows; i++ {
		binary.LittleEndian.PutUint64(body[i*8:], uint64(i))
	}
	header := pqt.EncodePageHeader(&pqt.PageHeader{
		Type:                 pqt.PageDataV1,
		UncompressedPageSize: int32(len(body)),
		CompressedPageSize:   int32(len(body)),
		DataPageHeader: &pqt.DataPageHeader{
			NumValues:               declared,
			Encoding:                pqt.EncodingPlain,
			DefinitionLevelEncoding: pqt.EncodingRLE,
			RepetitionLevelEncoding: pqt.EncodingRLE,
		},
	})

	const dataStart = 4 // after the leading PAR1
	chunkBytes := int64(len(header) + len(body))
	int64Phys := pqt.PhysicalInt64
	md := &pqt.FileMetaData{
		Version: 1,
		Schema: []pqt.SchemaElement{
			{Name: "hand_built", NumChildren: 1},
			{Name: "c", Type: &int64Phys, RepetitionType: pqt.FieldRequired},
		},
		NumRows: rows,
		RowGroups: []pqt.RowGroup{{
			NumRows:       rows,
			TotalByteSize: chunkBytes,
			Columns: []pqt.ColumnChunk{{
				FileOffset: dataStart,
				MetaData: &pqt.ColumnMetaData{
					Type:                  pqt.PhysicalInt64,
					Encodings:             []pqt.Encoding{pqt.EncodingPlain},
					PathInSchema:          []string{"c"},
					Codec:                 pqt.CodecNone,
					NumValues:             rows,
					TotalUncompressedSize: chunkBytes,
					TotalCompressedSize:   chunkBytes,
					DataPageOffset:        dataStart,
				},
			}},
		}},
		CreatedBy: "wadjet malformed-page test",
	}

	out := append([]byte("PAR1"), header...)
	out = append(out, body...)
	return withFooter(out, pqt.EncodeFileMetaData(md))
}

func withFooter(prefix, footer []byte) []byte {
	out := make([]byte, 0, len(prefix)+len(footer)+8)
	out = append(out, prefix...)
	out = append(out, footer...)
	var l [4]byte
	binary.LittleEndian.PutUint32(l[:], uint32(len(footer)))
	out = append(out, l[:]...)
	return append(out, "PAR1"...)
}

// readBothPaths runs the same file through the native scan and the row
// reader with the same column type, and reports what each answered.
func readBothPaths(t *testing.T, raw []byte, col pqt.Column) (nativeErr, rowErr error, rowsRead, nonNull int) {
	t.Helper()
	fr, err := pqt.OpenFileReaderFromBytes(raw)
	if err != nil {
		t.Fatalf("opening the file: %v", err)
	}
	_, nativeErr = ReadRowGroupNative(fr, 0, []pqt.Column{col}, nil)

	r, err := pqt.NewReaderFromBytes(raw)
	if err != nil {
		t.Fatalf("opening the file: %v", err)
	}
	rows, rowErr := r.ReadRowsAs([]pqt.Column{col}, nil)
	rowsRead = len(rows)
	for _, m := range rows {
		if _, ok := m[col.Name]; ok {
			nonNull++
		}
	}
	return nativeErr, rowErr, rowsRead, nonNull
}

func TestPhysicalTypeMismatchInTheFooterIsAnError(t *testing.T) {
	raw := annotatedInt64File(t)

	// Precondition: the footer really does now recover this INT64 leaf as an
	// INT32 column. Without that the test proves nothing.
	fr, err := pqt.OpenFileReaderFromBytes(raw)
	if err != nil {
		t.Fatalf("opening the file: %v", err)
	}
	leaf := fr.Leaves()[0]
	if leaf.Type == nil || *leaf.Type != pqt.PhysicalInt64 {
		t.Fatalf("leaf physical type = %v, want INT64", leaf.Type)
	}
	if got := pqt.TypeIDFromSchemaNode(leaf); got != pqt.TypeInt32 {
		t.Fatalf("recovered type = %s, want INT32 (the annotation did not take)", got)
	}

	nativeErr, rowErr, rowsRead, nonNull := readBothPaths(t, raw,
		pqt.Column{Name: "c", Type: pqt.TypeInt32})

	if nativeErr == nil {
		t.Error("the native scan read an INT64 page as INT32 without error")
	} else if !strings.Contains(nativeErr.Error(), "INT64") || !strings.Contains(nativeErr.Error(), "INT32") {
		t.Errorf("native error %q names neither the expected nor the actual physical type", nativeErr)
	}
	if rowErr == nil {
		t.Errorf("the row reader read an INT64 page as INT32 without error: %d rows, %d non-NULL",
			rowsRead, nonNull)
	}
}

func TestInflatedPageValueCountIsAnError(t *testing.T) {
	// 1,048,576 declared over a 1000-value body: the reviewer's number.
	raw := inflatedNumValuesFile(t, 1<<20)

	nativeErr, rowErr, rowsRead, nonNull := readBothPaths(t, raw,
		pqt.Column{Name: "c", Type: pqt.TypeInt64})
	if nativeErr == nil {
		t.Error("the native scan accepted a page declaring 2^20 values over a 1000-value body")
	}
	if rowErr == nil {
		t.Errorf("the row reader accepted a page declaring 2^20 values over a 1000-value body: %d rows, %d non-NULL",
			rowsRead, nonNull)
	}

	// The same file with an honest count reads correctly, so the guard is
	// refusing the corruption and not the shape.
	good := inflatedNumValuesFile(t, 1000)
	fr, err := pqt.OpenFileReaderFromBytes(good)
	if err != nil {
		t.Fatalf("opening the file: %v", err)
	}
	b, err := ReadRowGroupNative(fr, 0, []pqt.Column{{Name: "c", Type: pqt.TypeInt64}}, nil)
	if err != nil {
		t.Fatalf("the hand-built file does not read at all: %v", err)
	}
	if b == nil || b.Len != 1000 {
		t.Fatalf("read %v rows, want 1000", b)
	}
	for i := 0; i < b.Len; i++ {
		if b.Columns[0].Int64Data[i] != int64(i) {
			t.Fatalf("row %d = %d, want %d", i, b.Columns[0].Int64Data[i], i)
		}
	}
}
