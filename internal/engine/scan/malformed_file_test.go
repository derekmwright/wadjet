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

// shortRowGroupFile hand-builds a one-column file whose single PLAIN page
// carries `pageValues` honest int64s — header and body agree — while the row
// group's own num_rows says `declaredRows`. Both numbers are the file's own
// claims and the format does not reconcile them.
func shortRowGroupFile(t *testing.T, pageValues, declaredRows int) []byte {
	t.Helper()
	body := make([]byte, pageValues*8)
	for i := 0; i < pageValues; i++ {
		binary.LittleEndian.PutUint64(body[i*8:], uint64(i))
	}
	header := pqt.EncodePageHeader(&pqt.PageHeader{
		Type:                 pqt.PageDataV1,
		UncompressedPageSize: int32(len(body)),
		CompressedPageSize:   int32(len(body)),
		DataPageHeader: &pqt.DataPageHeader{
			NumValues:               int32(pageValues),
			Encoding:                pqt.EncodingPlain,
			DefinitionLevelEncoding: pqt.EncodingRLE,
			RepetitionLevelEncoding: pqt.EncodingRLE,
		},
	})

	const dataStart = 4
	chunkBytes := int64(len(header) + len(body))
	int64Phys := pqt.PhysicalInt64
	md := &pqt.FileMetaData{
		Version: 1,
		Schema: []pqt.SchemaElement{
			{Name: "hand_built", NumChildren: 1},
			{Name: "c", Type: &int64Phys, RepetitionType: pqt.FieldRequired},
		},
		NumRows: int64(declaredRows),
		RowGroups: []pqt.RowGroup{{
			NumRows:       int64(declaredRows),
			TotalByteSize: chunkBytes,
			Columns: []pqt.ColumnChunk{{
				FileOffset: dataStart,
				MetaData: &pqt.ColumnMetaData{
					Type:                  pqt.PhysicalInt64,
					Encodings:             []pqt.Encoding{pqt.EncodingPlain},
					PathInSchema:          []string{"c"},
					Codec:                 pqt.CodecNone,
					NumValues:             int64(pageValues),
					TotalUncompressedSize: chunkBytes,
					TotalCompressedSize:   chunkBytes,
					DataPageOffset:        dataStart,
				},
			}},
		}},
		CreatedBy: "wadjet short-row-group test",
	}

	out := append([]byte("PAR1"), header...)
	out = append(out, body...)
	return withFooter(out, pqt.EncodeFileMetaData(md))
}

// TestPagesOverrunningTheRowGroupAreAnError: the destination vectors are
// sized from the ROW GROUP's num_rows, and the page loop advanced its write
// cursor by each page's own value count with nothing comparing the two. A
// file whose pages declare 600 values into a 300-row group copied into
// vec.Int64Data[0:600] over an array with capacity 300 — a slice-bounds panic
// inside the per-column errgroup, which in a worker process is the worker.
// This is the whole-file fuzz crasher shape, reduced to its cause.
func TestPagesOverrunningTheRowGroupAreAnError(t *testing.T) {
	raw := shortRowGroupFile(t, 600, 300)

	nativeErr, rowErr, rowsRead, nonNull := readBothPaths(t, raw,
		pqt.Column{Name: "c", Type: pqt.TypeInt64})
	if nativeErr == nil {
		t.Error("the native scan copied 600 page values into a 300-row group without error")
	} else {
		for _, want := range []string{"600", "300"} {
			if !strings.Contains(nativeErr.Error(), want) {
				t.Errorf("native error %q does not name %s", nativeErr, want)
			}
		}
	}
	// The row reader already refused this shape; it must keep doing so.
	if rowErr == nil {
		t.Errorf("the row reader accepted the overrun: %d rows, %d non-NULL", rowsRead, nonNull)
	}

	// The consistent file still reads, so the bound refuses the corruption
	// and not the shape.
	fr, err := pqt.OpenFileReaderFromBytes(shortRowGroupFile(t, 300, 300))
	if err != nil {
		t.Fatalf("opening the consistent file: %v", err)
	}
	b, err := ReadRowGroupNative(fr, 0, []pqt.Column{{Name: "c", Type: pqt.TypeInt64}}, nil)
	if err != nil {
		t.Fatalf("the consistent file does not read: %v", err)
	}
	if b == nil || b.Len != 300 {
		t.Fatalf("read %v, want 300 rows", b)
	}
}

// reannotatedInt64File writes a real 1000-row INT64 column and re-encodes the
// footer so the leaf carries `lt` instead of its own annotation. The page
// bodies are untouched and still hold int64s.
func reannotatedInt64File(t *testing.T, lt *pqt.LogicalType) []byte {
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
	for i := range md.Schema {
		if md.Schema[i].Name == "c" {
			md.Schema[i].LogicalType = lt
			md.Schema[i].ConvertedType = nil
		}
	}
	return withFooter(raw[:start], pqt.EncodeFileMetaData(md))
}

// TestByteArrayAccessorChecksThePagePhysicalType: Values.ByteArray was the
// one typed accessor with no physical-type check. An INT64 page answered it
// with its raw eight-byte-per-value buffer and a nil offsets slice — and a
// nil offsets slice is how the byte-array copy paths spell "PLAIN, four-byte
// length prefix per value". So a column of integers decoded into a STRING
// vector as whatever those bytes happened to say, with err == nil, while the
// ROW reader refused the same file. One corrupt annotation, two different
// answers, chosen by a schema shape the query never mentions.
func TestByteArrayAccessorChecksThePagePhysicalType(t *testing.T) {
	cases := []struct {
		name    string
		logical *pqt.LogicalType
		catalog pqt.TypeID
	}{
		{"LogicalString over INT64", &pqt.LogicalType{Type: pqt.LogicalString}, pqt.TypeString},
		{"LogicalVector over INT64", &pqt.LogicalType{Type: pqt.LogicalVector, Dimension: 2}, pqt.TypeVector},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := reannotatedInt64File(t, tc.logical)

			fr, err := pqt.OpenFileReaderFromBytes(raw)
			if err != nil {
				t.Fatalf("opening: %v", err)
			}
			leaf := fr.Leaves()[0]
			if leaf.Type == nil || *leaf.Type != pqt.PhysicalInt64 {
				t.Fatalf("leaf physical = %v, want INT64", leaf.Type)
			}
			if got := pqt.TypeIDFromSchemaNode(leaf); got != tc.catalog {
				t.Fatalf("recovered type = %s, want %s (the annotation did not take)", got, tc.catalog)
			}

			col := pqt.Column{Name: "c", Type: tc.catalog, Dimension: 2}
			nativeErr, rowErr, rowsRead, nonNull := readBothPaths(t, raw, col)
			if nativeErr == nil {
				t.Error("the native scan decoded an INT64 page through the byte-array path without error")
			}
			if rowErr == nil {
				t.Errorf("the row reader accepted it: %d rows, %d non-NULL", rowsRead, nonNull)
			}
		})
	}
}

// TestSelDecodeEligibilityAgreesWithTheFullDecode: the sel-pruned path picked
// its columns by the RECOVERED type alone, so a LogicalString annotation on
// an INT64 leaf made it eligible for a decoder that walks per-value byte
// offsets a page of integers does not have. Eligibility now asks the leaf's
// PHYSICAL type as well, which is the same question byteArraySrc asks of the
// page — the two must not disagree, or the sel path answers a query the full
// path refuses.
func TestSelDecodeEligibilityAgreesWithTheFullDecode(t *testing.T) {
	raw := reannotatedInt64File(t, &pqt.LogicalType{Type: pqt.LogicalString})
	fr, err := pqt.OpenFileReaderFromBytes(raw)
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	if selEligibleLeaf(fr, 0, pqt.TypeString) {
		t.Error("an INT64 leaf annotated STRING was admitted to the sel-decode path")
	}

	// A real BYTE_ARRAY column is still eligible, so the gate refuses the
	// annotation mismatch and not the path.
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, pqt.Schema{Columns: []pqt.Column{{Name: "s", Type: pqt.TypeString}}},
		pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows([]map[string]any{{"s": "a"}, {"s": "b"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	good, err := pqt.OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !selEligibleLeaf(good, 0, pqt.TypeString) {
		t.Error("a real BYTE_ARRAY STRING column is no longer sel-eligible")
	}
}
