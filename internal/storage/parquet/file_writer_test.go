package parquet

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	goparquet "github.com/parquet-go/parquet-go"
)

// readAllPages reads all pages from a column via ColumnPageReader.
func readAllPages(t *testing.T, fr *FileReader, rg, col int) []*PageData {
	t.Helper()
	pr := fr.ColumnPages(rg, col)
	if pr == nil {
		t.Fatalf("ColumnPages(%d, %d) returned nil", rg, col)
	}
	var pages []*PageData
	for {
		page, err := pr.NextPage()
		if err != nil {
			t.Fatalf("NextPage: %v", err)
		}
		if page == nil {
			break
		}
		pages = append(pages, page)
	}
	return pages
}

// TestNativeWriterRoundTrip writes data with our NativeWriter and reads it back
// with both our custom reader and parquet-go to verify compatibility.
func TestNativeWriterRoundTrip(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "name", Type: TypeString, Nullable: true},
			{Name: "value", Type: TypeFloat64},
			{Name: "active", Type: TypeBool},
		},
	}

	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "value": 1.5, "active": true},
		{"id": int64(2), "name": nil, "value": 2.5, "active": false},
		{"id": int64(3), "name": "charlie", "value": 3.5, "active": true},
		{"id": int64(4), "name": "diana", "value": 4.5, "active": false},
		{"id": int64(5), "name": nil, "value": 5.5, "active": true},
	}

	var buf bytes.Buffer
	w := NewNativeWriter(&buf, schema, WriterConfig{
		RowGroupSize: 100,
		Compression:  CompressionSnappy,
	})
	if err := w.WriteMapRows(rows); err != nil {
		t.Fatalf("WriteMapRows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := buf.Bytes()
	t.Logf("wrote %d bytes", len(data))

	// Verify magic bytes.
	if string(data[:4]) != "PAR1" {
		t.Fatalf("missing header magic: got %q", data[:4])
	}
	if string(data[len(data)-4:]) != "PAR1" {
		t.Fatalf("missing trailer magic: got %q", data[len(data)-4:])
	}

	// Read back with our custom reader.
	fr, err := OpenFileReaderFromBytes(data)
	if err != nil {
		t.Fatalf("OpenFileReaderFromBytes: %v", err)
	}

	if fr.NumRowGroups() != 1 {
		t.Fatalf("expected 1 row group, got %d", fr.NumRowGroups())
	}
	if fr.NumRows() != 5 {
		t.Fatalf("expected 5 rows, got %d", fr.NumRows())
	}

	// Read id column.
	idPages := readAllPages(t, fr, 0, 0)
	if len(idPages) == 0 {
		t.Fatal("no pages for id column")
	}
	ids := idPages[0].Data.Int64()
	if len(ids) != 5 {
		t.Fatalf("expected 5 ids, got %d", len(ids))
	}
	for i, want := range []int64{1, 2, 3, 4, 5} {
		if ids[i] != want {
			t.Errorf("id[%d] = %d, want %d", i, ids[i], want)
		}
	}

	// Read string column and check nulls.
	namePages := readAllPages(t, fr, 0, 1)
	if len(namePages) == 0 {
		t.Fatal("no pages for name column")
	}
	names, offsets := namePages[0].Data.ByteArray()
	defLevels := namePages[0].DefinitionLevels
	if len(defLevels) != 5 {
		t.Fatalf("expected 5 def levels, got %d", len(defLevels))
	}
	wantDefs := []int32{1, 0, 1, 1, 0}
	for i, want := range wantDefs {
		if defLevels[i] != want {
			t.Errorf("defLevel[%d] = %d, want %d", i, defLevels[i], want)
		}
	}
	wantNames := []string{"alice", "charlie", "diana"}
	for i, want := range wantNames {
		got := string(names[offsets[i]:offsets[i+1]])
		if got != want {
			t.Errorf("name[%d] = %q, want %q", i, got, want)
		}
	}

	// Read with parquet-go.
	pqFile, err := goparquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("parquet-go OpenFile: %v", err)
	}
	if pqFile.NumRows() != 5 {
		t.Fatalf("parquet-go: expected 5 rows, got %d", pqFile.NumRows())
	}
	t.Logf("parquet-go schema: %s", pqFile.Schema())
}

// TestNativeWriterUncompressed tests writing without compression.
func TestNativeWriterUncompressed(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "x", Type: TypeInt32},
		},
	}

	rows := []map[string]any{
		{"x": int32(42)},
		{"x": int32(100)},
	}

	var buf bytes.Buffer
	w := NewNativeWriter(&buf, schema, WriterConfig{
		RowGroupSize: 100,
		Compression:  CompressionNone,
	})
	if err := w.WriteMapRows(rows); err != nil {
		t.Fatalf("WriteMapRows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fr, err := OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenFileReaderFromBytes: %v", err)
	}

	pages := readAllPages(t, fr, 0, 0)
	vals := pages[0].Data.Int32()
	if len(vals) != 2 || vals[0] != 42 || vals[1] != 100 {
		t.Fatalf("got %v, want [42 100]", vals)
	}
}

// TestNativeWriterZstd tests writing with Zstd compression.
func TestNativeWriterZstd(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "x", Type: TypeInt64},
		},
	}

	var rows []map[string]any
	for i := 0; i < 100; i++ {
		rows = append(rows, map[string]any{"x": int64(i)})
	}

	var buf bytes.Buffer
	w := NewNativeWriter(&buf, schema, WriterConfig{
		RowGroupSize: 1000,
		Compression:  CompressionZstd,
	})
	if err := w.WriteMapRows(rows); err != nil {
		t.Fatalf("WriteMapRows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fr, err := OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenFileReaderFromBytes: %v", err)
	}
	if fr.NumRows() != 100 {
		t.Fatalf("expected 100 rows, got %d", fr.NumRows())
	}
}

// TestNativeWriterMultiRowGroup tests automatic row group splitting.
func TestNativeWriterMultiRowGroup(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
		},
	}

	var rows []map[string]any
	for i := 0; i < 25; i++ {
		rows = append(rows, map[string]any{"id": int64(i)})
	}

	var buf bytes.Buffer
	w := NewNativeWriter(&buf, schema, WriterConfig{
		RowGroupSize: 10,
		Compression:  CompressionNone,
	})
	if err := w.WriteMapRows(rows); err != nil {
		t.Fatalf("WriteMapRows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fr, err := OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenFileReaderFromBytes: %v", err)
	}

	if fr.NumRowGroups() < 2 {
		t.Fatalf("expected multiple row groups, got %d", fr.NumRowGroups())
	}
	if fr.NumRows() != 25 {
		t.Fatalf("expected 25 total rows, got %d", fr.NumRows())
	}

	var allIDs []int64
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		pages := readAllPages(t, fr, rg, 0)
		allIDs = append(allIDs, pages[0].Data.Int64()...)
	}
	if len(allIDs) != 25 {
		t.Fatalf("expected 25 ids, got %d", len(allIDs))
	}
	for i, want := range allIDs {
		if want != int64(i) {
			t.Errorf("id[%d] = %d, want %d", i, want, i)
		}
	}
}

// TestNativeWriterAllTypes tests writing all commonly used types.
func TestNativeWriterAllTypes(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "b", Type: TypeBool},
			{Name: "i32", Type: TypeInt32},
			{Name: "i64", Type: TypeInt64},
			{Name: "f32", Type: TypeFloat32},
			{Name: "f64", Type: TypeFloat64},
			{Name: "s", Type: TypeString},
			{Name: "bin", Type: TypeBytes},
		},
	}

	rows := []map[string]any{
		{"b": true, "i32": int32(42), "i64": int64(100), "f32": float32(1.5), "f64": 2.5, "s": "hello", "bin": []byte{0xDE, 0xAD}},
		{"b": false, "i32": int32(-1), "i64": int64(-100), "f32": float32(-1.5), "f64": -2.5, "s": "world", "bin": []byte{0xBE, 0xEF}},
	}

	var buf bytes.Buffer
	w := NewNativeWriter(&buf, schema, WriterConfig{
		RowGroupSize: 100,
		Compression:  CompressionNone,
	})
	if err := w.WriteMapRows(rows); err != nil {
		t.Fatalf("WriteMapRows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fr, err := OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenFileReaderFromBytes: %v", err)
	}

	// Boolean column.
	pages := readAllPages(t, fr, 0, 0)
	boolBits := pages[0].Data.Boolean()
	if len(boolBits) < 1 || (boolBits[0]&1) != 1 || (boolBits[0]&2) != 0 {
		t.Errorf("bools unexpected: %v", boolBits)
	}

	// Int32 column.
	pages = readAllPages(t, fr, 0, 1)
	i32s := pages[0].Data.Int32()
	if len(i32s) != 2 || i32s[0] != 42 || i32s[1] != -1 {
		t.Errorf("int32s = %v, want [42 -1]", i32s)
	}

	// Float64 column.
	pages = readAllPages(t, fr, 0, 4)
	f64s := pages[0].Data.Double()
	if len(f64s) != 2 || f64s[0] != 2.5 || f64s[1] != -2.5 {
		t.Errorf("float64s = %v, want [2.5 -2.5]", f64s)
	}

	// String column.
	pages = readAllPages(t, fr, 0, 5)
	sData, sOff := pages[0].Data.ByteArray()
	if len(sOff) != 3 {
		t.Fatalf("expected 3 offsets, got %d", len(sOff))
	}
	s0 := string(sData[sOff[0]:sOff[1]])
	s1 := string(sData[sOff[1]:sOff[2]])
	if s0 != "hello" || s1 != "world" {
		t.Errorf("strings = [%q %q], want [hello world]", s0, s1)
	}

	// Verify parquet-go can also read it.
	pqFile, err := goparquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("parquet-go OpenFile: %v", err)
	}
	if pqFile.NumRows() != 2 {
		t.Fatalf("parquet-go: expected 2 rows, got %d", pqFile.NumRows())
	}
}

// TestNativeWriterStatistics verifies column statistics in the footer.
func TestNativeWriterStatistics(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "x", Type: TypeInt64},
		},
	}

	rows := []map[string]any{
		{"x": int64(10)},
		{"x": int64(5)},
		{"x": int64(20)},
		{"x": int64(-3)},
	}

	var buf bytes.Buffer
	w := NewNativeWriter(&buf, schema, WriterConfig{
		RowGroupSize: 100,
		Compression:  CompressionNone,
	})
	if err := w.WriteMapRows(rows); err != nil {
		t.Fatalf("WriteMapRows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fr, err := OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenFileReaderFromBytes: %v", err)
	}

	stats := fr.RowGroupStats(0)
	if stats.NumRows != 4 {
		t.Fatalf("stats.NumRows = %d, want 4", stats.NumRows)
	}
	// Check column stats from metadata directly.
	rg := fr.RowGroupMeta(0)
	if rg == nil || len(rg.Columns) == 0 || rg.Columns[0].MetaData == nil {
		t.Fatal("no column metadata")
	}
	s := rg.Columns[0].MetaData.Statistics
	if s == nil {
		t.Fatal("no statistics")
	}
	if len(s.MinValue) != 8 || len(s.MaxValue) != 8 {
		t.Fatalf("unexpected stat sizes: min=%d max=%d", len(s.MinValue), len(s.MaxValue))
	}
	minVal := int64(binary.LittleEndian.Uint64(s.MinValue))
	maxVal := int64(binary.LittleEndian.Uint64(s.MaxValue))
	if minVal != -3 || maxVal != 20 {
		t.Errorf("stats: min=%d max=%d, want min=-3 max=20", minVal, maxVal)
	}
}

// TestNativeWriterAllNulls tests a column that is entirely null.
func TestNativeWriterAllNulls(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "x", Type: TypeString, Nullable: true},
		},
	}

	rows := []map[string]any{
		{"x": nil},
		{"x": nil},
		{"x": nil},
	}

	var buf bytes.Buffer
	w := NewNativeWriter(&buf, schema, WriterConfig{
		RowGroupSize: 100,
		Compression:  CompressionNone,
	})
	if err := w.WriteMapRows(rows); err != nil {
		t.Fatalf("WriteMapRows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fr, err := OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenFileReaderFromBytes: %v", err)
	}

	pages := readAllPages(t, fr, 0, 0)
	if len(pages) == 0 {
		t.Fatal("no pages")
	}
	defs := pages[0].DefinitionLevels
	if len(defs) != 3 {
		t.Fatalf("expected 3 def levels, got %d", len(defs))
	}
	for i, d := range defs {
		if d != 0 {
			t.Errorf("defLevel[%d] = %d, want 0", i, d)
		}
	}
}

// TestNativeWriterEmptyFile tests writing a file with no rows.
func TestNativeWriterEmptyFile(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "x", Type: TypeInt64},
		},
	}

	var buf bytes.Buffer
	w := NewNativeWriter(&buf, schema, WriterConfig{
		RowGroupSize: 100,
		Compression:  CompressionNone,
	})
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fr, err := OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenFileReaderFromBytes: %v", err)
	}
	if fr.NumRows() != 0 {
		t.Fatalf("expected 0 rows, got %d", fr.NumRows())
	}
}

// TestThriftEncoderRoundTrip encodes FileMetaData and decodes it back to verify correctness.
func TestThriftEncoderRoundTrip(t *testing.T) {
	pt := PhysicalInt64
	ct := ConvertedInt64
	md := &FileMetaData{
		Version: 1,
		Schema: []SchemaElement{
			{Name: "root", NumChildren: 2},
			{Type: &pt, Name: "col_a", RepetitionType: FieldRequired, ConvertedType: &ct},
			{Type: &pt, Name: "col_b", RepetitionType: FieldOptional, ConvertedType: &ct},
		},
		NumRows: 1000,
		RowGroups: []RowGroup{
			{
				Columns: []ColumnChunk{
					{
						FileOffset: 4,
						MetaData: &ColumnMetaData{
							Type:                  PhysicalInt64,
							Encodings:             []Encoding{EncodingPlain, EncodingRLE},
							PathInSchema:          []string{"col_a"},
							Codec:                 CodecSnappy,
							NumValues:             1000,
							TotalUncompressedSize: 8000,
							TotalCompressedSize:   4000,
							DataPageOffset:        4,
							Statistics: &Statistics{
								NullCount: 0,
								MinValue:  []byte{1, 0, 0, 0, 0, 0, 0, 0},
								MaxValue:  []byte{0xE8, 0x03, 0, 0, 0, 0, 0, 0},
							},
						},
					},
				},
				TotalByteSize: 8000,
				NumRows:       1000,
			},
		},
		CreatedBy: "wadjet-test",
		KeyValueMetadata: []KeyValue{
			{Key: "test.key", Value: "test.value"},
		},
	}

	encoded := EncodeFileMetaData(md)
	t.Logf("encoded %d bytes", len(encoded))

	decoded, err := DecodeFileMetaData(encoded)
	if err != nil {
		t.Fatalf("DecodeFileMetaData: %v", err)
	}

	if decoded.Version != md.Version {
		t.Errorf("version = %d, want %d", decoded.Version, md.Version)
	}
	if decoded.NumRows != md.NumRows {
		t.Errorf("num_rows = %d, want %d", decoded.NumRows, md.NumRows)
	}
	if decoded.CreatedBy != md.CreatedBy {
		t.Errorf("created_by = %q, want %q", decoded.CreatedBy, md.CreatedBy)
	}
	if len(decoded.Schema) != len(md.Schema) {
		t.Fatalf("schema len = %d, want %d", len(decoded.Schema), len(md.Schema))
	}
	for i, s := range decoded.Schema {
		if s.Name != md.Schema[i].Name {
			t.Errorf("schema[%d].Name = %q, want %q", i, s.Name, md.Schema[i].Name)
		}
	}
	if len(decoded.RowGroups) != 1 {
		t.Fatalf("row_groups len = %d, want 1", len(decoded.RowGroups))
	}
	rg := decoded.RowGroups[0]
	if rg.NumRows != 1000 {
		t.Errorf("rg.num_rows = %d, want 1000", rg.NumRows)
	}
	cm := rg.Columns[0].MetaData
	if cm == nil {
		t.Fatal("column metadata is nil")
	}
	if cm.Type != PhysicalInt64 {
		t.Errorf("column type = %v, want INT64", cm.Type)
	}
	if cm.Codec != CodecSnappy {
		t.Errorf("codec = %v, want SNAPPY", cm.Codec)
	}
	if cm.Statistics == nil || cm.Statistics.NullCount != 0 {
		t.Errorf("stats unexpected: %+v", cm.Statistics)
	}
	if len(decoded.KeyValueMetadata) != 1 || decoded.KeyValueMetadata[0].Key != "test.key" {
		t.Errorf("kv_metadata unexpected: %v", decoded.KeyValueMetadata)
	}
}

// TestEncodeDefLevelsRLE tests RLE encoding of definition levels.
func TestEncodeDefLevelsRLE(t *testing.T) {
	tests := []struct {
		name     string
		nulls    []bool
		count    int
		wantDefs []int32
	}{
		{"all non-null", []bool{false, false, false, false}, 4, []int32{1, 1, 1, 1}},
		{"all null", []bool{true, true, true}, 3, []int32{0, 0, 0}},
		{"mixed", []bool{false, true, false, false, true}, 5, []int32{1, 0, 1, 1, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeDefLevelsRLE(tt.nulls, tt.count)
			decoded, err := DecodeRLEInt32(encoded, 1, tt.count)
			if err != nil {
				t.Fatalf("DecodeRLEInt32: %v", err)
			}
			if len(decoded) != len(tt.wantDefs) {
				t.Fatalf("decoded len = %d, want %d", len(decoded), len(tt.wantDefs))
			}
			for i, want := range tt.wantDefs {
				if decoded[i] != want {
					t.Errorf("decoded[%d] = %d, want %d", i, decoded[i], want)
				}
			}
		})
	}
}

// TestNativeWriterFloat32 tests float32 round-trip.
func TestNativeWriterFloat32(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "x", Type: TypeFloat32},
		},
	}

	rows := []map[string]any{
		{"x": float32(math.Pi)},
		{"x": float32(-1.0)},
		{"x": float32(0)},
	}

	var buf bytes.Buffer
	w := NewNativeWriter(&buf, schema, WriterConfig{
		RowGroupSize: 100,
		Compression:  CompressionNone,
	})
	if err := w.WriteMapRows(rows); err != nil {
		t.Fatalf("WriteMapRows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fr, err := OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenFileReaderFromBytes: %v", err)
	}

	pages := readAllPages(t, fr, 0, 0)
	f32s := pages[0].Data.Float()
	if len(f32s) != 3 {
		t.Fatalf("expected 3 values, got %d", len(f32s))
	}
	if math.Abs(float64(f32s[0]-float32(math.Pi))) > 1e-6 {
		t.Errorf("f32[0] = %v, want ~pi", f32s[0])
	}
	if f32s[1] != -1.0 {
		t.Errorf("f32[1] = %v, want -1.0", f32s[1])
	}
}

// TestNewWriterRefusesAStructurallyImpossibleSchema: the footer's schema tree
// and the leaf buffers are built by two separate walks, and for a malformed
// MAP they agreed only by both doing nothing. buildMapSchemaElements writes
// the "key_value" group declaring NumChildren = 2 and then emits the key and
// value ONLY when ElementType is a two-field ROW — so a MAP built the
// natural-looking way, with Fields holding key and value and ElementType nil,
// produced a footer whose group promises two children and has none.
// BuildSchemaTree reads that back as a group borrowing the next two
// TOP-LEVEL columns, so it is not one missing map: every column after it is
// misplaced. Nothing complained, and Close returned nil.
func TestNewWriterRefusesAStructurallyImpossibleSchema(t *testing.T) {
	cases := []struct {
		name   string
		column Column
		want   string
	}{
		{"MAP with Fields instead of ElementType", Column{
			Name: "m", Type: TypeMap, Fields: []Column{
				{Name: "key", Type: TypeString}, {Name: "value", Type: TypeInt64},
			},
		}, "MAP needs ElementType"},
		{"MAP with no element at all", Column{Name: "m", Type: TypeMap}, "MAP needs ElementType"},
		{"MAP whose element is not a ROW", Column{
			Name: "m", Type: TypeMap, ElementType: &Column{Name: "kv", Type: TypeString},
		}, "MAP needs ElementType"},
		{"MAP whose element ROW has one field", Column{
			Name: "m", Type: TypeMap, ElementType: &Column{Name: "kv", Type: TypeRow,
				Fields: []Column{{Name: "key", Type: TypeString}}},
		}, "MAP needs ElementType"},
		{"ARRAY with no element type", Column{Name: "a", Type: TypeArray}, "ARRAY needs an ElementType"},
		{"ROW with no fields", Column{Name: "r", Type: TypeRow}, "ROW needs at least one field"},
		{"VECTOR with no dimension", Column{Name: "v", Type: TypeVector}, "positive Dimension"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w, err := NewWriter(&buf, Schema{Columns: []Column{tc.column, {Name: "after", Type: TypeInt64}}},
				DefaultWriterConfig())
			if err == nil {
				_ = w.Close()
				t.Fatalf("the schema was accepted; the file is %d bytes", buf.Len())
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
			if buf.Len() != 0 {
				t.Errorf("%d bytes were written before the refusal", buf.Len())
			}
		})
	}

	// The well-formed MAP still writes, so the check refuses the shape and
	// not the type.
	var buf bytes.Buffer
	w, err := NewWriter(&buf, Schema{Columns: []Column{{
		Name: "m", Type: TypeMap, Nullable: true,
		ElementType: &Column{Name: "kv", Type: TypeRow, Fields: []Column{
			{Name: "key", Type: TypeString}, {Name: "value", Type: TypeInt64, Nullable: true},
		}},
	}}}, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("a well-formed MAP was refused: %v", err)
	}
	if err := w.WriteRows([]map[string]any{{"m": map[string]any{"a": int64(1)}}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestMalformedMapSchemaMisplacesLaterColumns records what the refusal
// prevents, by driving the two walks that used to disagree directly.
func TestMalformedMapSchemaMisplacesLaterColumns(t *testing.T) {
	bad := Schema{Columns: []Column{
		{Name: "m", Type: TypeMap, Fields: []Column{
			{Name: "key", Type: TypeString}, {Name: "value", Type: TypeInt64},
		}},
		{Name: "a", Type: TypeInt64},
		{Name: "b", Type: TypeInt64},
	}}
	if err := ValidateWriteSchema(bad); err == nil {
		t.Fatal("ValidateWriteSchema accepted the malformed MAP")
	}

	elements := buildSchemaElements(bad)
	root, leaves := BuildSchemaTree(elements)
	if root == nil {
		t.Fatal("no schema root")
	}
	// The key_value group swallowed "a" and "b": the top level is left with
	// the map alone, and the two int columns are now its key and value.
	if len(root.Children) != 1 {
		t.Errorf("top-level columns after the malformed MAP: %d, want 1 (they were absorbed)", len(root.Children))
	}
	names := make([]string, 0, len(leaves))
	for _, l := range leaves {
		names = append(names, l.Name)
	}
	if strings.Join(names, ",") != "a,b" {
		t.Errorf("leaves = %v; the map's own key/value never made it into the footer", names)
	}
}
