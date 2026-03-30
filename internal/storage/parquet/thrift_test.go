package parquet

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

// --- Low-level Thrift decoder tests ---

func TestThriftVarint(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want uint64
	}{
		{"zero", []byte{0x00}, 0},
		{"one", []byte{0x01}, 1},
		{"127", []byte{0x7F}, 127},
		{"128", []byte{0x80, 0x01}, 128},
		{"300", []byte{0xAC, 0x02}, 300},
		{"max_uint32", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}, math.MaxUint32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newThriftDecoder(tt.data)
			got, err := d.readVarint()
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("readVarint() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestThriftVarintOverflow(t *testing.T) {
	// 10 bytes of 0x80 — valid varint encoding but would overflow
	data := make([]byte, 11)
	for i := 0; i < 10; i++ {
		data[i] = 0x80
	}
	data[10] = 0x01
	d := newThriftDecoder(data)
	_, err := d.readVarint()
	if err == nil {
		t.Error("expected overflow error")
	}
}

func TestThriftZigzag(t *testing.T) {
	tests := []struct {
		encoded uint64
		want    int64
	}{
		{0, 0},
		{1, -1},
		{2, 1},
		{3, -2},
		{4, 2},
		{0xFFFFFFFFFFFFFFFE, math.MaxInt64},
		{0xFFFFFFFFFFFFFFFF, math.MinInt64},
	}
	for _, tt := range tests {
		got := zigzagDecode(tt.encoded)
		if got != tt.want {
			t.Errorf("zigzagDecode(%d) = %d, want %d", tt.encoded, got, tt.want)
		}
	}
}

func TestThriftReadBinary(t *testing.T) {
	// varint length 5, then "hello"
	data := []byte{0x05, 'h', 'e', 'l', 'l', 'o'}
	d := newThriftDecoder(data)
	got, err := d.readBinary()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Errorf("readBinary() = %q, want %q", got, "hello")
	}
}

func TestThriftReadBinaryEmpty(t *testing.T) {
	data := []byte{0x00}
	d := newThriftDecoder(data)
	got, err := d.readBinary()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("readBinary() len = %d, want 0", len(got))
	}
}

func TestThriftReadDouble(t *testing.T) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, math.Float64bits(3.14))
	d := newThriftDecoder(data)
	got, err := d.readDouble()
	if err != nil {
		t.Fatal(err)
	}
	if got != 3.14 {
		t.Errorf("readDouble() = %v, want 3.14", got)
	}
}

func TestThriftFieldHeaderShortForm(t *testing.T) {
	// Short form: delta=1, type=I32(5) → byte = (1 << 4) | 5 = 0x15
	data := []byte{0x15}
	d := newThriftDecoder(data)
	var lastFieldID int16
	fieldID, ft, err := d.readFieldHeader(&lastFieldID)
	if err != nil {
		t.Fatal(err)
	}
	if fieldID != 1 {
		t.Errorf("fieldID = %d, want 1", fieldID)
	}
	if ft != thriftI32 {
		t.Errorf("fieldType = %d, want %d (I32)", ft, thriftI32)
	}
}

func TestThriftFieldHeaderLongForm(t *testing.T) {
	// Long form: delta=0, type=I32(5), then zigzag-encoded field ID 20
	// zigzag(20) = 40 = 0x28
	data := []byte{0x05, 0x28}
	d := newThriftDecoder(data)
	var lastFieldID int16
	fieldID, ft, err := d.readFieldHeader(&lastFieldID)
	if err != nil {
		t.Fatal(err)
	}
	if fieldID != 20 {
		t.Errorf("fieldID = %d, want 20", fieldID)
	}
	if ft != thriftI32 {
		t.Errorf("fieldType = %d, want %d (I32)", ft, thriftI32)
	}
}

func TestThriftFieldHeaderSequential(t *testing.T) {
	// Two fields: field 1 (I32), field 2 (I64)
	// Field 1: delta=1, type=5 → 0x15
	// Field 2: delta=1, type=6 → 0x16
	data := []byte{0x15, 0x02, 0x16, 0x04} // 0x02 = zigzag(1), 0x04 = zigzag(2)
	d := newThriftDecoder(data)
	var lastFieldID int16

	// Field 1
	id1, ft1, err := d.readFieldHeader(&lastFieldID)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != 1 || ft1 != thriftI32 {
		t.Errorf("field 1: id=%d type=%d, want id=1 type=5", id1, ft1)
	}
	_, _ = d.readI32() // consume value

	// Field 2
	id2, ft2, err := d.readFieldHeader(&lastFieldID)
	if err != nil {
		t.Fatal(err)
	}
	if id2 != 2 || ft2 != thriftI64 {
		t.Errorf("field 2: id=%d type=%d, want id=2 type=6", id2, ft2)
	}
}

func TestThriftListHeader(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		wantType thriftType
		wantSize int
	}{
		{"small", []byte{0x35}, thriftI32, 3},       // size=3, type=5(I32)
		{"zero", []byte{0x05}, thriftI32, 0},         // size=0, type=5(I32)
		{"large", []byte{0xF5, 0x10}, thriftI32, 16}, // size≥15, varint 16 follows
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newThriftDecoder(tt.data)
			elemType, size, err := d.readListHeader()
			if err != nil {
				t.Fatal(err)
			}
			if elemType != tt.wantType {
				t.Errorf("elemType = %d, want %d", elemType, tt.wantType)
			}
			if size != tt.wantSize {
				t.Errorf("size = %d, want %d", size, tt.wantSize)
			}
		})
	}
}

func TestThriftSkipField(t *testing.T) {
	tests := []struct {
		name string
		typ  thriftType
		data []byte
	}{
		{"bool_true", thriftBoolTrue, nil},
		{"bool_false", thriftBoolFalse, nil},
		{"byte", thriftByte, []byte{0x42}},
		{"i16", thriftI16, []byte{0x04}},          // zigzag(2)
		{"i32", thriftI32, []byte{0x08}},          // zigzag(4)
		{"i64", thriftI64, []byte{0x10}},          // zigzag(8)
		{"double", thriftDouble, make([]byte, 8)},
		{"binary", thriftBinary, []byte{0x03, 'a', 'b', 'c'}},
		{"empty_list", thriftList, []byte{0x05}},                    // 0 elements, type I32
		{"struct", thriftStruct, []byte{0x00}},                      // empty struct (stop byte)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newThriftDecoder(tt.data)
			err := d.skipField(tt.typ)
			if err != nil {
				t.Fatalf("skipField(%d): %v", tt.typ, err)
			}
			if d.remaining() != 0 {
				t.Errorf("remaining = %d, want 0", d.remaining())
			}
		})
	}
}

func TestThriftEOF(t *testing.T) {
	d := newThriftDecoder(nil)
	_, err := d.readByte()
	if err == nil {
		t.Error("expected EOF error")
	}
}

// --- FileMetaData decoding tests ---

// buildThriftI32 encodes a zigzag-varint i32.
func buildThriftI32(v int32) []byte {
	zz := uint64(uint32(v<<1) ^ uint32(v>>31))
	return buildVarint(zz)
}

// buildThriftI64 encodes a zigzag-varint i64.
func buildThriftI64(v int64) []byte {
	zz := uint64(v<<1) ^ uint64(v>>63)
	return buildVarint(zz)
}

// buildVarint encodes a uint64 as varint.
func buildVarint(v uint64) []byte {
	var buf [10]byte
	n := 0
	for v >= 0x80 {
		buf[n] = byte(v) | 0x80
		v >>= 7
		n++
	}
	buf[n] = byte(v)
	return buf[:n+1]
}

// buildFieldHeader encodes a field header with short-form delta.
func buildFieldHeader(delta int, typ thriftType) byte {
	return byte(delta<<4) | byte(typ)
}

func TestDecodeFileMetaDataMinimal(t *testing.T) {
	// Build a minimal FileMetaData:
	// field 1 (version): i32 = 2
	// field 3 (num_rows): i64 = 1000
	// stop
	var buf bytes.Buffer
	buf.WriteByte(buildFieldHeader(1, thriftI32)) // field 1: version
	buf.Write(buildThriftI32(2))
	buf.WriteByte(buildFieldHeader(2, thriftI64)) // field 3: num_rows (delta=2 from field 1)
	buf.Write(buildThriftI64(1000))
	buf.WriteByte(0x00) // stop

	md, err := DecodeFileMetaData(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if md.Version != 2 {
		t.Errorf("Version = %d, want 2", md.Version)
	}
	if md.NumRows != 1000 {
		t.Errorf("NumRows = %d, want 1000", md.NumRows)
	}
}

func TestDecodeFileMetaDataWithSchema(t *testing.T) {
	// Build FileMetaData with a simple schema: root group with one INT64 leaf.
	var buf bytes.Buffer

	// field 1: version = 2
	buf.WriteByte(buildFieldHeader(1, thriftI32))
	buf.Write(buildThriftI32(2))

	// field 2: schema = list<SchemaElement> with 2 elements
	buf.WriteByte(buildFieldHeader(1, thriftList)) // delta=1 from field 1 → field 2
	buf.WriteByte(0x2C)                            // size=2, type=struct(12)

	// SchemaElement 0: root group "schema" with 1 child
	buf.WriteByte(buildFieldHeader(3, thriftI32)) // field 3: repetition_type = REQUIRED(0)
	buf.Write(buildThriftI32(0))
	buf.WriteByte(buildFieldHeader(1, thriftBinary)) // field 4: name = "schema"
	buf.Write([]byte{0x06})                          // string length 6
	buf.WriteString("schema")
	buf.WriteByte(buildFieldHeader(1, thriftI32)) // field 5: num_children = 1
	buf.Write(buildThriftI32(1))
	buf.WriteByte(0x00) // stop

	// SchemaElement 1: leaf "id" (INT64, OPTIONAL)
	buf.WriteByte(buildFieldHeader(1, thriftI32)) // field 1: type = INT64(2)
	buf.Write(buildThriftI32(2))
	buf.WriteByte(buildFieldHeader(2, thriftI32)) // field 3: repetition_type = OPTIONAL(1)
	buf.Write(buildThriftI32(1))
	buf.WriteByte(buildFieldHeader(1, thriftBinary)) // field 4: name = "id"
	buf.Write([]byte{0x02})
	buf.WriteString("id")
	buf.WriteByte(0x00) // stop

	// field 3: num_rows = 500
	buf.WriteByte(buildFieldHeader(1, thriftI64)) // delta=1 from field 2 → field 3
	buf.Write(buildThriftI64(500))

	// field 4: row_groups = empty list
	buf.WriteByte(buildFieldHeader(1, thriftList)) // field 4
	buf.WriteByte(0x0C)                            // size=0, type=struct(12)

	buf.WriteByte(0x00) // stop FileMetaData

	md, err := DecodeFileMetaData(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if md.Version != 2 {
		t.Errorf("Version = %d, want 2", md.Version)
	}
	if md.NumRows != 500 {
		t.Errorf("NumRows = %d, want 500", md.NumRows)
	}
	if len(md.Schema) != 2 {
		t.Fatalf("Schema length = %d, want 2", len(md.Schema))
	}

	root := md.Schema[0]
	if root.Name != "schema" {
		t.Errorf("Schema[0].Name = %q, want %q", root.Name, "schema")
	}
	if root.NumChildren != 1 {
		t.Errorf("Schema[0].NumChildren = %d, want 1", root.NumChildren)
	}
	if root.Type != nil {
		t.Errorf("Schema[0].Type should be nil (group node)")
	}

	leaf := md.Schema[1]
	if leaf.Name != "id" {
		t.Errorf("Schema[1].Name = %q, want %q", leaf.Name, "id")
	}
	if leaf.Type == nil || *leaf.Type != PhysicalInt64 {
		t.Errorf("Schema[1].Type = %v, want INT64", leaf.Type)
	}
	if leaf.RepetitionType != FieldOptional {
		t.Errorf("Schema[1].RepetitionType = %d, want OPTIONAL(1)", leaf.RepetitionType)
	}
}

func TestDecodePageHeaderDataV1(t *testing.T) {
	var buf bytes.Buffer

	// field 1: type = DATA_PAGE(0)
	buf.WriteByte(buildFieldHeader(1, thriftI32))
	buf.Write(buildThriftI32(0))
	// field 2: uncompressed_page_size = 4096
	buf.WriteByte(buildFieldHeader(1, thriftI32))
	buf.Write(buildThriftI32(4096))
	// field 3: compressed_page_size = 2048
	buf.WriteByte(buildFieldHeader(1, thriftI32))
	buf.Write(buildThriftI32(2048))
	// field 5: data_page_header
	buf.WriteByte(buildFieldHeader(2, thriftStruct)) // delta=2 from field 3 → field 5
	// DataPageHeader:
	buf.WriteByte(buildFieldHeader(1, thriftI32)) // field 1: num_values = 100
	buf.Write(buildThriftI32(100))
	buf.WriteByte(buildFieldHeader(1, thriftI32)) // field 2: encoding = PLAIN(0)
	buf.Write(buildThriftI32(0))
	buf.WriteByte(buildFieldHeader(1, thriftI32)) // field 3: def_level_encoding = RLE(3)
	buf.Write(buildThriftI32(3))
	buf.WriteByte(buildFieldHeader(1, thriftI32)) // field 4: rep_level_encoding = RLE(3)
	buf.Write(buildThriftI32(3))
	buf.WriteByte(0x00) // stop DataPageHeader
	buf.WriteByte(0x00) // stop PageHeader

	ph, consumed, err := DecodePageHeader(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if consumed != buf.Len() {
		t.Errorf("consumed = %d, want %d", consumed, buf.Len())
	}
	if ph.Type != PageDataV1 {
		t.Errorf("Type = %d, want DATA_PAGE(0)", ph.Type)
	}
	if ph.UncompressedPageSize != 4096 {
		t.Errorf("UncompressedPageSize = %d, want 4096", ph.UncompressedPageSize)
	}
	if ph.CompressedPageSize != 2048 {
		t.Errorf("CompressedPageSize = %d, want 2048", ph.CompressedPageSize)
	}
	if ph.DataPageHeader == nil {
		t.Fatal("DataPageHeader is nil")
	}
	if ph.DataPageHeader.NumValues != 100 {
		t.Errorf("NumValues = %d, want 100", ph.DataPageHeader.NumValues)
	}
	if ph.DataPageHeader.Encoding != EncodingPlain {
		t.Errorf("Encoding = %d, want PLAIN(0)", ph.DataPageHeader.Encoding)
	}
}

func TestDecodePageHeaderDictionary(t *testing.T) {
	var buf bytes.Buffer

	// field 1: type = DICTIONARY_PAGE(2)
	buf.WriteByte(buildFieldHeader(1, thriftI32))
	buf.Write(buildThriftI32(2))
	// field 2: uncompressed = 8192
	buf.WriteByte(buildFieldHeader(1, thriftI32))
	buf.Write(buildThriftI32(8192))
	// field 3: compressed = 4096
	buf.WriteByte(buildFieldHeader(1, thriftI32))
	buf.Write(buildThriftI32(4096))
	// field 7: dictionary_page_header (delta=4 from field 3)
	buf.WriteByte(buildFieldHeader(4, thriftStruct))
	buf.WriteByte(buildFieldHeader(1, thriftI32)) // num_values = 256
	buf.Write(buildThriftI32(256))
	buf.WriteByte(buildFieldHeader(1, thriftI32)) // encoding = PLAIN(0)
	buf.Write(buildThriftI32(0))
	buf.WriteByte(0x00) // stop DictionaryPageHeader
	buf.WriteByte(0x00) // stop PageHeader

	ph, _, err := DecodePageHeader(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if ph.Type != PageDictionary {
		t.Errorf("Type = %d, want DICTIONARY_PAGE(2)", ph.Type)
	}
	if ph.DictionaryPageHeader == nil {
		t.Fatal("DictionaryPageHeader is nil")
	}
	if ph.DictionaryPageHeader.NumValues != 256 {
		t.Errorf("NumValues = %d, want 256", ph.DictionaryPageHeader.NumValues)
	}
}

func TestDecodeStatistics(t *testing.T) {
	var buf bytes.Buffer

	// Build a Statistics struct with min/max values.
	buf.WriteByte(buildFieldHeader(3, thriftI64)) // field 3: null_count = 5
	buf.Write(buildThriftI64(5))
	buf.WriteByte(buildFieldHeader(1, thriftI64)) // field 4: distinct_count = 100
	buf.Write(buildThriftI64(100))
	buf.WriteByte(buildFieldHeader(1, thriftBinary)) // field 5: max_value
	buf.Write([]byte{0x04, 0x00, 0x00, 0x00, 0x64}) // length 4, then int32 LE 100
	buf.WriteByte(buildFieldHeader(1, thriftBinary)) // field 6: min_value
	buf.Write([]byte{0x04, 0x00, 0x00, 0x00, 0x01}) // length 4, then int32 LE 1
	buf.WriteByte(0x00)                               // stop

	d := newThriftDecoder(buf.Bytes())
	s := &Statistics{}
	err := d.decodeStatistics(s)
	if err != nil {
		t.Fatal(err)
	}
	if s.NullCount != 5 {
		t.Errorf("NullCount = %d, want 5", s.NullCount)
	}
	if s.DistinctCount != 100 {
		t.Errorf("DistinctCount = %d, want 100", s.DistinctCount)
	}
	if len(s.MaxValue) != 4 {
		t.Errorf("MaxValue len = %d, want 4", len(s.MaxValue))
	}
	if len(s.MinValue) != 4 {
		t.Errorf("MinValue len = %d, want 4", len(s.MinValue))
	}
}

func TestDecodeColumnMetaData(t *testing.T) {
	var buf bytes.Buffer

	// field 1: type = INT64(2)
	buf.WriteByte(buildFieldHeader(1, thriftI32))
	buf.Write(buildThriftI32(2))
	// field 2: encodings = [PLAIN, RLE_DICTIONARY]
	buf.WriteByte(buildFieldHeader(1, thriftList))
	buf.WriteByte(0x25) // size=2, type=I32(5)
	buf.Write(buildThriftI32(0))
	buf.Write(buildThriftI32(8))
	// field 3: path_in_schema = ["id"]
	buf.WriteByte(buildFieldHeader(1, thriftList))
	buf.WriteByte(0x18) // size=1, type=binary(8)
	buf.Write([]byte{0x02})
	buf.WriteString("id")
	// field 4: codec = SNAPPY(1)
	buf.WriteByte(buildFieldHeader(1, thriftI32))
	buf.Write(buildThriftI32(1))
	// field 5: num_values = 1000
	buf.WriteByte(buildFieldHeader(1, thriftI64))
	buf.Write(buildThriftI64(1000))
	// field 6: total_uncompressed_size = 8000
	buf.WriteByte(buildFieldHeader(1, thriftI64))
	buf.Write(buildThriftI64(8000))
	// field 7: total_compressed_size = 4000
	buf.WriteByte(buildFieldHeader(1, thriftI64))
	buf.Write(buildThriftI64(4000))
	// field 9: data_page_offset = 100 (delta=2 from field 7, skipping field 8)
	buf.WriteByte(buildFieldHeader(2, thriftI64))
	buf.Write(buildThriftI64(100))
	buf.WriteByte(0x00) // stop

	d := newThriftDecoder(buf.Bytes())
	cm := &ColumnMetaData{}
	err := d.decodeColumnMetaData(cm)
	if err != nil {
		t.Fatal(err)
	}
	if cm.Type != PhysicalInt64 {
		t.Errorf("Type = %d, want INT64(2)", cm.Type)
	}
	if len(cm.Encodings) != 2 {
		t.Fatalf("Encodings len = %d, want 2", len(cm.Encodings))
	}
	if cm.Encodings[0] != EncodingPlain || cm.Encodings[1] != EncodingRLEDictionary {
		t.Errorf("Encodings = %v, want [PLAIN, RLE_DICTIONARY]", cm.Encodings)
	}
	if len(cm.PathInSchema) != 1 || cm.PathInSchema[0] != "id" {
		t.Errorf("PathInSchema = %v, want [id]", cm.PathInSchema)
	}
	if cm.Codec != CodecSnappy {
		t.Errorf("Codec = %d, want SNAPPY(1)", cm.Codec)
	}
	if cm.NumValues != 1000 {
		t.Errorf("NumValues = %d, want 1000", cm.NumValues)
	}
	if cm.TotalUncompressedSize != 8000 {
		t.Errorf("TotalUncompressedSize = %d, want 8000", cm.TotalUncompressedSize)
	}
	if cm.TotalCompressedSize != 4000 {
		t.Errorf("TotalCompressedSize = %d, want 4000", cm.TotalCompressedSize)
	}
	if cm.DataPageOffset != 100 {
		t.Errorf("DataPageOffset = %d, want 100", cm.DataPageOffset)
	}
}

func TestDecodeUnknownFieldsSkipped(t *testing.T) {
	// FileMetaData with field 1 (version) and an unknown field 99.
	var buf bytes.Buffer
	buf.WriteByte(buildFieldHeader(1, thriftI32))
	buf.Write(buildThriftI32(2))
	// Unknown field 99 as binary — use long form (delta=0)
	buf.WriteByte(byte(thriftBinary)) // type=binary, delta=0
	buf.Write(buildThriftI32(99))     // zigzag field ID (but readI16, so int16)
	// Wait — field header long form reads I16, not I32. Let me re-check.
	// Actually we need to set lastFieldID=1, then build correctly.
	// Let me just use a simpler approach: high delta that triggers long form.

	// Actually, let me just test that unknown fields get skipped by using
	// a field ID that the decoder doesn't recognize.
	buf.Reset()

	// Build: field 1 (I32) = 2, field 15 (binary) = "unknown", stop
	buf.WriteByte(buildFieldHeader(1, thriftI32)) // field 1
	buf.Write(buildThriftI32(2))
	// field 15 uses short form delta from field 1: delta=14
	buf.WriteByte(byte(14<<4) | byte(thriftBinary)) // field 15, binary
	buf.Write([]byte{0x07, 'u', 'n', 'k', 'n', 'o', 'w', 'n'})
	buf.WriteByte(0x00) // stop

	md, err := DecodeFileMetaData(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if md.Version != 2 {
		t.Errorf("Version = %d, want 2", md.Version)
	}
}
