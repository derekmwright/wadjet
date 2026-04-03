package parquet

import (
	"encoding/binary"
	"math"
)

// Thrift Compact Protocol encoder for Parquet metadata.
//
// Mirrors the decoder in thrift.go — encodes FileMetaData, PageHeader,
// and related structures into binary Thrift compact protocol format.
//
// Reference: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md

// thriftEncoder writes Thrift compact protocol data to a byte buffer.
type thriftEncoder struct {
	buf []byte
}

func newThriftEncoder() *thriftEncoder {
	return &thriftEncoder{buf: make([]byte, 0, 256)}
}

// Bytes returns the encoded data.
func (e *thriftEncoder) Bytes() []byte {
	return e.buf
}

// writeByte writes a single byte.
func (e *thriftEncoder) writeByte(b byte) {
	e.buf = append(e.buf, b)
}

// writeVarint writes a varint-encoded unsigned integer.
func (e *thriftEncoder) writeVarint(v uint64) {
	for v >= 0x80 {
		e.buf = append(e.buf, byte(v)|0x80)
		v >>= 7
	}
	e.buf = append(e.buf, byte(v))
}

// writeZigzag writes a zigzag-encoded signed integer.
func (e *thriftEncoder) writeZigzag(v int64) {
	e.writeVarint(zigzagEncode(v))
}

// zigzagEncode converts a signed int64 to zigzag-encoded uint64.
func zigzagEncode(n int64) uint64 {
	return uint64((n << 1) ^ (n >> 63))
}

// writeI16 writes a zigzag-encoded 16-bit integer.
func (e *thriftEncoder) writeI16(v int16) {
	e.writeZigzag(int64(v))
}

// writeI32 writes a zigzag-encoded 32-bit integer.
func (e *thriftEncoder) writeI32(v int32) {
	e.writeZigzag(int64(v))
}

// writeI64 writes a zigzag-encoded 64-bit integer.
func (e *thriftEncoder) writeI64(v int64) {
	e.writeZigzag(v)
}

// writeDouble writes a 64-bit IEEE 754 double.
func (e *thriftEncoder) writeDouble(v float64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
	e.buf = append(e.buf, buf[:]...)
}

// writeBinary writes a length-prefixed byte sequence.
func (e *thriftEncoder) writeBinary(b []byte) {
	e.writeVarint(uint64(len(b)))
	e.buf = append(e.buf, b...)
}

// writeString writes a length-prefixed UTF-8 string.
func (e *thriftEncoder) writeString(s string) {
	e.writeVarint(uint64(len(s)))
	e.buf = append(e.buf, s...)
}

// writeFieldHeader writes a field header using delta encoding.
// Returns the new lastFieldID.
func (e *thriftEncoder) writeFieldHeader(fieldID int16, fieldType thriftType, lastFieldID *int16) {
	delta := fieldID - *lastFieldID
	if delta > 0 && delta <= 15 {
		// Short form: delta encoded.
		e.writeByte(byte(delta<<4) | byte(fieldType))
	} else {
		// Long form: type byte + full field ID.
		e.writeByte(byte(fieldType))
		e.writeI16(fieldID)
	}
	*lastFieldID = fieldID
}

// writeBoolField writes a boolean field (type encoded in header).
func (e *thriftEncoder) writeBoolField(fieldID int16, val bool, lastFieldID *int16) {
	t := thriftBoolFalse
	if val {
		t = thriftBoolTrue
	}
	e.writeFieldHeader(fieldID, t, lastFieldID)
}

// writeListHeader writes a list header.
func (e *thriftEncoder) writeListHeader(elemType thriftType, size int) {
	if size < 15 {
		e.writeByte(byte(size<<4) | byte(elemType))
	} else {
		e.writeByte(0xF0 | byte(elemType))
		e.writeVarint(uint64(size))
	}
}

// writeStop writes a struct end marker.
func (e *thriftEncoder) writeStop() {
	e.writeByte(0)
}

// --- Parquet metadata encoders ---

// EncodeFileMetaData encodes FileMetaData to Thrift compact protocol.
func EncodeFileMetaData(md *FileMetaData) []byte {
	e := newThriftEncoder()
	var lastFieldID int16

	// field 1: version (i32)
	e.writeFieldHeader(1, thriftI32, &lastFieldID)
	e.writeI32(md.Version)

	// field 2: schema (list<SchemaElement>)
	e.writeFieldHeader(2, thriftList, &lastFieldID)
	e.writeListHeader(thriftStruct, len(md.Schema))
	for i := range md.Schema {
		e.encodeSchemaElement(&md.Schema[i])
	}

	// field 3: num_rows (i64)
	e.writeFieldHeader(3, thriftI64, &lastFieldID)
	e.writeI64(md.NumRows)

	// field 4: row_groups (list<RowGroup>)
	e.writeFieldHeader(4, thriftList, &lastFieldID)
	e.writeListHeader(thriftStruct, len(md.RowGroups))
	for i := range md.RowGroups {
		e.encodeRowGroup(&md.RowGroups[i])
	}

	// field 5: key_value_metadata (list<KeyValue>) — optional
	if len(md.KeyValueMetadata) > 0 {
		e.writeFieldHeader(5, thriftList, &lastFieldID)
		e.writeListHeader(thriftStruct, len(md.KeyValueMetadata))
		for i := range md.KeyValueMetadata {
			e.encodeKeyValue(&md.KeyValueMetadata[i])
		}
	}

	// field 6: created_by (string) — optional
	if md.CreatedBy != "" {
		e.writeFieldHeader(6, thriftBinary, &lastFieldID)
		e.writeString(md.CreatedBy)
	}

	// field 7: column_orders (list<ColumnOrder>) — optional
	if len(md.ColumnOrders) > 0 {
		e.writeFieldHeader(7, thriftList, &lastFieldID)
		e.writeListHeader(thriftStruct, len(md.ColumnOrders))
		for range md.ColumnOrders {
			// ColumnOrder contains a single field: TypeDefinedOrder (empty struct).
			var co int16
			e.writeFieldHeader(1, thriftStruct, &co)
			e.writeStop() // empty TypeDefinedOrder struct
			e.writeStop() // end ColumnOrder
		}
	}

	e.writeStop()
	return e.Bytes()
}

func (e *thriftEncoder) encodeSchemaElement(se *SchemaElement) {
	var lastFieldID int16

	// field 1: type (optional, nil for group nodes)
	if se.Type != nil {
		e.writeFieldHeader(1, thriftI32, &lastFieldID)
		e.writeI32(int32(*se.Type))
	}

	// field 2: type_length (optional, for FIXED_LEN_BYTE_ARRAY)
	if se.TypeLength > 0 {
		e.writeFieldHeader(2, thriftI32, &lastFieldID)
		e.writeI32(se.TypeLength)
	}

	// field 3: repetition_type
	e.writeFieldHeader(3, thriftI32, &lastFieldID)
	e.writeI32(int32(se.RepetitionType))

	// field 4: name
	e.writeFieldHeader(4, thriftBinary, &lastFieldID)
	e.writeString(se.Name)

	// field 5: num_children (optional, 0 for leaf nodes)
	if se.NumChildren > 0 {
		e.writeFieldHeader(5, thriftI32, &lastFieldID)
		e.writeI32(se.NumChildren)
	}

	// field 6: converted_type (optional)
	if se.ConvertedType != nil {
		e.writeFieldHeader(6, thriftI32, &lastFieldID)
		e.writeI32(int32(*se.ConvertedType))
	}

	// field 7: scale (optional, for DECIMAL)
	if se.Scale > 0 {
		e.writeFieldHeader(7, thriftI32, &lastFieldID)
		e.writeI32(se.Scale)
	}

	// field 8: precision (optional, for DECIMAL)
	if se.Precision > 0 {
		e.writeFieldHeader(8, thriftI32, &lastFieldID)
		e.writeI32(se.Precision)
	}

	// field 10: logicalType (optional)
	if se.LogicalType != nil {
		e.writeFieldHeader(10, thriftStruct, &lastFieldID)
		e.encodeLogicalType(se.LogicalType)
	}

	e.writeStop()
}

func (e *thriftEncoder) encodeLogicalType(lt *LogicalType) {
	var lastFieldID int16
	switch lt.Type {
	case LogicalString:
		e.writeFieldHeader(1, thriftStruct, &lastFieldID)
		e.writeStop() // empty STRING struct
	case LogicalMap:
		e.writeFieldHeader(2, thriftStruct, &lastFieldID)
		e.writeStop()
	case LogicalList:
		e.writeFieldHeader(3, thriftStruct, &lastFieldID)
		e.writeStop()
	case LogicalEnum:
		e.writeFieldHeader(4, thriftStruct, &lastFieldID)
		e.writeStop()
	case LogicalDecimal:
		e.writeFieldHeader(5, thriftStruct, &lastFieldID)
		var decLast int16
		e.writeFieldHeader(1, thriftI32, &decLast) // scale
		e.writeI32(int32(lt.Scale))
		e.writeFieldHeader(2, thriftI32, &decLast) // precision
		e.writeI32(int32(lt.Precision))
		e.writeStop()
	case LogicalDate:
		e.writeFieldHeader(6, thriftStruct, &lastFieldID)
		e.writeStop()
	case LogicalTimeMillis, LogicalTimeMicros:
		e.writeFieldHeader(7, thriftStruct, &lastFieldID)
		e.encodeTimeType(lt)
	case LogicalTimestampMillis, LogicalTimestampMicros, LogicalTimestampNanos:
		e.writeFieldHeader(8, thriftStruct, &lastFieldID)
		e.encodeTimestampType(lt)
	case LogicalInteger:
		e.writeFieldHeader(10, thriftStruct, &lastFieldID)
		var intLast int16
		e.writeFieldHeader(1, thriftByte, &intLast) // bitWidth
		e.writeByte(byte(lt.BitWidth))
		e.writeBoolField(2, lt.IsSigned, &intLast) // isSigned
		e.writeStop()
	case LogicalNull:
		e.writeFieldHeader(11, thriftStruct, &lastFieldID)
		e.writeStop()
	case LogicalJSON:
		e.writeFieldHeader(12, thriftStruct, &lastFieldID)
		e.writeStop()
	case LogicalBSON:
		e.writeFieldHeader(13, thriftStruct, &lastFieldID)
		e.writeStop()
	case LogicalUUID:
		e.writeFieldHeader(14, thriftStruct, &lastFieldID)
		e.writeStop()
	case LogicalVector:
		// Wadjet extension: field 100 with dimension as i32
		e.writeFieldHeader(100, thriftStruct, &lastFieldID)
		var vecLast int16
		e.writeFieldHeader(1, thriftI32, &vecLast)
		e.writeI32(int32(lt.Dimension))
		e.writeStop()
	}
	e.writeStop()
}

func (e *thriftEncoder) encodeTimeType(lt *LogicalType) {
	var lastFieldID int16
	e.writeBoolField(1, lt.IsAdjustedToUTC, &lastFieldID) // isAdjustedToUTC
	e.writeFieldHeader(2, thriftStruct, &lastFieldID)      // unit
	var unitLast int16
	switch lt.Type {
	case LogicalTimeMillis:
		e.writeFieldHeader(1, thriftStruct, &unitLast) // MILLIS
		e.writeStop()
	case LogicalTimeMicros:
		e.writeFieldHeader(2, thriftStruct, &unitLast) // MICROS
		e.writeStop()
	}
	e.writeStop() // end TimeUnit
	e.writeStop() // end TimeType
}

func (e *thriftEncoder) encodeTimestampType(lt *LogicalType) {
	var lastFieldID int16
	e.writeBoolField(1, lt.IsAdjustedToUTC, &lastFieldID) // isAdjustedToUTC
	e.writeFieldHeader(2, thriftStruct, &lastFieldID)      // unit
	var unitLast int16
	switch lt.Type {
	case LogicalTimestampMillis:
		e.writeFieldHeader(1, thriftStruct, &unitLast) // MILLIS
		e.writeStop()
	case LogicalTimestampMicros:
		e.writeFieldHeader(2, thriftStruct, &unitLast) // MICROS
		e.writeStop()
	case LogicalTimestampNanos:
		e.writeFieldHeader(3, thriftStruct, &unitLast) // NANOS
		e.writeStop()
	}
	e.writeStop() // end TimeUnit
	e.writeStop() // end TimestampType
}

func (e *thriftEncoder) encodeRowGroup(rg *RowGroup) {
	var lastFieldID int16

	// field 1: columns (list<ColumnChunk>)
	e.writeFieldHeader(1, thriftList, &lastFieldID)
	e.writeListHeader(thriftStruct, len(rg.Columns))
	for i := range rg.Columns {
		e.encodeColumnChunk(&rg.Columns[i])
	}

	// field 2: total_byte_size (i64)
	e.writeFieldHeader(2, thriftI64, &lastFieldID)
	e.writeI64(rg.TotalByteSize)

	// field 3: num_rows (i64)
	e.writeFieldHeader(3, thriftI64, &lastFieldID)
	e.writeI64(rg.NumRows)

	e.writeStop()
}

func (e *thriftEncoder) encodeColumnChunk(cc *ColumnChunk) {
	var lastFieldID int16

	// field 2: file_offset (i64)
	e.writeFieldHeader(2, thriftI64, &lastFieldID)
	e.writeI64(cc.FileOffset)

	// field 3: meta_data (ColumnMetaData struct)
	if cc.MetaData != nil {
		e.writeFieldHeader(3, thriftStruct, &lastFieldID)
		e.encodeColumnMetaData(cc.MetaData)
	}

	e.writeStop()
}

func (e *thriftEncoder) encodeColumnMetaData(cm *ColumnMetaData) {
	var lastFieldID int16

	// field 1: type (i32 enum)
	e.writeFieldHeader(1, thriftI32, &lastFieldID)
	e.writeI32(int32(cm.Type))

	// field 2: encodings (list<Encoding>)
	e.writeFieldHeader(2, thriftList, &lastFieldID)
	e.writeListHeader(thriftI32, len(cm.Encodings))
	for _, enc := range cm.Encodings {
		e.writeI32(int32(enc))
	}

	// field 3: path_in_schema (list<string>)
	e.writeFieldHeader(3, thriftList, &lastFieldID)
	e.writeListHeader(thriftBinary, len(cm.PathInSchema))
	for _, s := range cm.PathInSchema {
		e.writeString(s)
	}

	// field 4: codec (i32 enum)
	e.writeFieldHeader(4, thriftI32, &lastFieldID)
	e.writeI32(int32(cm.Codec))

	// field 5: num_values (i64)
	e.writeFieldHeader(5, thriftI64, &lastFieldID)
	e.writeI64(cm.NumValues)

	// field 6: total_uncompressed_size (i64)
	e.writeFieldHeader(6, thriftI64, &lastFieldID)
	e.writeI64(cm.TotalUncompressedSize)

	// field 7: total_compressed_size (i64)
	e.writeFieldHeader(7, thriftI64, &lastFieldID)
	e.writeI64(cm.TotalCompressedSize)

	// field 9: data_page_offset (i64)
	e.writeFieldHeader(9, thriftI64, &lastFieldID)
	e.writeI64(cm.DataPageOffset)

	// field 11: dictionary_page_offset (i64, optional)
	if cm.DictionaryPageOffset > 0 {
		e.writeFieldHeader(11, thriftI64, &lastFieldID)
		e.writeI64(cm.DictionaryPageOffset)
	}

	// field 12: statistics (optional)
	if cm.Statistics != nil {
		e.writeFieldHeader(12, thriftStruct, &lastFieldID)
		e.encodeStatistics(cm.Statistics)
	}

	e.writeStop()
}

func (e *thriftEncoder) encodeStatistics(s *Statistics) {
	var lastFieldID int16

	// field 3: null_count (i64)
	e.writeFieldHeader(3, thriftI64, &lastFieldID)
	e.writeI64(s.NullCount)

	// field 5: max_value (binary, v2 order-aware)
	if len(s.MaxValue) > 0 {
		e.writeFieldHeader(5, thriftBinary, &lastFieldID)
		e.writeBinary(s.MaxValue)
	}

	// field 6: min_value (binary, v2 order-aware)
	if len(s.MinValue) > 0 {
		e.writeFieldHeader(6, thriftBinary, &lastFieldID)
		e.writeBinary(s.MinValue)
	}

	e.writeStop()
}

func (e *thriftEncoder) encodeKeyValue(kv *KeyValue) {
	var lastFieldID int16
	e.writeFieldHeader(1, thriftBinary, &lastFieldID)
	e.writeString(kv.Key)
	e.writeFieldHeader(2, thriftBinary, &lastFieldID)
	e.writeString(kv.Value)
	e.writeStop()
}

// EncodePageHeader encodes a PageHeader to Thrift compact protocol.
func EncodePageHeader(ph *PageHeader) []byte {
	e := newThriftEncoder()
	var lastFieldID int16

	// field 1: type (i32 enum)
	e.writeFieldHeader(1, thriftI32, &lastFieldID)
	e.writeI32(int32(ph.Type))

	// field 2: uncompressed_page_size (i32)
	e.writeFieldHeader(2, thriftI32, &lastFieldID)
	e.writeI32(ph.UncompressedPageSize)

	// field 3: compressed_page_size (i32)
	e.writeFieldHeader(3, thriftI32, &lastFieldID)
	e.writeI32(ph.CompressedPageSize)

	// field 5: data_page_header (optional)
	if ph.DataPageHeader != nil {
		e.writeFieldHeader(5, thriftStruct, &lastFieldID)
		e.encodeDataPageHeader(ph.DataPageHeader)
	}

	// field 7: dictionary_page_header (optional)
	if ph.DictionaryPageHeader != nil {
		e.writeFieldHeader(7, thriftStruct, &lastFieldID)
		e.encodeDictionaryPageHeader(ph.DictionaryPageHeader)
	}

	e.writeStop()
	return e.Bytes()
}

func (e *thriftEncoder) encodeDataPageHeader(dph *DataPageHeader) {
	var lastFieldID int16

	// field 1: num_values (i32)
	e.writeFieldHeader(1, thriftI32, &lastFieldID)
	e.writeI32(dph.NumValues)

	// field 2: encoding (i32 enum)
	e.writeFieldHeader(2, thriftI32, &lastFieldID)
	e.writeI32(int32(dph.Encoding))

	// field 3: definition_level_encoding (i32 enum)
	e.writeFieldHeader(3, thriftI32, &lastFieldID)
	e.writeI32(int32(dph.DefinitionLevelEncoding))

	// field 4: repetition_level_encoding (i32 enum)
	e.writeFieldHeader(4, thriftI32, &lastFieldID)
	e.writeI32(int32(dph.RepetitionLevelEncoding))

	// field 5: statistics (optional)
	if dph.Statistics != nil {
		e.writeFieldHeader(5, thriftStruct, &lastFieldID)
		e.encodeStatistics(dph.Statistics)
	}

	e.writeStop()
}

func (e *thriftEncoder) encodeDictionaryPageHeader(dph *DictionaryPageHeader) {
	var lastFieldID int16

	// field 1: num_values (i32)
	e.writeFieldHeader(1, thriftI32, &lastFieldID)
	e.writeI32(dph.NumValues)

	// field 2: encoding (i32 enum)
	e.writeFieldHeader(2, thriftI32, &lastFieldID)
	e.writeI32(int32(dph.Encoding))

	e.writeStop()
}
