package parquet

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Thrift Compact Protocol decoder for Parquet metadata.
//
// Parquet serializes FileMetaData (footer) and PageHeader using Thrift
// Compact Protocol. This decoder reads those structures directly from
// raw bytes without depending on a Thrift code generator.
//
// Reference: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md

// thriftType represents a Thrift compact protocol type ID.
type thriftType byte

const (
	thriftStop        thriftType = 0
	thriftBoolTrue    thriftType = 1
	thriftBoolFalse   thriftType = 2
	thriftByte        thriftType = 3
	thriftI16         thriftType = 4
	thriftI32         thriftType = 5
	thriftI64         thriftType = 6
	thriftDouble      thriftType = 7
	thriftBinary      thriftType = 8
	thriftList        thriftType = 9
	thriftSet         thriftType = 10
	thriftMap         thriftType = 11
	thriftStruct      thriftType = 12
)

// thriftDecoder reads Thrift compact protocol data from a byte slice.
type thriftDecoder struct {
	buf []byte
	off int
}

func newThriftDecoder(data []byte) *thriftDecoder {
	return &thriftDecoder{buf: data}
}

// Offset returns the current read position.
func (d *thriftDecoder) Offset() int { return d.off }

// remaining returns how many bytes are left unread.
func (d *thriftDecoder) remaining() int { return len(d.buf) - d.off }

// readByte reads a single byte.
func (d *thriftDecoder) readByte() (byte, error) {
	if d.off >= len(d.buf) {
		return 0, fmt.Errorf("thrift: unexpected EOF at offset %d", d.off)
	}
	b := d.buf[d.off]
	d.off++
	return b, nil
}

// readBytes reads n bytes.
func (d *thriftDecoder) readBytes(n int) ([]byte, error) {
	if d.off+n > len(d.buf) {
		return nil, fmt.Errorf("thrift: need %d bytes at offset %d, have %d", n, d.off, d.remaining())
	}
	b := d.buf[d.off : d.off+n]
	d.off += n
	return b, nil
}

// readVarint reads a varint-encoded unsigned integer.
func (d *thriftDecoder) readVarint() (uint64, error) {
	var result uint64
	var shift uint
	for {
		if d.off >= len(d.buf) {
			return 0, fmt.Errorf("thrift: varint truncated at offset %d", d.off)
		}
		b := d.buf[d.off]
		d.off++
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return result, nil
		}
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("thrift: varint overflow at offset %d", d.off)
		}
	}
}

// readZigzag reads a zigzag-encoded signed integer as int64.
func (d *thriftDecoder) readZigzag() (int64, error) {
	v, err := d.readVarint()
	if err != nil {
		return 0, err
	}
	return zigzagDecode(v), nil
}

// zigzagDecode converts a zigzag-encoded uint64 to signed int64.
func zigzagDecode(n uint64) int64 {
	return int64(n>>1) ^ -int64(n&1)
}

// readI16 reads a zigzag-encoded 16-bit integer.
func (d *thriftDecoder) readI16() (int16, error) {
	v, err := d.readZigzag()
	if err != nil {
		return 0, err
	}
	return int16(v), nil
}

// readI32 reads a zigzag-encoded 32-bit integer.
func (d *thriftDecoder) readI32() (int32, error) {
	v, err := d.readZigzag()
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

// readI64 reads a zigzag-encoded 64-bit integer.
func (d *thriftDecoder) readI64() (int64, error) {
	return d.readZigzag()
}

// readDouble reads a 64-bit IEEE 754 double.
func (d *thriftDecoder) readDouble() (float64, error) {
	b, err := d.readBytes(8)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil
}

// readBinary reads a length-prefixed byte sequence.
func (d *thriftDecoder) readBinary() ([]byte, error) {
	length, err := d.readVarint()
	if err != nil {
		return nil, fmt.Errorf("thrift: reading binary length: %w", err)
	}
	if length > uint64(d.remaining()) {
		return nil, fmt.Errorf("thrift: binary length %d exceeds remaining %d at offset %d", length, d.remaining(), d.off)
	}
	return d.readBytes(int(length))
}

// readString reads a length-prefixed UTF-8 string.
func (d *thriftDecoder) readString() (string, error) {
	b, err := d.readBinary()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// readFieldHeader reads the next field header, returning the field ID and type.
// Returns thriftStop when the struct end marker is reached.
func (d *thriftDecoder) readFieldHeader(lastFieldID *int16) (int16, thriftType, error) {
	b, err := d.readByte()
	if err != nil {
		return 0, 0, err
	}
	if b == 0 {
		return 0, thriftStop, nil
	}

	fieldType := thriftType(b & 0x0F)
	delta := int16((b >> 4) & 0x0F)

	var fieldID int16
	if delta != 0 {
		// Short form: delta encoded field ID.
		fieldID = *lastFieldID + delta
	} else {
		// Long form: full field ID follows as zigzag i16.
		fieldID, err = d.readI16()
		if err != nil {
			return 0, 0, fmt.Errorf("thrift: reading field ID: %w", err)
		}
	}

	*lastFieldID = fieldID
	return fieldID, fieldType, nil
}

// readListHeader reads a list header, returning the element type and count.
// maxThriftCollectionSize prevents OOM from malicious/corrupted metadata.
// A Parquet file with millions of schema elements or row groups is not realistic.
const maxThriftCollectionSize = 1_000_000

func (d *thriftDecoder) readListHeader() (thriftType, int, error) {
	b, err := d.readByte()
	if err != nil {
		return 0, 0, err
	}
	elemType := thriftType(b & 0x0F)
	size := int((b >> 4) & 0x0F)
	if size == 15 {
		// Large list: size follows as varint.
		n, err := d.readVarint()
		if err != nil {
			return 0, 0, fmt.Errorf("thrift: reading list size: %w", err)
		}
		size = int(n)
	}
	if size < 0 || size > maxThriftCollectionSize {
		return 0, 0, fmt.Errorf("thrift: list size %d exceeds maximum %d", size, maxThriftCollectionSize)
	}
	return elemType, size, nil
}

// skipField skips over a field value of the given type.
func (d *thriftDecoder) skipField(t thriftType) error {
	switch t {
	case thriftBoolTrue, thriftBoolFalse:
		return nil // no data bytes
	case thriftByte:
		_, err := d.readByte()
		return err
	case thriftI16:
		_, err := d.readVarint()
		return err
	case thriftI32:
		_, err := d.readVarint()
		return err
	case thriftI64:
		_, err := d.readVarint()
		return err
	case thriftDouble:
		_, err := d.readBytes(8)
		return err
	case thriftBinary:
		_, err := d.readBinary()
		return err
	case thriftList, thriftSet:
		elemType, size, err := d.readListHeader()
		if err != nil {
			return err
		}
		for i := 0; i < size; i++ {
			if err := d.skipField(elemType); err != nil {
				return err
			}
		}
		return nil
	case thriftMap:
		// Compact protocol map: varint(size), then if size > 0:
		// one byte (keyType<<4 | valType), then size key-value pairs.
		size, err := d.readVarint()
		if err != nil {
			return err
		}
		if size == 0 {
			return nil // empty map
		}
		if size > maxThriftCollectionSize {
			return fmt.Errorf("thrift: map size %d exceeds maximum %d", size, maxThriftCollectionSize)
		}
		b, err := d.readByte()
		if err != nil {
			return err
		}
		keyType := thriftType((b >> 4) & 0x0F)
		valType := thriftType(b & 0x0F)
		for i := uint64(0); i < size; i++ {
			if err := d.skipField(keyType); err != nil {
				return err
			}
			if err := d.skipField(valType); err != nil {
				return err
			}
		}
		return nil
	case thriftStruct:
		return d.skipStruct()
	default:
		return fmt.Errorf("thrift: cannot skip unknown type %d at offset %d", t, d.off)
	}
}

// skipStruct skips over an entire struct.
func (d *thriftDecoder) skipStruct() error {
	var lastFieldID int16
	for {
		_, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		if err := d.skipField(ft); err != nil {
			return err
		}
	}
}

// --- Parquet metadata decoders ---

// DecodeFileMetaData decodes a Thrift-encoded FileMetaData from raw bytes.
func DecodeFileMetaData(data []byte) (*FileMetaData, error) {
	d := newThriftDecoder(data)
	md := &FileMetaData{}
	var lastFieldID int16

	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return nil, fmt.Errorf("decoding FileMetaData: %w", err)
		}
		if ft == thriftStop {
			break
		}

		switch fieldID {
		case 1: // version: i32
			v, err := d.readI32()
			if err != nil {
				return nil, fmt.Errorf("decoding FileMetaData.version: %w", err)
			}
			md.Version = v
		case 2: // schema: list<SchemaElement>
			_, size, err := d.readListHeader()
			if err != nil {
				return nil, fmt.Errorf("decoding FileMetaData.schema list: %w", err)
			}
			md.Schema = make([]SchemaElement, size)
			for i := 0; i < size; i++ {
				if err := d.decodeSchemaElement(&md.Schema[i]); err != nil {
					return nil, fmt.Errorf("decoding schema[%d]: %w", i, err)
				}
			}
		case 3: // num_rows: i64
			v, err := d.readI64()
			if err != nil {
				return nil, fmt.Errorf("decoding FileMetaData.num_rows: %w", err)
			}
			md.NumRows = v
		case 4: // row_groups: list<RowGroup>
			_, size, err := d.readListHeader()
			if err != nil {
				return nil, fmt.Errorf("decoding FileMetaData.row_groups list: %w", err)
			}
			md.RowGroups = make([]RowGroup, size)
			for i := 0; i < size; i++ {
				if err := d.decodeRowGroup(&md.RowGroups[i]); err != nil {
					return nil, fmt.Errorf("decoding row_group[%d]: %w", i, err)
				}
			}
		case 5: // key_value_metadata: list<KeyValue>
			_, size, err := d.readListHeader()
			if err != nil {
				return nil, fmt.Errorf("decoding FileMetaData.kv_metadata list: %w", err)
			}
			md.KeyValueMetadata = make([]KeyValue, size)
			for i := 0; i < size; i++ {
				if err := d.decodeKeyValue(&md.KeyValueMetadata[i]); err != nil {
					return nil, fmt.Errorf("decoding kv_metadata[%d]: %w", i, err)
				}
			}
		case 6: // created_by: string
			v, err := d.readString()
			if err != nil {
				return nil, fmt.Errorf("decoding FileMetaData.created_by: %w", err)
			}
			md.CreatedBy = v
		case 7: // column_orders: list<ColumnOrder>
			_, size, err := d.readListHeader()
			if err != nil {
				return nil, fmt.Errorf("decoding FileMetaData.column_orders list: %w", err)
			}
			md.ColumnOrders = make([]ColumnOrder, size)
			for i := 0; i < size; i++ {
				if err := d.decodeColumnOrder(&md.ColumnOrders[i]); err != nil {
					return nil, fmt.Errorf("decoding column_order[%d]: %w", i, err)
				}
			}
		default:
			if err := d.skipField(ft); err != nil {
				return nil, fmt.Errorf("skipping FileMetaData field %d: %w", fieldID, err)
			}
		}
	}
	return md, nil
}

func (d *thriftDecoder) decodeSchemaElement(se *SchemaElement) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // type: PhysicalType (i32 enum)
			v, err := d.readI32()
			if err != nil {
				return err
			}
			pt := PhysicalType(v)
			se.Type = &pt
		case 2: // type_length: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			se.TypeLength = v
		case 3: // repetition_type: FieldRepetitionType (i32 enum)
			v, err := d.readI32()
			if err != nil {
				return err
			}
			se.RepetitionType = FieldRepetitionType(v)
		case 4: // name: string
			v, err := d.readString()
			if err != nil {
				return err
			}
			se.Name = v
		case 5: // num_children: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			se.NumChildren = v
		case 6: // converted_type: ConvertedType (i32 enum)
			v, err := d.readI32()
			if err != nil {
				return err
			}
			ct := ConvertedType(v)
			se.ConvertedType = &ct
		case 7: // scale: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			se.Scale = v
		case 8: // precision: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			se.Precision = v
		case 9: // field_id: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			se.FieldID = v
		case 10: // logicalType: LogicalType (struct)
			lt, err := d.decodeLogicalType()
			if err != nil {
				return err
			}
			se.LogicalType = lt
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

func (d *thriftDecoder) decodeLogicalType() (*LogicalType, error) {
	// LogicalType in Thrift is a union — only one field is set.
	// Field IDs: 1=STRING, 2=MAP, 3=LIST, 4=ENUM, 5=DECIMAL,
	// 6=DATE, 7=TIME, 8=TIMESTAMP, 10=INTEGER, 11=NULL,
	// 12=JSON, 13=BSON, 14=UUID, 15=FLOAT16
	lt := &LogicalType{}
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return nil, err
		}
		if ft == thriftStop {
			return lt, nil
		}
		switch fieldID {
		case 1: // STRING
			lt.Type = LogicalString
			if err := d.skipField(ft); err != nil {
				return nil, err
			}
		case 2: // MAP
			lt.Type = LogicalMap
			if err := d.skipField(ft); err != nil {
				return nil, err
			}
		case 3: // LIST
			lt.Type = LogicalList
			if err := d.skipField(ft); err != nil {
				return nil, err
			}
		case 4: // ENUM
			lt.Type = LogicalEnum
			if err := d.skipField(ft); err != nil {
				return nil, err
			}
		case 5: // DECIMAL {scale, precision}
			lt.Type = LogicalDecimal
			if err := d.decodeDecimalLogicalType(lt); err != nil {
				return nil, err
			}
		case 6: // DATE
			lt.Type = LogicalDate
			if err := d.skipField(ft); err != nil {
				return nil, err
			}
		case 7: // TIME {isAdjustedToUTC, unit}
			if err := d.decodeTimeLogicalType(lt); err != nil {
				return nil, err
			}
		case 8: // TIMESTAMP {isAdjustedToUTC, unit}
			if err := d.decodeTimestampLogicalType(lt); err != nil {
				return nil, err
			}
		case 10: // INTEGER {bitWidth, isSigned}
			lt.Type = LogicalInteger
			if err := d.decodeIntegerLogicalType(lt); err != nil {
				return nil, err
			}
		case 11: // NULL
			lt.Type = LogicalNull
			if err := d.skipField(ft); err != nil {
				return nil, err
			}
		case 12: // JSON
			lt.Type = LogicalJSON
			if err := d.skipField(ft); err != nil {
				return nil, err
			}
		case 13: // BSON
			lt.Type = LogicalBSON
			if err := d.skipField(ft); err != nil {
				return nil, err
			}
		case 14: // UUID
			lt.Type = LogicalUUID
			if err := d.skipField(ft); err != nil {
				return nil, err
			}
		case 15: // FLOAT16
			lt.Type = LogicalFloat16
			if err := d.skipField(ft); err != nil {
				return nil, err
			}
		case 100: // VECTOR {dimension} — wadjet extension
			lt.Type = LogicalVector
			if err := d.decodeVectorLogicalType(lt); err != nil {
				return nil, err
			}
		default:
			if err := d.skipField(ft); err != nil {
				return nil, err
			}
		}
	}
}

func (d *thriftDecoder) decodeDecimalLogicalType(lt *LogicalType) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // scale: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			lt.Scale = int(v)
		case 2: // precision: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			lt.Precision = int(v)
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

func (d *thriftDecoder) decodeVectorLogicalType(lt *LogicalType) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // dimension: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			lt.Dimension = int(v)
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

func (d *thriftDecoder) decodeTimeLogicalType(lt *LogicalType) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // isAdjustedToUTC: bool
			lt.IsAdjustedToUTC = (ft == thriftBoolTrue)
		case 2: // unit: TimeUnit (union)
			unit, err := d.decodeTimeUnit()
			if err != nil {
				return err
			}
			switch unit {
			case 1: // MILLIS
				lt.Type = LogicalTimeMillis
			case 2: // MICROS
				lt.Type = LogicalTimeMicros
			}
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

func (d *thriftDecoder) decodeTimestampLogicalType(lt *LogicalType) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // isAdjustedToUTC: bool
			lt.IsAdjustedToUTC = (ft == thriftBoolTrue)
		case 2: // unit: TimeUnit (union)
			unit, err := d.decodeTimeUnit()
			if err != nil {
				return err
			}
			switch unit {
			case 1:
				lt.Type = LogicalTimestampMillis
			case 2:
				lt.Type = LogicalTimestampMicros
			case 3:
				lt.Type = LogicalTimestampNanos
			}
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

// decodeTimeUnit reads a TimeUnit union and returns 1=MILLIS, 2=MICROS, 3=NANOS.
func (d *thriftDecoder) decodeTimeUnit() (int, error) {
	var lastFieldID int16
	unit := 0
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return 0, err
		}
		if ft == thriftStop {
			return unit, nil
		}
		switch fieldID {
		case 1: // MILLIS
			unit = 1
			if err := d.skipField(ft); err != nil {
				return 0, err
			}
		case 2: // MICROS
			unit = 2
			if err := d.skipField(ft); err != nil {
				return 0, err
			}
		case 3: // NANOS
			unit = 3
			if err := d.skipField(ft); err != nil {
				return 0, err
			}
		default:
			if err := d.skipField(ft); err != nil {
				return 0, err
			}
		}
	}
}

func (d *thriftDecoder) decodeIntegerLogicalType(lt *LogicalType) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // bitWidth: byte
			b, err := d.readByte()
			if err != nil {
				return err
			}
			lt.BitWidth = int(b)
		case 2: // isSigned: bool
			lt.IsSigned = (ft == thriftBoolTrue)
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

func (d *thriftDecoder) decodeRowGroup(rg *RowGroup) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // columns: list<ColumnChunk>
			_, size, err := d.readListHeader()
			if err != nil {
				return err
			}
			rg.Columns = make([]ColumnChunk, size)
			for i := 0; i < size; i++ {
				if err := d.decodeColumnChunk(&rg.Columns[i]); err != nil {
					return err
				}
			}
		case 2: // total_byte_size: i64
			v, err := d.readI64()
			if err != nil {
				return err
			}
			rg.TotalByteSize = v
		case 3: // num_rows: i64
			v, err := d.readI64()
			if err != nil {
				return err
			}
			rg.NumRows = v
		case 4: // sorting_columns: list<SortingColumn>
			_, size, err := d.readListHeader()
			if err != nil {
				return err
			}
			rg.SortingColumns = make([]SortingColumn, size)
			for i := 0; i < size; i++ {
				if err := d.decodeSortingColumn(&rg.SortingColumns[i]); err != nil {
					return err
				}
			}
		case 5: // file_offset: i64
			v, err := d.readI64()
			if err != nil {
				return err
			}
			rg.FileOffset = v
		case 6: // total_compressed_size: i64
			v, err := d.readI64()
			if err != nil {
				return err
			}
			rg.TotalCompressedSize = v
		case 7: // ordinal: i16
			v, err := d.readI16()
			if err != nil {
				return err
			}
			rg.Ordinal = v
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

func (d *thriftDecoder) decodeColumnChunk(cc *ColumnChunk) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // file_path: string
			v, err := d.readString()
			if err != nil {
				return err
			}
			cc.FilePath = v
		case 2: // file_offset: i64
			v, err := d.readI64()
			if err != nil {
				return err
			}
			cc.FileOffset = v
		case 3: // meta_data: ColumnMetaData (struct)
			cc.MetaData = &ColumnMetaData{}
			if err := d.decodeColumnMetaData(cc.MetaData); err != nil {
				return err
			}
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

func (d *thriftDecoder) decodeColumnMetaData(cm *ColumnMetaData) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // type: PhysicalType (i32 enum)
			v, err := d.readI32()
			if err != nil {
				return err
			}
			cm.Type = PhysicalType(v)
		case 2: // encodings: list<Encoding>
			_, size, err := d.readListHeader()
			if err != nil {
				return err
			}
			cm.Encodings = make([]Encoding, size)
			for i := 0; i < size; i++ {
				v, err := d.readI32()
				if err != nil {
					return err
				}
				cm.Encodings[i] = Encoding(v)
			}
		case 3: // path_in_schema: list<string>
			_, size, err := d.readListHeader()
			if err != nil {
				return err
			}
			cm.PathInSchema = make([]string, size)
			for i := 0; i < size; i++ {
				v, err := d.readString()
				if err != nil {
					return err
				}
				cm.PathInSchema[i] = v
			}
		case 4: // codec: CompressionCodec (i32 enum)
			v, err := d.readI32()
			if err != nil {
				return err
			}
			cm.Codec = CompressionCodec(v)
		case 5: // num_values: i64
			v, err := d.readI64()
			if err != nil {
				return err
			}
			cm.NumValues = v
		case 6: // total_uncompressed_size: i64
			v, err := d.readI64()
			if err != nil {
				return err
			}
			cm.TotalUncompressedSize = v
		case 7: // total_compressed_size: i64
			v, err := d.readI64()
			if err != nil {
				return err
			}
			cm.TotalCompressedSize = v
		case 8: // key_value_metadata: list<KeyValue> (skip, rarely used)
			if err := d.skipField(ft); err != nil {
				return err
			}
		case 9: // data_page_offset: i64
			v, err := d.readI64()
			if err != nil {
				return err
			}
			cm.DataPageOffset = v
		case 10: // index_page_offset: i64
			v, err := d.readI64()
			if err != nil {
				return err
			}
			cm.IndexPageOffset = v
		case 11: // dictionary_page_offset: i64
			v, err := d.readI64()
			if err != nil {
				return err
			}
			cm.DictionaryPageOffset = v
		case 12: // statistics: Statistics (struct)
			cm.Statistics = &Statistics{}
			if err := d.decodeStatistics(cm.Statistics); err != nil {
				return err
			}
		case 13: // encoding_stats: list<PageEncodingStats>
			_, size, err := d.readListHeader()
			if err != nil {
				return err
			}
			cm.EncodingStats = make([]PageEncodingStats, size)
			for i := 0; i < size; i++ {
				if err := d.decodePageEncodingStats(&cm.EncodingStats[i]); err != nil {
					return err
				}
			}
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

func (d *thriftDecoder) decodeStatistics(s *Statistics) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // max (deprecated)
			v, err := d.readBinary()
			if err != nil {
				return err
			}
			s.Max = v
		case 2: // min (deprecated)
			v, err := d.readBinary()
			if err != nil {
				return err
			}
			s.Min = v
		case 3: // null_count: i64
			v, err := d.readI64()
			if err != nil {
				return err
			}
			s.NullCount = v
		case 4: // distinct_count: i64
			v, err := d.readI64()
			if err != nil {
				return err
			}
			s.DistinctCount = v
		case 5: // max_value (order-aware)
			v, err := d.readBinary()
			if err != nil {
				return err
			}
			s.MaxValue = v
		case 6: // min_value (order-aware)
			v, err := d.readBinary()
			if err != nil {
				return err
			}
			s.MinValue = v
		case 7: // is_max_value_exact
			s.IsMaxValueExact = (ft == thriftBoolTrue)
		case 8: // is_min_value_exact
			s.IsMinValueExact = (ft == thriftBoolTrue)
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

func (d *thriftDecoder) decodeKeyValue(kv *KeyValue) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // key: string
			v, err := d.readString()
			if err != nil {
				return err
			}
			kv.Key = v
		case 2: // value: string
			v, err := d.readString()
			if err != nil {
				return err
			}
			kv.Value = v
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

func (d *thriftDecoder) decodeSortingColumn(sc *SortingColumn) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // column_idx: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			sc.ColumnIdx = v
		case 2: // descending: bool
			sc.Descending = (ft == thriftBoolTrue)
		case 3: // nulls_first: bool
			sc.NullsFirst = (ft == thriftBoolTrue)
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

func (d *thriftDecoder) decodeColumnOrder(co *ColumnOrder) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // TYPE_ORDER
			co.TypeOrder = &TypeDefinedOrder{}
			if err := d.skipField(ft); err != nil {
				return err
			}
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

func (d *thriftDecoder) decodePageEncodingStats(pe *PageEncodingStats) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // page_type: PageType (i32 enum)
			v, err := d.readI32()
			if err != nil {
				return err
			}
			pe.PageType = PageType(v)
		case 2: // encoding: Encoding (i32 enum)
			v, err := d.readI32()
			if err != nil {
				return err
			}
			pe.Encoding = Encoding(v)
		case 3: // count: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			pe.Count = v
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

// DecodePageHeader decodes a Thrift-encoded PageHeader from raw bytes.
// Returns the decoded header and the number of bytes consumed.
func DecodePageHeader(data []byte) (*PageHeader, int, error) {
	d := newThriftDecoder(data)
	ph := &PageHeader{}
	var lastFieldID int16

	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return nil, 0, fmt.Errorf("decoding PageHeader: %w", err)
		}
		if ft == thriftStop {
			break
		}
		switch fieldID {
		case 1: // type: PageType (i32 enum)
			v, err := d.readI32()
			if err != nil {
				return nil, 0, err
			}
			ph.Type = PageType(v)
		case 2: // uncompressed_page_size: i32
			v, err := d.readI32()
			if err != nil {
				return nil, 0, err
			}
			ph.UncompressedPageSize = v
		case 3: // compressed_page_size: i32
			v, err := d.readI32()
			if err != nil {
				return nil, 0, err
			}
			ph.CompressedPageSize = v
		case 4: // crc: i32
			v, err := d.readI32()
			if err != nil {
				return nil, 0, err
			}
			ph.CRC = v
		case 5: // data_page_header: DataPageHeader (struct)
			ph.DataPageHeader = &DataPageHeader{}
			if err := d.decodeDataPageHeader(ph.DataPageHeader); err != nil {
				return nil, 0, err
			}
		case 6: // index_page_header: IndexPageHeader (struct)
			ph.IndexPageHeader = &IndexPageHeader{}
			if err := d.skipStruct(); err != nil {
				return nil, 0, err
			}
		case 7: // dictionary_page_header: DictionaryPageHeader (struct)
			ph.DictionaryPageHeader = &DictionaryPageHeader{}
			if err := d.decodeDictionaryPageHeader(ph.DictionaryPageHeader); err != nil {
				return nil, 0, err
			}
		case 8: // data_page_header_v2: DataPageHeaderV2 (struct)
			ph.DataPageHeaderV2 = &DataPageHeaderV2{IsCompressed: true} // default
			if err := d.decodeDataPageHeaderV2(ph.DataPageHeaderV2); err != nil {
				return nil, 0, err
			}
		default:
			if err := d.skipField(ft); err != nil {
				return nil, 0, err
			}
		}
	}
	return ph, d.off, nil
}

func (d *thriftDecoder) decodeDataPageHeader(dph *DataPageHeader) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // num_values: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			dph.NumValues = v
		case 2: // encoding: Encoding (i32 enum)
			v, err := d.readI32()
			if err != nil {
				return err
			}
			dph.Encoding = Encoding(v)
		case 3: // definition_level_encoding: Encoding
			v, err := d.readI32()
			if err != nil {
				return err
			}
			dph.DefinitionLevelEncoding = Encoding(v)
		case 4: // repetition_level_encoding: Encoding
			v, err := d.readI32()
			if err != nil {
				return err
			}
			dph.RepetitionLevelEncoding = Encoding(v)
		case 5: // statistics: Statistics (struct)
			dph.Statistics = &Statistics{}
			if err := d.decodeStatistics(dph.Statistics); err != nil {
				return err
			}
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

func (d *thriftDecoder) decodeDictionaryPageHeader(dph *DictionaryPageHeader) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // num_values: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			dph.NumValues = v
		case 2: // encoding: Encoding (i32 enum)
			v, err := d.readI32()
			if err != nil {
				return err
			}
			dph.Encoding = Encoding(v)
		case 3: // is_sorted: bool
			dph.IsSorted = (ft == thriftBoolTrue)
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}

func (d *thriftDecoder) decodeDataPageHeaderV2(dph *DataPageHeaderV2) error {
	var lastFieldID int16
	for {
		fieldID, ft, err := d.readFieldHeader(&lastFieldID)
		if err != nil {
			return err
		}
		if ft == thriftStop {
			return nil
		}
		switch fieldID {
		case 1: // num_values: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			dph.NumValues = v
		case 2: // num_nulls: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			dph.NumNulls = v
		case 3: // num_rows: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			dph.NumRows = v
		case 4: // encoding: Encoding (i32 enum)
			v, err := d.readI32()
			if err != nil {
				return err
			}
			dph.Encoding = Encoding(v)
		case 5: // definition_levels_byte_length: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			dph.DefinitionLevelsByteLength = v
		case 6: // repetition_levels_byte_length: i32
			v, err := d.readI32()
			if err != nil {
				return err
			}
			dph.RepetitionLevelsByteLength = v
		case 7: // is_compressed: bool
			dph.IsCompressed = (ft == thriftBoolTrue)
		case 8: // statistics: Statistics (struct)
			dph.Statistics = &Statistics{}
			if err := d.decodeStatistics(dph.Statistics); err != nil {
				return err
			}
		default:
			if err := d.skipField(ft); err != nil {
				return err
			}
		}
	}
}
