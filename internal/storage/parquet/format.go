package parquet

// Parquet format constants and types.
// These mirror the Apache Parquet format specification (Thrift definitions)
// and replace the dependency on github.com/parquet-go/parquet-go/format.

// PhysicalType is the physical storage type of a Parquet column.
type PhysicalType int32

const (
	PhysicalBoolean           PhysicalType = 0
	PhysicalInt32             PhysicalType = 1
	PhysicalInt64             PhysicalType = 2
	PhysicalInt96             PhysicalType = 3 // deprecated, used by Impala for timestamps
	PhysicalFloat             PhysicalType = 4
	PhysicalDouble            PhysicalType = 5
	PhysicalByteArray         PhysicalType = 6
	PhysicalFixedLenByteArray PhysicalType = 7
)

func (t PhysicalType) String() string {
	switch t {
	case PhysicalBoolean:
		return "BOOLEAN"
	case PhysicalInt32:
		return "INT32"
	case PhysicalInt64:
		return "INT64"
	case PhysicalInt96:
		return "INT96"
	case PhysicalFloat:
		return "FLOAT"
	case PhysicalDouble:
		return "DOUBLE"
	case PhysicalByteArray:
		return "BYTE_ARRAY"
	case PhysicalFixedLenByteArray:
		return "FIXED_LEN_BYTE_ARRAY"
	default:
		return "UNKNOWN"
	}
}

// ByteWidth returns the fixed byte width for fixed-size types.
// Returns 0 for variable-length types (BYTE_ARRAY, BOOLEAN).
func (t PhysicalType) ByteWidth() int {
	switch t {
	case PhysicalBoolean:
		return 0 // bit-packed
	case PhysicalInt32, PhysicalFloat:
		return 4
	case PhysicalInt64, PhysicalDouble:
		return 8
	case PhysicalInt96:
		return 12
	default:
		return 0
	}
}

// Encoding identifies the encoding used for Parquet data.
type Encoding int32

const (
	EncodingPlain                Encoding = 0
	EncodingGroupVarInt          Encoding = 1 // deprecated
	EncodingPlainDictionary      Encoding = 2
	EncodingRLE                  Encoding = 3
	EncodingBitPacked            Encoding = 4 // deprecated
	EncodingDeltaBinaryPacked    Encoding = 5
	EncodingDeltaLengthByteArray Encoding = 6
	EncodingDeltaByteArray       Encoding = 7
	EncodingRLEDictionary        Encoding = 8
	EncodingByteStreamSplit      Encoding = 9
)

func (e Encoding) String() string {
	switch e {
	case EncodingPlain:
		return "PLAIN"
	case EncodingPlainDictionary:
		return "PLAIN_DICTIONARY"
	case EncodingRLE:
		return "RLE"
	case EncodingBitPacked:
		return "BIT_PACKED"
	case EncodingDeltaBinaryPacked:
		return "DELTA_BINARY_PACKED"
	case EncodingDeltaLengthByteArray:
		return "DELTA_LENGTH_BYTE_ARRAY"
	case EncodingDeltaByteArray:
		return "DELTA_BYTE_ARRAY"
	case EncodingRLEDictionary:
		return "RLE_DICTIONARY"
	case EncodingByteStreamSplit:
		return "BYTE_STREAM_SPLIT"
	default:
		return "UNKNOWN"
	}
}

// CompressionCodec identifies the compression algorithm in Parquet Thrift metadata.
// Values match the Parquet format specification. This is distinct from the
// higher-level Compression type in writer.go which is a user-facing config enum.
type CompressionCodec int32

const (
	CodecNone   CompressionCodec = 0
	CodecSnappy CompressionCodec = 1
	CodecGzip   CompressionCodec = 2
	CodecLZ4    CompressionCodec = 3
	CodecZstd   CompressionCodec = 4
	CodecLZ4Raw CompressionCodec = 7
)

func (c CompressionCodec) String() string {
	switch c {
	case CodecNone:
		return "UNCOMPRESSED"
	case CodecSnappy:
		return "SNAPPY"
	case CodecGzip:
		return "GZIP"
	case CodecLZ4:
		return "LZ4"
	case CodecZstd:
		return "ZSTD"
	case CodecLZ4Raw:
		return "LZ4_RAW"
	default:
		return "UNKNOWN"
	}
}

// PageType identifies the type of a Parquet page.
type PageType int32

const (
	PageDataV1     PageType = 0
	PageIndex      PageType = 1
	PageDictionary PageType = 2
	PageDataV2     PageType = 3
)

// FieldRepetitionType defines the repetition of a schema element.
type FieldRepetitionType int32

const (
	FieldRequired FieldRepetitionType = 0
	FieldOptional FieldRepetitionType = 1
	FieldRepeated FieldRepetitionType = 2
)

// ConvertedType is the deprecated logical type annotation.
type ConvertedType int32

const (
	ConvertedUTF8             ConvertedType = 0
	ConvertedMap              ConvertedType = 1
	ConvertedMapKeyValue      ConvertedType = 2
	ConvertedList             ConvertedType = 3
	ConvertedEnum             ConvertedType = 4
	ConvertedDecimal          ConvertedType = 5
	ConvertedDate             ConvertedType = 6
	ConvertedTimeMillis       ConvertedType = 7
	ConvertedTimeMicros       ConvertedType = 8
	ConvertedTimestampMillis  ConvertedType = 9
	ConvertedTimestampMicros  ConvertedType = 10
	ConvertedUint8            ConvertedType = 11
	ConvertedUint16           ConvertedType = 12
	ConvertedUint32           ConvertedType = 13
	ConvertedUint64           ConvertedType = 14
	ConvertedInt8             ConvertedType = 15
	ConvertedInt16            ConvertedType = 16
	ConvertedInt32            ConvertedType = 17
	ConvertedInt64            ConvertedType = 18
	ConvertedJSON             ConvertedType = 19
	ConvertedBSON             ConvertedType = 20
	ConvertedInterval         ConvertedType = 21
	ConvertedNone             ConvertedType = -1
)

// LogicalTypeID identifies the new-style logical type.
type LogicalTypeID int

const (
	LogicalNone LogicalTypeID = iota
	LogicalString
	LogicalMap
	LogicalList
	LogicalEnum
	LogicalDecimal
	LogicalDate
	LogicalTimeMillis
	LogicalTimeMicros
	LogicalTimestampMillis
	LogicalTimestampMicros
	LogicalTimestampNanos
	LogicalInteger
	LogicalNull
	LogicalJSON
	LogicalBSON
	LogicalUUID
	LogicalFloat16
)

// LogicalType carries the new-style logical type with parameters.
type LogicalType struct {
	Type      LogicalTypeID
	BitWidth  int  // for INTEGER: 8, 16, 32, 64
	IsSigned  bool // for INTEGER
	Precision int  // for DECIMAL
	Scale     int  // for DECIMAL
	IsAdjustedToUTC bool // for TIMESTAMP/TIME
}
