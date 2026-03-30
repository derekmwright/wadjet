package parquet

// Parquet Thrift metadata structures.
// These mirror the Apache Parquet format specification (parquet.thrift)
// and will be populated by our Thrift compact protocol decoder.
//
// Field IDs in comments correspond to Thrift field IDs for decoding.

// FileMetaData is the root metadata structure stored in the Parquet file footer.
type FileMetaData struct {
	Version          int32           // field 1: Parquet format version (1 or 2)
	Schema           []SchemaElement // field 2: flattened schema tree (depth-first)
	NumRows          int64           // field 3: total rows in file
	RowGroups        []RowGroup      // field 4: row group metadata
	KeyValueMetadata []KeyValue      // field 5: optional user metadata
	CreatedBy        string          // field 6: creator library identifier
	ColumnOrders     []ColumnOrder   // field 7: column sort orders (v2)
}

// SchemaElement describes a single node in the flattened schema tree.
// Leaf nodes have Type set; group nodes have NumChildren > 0.
type SchemaElement struct {
	Type           *PhysicalType       // field 1: physical type (nil for group nodes)
	TypeLength     int32               // field 2: byte width for FIXED_LEN_BYTE_ARRAY
	RepetitionType FieldRepetitionType // field 3: REQUIRED, OPTIONAL, REPEATED
	Name           string              // field 4: field name
	NumChildren    int32               // field 5: number of child elements (0 for leaf)
	ConvertedType  *ConvertedType      // field 6: deprecated logical type
	Scale          int32               // field 7: DECIMAL scale
	Precision      int32               // field 8: DECIMAL precision
	FieldID        int32               // field 9: Thrift field ID
	LogicalType    *LogicalType        // field 10: new-style logical type
}

// RowGroup contains metadata for a horizontal partition of rows.
type RowGroup struct {
	Columns       []ColumnChunk // field 1: column chunk metadata
	TotalByteSize int64         // field 2: uncompressed size of all column data
	NumRows       int64         // field 3: number of rows in this row group
	SortingColumns []SortingColumn // field 4: sort order of rows (v2)
	FileOffset    int64         // field 5: byte offset of row group in file
	TotalCompressedSize int64   // field 6: compressed size of all column data
	Ordinal       int16         // field 7: row group ordinal in file
}

// ColumnChunk contains metadata for a single column within a row group.
type ColumnChunk struct {
	FilePath string          // field 1: relative path (nil if same file)
	FileOffset int64         // field 2: byte offset of column chunk in file
	MetaData *ColumnMetaData // field 3: inline column metadata (always present in practice)
}

// ColumnMetaData describes the encoding, compression, and location of column data.
type ColumnMetaData struct {
	Type                  PhysicalType     // field 1: physical type
	Encodings             []Encoding       // field 2: encodings used in this column
	PathInSchema          []string         // field 3: column path from root
	Codec                 CompressionCodec // field 4: compression codec
	NumValues             int64            // field 5: total values (including nulls)
	TotalUncompressedSize int64            // field 6: uncompressed byte size
	TotalCompressedSize   int64            // field 7: compressed byte size
	// field 8: key_value_metadata (skipped during decode)
	DataPageOffset        int64            // field 9: byte offset of first data page
	IndexPageOffset       int64            // field 10: byte offset of index page (0 if none)
	DictionaryPageOffset  int64            // field 11: byte offset of dictionary page (0 if none)
	Statistics            *Statistics      // field 12: column statistics
	EncodingStats         []PageEncodingStats // field 13: per-encoding page counts
}

// PageHeader is the header prepended to every Parquet page.
type PageHeader struct {
	Type                 PageType              // field 1: page type
	UncompressedPageSize int32                 // field 2: uncompressed byte size
	CompressedPageSize   int32                 // field 3: compressed byte size
	CRC                  int32                 // field 4: CRC32 checksum (0 if not set)
	DataPageHeader       *DataPageHeader       // field 5: data page v1 header
	IndexPageHeader      *IndexPageHeader      // field 6: index page header
	DictionaryPageHeader *DictionaryPageHeader // field 7: dictionary page header
	DataPageHeaderV2     *DataPageHeaderV2     // field 8: data page v2 header
}

// DataPageHeader is the header for a data page (format v1).
type DataPageHeader struct {
	NumValues              int32    // field 1: number of values in page
	Encoding               Encoding // field 2: encoding of values
	DefinitionLevelEncoding Encoding // field 3: encoding of definition levels
	RepetitionLevelEncoding Encoding // field 4: encoding of repetition levels
	Statistics             *Statistics // field 5: page-level statistics
}

// DataPageHeaderV2 is the header for a data page (format v2).
type DataPageHeaderV2 struct {
	NumValues              int32    // field 1: number of values
	NumNulls               int32    // field 2: number of null values
	NumRows                int32    // field 3: number of rows
	Encoding               Encoding // field 4: encoding of values
	DefinitionLevelsByteLength int32 // field 5: byte length of definition levels
	RepetitionLevelsByteLength int32 // field 6: byte length of repetition levels
	IsCompressed           bool     // field 7: whether data section is compressed (default true)
	Statistics             *Statistics // field 8: page-level statistics
}

// DictionaryPageHeader is the header for a dictionary page.
type DictionaryPageHeader struct {
	NumValues int32    // field 1: number of dictionary entries
	Encoding  Encoding // field 2: encoding of dictionary values (always PLAIN)
	IsSorted  bool     // field 3: whether dictionary is sorted
}

// IndexPageHeader is a placeholder for index page headers (currently unused).
type IndexPageHeader struct{}

// Statistics holds optional min/max/null statistics for a column or page.
type Statistics struct {
	Max       []byte // field 1: deprecated max value (not order-aware)
	Min       []byte // field 2: deprecated min value (not order-aware)
	NullCount int64  // field 3: number of null values
	DistinctCount int64  // field 4: approximate distinct count
	MaxValue  []byte // field 5: max value (order-aware, v2)
	MinValue  []byte // field 6: min value (order-aware, v2)
	IsMaxValueExact bool // field 7: whether MaxValue is exact
	IsMinValueExact bool // field 8: whether MinValue is exact
}

// KeyValue is a key-value pair for user metadata.
type KeyValue struct {
	Key   string // field 1
	Value string // field 2
}

// SortingColumn describes the sort order of a column within a row group.
type SortingColumn struct {
	ColumnIdx  int32 // field 1: column index in schema
	Descending bool  // field 2: descending sort order
	NullsFirst bool  // field 3: nulls sort first
}

// ColumnOrder defines the sort order used for min/max statistics.
type ColumnOrder struct {
	TypeOrder *TypeDefinedOrder // field 1: use type-natural ordering
}

// TypeDefinedOrder indicates statistics use the type's natural ordering.
type TypeDefinedOrder struct{}

// PageEncodingStats tracks page counts per encoding type.
type PageEncodingStats struct {
	PageType PageType // field 1
	Encoding Encoding // field 2
	Count    int32    // field 3
}
