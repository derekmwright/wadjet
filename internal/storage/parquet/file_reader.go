package parquet

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// FileReader provides direct access to a Parquet file using our custom
// Thrift decoder and page readers, without depending on parquet-go.
//
// Usage:
//
//	fr, err := OpenFileReader(readerAt, fileSize)
//	for rgIdx := range fr.NumRowGroups() {
//	    rg := fr.RowGroupMeta(rgIdx)
//	    for colIdx := range rg.Columns {
//	        pr := fr.ColumnPages(rgIdx, colIdx)
//	        dict, _ := pr.NextDictionary()
//	        for { page, _ := pr.NextPage(); if page == nil { break } ... }
//	    }
//	}
type FileReader struct {
	data       []byte          // entire file in memory; nil in staged (pread) mode
	src        io.ReaderAt     // staged mode: chunk reads issue ranged reads here
	size       int64           // staged mode: file size (chunk-range clamp)
	meta       *FileMetaData   // decoded footer
	schemaRoot *SchemaNode     // schema tree
	leaves     []*SchemaNode   // leaf nodes indexed by column position
	schema     Schema          // Wadjet-level schema (for compatibility)
}

// OpenFileReader opens a Parquet file from an io.ReaderAt.
// The entire file is read into memory for zero-copy page access.
func OpenFileReader(r io.ReaderAt, size int64) (*FileReader, error) {
	// Read footer metadata.
	meta, err := ReadFileMetaData(r, size)
	if err != nil {
		return nil, err
	}

	// Validate header.
	if err := ValidateHeader(r); err != nil {
		return nil, err
	}

	// Read the entire file into memory for page access.
	data := make([]byte, size)
	if _, err := r.ReadAt(data, 0); err != nil && err != io.EOF {
		return nil, fmt.Errorf("parquet: reading file data: %w", err)
	}

	root, leaves := BuildSchemaTree(meta.Schema)
	schema := schemaFromTree(root, leaves)

	return &FileReader{
		data:       data,
		meta:       meta,
		schemaRoot: root,
		leaves:     leaves,
		schema:     schema,
	}, nil
}

// OpenFileReaderMetadata opens a Parquet file in metadata-only mode by reading
// just the footer (typically a few KB) via the supplied io.ReaderAt — without
// loading the file's row group / page bytes into memory. The returned reader
// supports schema inspection, NumRows / NumRowGroups, RowGroupNumRows, and
// RowGroupStats, but its data-reading methods (ColumnPages, ReadRowGroup) will
// fail because there is no backing data buffer.
//
// Use this when planning a scan — to apply predicate / bloom / partition
// pruning across many files without paying the per-file heap cost of a full
// load. Once a row group is selected for actual reading, open the file again
// with OpenFileReader (or OpenFileReaderFromBytes) to get the page-decoding
// path.
//
// This is the structural fix for the SF100 OOM hunt: the previous scan path
// downloaded every parquet file in the worker's partition into a heap []byte
// before processing, which at SF100 lineitem (21 files × 283 MB per worker)
// added ~6 GB of heap pressure per scan.
func OpenFileReaderMetadata(r io.ReaderAt, size int64) (*FileReader, error) {
	meta, err := ReadFileMetaData(r, size)
	if err != nil {
		return nil, err
	}
	if err := ValidateHeader(r); err != nil {
		return nil, err
	}
	root, leaves := BuildSchemaTree(meta.Schema)
	schema := schemaFromTree(root, leaves)
	return &FileReader{
		data:       nil, // metadata-only — page reads will fail
		meta:       meta,
		schemaRoot: root,
		leaves:     leaves,
		schema:     schema,
	}, nil
}

// OpenFileReaderAt opens a Parquet file in staged (pread) mode: only the
// footer is read eagerly; each ColumnPages call reads exactly that column
// chunk's byte range from r into a pooled buffer released on the page
// reader's Close (docs/design/scan-pread-reads.md). Unlike OpenFileReader
// there is never a whole-file buffer — and unlike an mmap of the file,
// decode goroutines never take page faults: the I/O happens in ranged
// read syscalls, which park at GC-safe points instead of stretching
// stop-the-world pauses.
//
// r must support concurrent ReadAt (an *os.File does): row groups decode
// in parallel, one staged chunk per projected column.
func OpenFileReaderAt(r io.ReaderAt, size int64) (*FileReader, error) {
	meta, err := ReadFileMetaData(r, size)
	if err != nil {
		return nil, err
	}
	if err := ValidateHeader(r); err != nil {
		return nil, err
	}
	root, leaves := BuildSchemaTree(meta.Schema)
	schema := schemaFromTree(root, leaves)
	return &FileReader{
		src:        r,
		size:       size,
		meta:       meta,
		schemaRoot: root,
		leaves:     leaves,
		schema:     schema,
	}, nil
}

// OpenFileReaderFromBytes opens a Parquet file from a byte slice.
// Zero-copy: the slice is used directly without copying.
func OpenFileReaderFromBytes(data []byte) (*FileReader, error) {
	size := int64(len(data))

	meta, err := ReadFileMetaData(newBytesReaderAt(data), size)
	if err != nil {
		return nil, err
	}

	root, leaves := BuildSchemaTree(meta.Schema)
	schema := schemaFromTree(root, leaves)

	return &FileReader{
		data:       data,
		meta:       meta,
		schemaRoot: root,
		leaves:     leaves,
		schema:     schema,
	}, nil
}

// Meta returns the decoded file metadata.
func (f *FileReader) Meta() *FileMetaData { return f.meta }

// Schema returns the Wadjet-level schema.
func (f *FileReader) Schema() Schema { return f.schema }

// SchemaRoot returns the Parquet-level schema tree root.
func (f *FileReader) SchemaRoot() *SchemaNode { return f.schemaRoot }

// Leaves returns all leaf column schema nodes.
func (f *FileReader) Leaves() []*SchemaNode { return f.leaves }

// NumRows returns the total row count.
func (f *FileReader) NumRows() int64 { return f.meta.NumRows }

// NumRowGroups returns the number of row groups.
func (f *FileReader) NumRowGroups() int { return len(f.meta.RowGroups) }

// RowGroupMeta returns metadata for a row group.
func (f *FileReader) RowGroupMeta(index int) *RowGroup {
	if index < 0 || index >= len(f.meta.RowGroups) {
		return nil
	}
	return &f.meta.RowGroups[index]
}

// RowGroupNumRows returns the row count for a row group.
func (f *FileReader) RowGroupNumRows(index int) int64 {
	rg := f.RowGroupMeta(index)
	if rg == nil {
		return 0
	}
	return rg.NumRows
}

// ColumnPages returns a page reader for a column in a row group.
// The colIdx is the leaf column index (matching leaves[colIdx]).
func (f *FileReader) ColumnPages(rgIdx, colIdx int) *ColumnPageReader {
	rg := f.RowGroupMeta(rgIdx)
	if rg == nil || colIdx < 0 || colIdx >= len(rg.Columns) {
		return nil
	}

	cc := &rg.Columns[colIdx]
	if cc.MetaData == nil {
		return nil
	}

	var maxDef, maxRep int
	if colIdx < len(f.leaves) {
		leaf := f.leaves[colIdx]
		maxDef = leaf.MaxDefLevel
		maxRep = leaf.MaxRepLevel
	}

	var pr *ColumnPageReader
	if f.data == nil && f.src != nil {
		pr = NewColumnPageReaderAt(f.src, f.size, cc.MetaData, maxDef, maxRep)
	} else {
		pr = NewColumnPageReader(f.data, cc.MetaData, maxDef, maxRep)
	}

	// Set type length for FIXED_LEN_BYTE_ARRAY from schema.
	if colIdx < len(f.leaves) && f.leaves[colIdx].TypeLength > 0 {
		pr.SetTypeLength(int(f.leaves[colIdx].TypeLength))
	}

	return pr
}

// RowGroupStats returns statistics for a row group using our metadata.
func (f *FileReader) RowGroupStats(index int) RowGroupStats {
	rg := f.RowGroupMeta(index)
	if rg == nil {
		return RowGroupStats{}
	}

	stats := RowGroupStats{
		NumRows: rg.NumRows,
		Columns: make(map[string]ColumnStats),
	}

	for i, cc := range rg.Columns {
		if cc.MetaData == nil {
			continue
		}
		cm := cc.MetaData
		colName := cm.PathInSchema[len(cm.PathInSchema)-1]

		cs := ColumnStats{HasStats: cm.Statistics != nil}
		if cs.HasStats {
			s := cm.Statistics
			cs.NullCount = s.NullCount

			// Extract typed min/max from raw statistics bytes.
			var physType PhysicalType
			if i < len(f.leaves) && f.leaves[i].Type != nil {
				physType = *f.leaves[i].Type
			} else {
				physType = cm.Type
			}

			if len(s.MinValue) > 0 {
				cs.MinValue = statsToNative(s.MinValue, physType)
			} else if len(s.Min) > 0 {
				cs.MinValue = statsToNative(s.Min, physType)
			}
			if len(s.MaxValue) > 0 {
				cs.MaxValue = statsToNative(s.MaxValue, physType)
			} else if len(s.Max) > 0 {
				cs.MaxValue = statsToNative(s.Max, physType)
			}
		}
		stats.Columns[colName] = cs
	}

	return stats
}

// statsToNative converts raw statistics bytes to a Go native type.
func statsToNative(data []byte, physType PhysicalType) any {
	switch physType {
	case PhysicalBoolean:
		if len(data) >= 1 {
			return data[0] != 0
		}
	case PhysicalInt32:
		if len(data) >= 4 {
			return int64(int32(binary.LittleEndian.Uint32(data)))
		}
	case PhysicalInt64:
		if len(data) >= 8 {
			return int64(binary.LittleEndian.Uint64(data))
		}
	case PhysicalFloat:
		if len(data) >= 4 {
			return float64(math.Float32frombits(binary.LittleEndian.Uint32(data)))
		}
	case PhysicalDouble:
		if len(data) >= 8 {
			return math.Float64frombits(binary.LittleEndian.Uint64(data))
		}
	case PhysicalByteArray, PhysicalFixedLenByteArray:
		return string(data)
	}
	return nil
}

// schemaFromTree builds a Wadjet Schema from the parsed Parquet schema tree.
func schemaFromTree(root *SchemaNode, leaves []*SchemaNode) Schema {
	if root == nil {
		return Schema{}
	}

	var columns []Column
	for _, child := range root.Children {
		columns = append(columns, nodeToColumn(child))
	}
	return Schema{Columns: columns}
}

// nodeToColumn converts a SchemaNode to our Column type.
func nodeToColumn(n *SchemaNode) Column {
	if n.IsLeaf() {
		col := Column{
			Name:     n.Name,
			Type:     TypeIDFromSchemaNode(n),
			Nullable: n.IsOptional(),
		}
		if col.Type == TypeVector && n.LogicalType != nil {
			col.Dimension = n.LogicalType.Dimension
		}
		if col.Type == TypeVector && col.Dimension <= 0 && n.TypeLength > 0 {
			col.Dimension = int(n.TypeLength) / 4 // TypeLength = dim × sizeof(float32)
		}
		if col.Type == TypeDecimal {
			// Without precision/scale the read-side vector decodes the
			// scaled integer with scale 0 — every decimal value silently
			// off by 10^scale (issue #144 suite finding).
			if n.LogicalType != nil {
				col.Precision = n.LogicalType.Precision
				col.Scale = n.LogicalType.Scale
			}
			if col.Precision == 0 && n.Precision > 0 {
				col.Precision = int(n.Precision)
				col.Scale = int(n.Scale)
			}
		}
		return col
	}

	// Group node — detect LIST/MAP/STRUCT patterns.
	children := n.Children

	// LIST pattern: one repeated child with one inner child
	if len(children) == 1 && children[0].IsRepeated() {
		inner := children[0].Children
		if len(inner) == 1 {
			elemCol := nodeToColumn(inner[0])
			return Column{
				Name:        n.Name,
				Type:        TypeArray,
				Nullable:    n.IsOptional(),
				ElementType: &elemCol,
			}
		}
		if len(inner) == 2 {
			keyCol := nodeToColumn(inner[0])
			valCol := nodeToColumn(inner[1])
			entryCol := Column{Name: "entry", Type: TypeRow, Fields: []Column{keyCol, valCol}}
			return Column{
				Name:        n.Name,
				Type:        TypeMap,
				Nullable:    n.IsOptional(),
				ElementType: &entryCol,
			}
		}
	}

	// STRUCT: group with named children
	fields := make([]Column, len(children))
	for i, child := range children {
		fields[i] = nodeToColumn(child)
	}
	return Column{
		Name:     n.Name,
		Type:     TypeRow,
		Nullable: n.IsOptional(),
		Fields:   fields,
	}
}

// TypeIDFromSchemaNode maps a schema node's physical + logical type to Wadjet TypeID.
func TypeIDFromSchemaNode(n *SchemaNode) TypeID {
	if n.LogicalType != nil {
		switch n.LogicalType.Type {
		case LogicalString:
			return TypeString
		case LogicalDate:
			return TypeDate
		case LogicalTimestampMillis, LogicalTimestampMicros, LogicalTimestampNanos:
			return TypeTimestamp
		case LogicalDecimal:
			return TypeDecimal
		case LogicalInteger:
			if n.LogicalType.BitWidth <= 32 {
				return TypeInt32
			}
			return TypeInt64
		case LogicalUUID:
			return TypeUUID
		case LogicalJSON:
			return TypeString
		case LogicalEnum:
			return TypeString
		case LogicalVector:
			return TypeVector
		}
	}

	if n.ConvertedType != nil {
		switch *n.ConvertedType {
		case ConvertedUTF8:
			return TypeString
		case ConvertedDate:
			return TypeDate
		case ConvertedTimestampMillis, ConvertedTimestampMicros:
			return TypeTimestamp
		case ConvertedDecimal:
			return TypeDecimal
		case ConvertedInt8, ConvertedInt16, ConvertedInt32, ConvertedUint8, ConvertedUint16, ConvertedUint32:
			return TypeInt32
		case ConvertedInt64, ConvertedUint64:
			return TypeInt64
		}
	}

	if n.Type == nil {
		return TypeString // group node fallback
	}
	switch *n.Type {
	case PhysicalBoolean:
		return TypeBool
	case PhysicalInt32:
		return TypeInt32
	case PhysicalInt64:
		return TypeInt64
	case PhysicalFloat:
		return TypeFloat32
	case PhysicalDouble:
		return TypeFloat64
	case PhysicalByteArray, PhysicalFixedLenByteArray:
		return TypeString
	default:
		return TypeString
	}
}

// bytesReaderAt wraps a []byte as io.ReaderAt.
type bytesReaderAt struct {
	data []byte
}

func newBytesReaderAt(data []byte) *bytesReaderAt {
	return &bytesReaderAt{data: data}
}

func (r *bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
