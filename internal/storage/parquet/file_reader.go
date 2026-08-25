package parquet

import (
	"encoding/binary"
	"encoding/json"
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
	data       []byte        // entire file in memory; nil in staged (pread) mode
	src        io.ReaderAt   // staged mode: chunk reads issue ranged reads here
	size       int64         // staged mode: file size (chunk-range clamp)
	meta       *FileMetaData // decoded footer
	schemaRoot *SchemaNode   // schema tree
	leaves     []*SchemaNode // leaf nodes indexed by column position
	schema     Schema        // Wadjet-level schema (for compatibility)

	// cacheIdentity is an opaque, content-stable identity for the underlying
	// object (e.g. "<bucket>/<key>#<size>"), set by callers that know it via
	// SetCacheIdentity. It is PASSIVE: nothing in this package reads it — it
	// exists so downstream layers (the scan decoded-chunk cache) can key
	// per-(row group, column) work on the object without re-plumbing identity
	// through every call. Empty means "unknown" and disables such caching.
	cacheIdentity string
}

// SetCacheIdentity attaches a content-stable object identity to the reader
// (see the cacheIdentity field). Call once, before handing the reader to
// concurrent consumers — the field is read without synchronization.
func (fr *FileReader) SetCacheIdentity(id string) { fr.cacheIdentity = id }

// CacheIdentity returns the identity set by SetCacheIdentity, or "" when none
// was attached (caching layers must treat "" as uncacheable).
func (fr *FileReader) CacheIdentity() string { return fr.cacheIdentity }

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
	schema := readerSchema(root, leaves, meta.KeyValueMetadata)

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
	schema := readerSchema(root, leaves, meta.KeyValueMetadata)
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
	schema := readerSchema(root, leaves, meta.KeyValueMetadata)
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
	schema := readerSchema(root, leaves, meta.KeyValueMetadata)

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

	// The row group's row count is the exact bound on what this chunk's page
	// headers may claim, for a flat leaf. Setting it here rather than at each
	// of the seven call sites is what makes it hold everywhere.
	if rg.NumRows > 0 && rg.NumRows <= MaxRowsPerRowGroup {
		pr.SetRowBudget(int(rg.NumRows))
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
			var leaf *SchemaNode
			if i < len(f.leaves) {
				leaf = f.leaves[i]
			}
			if leaf != nil && leaf.Type != nil {
				physType = *leaf.Type
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

			// Statistics are raw file values, so a micro/nano TIMESTAMP
			// column hands out bounds 1000x-1000000x away from the engine
			// unit the query literal is in. Every consumer of ColumnStats
			// compares against engine values — zone-map and dynamic-range
			// row-group pruning, bloom pruning, the footer-answered
			// MIN/MAX, and the catalog's persisted per-file stats — and
			// none of them can see the schema, so the unit has to be
			// resolved here, at the one place that still holds the leaf.
			//
			// Flooring keeps the bounds sound: it is monotonic, so the
			// scaled [min,max] still contains every scaled value in the
			// row group and pruning can never drop a matching row.
			if div := TimestampDivisorFromSchemaNode(leaf); div != 1 {
				if mn, ok := cs.MinValue.(int64); ok {
					cs.MinValue = TimestampToEngineMillis(mn, div)
				}
				if mx, ok := cs.MaxValue.(int64); ok {
					cs.MaxValue = TimestampToEngineMillis(mx, div)
				}
			}

			// A CIDR column's stored bounds are the address TEXT's, and
			// the engine compares PostgreSQL's inet order — the same
			// mismatch TimestampDivisorFromSchemaNode resolves for a unit,
			// resolved here for an ORDER (#523, ADR-0018 §6). Only a file
			// whose writer promised every CIDR value parsed
			// (CidrStatsOrderKey) may have its min/max re-keyed into that
			// order; every other file — old, or one this reader cannot
			// even identify as CIDR because it carries no declared-schema
			// blob — keeps cs as statsToNative already built it, which for
			// a recognized CIDR column without the promise is withheld
			// below rather than left comparable-looking but wrong.
			if f.declaredColumnType(colName) == TypeCIDR {
				if !f.cidrStatsAreInetOrder() {
					cs = ColumnStats{}
				} else if mn, ok := cs.MinValue.(string); ok {
					mnKey, okMn := CidrStatsSortKey(mn)
					mx, okMx0 := cs.MaxValue.(string)
					var mxKey string
					if okMx0 {
						mxKey, okMx0 = CidrStatsSortKey(mx)
					}
					if okMn && okMx0 {
						// The bound keeps BOTH forms: Key is what the prune
						// layer compares, Text is the winning row's address
						// as the file stores it, which is what a catalog
						// stat must persist (see CidrInetBound's doc — the
						// Key is binary and JSON destroys it).
						cs.MinValue = CidrInetBound{Key: mnKey, Text: mn}
						cs.MaxValue = CidrInetBound{Key: mxKey, Text: mx}
					} else {
						// The writer promised every value in the file
						// parsed; a footer that still fails to re-key is
						// corrupt or foreign rather than merely old, and
						// the safe answer is the same as for an old file.
						cs = ColumnStats{NullCount: cs.NullCount}
					}
				}
			}
		}
		stats.Columns[colName] = cs
	}

	return stats
}

// declaredColumnType returns name's DECLARED type per the file's own schema
// (f.schema, which restores TypeCIDR from DeclaredSchemaKey per
// declaredOverlayUTF8Types), or TypeID(0) if the column is unknown. A file
// with no declared-schema blob — never written by this writer, or older
// than #396 — reports whatever the bare parquet annotation infers, which
// for a UTF8 byte-array column is TypeString, never TypeCIDR: this is a
// query against the FILE's own self-description, not the caller's catalog,
// so a CIDR column a file cannot identify as its own keeps its pre-#523
// behavior rather than risk comparing a bound this reader never confirmed
// is in the right order.
func (f *FileReader) declaredColumnType(name string) TypeID {
	if i := f.schema.ColumnIndex(name); i >= 0 {
		return f.schema.Columns[i].Type
	}
	return TypeID(0)
}

// cidrStatsAreInetOrder reports whether this file's writer promised every
// CIDR value it wrote parsed as an address (CidrStatsOrderKey).
func (f *FileReader) cidrStatsAreInetOrder() bool {
	if f.meta == nil {
		return false
	}
	for i := range f.meta.KeyValueMetadata {
		if f.meta.KeyValueMetadata[i].Key == CidrStatsOrderKey {
			return f.meta.KeyValueMetadata[i].Value == CidrStatsOrderInet
		}
	}
	return false
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

// TimestampDivisorFromSchemaNode reports the divisor that converts values
// stored under this leaf's TIMESTAMP unit into the engine unit, epoch
// MILLISECONDS: 1 for MILLIS, 1_000 for MICROS, 1_000_000 for NANOS.
//
// The engine has exactly one timestamp unit — file_writer.go emits MILLIS and
// the expression layer's parseTemporalInt64 reads MILLIS — but parquet files
// written elsewhere carry whichever unit their producer chose (MICROS is the
// PyArrow, Spark and Iceberg default; NANOS is common from pandas). Decoding
// those verbatim puts every instant off by 1000x or 1000000x, silently. Any
// path that lifts a raw int64 out of a TIMESTAMP column — page values or
// footer statistics — must divide by this first.
//
// Returns 1 for every non-timestamp node, so callers may apply it
// unconditionally.
func TimestampDivisorFromSchemaNode(n *SchemaNode) int64 {
	if n == nil {
		return 1
	}
	if n.LogicalType != nil {
		switch n.LogicalType.Type {
		case LogicalTimestampMicros:
			return 1_000
		case LogicalTimestampNanos:
			return 1_000_000
		case LogicalTimestampMillis:
			return 1
		}
	}
	// Old-style ConvertedType files (parquet-mr before the LogicalType
	// union, and anything writing for maximum compatibility) carry only
	// TIMESTAMP_MICROS / TIMESTAMP_MILLIS. There is no NANOS spelling in
	// ConvertedType, which is why NANOS files always set a LogicalType.
	if n.ConvertedType != nil && *n.ConvertedType == ConvertedTimestampMicros {
		return 1_000
	}
	return 1
}

// TimestampToEngineMillis converts one stored timestamp value to the engine
// unit given the divisor from TimestampDivisorFromSchemaNode.
//
// Sub-millisecond precision cannot survive the trip, so the instant is
// reported as the millisecond that CONTAINS it — division truncating toward
// the PAST, not toward zero. Go's `/` truncates toward zero, which would move
// pre-1970 instants FORWARD in time and break the invariant that a stored
// value never decodes to a later millisecond than one that follows it in the
// same column. That asymmetry is exactly the kind of silent, sign-dependent
// skew this whole conversion exists to prevent.
func TimestampToEngineMillis(v, div int64) int64 {
	if div == 1 {
		return v
	}
	q := v / div
	if v%div != 0 && v < 0 {
		q--
	}
	return q
}

// ScaleTimestampsToEngine rescales a decoded run of TIMESTAMP values in
// place. Null slots may hold arbitrary residue from a pooled vector; scaling
// them is harmless because Nulls gates every read, and skipping them would
// cost a per-row branch on the scan hot path.
func ScaleTimestampsToEngine(vals []int64, div int64) {
	if div == 1 {
		return
	}
	for i, v := range vals {
		vals[i] = TimestampToEngineMillis(v, div)
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
			// All three precisions land on the one engine timestamp unit;
			// the decode paths divide by TimestampDivisorFromSchemaNode to
			// get there.
			return TypeTimestamp
		case LogicalTimeMillis:
			// TIME is time-of-day with no date, which the engine's 22 types
			// cannot express. Rather than pretend it is an instant, it stays
			// the file's own integer IN THE FILE'S OWN UNIT: TIME_MILLIS is
			// physical INT32, TIME_MICROS is INT64. Rescaling to a common
			// unit would silently discard sub-millisecond precision from a
			// column whose declared type never promised milliseconds, and
			// there is no TypeTime for the result to mean anything as. Add a
			// real TIME type before changing this.
			return TypeInt32
		case LogicalTimeMicros:
			return TypeInt64
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

// DeclaredSchemaKey is the footer KeyValueMetadata key under which the
// native writer stamps the wadjet schema it was handed, JSON-encoded.
//
// Parquet's own schema cannot express eight of wadjet's 22 types: IPv4, IPv6,
// MAC, UUID, Bytes, Port, Protocol and Duration get no logical annotation
// from buildLeafSchemaElement (there is none to give — they are not parquet
// concepts), so TypeIDFromSchemaNode reads them back as INT64 or STRING. A
// consumer that takes its column types from the CATALOG never noticed; one
// that takes them from the FILE — the DAG worker's parquet scan, the
// coordinator's scalar extraction, any tool pointed at a wadjet file —
// rendered 10.0.0.5 as 167772165 and a UUID as 16 raw bytes (#396).
//
// Those eight are also the ONLY types the read side takes from this blob:
// see declaredOverlayTypes for why an annotated leaf ignores it.
const DeclaredSchemaKey = "wadjet.schema"

// CidrStatsOrderKey is the footer KeyValueMetadata key a writer stamps with
// CidrStatsOrderInet once every CIDR value it wrote to this file parsed as
// an address, meaning every CIDR leaf's min/max are the row-group's true
// PostgreSQL inet-order extremes (kernel.CidrSortKey's ordering) rather
// than the column's raw TEXT byte-order ones.
//
// Its absence — an older file, or a file with even one unparseable CIDR
// value — means a CIDR column's min/max, if the footer has any, are in the
// wrong order for the engine's comparisons (#492, ADR-0018 §6) and
// RowGroupStats withholds them rather than converting a bound it cannot
// repair (#523). This is a promise about the WHOLE FILE, not one row
// group: a file with several CIDR row groups gets one flag or none.
const CidrStatsOrderKey = "wadjet.cidr_stats_order"

// CidrStatsOrderInet is CidrStatsOrderKey's one defined value.
const CidrStatsOrderInet = "inet"

// readerSchema is the schema a FileReader reports: the one inferred from the
// parquet schema tree, with the declared types restored from
// DeclaredSchemaKey where the file carries it.
func readerSchema(root *SchemaNode, leaves []*SchemaNode, kv []KeyValue) Schema {
	var top []*SchemaNode
	if root != nil {
		top = root.Children
	}
	return overlayDeclaredSchema(schemaFromTree(root, leaves), top, kv)
}

// maxDeclaredSchemaBytes bounds the footer blob this reader will decode. A
// thousand-column schema encodes to roughly 100 KB; past a megabyte the value
// under the key is not a schema the native writer produced, and the footer is
// attacker-controlled input like any other part of the file.
const maxDeclaredSchemaBytes = 1 << 20

// declaredOverlayTypes is the set of types the overlay will install on an
// UNANNOTATED leaf, and it is exactly the set parquet's own schema cannot
// express: the eight for which buildLeafSchemaElement writes no LogicalType
// and no ConvertedType, because there is no annotation to write. That set is
// the reason the blob exists (#396), so it is the reach the blob gets.
//
// Everything else is annotated in the file itself and the FILE wins: DECIMAL
// carries its own precision and scale, TIMESTAMP/DATE/INT32/INT64 their
// logical type, STRING the UTF8 annotation, VECTOR the wadjet LogicalVector
// extension with its dimension. An annotated leaf is immune to the blob, so a
// stale or hostile blob cannot relabel DECIMAL(18,4) as DECIMAL(12,1) (every
// value 1000× off), retype a TIMESTAMP as IPv4, or hand the batch allocator a
// fabricated VECTOR dimension.
var declaredOverlayTypes = map[TypeID]bool{
	TypeIPv4:     true,
	TypeIPv6:     true,
	TypeMAC:      true,
	TypeUUID:     true,
	TypeBytes:    true,
	TypePort:     true,
	TypeProtocol: true,
	TypeDuration: true,
}

// declaredOverlayUTF8Types is the one exception to "an annotated leaf is
// immune", and it holds exactly one type.
//
// CIDR has no parquet annotation of its own either, so buildLeafSchemaElement
// writes it as UTF8 STRING — the annotation describes the STORAGE truthfully
// and loses only the name. Restoring the name over a UTF8 leaf changes
// nothing about how the page is decoded (BYTE_ARRAY either way) and nothing
// about how a value renders (Vector.GetValue returns the same text for both
// STRING and CIDR), which is what makes this safe where STRING→IPv6 is not:
// IPv6's storage contract is exactly 16 bytes and GetValue renders anything
// else as "".
//
// It is not cosmetic. The engine dispatches on the TYPE, and CIDR and STRING
// do not behave identically everywhere: with CIDR reverting to STRING, the
// stage DAG and the single-process engine started answering
// `SELECT MIN(c_cidr), MAX(c_cidr)` differently — the DAG correctly, the
// single-process arm with NULLs — because one saw a STRING column and the
// other the catalog's CIDR (TestTypeMatrixTwoPath/minmax_c_cidr; the
// single-process NULL is a separate defect, and #392's MIN_BY switch is
// another). Restoring the declared name is what keeps the two paths reading
// the same column as the same type.
var declaredOverlayUTF8Types = map[TypeID]bool{
	TypeCIDR: true,
}

// leafIsUTF8String reports whether a leaf is annotated as parquet UTF8 text
// and stored as BYTE_ARRAY — the annotation buildLeafSchemaElement writes for
// both STRING and CIDR.
func leafIsUTF8String(n *SchemaNode) bool {
	if n.Type == nil || *n.Type != PhysicalByteArray {
		return false
	}
	if n.LogicalType != nil && n.LogicalType.Type == LogicalString {
		return n.ConvertedType == nil || *n.ConvertedType == ConvertedUTF8
	}
	if n.LogicalType == nil && n.ConvertedType != nil && *n.ConvertedType == ConvertedUTF8 {
		return true
	}
	return false
}

// overlayDeclaredSchema restores the declared TYPE IDENTITY of each leaf
// column from the footer's declared-schema blob. Nothing else is taken: name,
// nullability, precision, scale, dimension and nested structure all stay as
// the parquet tree described them, because those the tree CAN express and the
// tree is what the page decoders are driven by.
//
// A file written before this key existed, or by any other producer, has no
// blob and keeps the inferred schema — the behaviour this replaces.
//
// The blob is UNTRUSTED INPUT: it is bytes in a file, and a reader that lets
// it choose how pages are interpreted has handed a file the power to make the
// engine misread its own data. Five conditions must all hold before a single
// column is touched, and any failure leaves that column — or the whole
// schema — exactly as the tree described it:
//
//  1. the blob is under maxDeclaredSchemaBytes and decodes as JSON;
//  2. it describes the same number of top-level columns, with the same names
//     in the same order;
//  3. the leaf carries NO LogicalType and NO ConvertedType — the file itself
//     said nothing about what the column means, which is the only situation
//     the blob is here to fix — or it is annotated UTF8 text, the single
//     exception below;
//  4. the declared type is one of declaredOverlayTypes (the eight types
//     parquet cannot annotate) on an unannotated leaf, or CIDR on a UTF8 one
//     (declaredOverlayUTF8Types: same storage, same rendering, name only);
//  5. the physical parquet type of that declared type is the physical type
//     the leaf ACTUALLY has in the file.
//
// Together those make a stale or mismatched blob inert rather than a source
// of misread pages, and they bound the blast radius of a hostile one to
// relabelling an unannotated INT32/INT64/BYTE_ARRAY column as another type
// with the identical storage.
func overlayDeclaredSchema(inferred Schema, nodes []*SchemaNode, kv []KeyValue) Schema {
	var raw string
	for i := range kv {
		if kv[i].Key == DeclaredSchemaKey {
			raw = kv[i].Value
			break
		}
	}
	if raw == "" || len(raw) > maxDeclaredSchemaBytes {
		return inferred
	}
	var declared Schema
	if err := json.Unmarshal([]byte(raw), &declared); err != nil {
		return inferred
	}
	if len(declared.Columns) != len(inferred.Columns) || len(nodes) != len(inferred.Columns) {
		return inferred
	}
	for i := range inferred.Columns {
		if declared.Columns[i].Name != inferred.Columns[i].Name {
			return inferred
		}
	}
	for i := range inferred.Columns {
		ic, dc, n := &inferred.Columns[i], &declared.Columns[i], nodes[i]
		if n == nil || !n.IsLeaf() {
			// Group node: LIST/MAP/STRUCT round trip through parquet's own
			// annotations, and their element and field definitions carry
			// nullability the blob does not re-derive.
			continue
		}
		if ic.ElementType != nil || len(ic.Fields) > 0 ||
			dc.ElementType != nil || len(dc.Fields) > 0 {
			continue
		}
		if n.LogicalType != nil || n.ConvertedType != nil {
			// The file annotated this leaf, so the file wins — with one
			// exception: a UTF8 STRING leaf may carry back the name CIDR,
			// whose storage and rendering are that same text.
			if !(leafIsUTF8String(n) && declaredOverlayUTF8Types[dc.Type]) {
				continue
			}
		} else if !declaredOverlayTypes[dc.Type] {
			continue
		}
		if wadjetTypeToPhysical(dc.Type) != *n.Type {
			continue
		}
		// Type identity ONLY. None of the eight has a precision, scale or
		// dimension to carry, and copying those fields is how a blob would
		// reach the decode and allocation paths.
		ic.Type = dc.Type
	}
	return inferred
}
