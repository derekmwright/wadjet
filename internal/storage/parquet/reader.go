package parquet

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/big"
	"strings"
)

// RowGroupStats contains min/max statistics for a row group.
type RowGroupStats struct {
	NumRows int64
	Columns map[string]ColumnStats
}

// ColumnStats contains min/max statistics for a single column in a row group.
type ColumnStats struct {
	HasStats  bool
	MinValue  any
	MaxValue  any
	NullCount int64
}

// Reader reads rows from a Parquet file.
type Reader struct {
	fr     *FileReader
	schema Schema
}

// NewReader creates a Parquet reader from an io.ReaderAt.
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	fr, err := OpenFileReader(r, size)
	if err != nil {
		return nil, fmt.Errorf("opening parquet file: %w", err)
	}
	return &Reader{fr: fr, schema: fr.Schema()}, nil
}

// NewReaderAt creates a Parquet reader in staged (pread) mode: no
// whole-file buffer is ever held — each column-chunk read stages its
// byte range from r into a pooled buffer on demand (OpenFileReaderAt).
// Use this over NewReader when r is a local file: NewReader eagerly
// copies the entire file to heap, NewReaderAt reads footer-only.
func NewReaderAt(r io.ReaderAt, size int64) (*Reader, error) {
	fr, err := OpenFileReaderAt(r, size)
	if err != nil {
		return nil, fmt.Errorf("opening parquet file: %w", err)
	}
	return &Reader{fr: fr, schema: fr.Schema()}, nil
}

// NewReaderFromBytes creates a Parquet reader from a byte slice.
// Zero-copy: the slice is used directly by the underlying FileReader
// without allocating a separate copy. Use this when the data is already
// in memory (e.g. from readAllSized or a cache) to avoid the O(n) copy
// that NewReader performs via OpenFileReader.
func NewReaderFromBytes(data []byte) (*Reader, error) {
	fr, err := OpenFileReaderFromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("opening parquet file: %w", err)
	}
	return &Reader{fr: fr, schema: fr.Schema()}, nil
}

// NewReaderFromBytesCached is NewReaderFromBytes with the footer decode
// served from the process footer cache when identity is non-empty (see
// footer_cache.go). data is still used zero-copy for every page read; only
// the Thrift footer decode is elided.
func NewReaderFromBytesCached(data []byte, identity string) (*Reader, error) {
	fr, err := OpenFileReaderFromBytesCached(data, identity)
	if err != nil {
		return nil, fmt.Errorf("opening parquet file: %w", err)
	}
	return &Reader{fr: fr, schema: fr.Schema()}, nil
}

// Schema returns the schema of the Parquet file.
func (r *Reader) Schema() Schema { return r.schema }

// FileReader returns the underlying native FileReader.
func (r *Reader) FileReader() *FileReader { return r.fr }

// NumRowGroups returns the number of row groups in the file.
func (r *Reader) NumRowGroups() int { return r.fr.NumRowGroups() }

// NumRows returns the total number of rows across all row groups.
func (r *Reader) NumRows() int64 { return r.fr.NumRows() }

// RowGroupStats returns statistics for a row group.
func (r *Reader) RowGroupStats(index int) RowGroupStats {
	return r.fr.RowGroupStats(index)
}

// ReadRows reads all rows from the Parquet file, optionally selecting only
// specific columns. Uses our native page reader — no parquet-go dependency.
// Supports nested types (ARRAY, MAP, ROW) by assembling leaf-level data
// using definition and repetition levels.
func (r *Reader) ReadRows(selectedColumns []string) ([]map[string]any, error) {
	return r.ReadRowsAs(nil, selectedColumns)
}

// ReadRowsAs is ReadRows with the CALLER's types: every column that schema
// names is decoded as the type the caller declares for it, and the rest as
// the type recovered from the file.
//
// The difference is not cosmetic. A parquet file cannot describe eight of
// this engine's types: buildLeafSchemaElement writes no logical annotation
// for IPv4, IPv6, MAC, PORT, PROTOCOL, DURATION, BYTES or UUID, so
// TypeIDFromSchemaNode recovers them as plain INT64/BYTE_ARRAY columns.
// Decoded that way an IPv6 came back as a 16-byte Go STRING, which
// Vector.SetValue then handed net.ParseIP and stored as NULL — so every
// query that falls back to the row reader (which is every query on a table
// carrying a nested column) silently lost the value. The catalog knows what
// the file cannot say; this is how it says so.
func (r *Reader) ReadRowsAs(schema []Column, selectedColumns []string) ([]map[string]any, error) {
	readCols := r.schema.Columns
	if len(selectedColumns) > 0 {
		readCols = filterSchemaColumns(r.schema.Columns, selectedColumns)
	}
	readCols, err := retypeFromCatalog(readCols, schema, r.fr.Leaves())
	if err != nil {
		return nil, err
	}

	// Check if we have nested columns that need assembly.
	hasNested := false
	for _, col := range readCols {
		if col.Type == TypeArray || col.Type == TypeMap || col.Type == TypeRow {
			hasNested = true
			break
		}
	}

	if !hasNested {
		return r.readRowsFlat(readCols)
	}
	return r.readRowsNested(readCols)
}

// readRowsFlat is the original flat-schema read path (unchanged behavior).
func (r *Reader) readRowsFlat(readCols []Column) ([]map[string]any, error) {
	leafByName := TopLevelLeafIndex(r.fr.Leaves())

	var allRows []map[string]any
	for rgIdx := 0; rgIdx < r.fr.NumRowGroups(); rgIdx++ {
		if err := CheckRowGroupRowCount(rgIdx, r.fr.RowGroupNumRows(rgIdx)); err != nil {
			return nil, err
		}
		numRows := int(r.fr.RowGroupNumRows(rgIdx))
		if numRows == 0 {
			continue
		}

		colValues := make([][]any, len(readCols))
		for i, col := range readCols {
			leafIdx, ok := leafByName[col.Name]
			if !ok {
				colValues[i] = make([]any, numRows)
				continue
			}
			vals, err := readColumnToAny(r.fr, rgIdx, leafIdx, numRows, col)
			if err != nil {
				return nil, fmt.Errorf("reading column %s: %w", col.Name, err)
			}
			colValues[i] = vals
		}

		for row := 0; row < numRows; row++ {
			m := make(map[string]any, len(readCols))
			for i, col := range readCols {
				if row < len(colValues[i]) {
					if v := colValues[i][row]; v != nil {
						m[col.Name] = v
					}
				}
			}
			allRows = append(allRows, m)
		}
	}
	return allRows, nil
}

// readRowsNested handles schemas with ARRAY, MAP, or ROW columns by reading
// leaf-level pages with def/rep levels and assembling nested structures.
func (r *Reader) readRowsNested(readCols []Column) ([]map[string]any, error) {
	leaves := r.fr.Leaves()

	leafByName := TopLevelLeafIndex(leaves)
	nodeByName := make(map[string]*SchemaNode)
	if root := r.fr.SchemaRoot(); root != nil {
		for _, c := range root.Children {
			nodeByName[c.Name] = c
		}
	}

	// One assembly plan per nested column being read, built from the FILE's
	// schema subtree (see nested_assembly.go), and the set of leaves those
	// plans need. Only those leaves are paged in: the read used to decode
	// EVERY leaf in the file whether or not the query asked for it, then
	// decode the flat columns a second time through readColumnToAny.
	type nestedPlan struct {
		name string
		node *nestedNode
	}
	var plans []nestedPlan
	needLeaf := make([]bool, len(leaves))
	for _, col := range readCols {
		if !isNestedType(col.Type) {
			continue
		}
		node := buildAssemblyPlan(nodeByName[col.Name])
		if node == nil {
			continue
		}
		plans = append(plans, nestedPlan{col.Name, node})
		for _, li := range node.leaves {
			if li >= 0 && li < len(needLeaf) {
				needLeaf[li] = true
			}
		}
	}

	var allRows []map[string]any
	for rgIdx := 0; rgIdx < r.fr.NumRowGroups(); rgIdx++ {
		if err := CheckRowGroupRowCount(rgIdx, r.fr.RowGroupNumRows(rgIdx)); err != nil {
			return nil, err
		}
		numRows := int(r.fr.RowGroupNumRows(rgIdx))
		if numRows == 0 {
			continue
		}

		leafPages := make([]leafColumnData, len(leaves))
		for i := range leaves {
			if !needLeaf[i] {
				continue
			}
			lcd, err := readLeafColumn(r.fr, rgIdx, i)
			if err != nil {
				return nil, fmt.Errorf("reading leaf column %v: %w", leaves[i].Path, err)
			}
			leafPages[i] = lcd
		}

		rows := make([]map[string]any, numRows)
		for i := range rows {
			rows[i] = make(map[string]any, len(readCols))
		}

		asm := newRecordAssembler(leafPages)
		for _, p := range plans {
			asm.assembleNestedColumn(p.node, p.name, rows)
		}

		for _, col := range readCols {
			if isNestedType(col.Type) {
				continue
			}
			leafIdx, ok := leafByName[col.Name]
			if !ok {
				continue
			}
			vals, err := readColumnToAny(r.fr, rgIdx, leafIdx, numRows, col)
			if err != nil {
				return nil, fmt.Errorf("reading column %s: %w", col.Name, err)
			}
			for row := 0; row < numRows; row++ {
				if row < len(vals) && vals[row] != nil {
					rows[row][col.Name] = vals[row]
				}
			}
		}

		allRows = append(allRows, rows...)
	}
	return allRows, nil
}

// leafColumnData holds all values and levels for a single leaf column page.
type leafColumnData struct {
	values    []any   // decoded values (only non-null entries)
	defLevels []int32 // definition levels for each entry
	repLevels []int32 // repetition levels for each entry
	maxDef    int32
	maxRep    int32
	typeID    TypeID
}

// readLeafColumn reads all pages for a leaf column and returns raw values with levels.
func readLeafColumn(fr *FileReader, rgIdx, colIdx int) (leafColumnData, error) {
	leaves := fr.Leaves()
	leaf := leaves[colIdx]
	// The whole Column, not just its TypeID: a leaf below a container needs
	// its VECTOR dimension and its DECIMAL precision to decode, exactly as a
	// top-level leaf does. nodeToColumn is the same recovery the file's own
	// Schema is built from.
	col := nodeToColumn(leaf)
	typeID := col.Type

	lcd := leafColumnData{
		maxDef: int32(leaf.MaxDefLevel),
		maxRep: int32(leaf.MaxRepLevel),
		typeID: typeID,
	}

	pr := fr.ColumnPages(rgIdx, colIdx)
	if pr == nil {
		return lcd, nil
	}
	defer pr.Close()

	dict, err := pr.NextDictionary()
	if err != nil {
		return lcd, fmt.Errorf("reading dictionary: %w", err)
	}

	for {
		page, err := pr.NextPage()
		if err != nil {
			return lcd, fmt.Errorf("reading page: %w", err)
		}
		if page == nil {
			break
		}

		data := page.Data
		if page.IsDictEncoded() {
			if dict == nil {
				return lcd, fmt.Errorf("dictionary-encoded page but chunk has no dictionary page")
			}
			data, err = resolveDictForRows(dict, data, typeID)
			if err != nil {
				return lcd, fmt.Errorf("resolving dictionary page: %w", err)
			}
		}

		// Collect definition and repetition levels.
		if page.DefinitionLevels != nil {
			lcd.defLevels = append(lcd.defLevels, page.DefinitionLevels...)
		} else {
			// All values present — fill with maxDef.
			for i := 0; i < page.NumValues; i++ {
				lcd.defLevels = append(lcd.defLevels, lcd.maxDef)
			}
		}
		if page.RepetitionLevels != nil {
			lcd.repLevels = append(lcd.repLevels, page.RepetitionLevels...)
		} else {
			for i := 0; i < page.NumValues; i++ {
				lcd.repLevels = append(lcd.repLevels, 0)
			}
		}

		// Decode non-null values.
		pageVals, err := decodePageValues(data, lcd.maxDef, page.DefinitionLevels, col)
		if err != nil {
			page.Release()
			return lcd, fmt.Errorf("decoding page for %v: %w", leaf.Path, err)
		}
		lcd.values = append(lcd.values, pageVals...)
		page.Release()
	}

	// Nested (ARRAY/MAP/ROW) leaves come through here too, so this is the
	// scaling point for a timestamp buried inside a struct or list.
	if typeID == TypeTimestamp {
		if div := TimestampDivisorFromSchemaNode(leaf); div != 1 {
			for i, v := range lcd.values {
				if iv, ok := v.(int64); ok {
					lcd.values[i] = TimestampToEngineMillis(iv, div)
				}
			}
		}
	}

	return lcd, nil
}

// decodePageValues decodes a page's PRESENT values into a fresh []any, for
// the nested read path's per-leaf streams.
//
// It shares decodePresentValues with the flat row path's unpack functions
// rather than carrying its own arm per type. It used to carry its own, and
// the two sets had drifted: this one had no VECTOR arm and no DECIMAL arm at
// all, so a VECTOR or DECIMAL leaf ANYWHERE below a container decoded to
// nothing and every value under it read back NULL, while the same leaf as a
// top-level column read correctly (#407). ADR-0018 §3: a file is readable
// through all of the decode paths or through none of them.
func decodePageValues(data Values, maxDef int32, defLevels []int32, col Column) ([]any, error) {
	nonNull := data.Count()
	if defLevels != nil {
		nonNull = 0
		for _, d := range defLevels {
			if d == maxDef {
				nonNull++
			}
		}
	}
	if nonNull <= 0 {
		return nil, nil
	}
	if err := checkPageDecodable(col, data, nonNull); err != nil {
		return nil, err
	}
	vals := make([]any, nonNull)
	if err := decodePresentValues(vals, data, nonNull, col); err != nil {
		return nil, err
	}
	return vals, nil
}

// pathKey creates a lookup key from a path slice.
func pathKey(path []string) string {
	if len(path) == 0 {
		return ""
	}
	result := path[0]
	for _, p := range path[1:] {
		result += "." + p
	}
	return result
}

// ReadRowGroup reads all rows from a specific row group.
func (r *Reader) ReadRowGroup(index int, selectedColumns []string) ([]map[string]any, error) {
	if index < 0 || index >= r.fr.NumRowGroups() {
		return nil, fmt.Errorf("row group index %d out of range [0, %d)", index, r.fr.NumRowGroups())
	}

	readCols := r.schema.Columns
	if len(selectedColumns) > 0 {
		readCols = filterSchemaColumns(r.schema.Columns, selectedColumns)
	}

	leafByName := TopLevelLeafIndex(r.fr.Leaves())

	if err := CheckRowGroupRowCount(index, r.fr.RowGroupNumRows(index)); err != nil {
		return nil, err
	}
	numRows := int(r.fr.RowGroupNumRows(index))
	colValues := make([][]any, len(readCols))
	for i, col := range readCols {
		leafIdx, ok := leafByName[col.Name]
		if !ok {
			colValues[i] = make([]any, numRows)
			continue
		}
		vals, err := readColumnToAny(r.fr, index, leafIdx, numRows, col)
		if err != nil {
			return nil, fmt.Errorf("reading column %s: %w", col.Name, err)
		}
		colValues[i] = vals
	}

	rows := make([]map[string]any, numRows)
	for row := 0; row < numRows; row++ {
		m := make(map[string]any, len(readCols))
		for i, col := range readCols {
			if row < len(colValues[i]) {
				if v := colValues[i][row]; v != nil {
					m[col.Name] = v
				}
			}
		}
		rows[row] = m
	}
	return rows, nil
}

// float32sFromBytes decodes a VECTOR leaf's fixed-length bytes back into the
// []float32 the engine's row shape carries — the inverse of the writer's
// toBytes, little-endian, four bytes per dimension. Without it neither
// unpack function had a VECTOR arm at all, so Reader.ReadRows answered NULL
// for every VECTOR column it was asked for.
//
// dim is the dimension the SCHEMA declares, and a file whose entries are a
// different width is refused. The count is derived from the bytes, so a
// VECTOR(8) column read out of a file storing four floats used to come back
// as a four-element vector with no complaint at all — a shorter vector is
// not a truncated one, it is a different point, and every distance function
// downstream would answer over the wrong number of dimensions.
func float32sFromBytes(b []byte, dim int) ([]float32, error) {
	if len(b)%4 != 0 {
		return nil, fmt.Errorf("VECTOR value is %d bytes, not a whole number of float32s", len(b))
	}
	n := len(b) / 4
	if dim > 0 && n != dim {
		return nil, fmt.Errorf("VECTOR value holds %d dimensions (%d bytes) but the schema declares %d", n, len(b), dim)
	}
	out := make([]float32, n)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return out, nil
}

// PhysicalReadableAs reports whether a leaf physically stored as pt can be
// decoded as the engine type t — that is, whether the typed accessor t's
// decode arm reaches (values.go) is the one that pt's bytes are laid out
// for.
//
// This is a question about the FILE, and it is deliberately not asked of
// wadjetTypeToPhysical, which answers what OUR WRITER would have emitted.
// The two disagree wherever parquet allows a type more than one physical
// encoding, and DECIMAL is exactly that case: our writer stores DECIMAL as
// INT64, but the format allows INT32, INT64, BYTE_ARRAY and
// FIXED_LEN_BYTE_ARRAY, and pyarrow uses all of them. Asking the writer's
// mapping let a catalog INT64 (or TIMESTAMP, IPv4, MAC, DURATION) sit over a
// file DECIMAL backed by INT32 — same answer from wadjetTypeToPhysical, four
// bytes per value in the file, eight read out of it.
//
// The bytes family accepts both BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY because
// both decode through the same offset table, and INT96 because it decodes as
// a 12-byte fixed-length value.
func PhysicalReadableAs(t TypeID, pt PhysicalType) bool {
	switch t {
	case TypeInt64, TypeTimestamp, TypeDuration, TypeIPv4, TypeMAC:
		return pt == PhysicalInt64
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		return pt == PhysicalInt32
	case TypeFloat64:
		return pt == PhysicalDouble
	case TypeFloat32:
		return pt == PhysicalFloat
	case TypeBool:
		return pt == PhysicalBoolean
	case TypeDecimal:
		// All four physicals the format allows for DECIMAL.
		return pt == PhysicalInt32 || pt == PhysicalInt64 ||
			pt == PhysicalByteArray || pt == PhysicalFixedLenByteArray
	case TypeString, TypeCIDR, TypeBytes, TypeIPv6, TypeUUID, TypeVector:
		return pt == PhysicalByteArray || pt == PhysicalFixedLenByteArray || pt == PhysicalInt96
	case TypeArray, TypeRow, TypeMap:
		// Containers are not decoded from one leaf; their leaves are
		// checked individually.
		return true
	default:
		return true
	}
}

// StorageClassOf returns the vector backing a decoded value lands in: types
// that share a class have byte-identical in-memory storage, so a page decoded
// as one can be copied into a vector allocated for the other with no
// conversion at all.
//
// It is deliberately EXHAUSTIVE — no default arm. The relation it defines is
// what both read paths use to decide whether a (catalog, file) pairing may be
// decoded verbatim, and a catch-all bucket makes that decision by accident:
// the scan's own version had one, and it put DECIMAL, VECTOR and the
// bytes-backed types in a single class. A catalog STRING over a file DECIMAL
// therefore "matched", and the copy then indexed a Decimal vector's Int128
// array through a page of byte offsets. Every new TypeID must be given a
// class here on purpose.
//
// The classes are named by a representative TypeID rather than by a private
// enum so callers in other packages (the scan's storageClass, the planner's
// footer-statistics path) can hold the result without importing one.
func StorageClassOf(t TypeID) TypeID {
	switch t {
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		return TypeInt64
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		return TypeInt32
	case TypeFloat64:
		return TypeFloat64
	case TypeFloat32:
		return TypeFloat32
	case TypeBool:
		return TypeBool
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		return TypeString
	case TypeDecimal:
		return TypeDecimal
	case TypeVector:
		return TypeVector
	case TypeArray:
		return TypeArray
	case TypeRow:
		return TypeRow
	case TypeMap:
		return TypeMap
	}
	// Unreachable for every TypeID the engine defines. A type added without
	// a class above gets one of its own rather than joining a bucket, so the
	// failure is a refused read, not a wrong one.
	return t
}

// DecodeCompatible reports whether a page decoded as fileType can be copied
// into a vector allocated for catalogType without converting anything. It is
// storage-class equality, named for what the callers are asking.
func DecodeCompatible(fileType, catalogType TypeID) bool {
	return StorageClassOf(fileType) == StorageClassOf(catalogType)
}

// CoercibleTo reports whether values decoded as the type the FILE stores can
// be converted, after decode, to the type the CATALOG declares.
//
// This set is the contract between the two read paths. Which one runs is
// decided by the SHAPE of the schema — a table one column of ARRAY/MAP away
// from the row reader sends every query on it down that path (#393) — not by
// the query, so a pairing one path converts and the other refuses is a
// two-path divergence waiting for a schema change to expose it. The native
// scan implements exactly these three in copyNativeCoercedDirect /
// copyNativeCoercedScatter; readColumnToAny implements them here.
//
// Anything outside the set stays an error on both paths. Widening INT32 to
// INT64 is deliberately NOT here: it is indistinguishable from the
// catalog/file drift this guard exists to catch, and unlike the three below
// it has no scan-side implementation to agree with.
func CoercibleTo(file, want TypeID) bool {
	switch {
	case file == TypeInt64 && want == TypeInt32:
		return true
	case file == TypeInt64 && want == TypeFloat64:
		return true
	case (file == TypeDate || file == TypeInt32) && want == TypeString:
		return true
	}
	return false
}

// FormatDateDays renders a DATE (days since 1970-01-01) the way the engine
// does. batch.FormatDate delegates here so the row path and the native scan
// cannot drift apart on a DATE→STRING coercion.
func FormatDateDays(days int32) string {
	return epochDate.AddDate(0, 0, int(days)).Format("2006-01-02")
}

// fixedByteWidth is the exact entry width a type needs from a
// FIXED_LEN_BYTE_ARRAY leaf, or 0 when any width is acceptable. A UUID is
// sixteen bytes by definition and an IPv6 address is too; a leaf of another
// width is not a truncated one, it is a different value.
func fixedByteWidth(c Column) int {
	switch c.Type {
	case TypeUUID, TypeIPv6:
		return 16
	case TypeVector:
		if c.Dimension > 0 {
			return c.Dimension * 4
		}
	}
	return 0
}

// retypeFromCatalog replaces each read column's type with the catalog's,
// where the catalog names it, both sides are leaves, and the two types are
// carried by the same physical bytes.
//
// Leaves only, deliberately: a nested column's read plan is built from the
// FILE's shape (the assembly plan is built from the file's schema tree),
// and
// substituting a catalog Column whose children were resolved differently
// would look up leaves that do not exist. A lossy leaf INSIDE a container
// stays lossy — that is the same annotation gap, one level down, and it
// needs the annotations, not a substitution.
//
// Same physical bytes, non-negotiably: this substitution exists so that a
// column the file cannot ANNOTATE (IPv4, IPv6, MAC, PORT, PROTOCOL,
// DURATION, BYTES, UUID) is decoded as what it is, and every one of those
// eight has the same physical type as the type the file recovered for it.
// A catalog type of a DIFFERENT width is not a lost annotation, it is
// catalog/file drift, and honouring it means decoding the file's bytes as
// a wider element: unpackAllPresent would ask Values.Int64() for one int64
// per INT32 in the page, an unsafe.Slice twice as long as its backing array
// — megabytes of adjacent heap returned as query results.
//
// The comparison is against the FILE LEAF's RECOVERED TYPE — physical type
// plus the logical/converted annotations that TypeIDFromSchemaNode reads —
// not against a physical type alone and not against what our writer would
// have chosen. Each of those weaker questions admits pairings that decode to
// nonsense:
//
//   - Our writer's mapping on both sides compares what WE would have written,
//     so a pyarrow DECIMAL(9,2) (physically INT32) sitting under a catalog
//     INT64 compared INT64 to INT64 and passed, then read eight bytes per
//     four-byte value.
//   - The physical type alone cannot tell a DECIMAL from the INT32, INT64,
//     BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY it is stored in — the format allows
//     all four, and only the annotation says which one this is. Asking
//     "is DECIMAL readable from this physical?" therefore answered yes for
//     every leaf in the file, so a catalog DECIMAL(18,2) over a STRING column
//     was admitted and read ("hello","world") back as two integers made of
//     the letters. Same width, different meaning: the decode does not fault,
//     it just answers something else.
//
// The question that is actually being asked is whether the values the file's
// own type decodes to can be STORED as the catalog's type without converting
// them, and StorageClassOf is exactly that relation. DECIMAL and VECTOR are
// classes of their own, so a catalog DECIMAL is admissible only over a leaf
// the annotations already recovered AS a decimal — at which point there is
// nothing to substitute and the loop has already skipped it. The eight
// inexpressible types share a class with the plain INT32/INT64/BYTE_ARRAY
// their annotation-free leaves recover as, which is the whole mechanism.
// CoercibleTo names the only pairings admitted ACROSS classes, and those are
// decoded as the file's type and converted afterwards.
//
// Drift is an ERROR rather than a silent skip. Skipping would answer the
// query from the file's own type, which is a different answer from the one
// the catalog promised, arrived at without saying so; the caller cannot tell
// that from a correct read. A named error says which column, what the
// catalog claims and what the file actually holds — which is the whole
// diagnosis.
func retypeFromCatalog(readCols, catalog []Column, leaves []*SchemaNode) ([]Column, error) {
	if len(catalog) == 0 || len(readCols) == 0 {
		return readCols, nil
	}
	byName := make(map[string]Column, len(catalog))
	for _, c := range catalog {
		byName[strings.ToLower(c.Name)] = c
	}
	// Top-level first, for the same reason TopLevelLeafIndex exists: the
	// catalog names a TOP-LEVEL column, and a struct field of the same name
	// would otherwise decide what physical type this substitution is vetted
	// against.
	leafByName := make(map[string]*SchemaNode, len(leaves))
	for _, l := range leaves {
		if l == nil || !l.IsLeaf() {
			continue
		}
		k := strings.ToLower(l.Name)
		if prev, dup := leafByName[k]; dup && len(prev.Path) == 1 {
			continue
		}
		leafByName[k] = l
	}
	out := make([]Column, len(readCols))
	copy(out, readCols)
	for i, c := range out {
		want, ok := byName[strings.ToLower(c.Name)]
		if !ok || want.Type == c.Type {
			continue
		}
		if isNestedType(c.Type) || isNestedType(want.Type) {
			continue
		}
		// The file's own leaf is the authority, annotations included.
		// Without one (a column the file does not carry, read as all-NULL)
		// fall back to the type the reader recovered, which is what the
		// decode would use anyway.
		fileType := c.Type
		filePhys := wadjetTypeToPhysical(c.Type)
		var typeLen int
		if leaf := leafByName[strings.ToLower(c.Name)]; leaf != nil {
			fileType = TypeIDFromSchemaNode(leaf)
			if leaf.Type != nil {
				filePhys = *leaf.Type
				typeLen = int(leaf.TypeLength)
			}
		}
		if err := checkRetypeAdmissible(c.Name, fileType, filePhys, typeLen, want); err != nil {
			return nil, err
		}
		out[i].Type = want.Type
		out[i].Precision, out[i].Scale = want.Precision, want.Scale
		out[i].Dimension = want.Dimension
	}
	return out, nil
}

func isNestedType(t TypeID) bool {
	return t == TypeArray || t == TypeRow || t == TypeMap
}

// checkRetypeAdmissible decides whether the catalog's type may replace the
// type the file recovered for one leaf. See retypeFromCatalog for why the
// question is asked of the RECOVERED type rather than of a physical type.
//
// Three outcomes, in order:
//
//   - Same storage class: the page's values land in the same vector arrays
//     either way, so the substitution costs nothing and changes nothing about
//     the decode. Only the WIDTH is still open, below.
//   - CoercibleTo: a different class, but one of the three pairings both read
//     paths convert after decode. The decode itself still runs as the FILE's
//     type, so no width question arises.
//   - Anything else is catalog/file drift, and refused by name.
func checkRetypeAdmissible(name string, fileType TypeID, filePhys PhysicalType, typeLen int, want Column) error {
	if CoercibleTo(fileType, want.Type) {
		return nil
	}
	if !DecodeCompatible(fileType, want.Type) {
		return fmt.Errorf(
			"column %q: schema declares %s but the file stores %s (physical %s): "+
				"refusing to decode the file's bytes as a different type",
			name, want.Type, fileType, filePhys)
	}
	// Same class, so the bytes are copied verbatim — but a UUID is sixteen
	// bytes by definition and an IPv6 address is too, and a VECTOR(N) is
	// 4N. A leaf of another width is not a truncated value, it is a
	// different one.
	w := fixedByteWidth(want)
	if w == 0 {
		return nil
	}
	switch filePhys {
	case PhysicalFixedLenByteArray:
		// Every entry has the schema's width, so one check covers the column.
		if typeLen != w {
			return fmt.Errorf(
				"column %q: schema declares %s (%d bytes per value) but the file's "+
					"FIXED_LEN_BYTE_ARRAY entries are %d bytes",
				name, want.Type, w, typeLen)
		}
	case PhysicalByteArray:
		// Entries are individually sized, so the width cannot be settled
		// from the footer at all: it is checked per value at decode
		// (unpackAllPresent / unpackWithNulls). Admitting the column here
		// and refusing the wrong value there is the only order that can
		// name the offending row. Before this, a BYTE_ARRAY leaf skipped
		// the width check entirely — it only ran for FIXED_LEN_BYTE_ARRAY —
		// so a catalog UUID over a column of arbitrary strings read back
		// as UUIDs of whatever length the strings happened to be.
	default:
		return fmt.Errorf(
			"column %q: schema declares %s (%d bytes per value) but the file's leaf is physical %s",
			name, want.Type, w, filePhys)
	}
	return nil
}

func filterSchemaColumns(cols []Column, selected []string) []Column {
	need := make(map[string]bool, len(selected))
	for _, s := range selected {
		need[s] = true
	}
	var result []Column
	for _, c := range cols {
		if need[c.Name] {
			result = append(result, c)
		}
	}
	return result
}

// readColumnToAny reads all pages for a column and returns values as []any.
//
// col carries the type the values are decoded AS (the caller's, once
// retypeFromCatalog has vetted it against the file's physical type) plus the
// declared VECTOR dimension, which the unpack functions check the file's
// entry width against.
func readColumnToAny(fr *FileReader, rgIdx, colIdx, numRows int, col Column) ([]any, error) {
	pr := fr.ColumnPages(rgIdx, colIdx)
	if pr == nil {
		return make([]any, numRows), nil
	}
	defer pr.Close()

	leaves := fr.Leaves()
	maxDef := int32(0)
	if colIdx < len(leaves) {
		maxDef = int32(leaves[colIdx].MaxDefLevel)
	}

	// Schema evolution: the catalog names a type the file does not store,
	// but one the file's values can be CONVERTED to after decode. Decode as
	// the file's type and convert at the end — decoding as the catalog's
	// type would hand a page of the wrong width to the typed accessor,
	// which is the failure this whole guard exists for. The native scan
	// converts the same pairings in copyNativeCoerced*, gated by the same
	// CoercibleTo set, so the two paths answer a coerced column identically.
	convertTo, convert := col.Type, false
	if colIdx < len(leaves) {
		if ft := TypeIDFromSchemaNode(leaves[colIdx]); ft != col.Type && CoercibleTo(ft, col.Type) {
			col.Type = ft
			convert = true
		}
	}
	typeID := col.Type

	dict, err := pr.NextDictionary()
	if err != nil {
		return nil, fmt.Errorf("reading dictionary: %w", err)
	}

	values := make([]any, numRows)
	offset := 0

	for {
		page, err := pr.NextPage()
		if err != nil {
			return nil, fmt.Errorf("reading page: %w", err)
		}
		if page == nil {
			break
		}

		data := page.Data
		if page.IsDictEncoded() {
			if dict == nil {
				return nil, fmt.Errorf("dictionary-encoded page but chunk has no dictionary page")
			}
			data, err = resolveDictForRows(dict, data, typeID)
			if err != nil {
				return nil, fmt.Errorf("resolving dictionary page: %w", err)
			}
		}

		// The row group's num_rows sized `values`; the page headers are the
		// file's separate claim about how many the chunk carries, and the
		// format does not reconcile them. This used to CLAMP — read the
		// first numRows values and drop the rest — which answers a query
		// from a file that contradicts itself without saying so. The native
		// scan refuses the same shape, and a disagreement between the two
		// read paths on a corrupt file is how a corrupt file becomes two
		// different answers.
		n := page.NumValues
		if n < 0 || offset+n > numRows {
			page.Release()
			return nil, fmt.Errorf(
				"column %s: page declares %d values at row %d but the row group holds %d rows",
				col.Name, n, offset, numRows)
		}

		if page.NumNulls == 0 || page.DefinitionLevels == nil {
			err = unpackAllPresent(values, offset, data, n, col)
		} else {
			err = unpackWithNulls(values, offset, data, page.DefinitionLevels, maxDef, n, col)
		}
		offset += n
		page.Release()
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
	}

	// A micro/nano TIMESTAMP column decodes to the file's unit; the engine
	// speaks milliseconds. See TimestampDivisorFromSchemaNode.
	if typeID == TypeTimestamp && colIdx < len(leaves) {
		if div := TimestampDivisorFromSchemaNode(leaves[colIdx]); div != 1 {
			for i, v := range values {
				if iv, ok := v.(int64); ok {
					values[i] = TimestampToEngineMillis(iv, div)
				}
			}
		}
	}
	if convert {
		coerceDecoded(values, typeID, convertTo)
	}
	return values, nil
}

// coerceDecoded converts a column decoded as the file's type into the type
// the catalog names, for the pairings CoercibleTo admits.
//
// INT64→INT32 needs nothing: the row path boxes both as a Go int64 and the
// INT32 vector narrows on store, which is what the native path's
// int32(src[i]) does. The other two produce the value the native path
// produces, from the same helper in the DATE case.
func coerceDecoded(values []any, from, to TypeID) {
	switch {
	case from == TypeInt64 && to == TypeFloat64:
		for i, v := range values {
			if iv, ok := v.(int64); ok {
				values[i] = float64(iv)
			}
		}
	case (from == TypeDate || from == TypeInt32) && to == TypeString:
		for i, v := range values {
			if iv, ok := v.(int64); ok {
				values[i] = FormatDateDays(int32(iv))
			}
		}
	}
}

// checkPageDecodable refuses a page whose bytes the column's decode arm
// cannot read, INSTEAD of letting the typed accessor answer nil.
//
// nil was the whole bug on this path: every unpack loop is written
// `for i := 0; i < n && i < len(src); i++`, so a nil src ran zero
// iterations and left every dst slot untouched — a 1000-row column read
// back as 1000 NULLs with err == nil. The engine cannot tell that from a
// column that really is all NULL. The two ways to get here are a footer
// whose logical annotation disagrees with its own leaf's physical type
// (INT64 bytes annotated LogicalInteger{BitWidth: 32}) and catalog/file
// drift that retypeFromCatalog did not see because the column has no leaf
// of its own.
func checkPageDecodable(col Column, data Values, n int) error {
	if n <= 0 {
		return nil
	}
	if !PhysicalReadableAs(col.Type, data.PhysType()) {
		return fmt.Errorf("column %q: %s cannot be decoded from a %s page",
			col.Name, col.Type, data.PhysType())
	}
	// The accessor also refuses a page body too short for its declared
	// value count (values.go). Catch that here rather than as silent NULLs.
	short := false
	switch data.PhysType() {
	case PhysicalInt64:
		short = len(data.Int64()) == 0
	case PhysicalInt32:
		short = len(data.Int32()) == 0
	case PhysicalDouble:
		short = len(data.Double()) == 0
	case PhysicalFloat:
		short = len(data.Float()) == 0
	case PhysicalBoolean:
		// Eight values per byte, and Count() is the VALUE count (nulls do
		// not consume a bit), so this is the right comparison on both the
		// all-present and the nullable path.
		short = len(data.Boolean())*8 < data.Count()
	}
	if short {
		return fmt.Errorf("column %q: page declares %d %s values but its body cannot back them",
			col.Name, data.Count(), data.PhysType())
	}
	return nil
}

// checkByteWidth is the per-VALUE half of the width guard a BYTE_ARRAY leaf
// needs. A FIXED_LEN_BYTE_ARRAY declares one width for the whole column and
// retypeFromCatalog settles it from the footer; a BYTE_ARRAY declares
// nothing, so the only place a 16-byte type can be held to sixteen bytes is
// here, at the value. w comes from fixedByteWidth and is 0 for the types
// that have no fixed width, which is the fast exit.
//
// It used to carve out a zero-length value as "an absence, not a wrong
// width", because the writer stored an unparseable IPv6 literal as no bytes
// at all. That is not what the carve-out did: the value came back as a
// non-NULL empty string: false to IS NULL, and equal to the empty
// string. The absence is
// real, so it is now expressed as one — the callers set NULL for a
// zero-length entry in a fixed-width column and never reach this check with
// got == 0. What remains is the case the check is for: four bytes, or
// twenty-four, in a column whose entries are sixteen. That is a different
// value, not a missing one.
func checkByteWidth(col Column, w, got, row int) error {
	if w == 0 || got == w {
		return nil
	}
	return fmt.Errorf("column %q: %s is %d bytes per value but row %d holds %d",
		col.Name, col.Type, w, row, got)
}

// unpackAllPresent decodes a page with no nulls into dst[offset:offset+n].
func unpackAllPresent(dst []any, offset int, data Values, n int, col Column) error {
	if err := checkPageDecodable(col, data, n); err != nil {
		return err
	}
	return decodePresentValues(dst[offset:offset+n], data, n, col)
}

// unpackWithNulls decodes a page's PRESENT values and scatters them to the
// row slots its definition levels put them in, leaving the rest nil.
//
// The present values are decoded contiguously into the head of the
// destination and then moved outward from the END, which is safe because a
// value's source index is never greater than its destination index. Sharing
// one decode with unpackAllPresent is the point: this function and that one
// used to carry a duplicate arm per type, and duplicated arms drift — see
// decodePageValues.
func unpackWithNulls(dst []any, offset int, data Values, defLevels []int32, maxDef int32, n int, col Column) error {
	if err := checkPageDecodable(col, data, data.Count()); err != nil {
		return err
	}
	present := 0
	for i := 0; i < n && i < len(defLevels); i++ {
		if defLevels[i] == maxDef {
			present++
		}
	}
	if err := decodePresentValues(dst[offset:offset+present], data, present, col); err != nil {
		return err
	}
	vi := present - 1
	for i := n - 1; i >= 0; i-- {
		if i < len(defLevels) && defLevels[i] == maxDef {
			if vi >= 0 {
				dst[offset+i] = dst[offset+vi]
			}
			vi--
			continue
		}
		dst[offset+i] = nil
	}
	return nil
}

// decodePresentValues decodes the first n PRESENT values of a page into
// dst[0:n], one entry per value and no null slots.
//
// This is the row reader's single set of type arms. Every caller — the
// all-present page, the nullable page, and the nested path's per-leaf
// stream — decodes through it, so a type the reader can read as a top-level
// column it can also read three containers deep, and vice versa.
func decodePresentValues(dst []any, data Values, n int, col Column) error {
	if n <= 0 {
		return nil
	}
	switch col.Type {
	case TypeInt64, TypeTimestamp, TypeDuration, TypeIPv4, TypeMAC:
		src := data.Int64()
		for i := 0; i < n && i < len(src); i++ {
			dst[i] = src[i]
		}
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		src := data.Int32()
		for i := 0; i < n && i < len(src); i++ {
			dst[i] = int64(src[i])
		}
	case TypeFloat64:
		src := data.Double()
		for i := 0; i < n && i < len(src); i++ {
			dst[i] = src[i]
		}
	case TypeFloat32:
		src := data.Float()
		for i := 0; i < n && i < len(src); i++ {
			dst[i] = float64(src[i])
		}
	case TypeBool:
		boolBytes := data.Boolean()
		for i := 0; i < n; i++ {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			dst[i] = byteIdx < len(boolBytes) && (boolBytes[byteIdx]&(1<<bitIdx)) != 0
		}
	case TypeString, TypeCIDR:
		rawData, offsets := data.ByteArray()
		if offsets != nil {
			for i := 0; i < n && i+1 < len(offsets); i++ {
				dst[i] = string(rawData[offsets[i]:offsets[i+1]])
			}
		}
	case TypeBytes, TypeIPv6, TypeUUID:
		rawData, offsets := data.ByteArray()
		if offsets != nil {
			w := fixedByteWidth(col)
			for i := 0; i < n && i+1 < len(offsets); i++ {
				// A zero-length entry in a fixed-width column is an absence
				// (convertNetworkLiteral). Leaving dst nil is how this layer
				// says NULL. BYTES has no fixed width, so an empty BYTES
				// value stays the empty value it is.
				if w > 0 && offsets[i+1] == offsets[i] {
					continue
				}
				b := make([]byte, offsets[i+1]-offsets[i])
				copy(b, rawData[offsets[i]:offsets[i+1]])
				if err := checkByteWidth(col, w, len(b), i); err != nil {
					return err
				}
				dst[i] = b
			}
		}
	case TypeVector:
		rawData, offsets := data.ByteArray()
		if offsets != nil {
			for i := 0; i < n && i+1 < len(offsets); i++ {
				v, err := float32sFromBytes(rawData[offsets[i]:offsets[i+1]], col.Dimension)
				if err != nil {
					return err
				}
				dst[i] = v
			}
		}
	case TypeDecimal:
		return decodeDecimalValues(dst, data, n, col)
	}
	return nil
}

// decodeDecimalValues decodes a DECIMAL page, in any of the four physical
// encodings the format allows, into the row path's box for the column.
//
// A column whose declared precision exceeds 18 digits is boxed as
// Decimal128, because that is the point past which the unscaled value stops
// fitting in an int64. It used to be boxed as an int64 regardless: for a
// 16-byte DECIMAL(38,10) the accumulator shifted the top eight bytes
// straight out of the register and returned the low 64 bits of the unscaled
// value, reinterpreted as signed, with no error (#419). The native scan
// decodes the same bytes into a 128-bit value and was right; the two paths
// are chosen by the SHAPE of the table's schema, not by the query, so the
// same column answered differently depending on whether some OTHER column in
// the table happened to be nested.
//
// Under 19 digits the box stays an int64 — the shape every consumer of this
// reader already handles — and a value that does not fit one is an ERROR
// naming the column, not a different number. That case means the file's
// values contradict the precision the file itself declares.
func decodeDecimalValues(dst []any, data Values, n int, col Column) error {
	wide := decimalNeeds128(col)
	switch data.PhysType() {
	case PhysicalInt64:
		src := data.Int64()
		for i := 0; i < n && i < len(src); i++ {
			if wide {
				dst[i] = Decimal128From(src[i])
			} else {
				dst[i] = src[i]
			}
		}
	case PhysicalInt32:
		src := data.Int32()
		for i := 0; i < n && i < len(src); i++ {
			if wide {
				dst[i] = Decimal128From(int64(src[i]))
			} else {
				dst[i] = int64(src[i])
			}
		}
	default:
		rawData, offsets := data.ByteArray()
		if offsets == nil {
			return nil
		}
		for i := 0; i < n && i+1 < len(offsets); i++ {
			hi, lo := decimalFromBytesRaw(rawData[offsets[i]:offsets[i+1]])
			d := Decimal128{Hi: hi, Lo: lo}
			if wide {
				dst[i] = d
				continue
			}
			v, ok := d.Int64()
			if !ok {
				return fmt.Errorf("column %q: DECIMAL(%d,%d) value %s at entry %d does not fit "+
					"the 64 bits its declared precision allows", col.Name, col.Precision, col.Scale, d, i)
			}
			dst[i] = v
		}
	}
	return nil
}

// decimalNeeds128 reports whether a DECIMAL column's unscaled values must be
// boxed as Decimal128 on the row path. Eighteen digits is the last precision
// whose every value fits an int64 (int64 holds 19 digits, but not all
// 19-digit numbers), and 18 is also the widest DECIMAL parquet allows over
// an INT64, so this is exactly the set of columns the narrow box cannot
// carry.
//
// A column that declares no precision at all keeps the narrow box: an
// unannotated DECIMAL is a malformed file rather than a wide one, and the
// per-value fit check in decodeDecimalValues refuses a value that overflows
// instead of returning a different number for it.
func decimalNeeds128(col Column) bool {
	return col.Precision > 18
}

// ValidateDictIndices checks that a page's dictionary indices are all
// within [0, numValues) before any gather dereferences them. Corrupt or
// hostile files can carry out-of-range indices; without this pass the
// typed gather loops panic (or silently read the wrong entry).
func ValidateDictIndices(indices []int32, n, numValues int) error {
	if len(indices) < n {
		return fmt.Errorf("dictionary page: %d indices for %d values", len(indices), n)
	}
	for i := 0; i < n; i++ {
		if idx := indices[i]; idx < 0 || int(idx) >= numValues {
			return fmt.Errorf("dictionary index %d out of range [0, %d)", idx, numValues)
		}
	}
	return nil
}

// resolveDictForRows resolves dictionary indices to actual values.
//
// The indices are validated against the dictionary's declared entry count by
// ValidateDictIndices; dictEntries then validates that the dictionary's
// DECODED entries actually number that many. The two are not the same claim:
// the count comes from the page header, the entries come from the accessor
// for typeID, and the accessor answers nil when the dictionary page's
// physical type is not the one typeID reads (see values.go). Without the
// second check a validated index still indexes an empty slice.
func resolveDictForRows(dict *DictionaryData, page Values, typeID TypeID) (Values, error) {
	indices := page.Int32()
	n := page.Count()
	if err := ValidateDictIndices(indices, n, dict.NumValues); err != nil {
		return Values{}, err
	}

	switch typeID {
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		return gatherDictInt64(dict, indices, n, typeID)
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		return gatherDictInt32(dict, indices, n, typeID)
	case TypeDecimal:
		// DECIMAL is the one engine type parquet stores in four different
		// physicals, and which one a file used is a property of the FILE,
		// not of the type. Without these two arms a dict-encoded DECIMAL
		// backed by INT32 or INT64 — what pyarrow writes for precisions up
		// to 18 — fell through to the BYTE_ARRAY default and could not be
		// read at all.
		switch dict.Data.PhysType() {
		case PhysicalInt64:
			return gatherDictInt64(dict, indices, n, typeID)
		case PhysicalInt32:
			return gatherDictInt32(dict, indices, n, typeID)
		}
		return gatherDictBytes(dict, indices, n, typeID)
	case TypeFloat64:
		src := dict.Data.Double()
		if err := dictEntries(len(src), dict, typeID); err != nil {
			return Values{}, err
		}
		dst := make([]float64, n)
		for i := 0; i < n; i++ {
			dst[i] = src[indices[i]]
		}
		return PlainFloat64Values(dst), nil
	case TypeFloat32:
		src := dict.Data.Float()
		if err := dictEntries(len(src), dict, typeID); err != nil {
			return Values{}, err
		}
		dst := make([]float32, n)
		for i := 0; i < n; i++ {
			dst[i] = src[indices[i]]
		}
		return PlainFloat32Values(dst), nil
	default:
		return gatherDictBytes(dict, indices, n, typeID)
	}
}

func gatherDictInt64(dict *DictionaryData, indices []int32, n int, typeID TypeID) (Values, error) {
	src := dict.Data.Int64()
	if err := dictEntries(len(src), dict, typeID); err != nil {
		return Values{}, err
	}
	dst := make([]int64, n)
	for i := 0; i < n; i++ {
		dst[i] = src[indices[i]]
	}
	return PlainInt64Values(dst), nil
}

func gatherDictInt32(dict *DictionaryData, indices []int32, n int, typeID TypeID) (Values, error) {
	src := dict.Data.Int32()
	if err := dictEntries(len(src), dict, typeID); err != nil {
		return Values{}, err
	}
	dst := make([]int32, n)
	for i := 0; i < n; i++ {
		dst[i] = src[indices[i]]
	}
	return PlainInt32Values(dst), nil
}

func gatherDictBytes(dict *DictionaryData, indices []int32, n int, typeID TypeID) (Values, error) {
	dictData, dictOffsets := dict.Data.ByteArray()
	if err := dictEntries(len(dictOffsets)-1, dict, typeID); err != nil {
		return Values{}, err
	}
	var buf []byte
	offsets := make([]uint32, n+1)
	for i := 0; i < n; i++ {
		idx := indices[i]
		offsets[i] = uint32(len(buf))
		buf = append(buf, dictData[dictOffsets[idx]:dictOffsets[idx+1]]...)
	}
	offsets[n] = uint32(len(buf))
	return ByteArrayValues(buf, offsets), nil
}

// dictEntries checks that a dictionary page decoded as many entries as its
// header claims, so a validated index cannot run off the decoded slice.
func dictEntries(decoded int, dict *DictionaryData, typeID TypeID) error {
	if decoded < dict.NumValues {
		return fmt.Errorf("dictionary page declares %d entries but decoded %d as %s (physical %s)",
			dict.NumValues, decoded, typeID, dict.Data.PhysType())
	}
	return nil
}

// CompareNative compares two native Go values for ordering.
func CompareNative(a, b any) int {
	switch av := a.(type) {
	case int64:
		if bv, ok := b.(int64); ok {
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	case float64:
		if bv, ok := b.(float64); ok {
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	case string:
		if bv, ok := b.(string); ok {
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	case []byte:
		if bv, ok := b.([]byte); ok {
			la, lb := len(av), len(bv)
			for i := 0; i < la && i < lb; i++ {
				if av[i] < bv[i] {
					return -1
				}
				if av[i] > bv[i] {
					return 1
				}
			}
			if la < lb {
				return -1
			}
			if la > lb {
				return 1
			}
			return 0
		}
	}
	return 0
}

// RowGroupNumRows returns the row count for a row group (convenience for callers
// that don't want to go through FileReader).
func (r *Reader) RowGroupNumRows(index int) int64 {
	return r.fr.RowGroupNumRows(index)
}

// decimalFromBytes converts big-endian two's complement bytes to Int128.
// Exported for use by the physical planner's decimal page reader.
func DecimalFromBytes(b []byte) [2]uint64 {
	n := len(b)
	if n == 0 {
		return [2]uint64{}
	}

	negative := b[0]&0x80 != 0
	var hi, lo uint64
	if negative {
		hi = ^uint64(0)
		lo = ^uint64(0)
	}

	for i := 0; i < n; i++ {
		if i < n-8 {
			hi = (hi << 8) | uint64(b[i])
		} else {
			lo = (lo << 8) | uint64(b[i])
		}
	}

	// For short byte arrays (<= 8 bytes), hi should be sign-extended
	if n <= 8 {
		hi = 0
		if negative {
			hi = ^uint64(0)
		}
		lo = 0
		if negative {
			lo = ^uint64(0)
		}
		for _, c := range b {
			lo = (lo << 8) | uint64(c)
		}
	}

	return [2]uint64{hi, lo}
}

// Decimal128 is the row path's box for a DECIMAL column's UNSCALED value,
// in the same two-word form the engine's decimal vector stores
// (batch.Int128). It exists because the row shape is []any and this package
// cannot import the engine's batch package — batch imports this one.
//
// It is used only for a column whose declared precision exceeds 18 digits
// (decimalNeeds128). Narrower columns keep the plain int64 box every
// existing consumer of Reader.ReadRows already handles, so the new shape
// appears exactly where the old one was returning a wrong number.
type Decimal128 struct {
	Hi int64  // upper 64 bits, signed
	Lo uint64 // lower 64 bits
}

// Decimal128From widens an int64 unscaled value.
func Decimal128From(v int64) Decimal128 {
	hi := int64(0)
	if v < 0 {
		hi = -1
	}
	return Decimal128{Hi: hi, Lo: uint64(v)}
}

// Int64 returns the value as an int64 and whether it fits in one.
func (d Decimal128) Int64() (int64, bool) {
	if d.Hi == 0 && int64(d.Lo) >= 0 {
		return int64(d.Lo), true
	}
	if d.Hi == -1 && int64(d.Lo) < 0 {
		return int64(d.Lo), true
	}
	return 0, false
}

// String renders the unscaled integer in base 10, so an error message about
// a decimal that does not fit names the actual number.
func (d Decimal128) String() string {
	n := new(big.Int).SetInt64(d.Hi)
	n.Lsh(n, 64)
	return n.Or(n, new(big.Int).SetUint64(d.Lo)).String()
}

// decimalFromBytesRaw converts big-endian byte array to Int128 (hi, lo).
// Used internally by readDecimalPage.
func decimalFromBytesRaw(b []byte) (int64, uint64) {
	n := len(b)
	if n == 0 {
		return 0, 0
	}
	negative := b[0]&0x80 != 0
	var hi int64
	var lo uint64
	if negative {
		hi = -1
		lo = ^uint64(0)
	}
	if n <= 8 {
		lo = 0
		if negative {
			lo = ^uint64(0)
		}
		for _, c := range b {
			lo = (lo << 8) | uint64(c)
		}
		if negative {
			hi = -1
		}
	} else {
		for i := 0; i < n-8; i++ {
			hi = (hi << 8) | int64(b[i])
		}
		lo = 0
		for i := n - 8; i < n; i++ {
			lo = (lo << 8) | uint64(b[i])
		}
	}
	return hi, lo
}
