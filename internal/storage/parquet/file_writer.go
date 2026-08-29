package parquet

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

// NativeWriter writes Parquet files without depending on parquet-go.
// It uses PLAIN encoding, RLE definition levels, and configurable compression.
// Supports flat columns as well as nested types (ARRAY/LIST, MAP, ROW/STRUCT).
//
// Usage:
//
//	w := NewNativeWriter(out, schema, cfg)
//	w.WriteMapRows(rows)
//	w.Close()
type NativeWriter struct {
	w      io.Writer
	schema Schema
	config WriterConfig
	codec  CompressionCodec

	// One leaf buffer per leaf column in the flattened schema.
	leafBufs []leafBuffer
	// Maps top-level column index to its leaf range [startLeaf, endLeaf).
	colLeafRanges []leafRange
	numRows       int
	// rowsSeen counts rows across the whole file, where numRows resets at
	// every row group. It exists so a value error can name the row.
	rowsSeen int64

	written   int64 // total bytes written to w so far
	rowGroups []RowGroup

	// err latches the first structural error found while decomposing a row.
	// Decomposition walks a tree of leaf buffers and has nowhere to return
	// an error to, so it latches here and WriteMapRows/Close surface it.
	// Once set the writer is finished: the leaf buffers for the offending
	// row are already inconsistent with the schema and no later row can
	// repair them.
	err error
}

// leafRange identifies a contiguous range of leaf buffers for a top-level column.
type leafRange struct {
	start int // inclusive
	end   int // exclusive
}

// leafBuffer accumulates values for a single leaf column, supporting
// multi-level definition and repetition levels for nested schemas.
type leafBuffer struct {
	col      Column // the leaf column definition
	physical PhysicalType
	path     []string // full path from schema root (e.g. ["tags", "list", "element"])

	maxDefLevel int32
	maxRepLevel int32

	// PLAIN-encoded value data for fixed-width types.
	data []byte
	// For BYTE_ARRAY: separate offsets and packed data.
	offsets []uint32
	packed  []byte
	// For BOOLEAN: bit-packed into bytes.
	boolBuf []byte
	boolPos int

	// Multi-level definition and repetition levels.
	defLevels []int32
	repLevels []int32
	numNulls  int64
	count     int // total entries (values + nulls/absent markers)

	// Statistics tracking.
	hasStats           bool
	minI32, maxI32     int32
	minI64, maxI64     int64
	minF32, maxF32     float32
	minF64, maxF64     float64
	minBytes, maxBytes []byte

	// minCidrKey/maxCidrKey are the sort keys (CidrStatsSortKey) of whichever
	// TEXT values currently hold minBytes/maxBytes, for a TypeCIDR leaf only.
	// Reset alongside the stats they compare, per row group.
	minCidrKey, maxCidrKey string
	// cidrKeyFailed latches true the first time a TypeCIDR value in this
	// leaf does not parse as an address, ACROSS THE WHOLE FILE — unlike the
	// stats above, reset() must not clear it: a row group that parsed fine
	// does not undo an earlier row group's unparseable value, because
	// writeFooter's CidrStatsOrderKey flag is a promise about every CIDR
	// value in the FILE, not just the current row group's.
	cidrKeyFailed bool
}

// NewNativeWriter creates a Parquet writer that writes to the given io.Writer.
func NewNativeWriter(w io.Writer, schema Schema, cfg WriterConfig) *NativeWriter {
	if cfg.RowGroupSize <= 0 {
		cfg.RowGroupSize = 128 * 1024
	}

	codec := CodecSnappy
	switch cfg.Compression {
	case CompressionZstd:
		codec = CodecZstd
	case CompressionGzip:
		codec = CodecGzip
	case CompressionNone:
		codec = CodecNone
	case CompressionLZ4:
		codec = CodecNone // fall back to uncompressed for LZ4 (rarely used)
	default:
		codec = CodecSnappy
	}

	nw := &NativeWriter{
		w:      w,
		schema: schema,
		config: cfg,
		codec:  codec,
	}
	nw.initLeafBuffers()
	return nw
}

// initLeafBuffers flattens the schema into leaf columns and creates a
// leafBuffer for each one. For flat schemas this is 1:1 with schema columns.
func (nw *NativeWriter) initLeafBuffers() {
	nw.leafBufs = nil
	nw.colLeafRanges = make([]leafRange, len(nw.schema.Columns))

	for i, col := range nw.schema.Columns {
		startIdx := len(nw.leafBufs)
		nw.flattenColumn(col, nil, 0, 0)
		nw.colLeafRanges[i] = leafRange{start: startIdx, end: len(nw.leafBufs)}
	}
}

// flattenColumn recursively walks a Column and appends a leafBuffer for each
// leaf it finds. path, defLevel, repLevel track the nesting context.
func (nw *NativeWriter) flattenColumn(col Column, parentPath []string, defLevel, repLevel int32) {
	switch col.Type {
	case TypeArray:
		// LIST schema: optional group <name> (LIST) { repeated group list { optional <elem> element } }
		// The outer group is optional if col.Nullable.
		curDef := defLevel
		if col.Nullable {
			curDef++ // outer group presence
		}
		curDef++               // list group (repeated) adds 1 to def
		curRep := repLevel + 1 // repeated adds 1 to rep

		elemCol := Column{Name: "element", Type: TypeString, Nullable: true}
		if col.ElementType != nil {
			elemCol = *col.ElementType
			elemCol.Nullable = true // elements are always optional in LIST schema
		}
		elemPath := append(append([]string(nil), parentPath...), col.Name, "list")
		nw.flattenColumn(elemCol, elemPath, curDef, curRep)

	case TypeMap:
		// MAP schema: optional group <name> (MAP) { repeated group key_value (MAP_KEY_VALUE) { required key, optional value } }
		curDef := defLevel
		if col.Nullable {
			curDef++ // outer group presence
		}
		curDef++               // key_value group (repeated) adds 1 to def
		curRep := repLevel + 1 // repeated adds 1 to rep

		if col.ElementType != nil && col.ElementType.Type == TypeRow && len(col.ElementType.Fields) == 2 {
			kvPath := append(append([]string(nil), parentPath...), col.Name, "key_value")
			// The key/value repetition types are fixed by the MAP schema,
			// not by what the caller declared, and these MUST be the same
			// two lines buildMapSchemaElements writes into the footer: the
			// max definition level a leaf buffer stamps on its values is
			// the level the READER derives from that footer. They disagreed
			// — the value took +1 here AND another +1 from its own
			// Nullable, so a MAP whose value column was declared nullable
			// (the natural declaration) wrote every value at def 4 against
			// a file that said 3. The reader counted zero present values,
			// so every MAP value read back NULL, and a map carrying an
			// explicit NULL value desynchronised the level/value streams
			// and took the decoder out of bounds (#393).
			keyCol := col.ElementType.Fields[0]
			keyCol.Nullable = false // keys are required
			valCol := col.ElementType.Fields[1]
			valCol.Nullable = true // values are optional — counted ONCE, below

			keyPath := make([]string, len(kvPath))
			copy(keyPath, kvPath)
			nw.flattenColumn(keyCol, keyPath, curDef, curRep)

			valPath := make([]string, len(kvPath))
			copy(valPath, kvPath)
			nw.flattenColumn(valCol, valPath, curDef, curRep)
		}

	case TypeRow:
		// STRUCT schema: optional group <name> { fields... }
		curDef := defLevel
		if col.Nullable {
			curDef++ // group presence
		}
		groupPath := append(append([]string(nil), parentPath...), col.Name)
		for _, field := range col.Fields {
			nw.flattenColumn(field, groupPath, curDef, repLevel)
		}

	default:
		// Leaf column.
		curDef := defLevel
		if col.Nullable {
			curDef++ // leaf presence
		}
		fullPath := append(append([]string(nil), parentPath...), col.Name)

		lb := leafBuffer{
			col:         col,
			physical:    columnPhysical(col),
			path:        fullPath,
			maxDefLevel: curDef,
			maxRepLevel: repLevel,
		}
		nw.leafBufs = append(nw.leafBufs, lb)
	}
}

// ValidateWriteSchema refuses a schema the writer cannot turn into a
// well-formed file.
//
// The footer's schema tree and the leaf buffers are built by two separate
// walks (buildColumnSchemaElements and flattenColumn), and they agreed about
// a malformed MAP only by both doing nothing: buildMapSchemaElements emits
// the outer group and a "key_value" group declaring NumChildren = 2, then
// emits the key and value ONLY when ElementType is a two-field ROW. A MAP
// built the natural-looking way — Fields holding the key and value columns,
// ElementType nil — therefore wrote a footer whose key_value group promises
// two children and has none. BuildSchemaTree reads that back as a group
// borrowing the next two top-level columns as its children, so the file is
// not merely missing its map: every column after it is misplaced. Nothing
// in the write path complained, and Close returned nil.
//
// A file is the one artifact a writer cannot take back, so the refusal is at
// construction, before a single row is accepted.
func ValidateWriteSchema(schema Schema) error {
	for _, c := range schema.Columns {
		if err := validateWriteColumn(c, c.Name); err != nil {
			return err
		}
	}
	return nil
}

func validateWriteColumn(c Column, path string) error {
	switch c.Type {
	case TypeMap:
		// MAP is stored as ARRAY(ROW(key, value)); ElementType carries that
		// ROW. Fields is the ROW/STRUCT spelling and is not read here.
		if c.ElementType == nil || c.ElementType.Type != TypeRow || len(c.ElementType.Fields) != 2 {
			return fmt.Errorf("column %q: a MAP needs ElementType = ROW with exactly two fields "+
				"(key, value); got ElementType %v with %d Fields on the column itself",
				path, mapElementDesc(c.ElementType), len(c.Fields))
		}
		if err := validateWriteColumn(c.ElementType.Fields[0], path+".key"); err != nil {
			return err
		}
		return validateWriteColumn(c.ElementType.Fields[1], path+".value")
	case TypeArray:
		if c.ElementType == nil {
			return fmt.Errorf("column %q: an ARRAY needs an ElementType", path)
		}
		return validateWriteColumn(*c.ElementType, path+".element")
	case TypeRow:
		if len(c.Fields) == 0 {
			return fmt.Errorf("column %q: a ROW needs at least one field", path)
		}
		for _, f := range c.Fields {
			if err := validateWriteColumn(f, path+"."+f.Name); err != nil {
				return err
			}
		}
		return nil
	case TypeVector:
		// The leaf is FIXED_LEN_BYTE_ARRAY and its type_length is the
		// dimension: without one the footer declares a zero-width leaf,
		// which says nothing about how wide the values actually are.
		if c.Dimension <= 0 {
			return fmt.Errorf("column %q: a VECTOR needs a positive Dimension, got %d", path, c.Dimension)
		}
		return nil
	}
	return nil
}

func mapElementDesc(e *Column) string {
	if e == nil {
		return "nil"
	}
	return e.Type.String()
}

// decimalFLBAWidth is the byte width of a FIXED_LEN_BYTE_ARRAY DECIMAL leaf.
// Sixteen bytes is exactly the unscaled range of DECIMAL(38, s), the widest
// precision the format defines, and it is what pyarrow writes for anything
// past 18 digits.
const decimalFLBAWidth = 16

// decimalMaxInt64Precision is the last DECIMAL precision an INT64 leaf may
// carry. The format's rule, not a policy: INT32 backs precision ≤ 9 and
// INT64 precision ≤ 18, and a file that annotates a wider DECIMAL over an
// INT64 leaf is malformed. pyarrow refuses to open one at all —
//
//	Decimal(precision=38, scale=10) cannot be applied to primitive type INT64
//
// — which is what wadjet used to write for every DECIMAL(p > 18) column,
// because the physical type was chosen from the TypeID alone.
const decimalMaxInt64Precision = 18

// columnPhysical is the physical type this writer emits for one column.
//
// It takes the COLUMN, not the TypeID, because DECIMAL's physical encoding
// is a function of its precision (see decimalMaxInt64Precision) — the one
// place where the two disagree. wadjetTypeToPhysical stays as the TypeID-only
// mapping the reader's compatibility checks ask about.
func columnPhysical(col Column) PhysicalType {
	// decimalEffectivePrecision, not col.Precision: the annotation written
	// below defaults an unset precision to 38, and choosing the physical type
	// from the raw field instead put a `Precision: 0` column in an INT64 leaf
	// annotated DECIMAL(38, s) — the exact combination the corollary above
	// says the Apache implementation refuses to open (R8, #647).
	if col.Type == TypeDecimal && decimalEffectivePrecision(col.Precision) > decimalMaxInt64Precision {
		return PhysicalFixedLenByteArray
	}
	return wadjetTypeToPhysical(col.Type)
}

func wadjetTypeToPhysical(t TypeID) PhysicalType {
	switch t {
	case TypeBool:
		return PhysicalBoolean
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		return PhysicalInt32
	case TypeInt64, TypeIPv4, TypeMAC, TypeDuration, TypeTimestamp:
		return PhysicalInt64
	case TypeFloat32:
		return PhysicalFloat
	case TypeFloat64:
		return PhysicalDouble
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		return PhysicalByteArray
	case TypeDecimal:
		return PhysicalInt64
	case TypeVector:
		return PhysicalFixedLenByteArray
	default:
		return PhysicalByteArray
	}
}

// WriteMapRows writes rows from map[string]any format.
// Network types are converted to their binary storage format.
// Nested types (ARRAY, MAP, ROW) are decomposed into leaf-level
// (value, defLevel, repLevel) triples.
func (nw *NativeWriter) WriteMapRows(rows []map[string]any) error {
	if nw.err != nil {
		return nw.err
	}
	for _, row := range rows {
		for colIdx, col := range nw.schema.Columns {
			val, ok := row[col.Name]
			if !ok {
				val = nil
			}
			lr := nw.colLeafRanges[colIdx]
			leafIdx := lr.start
			nw.decomposeValue(col, val, 0, 0, 0, &leafIdx)
		}
		if nw.err != nil {
			return nw.err
		}
		nw.numRows++
		nw.rowsSeen++

		if nw.numRows >= nw.config.RowGroupSize {
			if err := nw.flushRowGroup(); err != nil {
				return err
			}
		}
	}
	return nil
}

// fail latches the first structural error. See NativeWriter.err.
func (nw *NativeWriter) fail(err error) {
	if nw.err == nil {
		nw.err = err
	}
}

// decomposeValue walks a value according to col's type definition and appends
// (value, defLevel, repLevel) triples to the appropriate leaf buffers.
// leafIdx is advanced as leaves are visited.
func (nw *NativeWriter) decomposeValue(col Column, val any, defLevel, repLevel, repDepth int32, leafIdx *int) {
	switch col.Type {
	case TypeArray:
		nw.decomposeArray(col, val, defLevel, repLevel, repDepth, leafIdx)
	case TypeMap:
		nw.decomposeMap(col, val, defLevel, repLevel, repDepth, leafIdx)
	case TypeRow:
		nw.decomposeRow(col, val, defLevel, repLevel, repDepth, leafIdx)
	default:
		nw.decomposeLeaf(col, val, defLevel, repLevel, leafIdx)
	}
}

// decomposeLeaf handles a primitive leaf column.
func (nw *NativeWriter) decomposeLeaf(col Column, val any, defLevel, repLevel int32, leafIdx *int) {
	lb := &nw.leafBufs[*leafIdx]
	*leafIdx++

	if val == nil {
		lb.appendEntry(defLevel, repLevel)
		return
	}
	// A text literal for a binary network type is converted HERE, where the
	// column and the row are still known and an error can name them. Below
	// this line the converters cannot report failure and never could.
	if s, ok := val.(string); ok && hasNetworkLiteralForm(col.Type) {
		conv, err := convertNetworkLiteral(col.Type, s)
		if err != nil {
			nw.fail(fmt.Errorf("column %q, row %d of this write: %w", col.Name, nw.rowsSeen, err))
			lb.appendEntry(defLevel, repLevel)
			return
		}
		if conv == nil {
			// The empty literal is an absence, and absence is NULL.
			lb.appendEntry(defLevel, repLevel)
			return
		}
		val = conv
	}
	// A DATE text literal is converted HERE too, at the leaf, so a DATE
	// nested in a ROW/ARRAY/MAP is validated on the same path a top-level
	// one is (prepareRows only rewrites top-level columns). An unparseable
	// or nonexistent calendar date used to reach toInt32 -> parseDateForWrite
	// and store the epoch silently — data corruption inside a container the
	// top-level guard never saw (#560). ParseDateDays is the one accept-set
	// and classification the filter path shares.
	if s, ok := val.(string); ok && col.Type == TypeDate {
		days, err := ParseDateDays(s)
		if err != nil {
			nw.fail(fmt.Errorf("column %q, row %d of this write: %w", col.Name, nw.rowsSeen, err))
			lb.appendEntry(defLevel, repLevel)
			return
		}
		val = days
	}
	// A DECIMAL box is resolved to its UNSCALED value HERE, at the leaf, for
	// the same reason a DATE text literal is: this is the last place the
	// column, its declared (p, s) and the row number are all still known, and
	// below this line the converters cannot report failure and never could.
	//
	// DecimalValueFromBox is the whole conversion — which boxes are already
	// unscaled (ADR-0018 §4) and which carry a decimal point, the exact
	// text/float parse, PostgreSQL's assignment rounding, and the declared
	// precision. Its predecessor ran every string and float through
	// strconv.ParseFloat and int64(math.Round(t*pow)): a value wider than the
	// column WRAPPED the int64, unparseable text and every NaN/Infinity stored
	// 0, and exactness was lost past ~16 significant digits — all silently
	// (#647). ADR-0018: a value this package cannot represent fails the WRITE
	// rather than producing a file that reads back as a different number.
	if col.Type == TypeDecimal {
		d, err := DecimalValueFromBox(val, col.Precision, col.Scale)
		if err != nil {
			nw.fail(fmt.Errorf("column %q, row %d of this write: %w", col.Name, nw.rowsSeen, err))
			lb.appendEntry(defLevel, repLevel)
			return
		}
		// A DECIMAL whose precision fits an INT64 leaf is stored in one, and
		// an unscaled value past 64 bits then has no encoding at all. A
		// DECIMAL(p > 18) column is FIXED_LEN_BYTE_ARRAY and has no such
		// bound, so the check is asked of the column's PHYSICAL type rather
		// than of TypeDecimal.
		if columnPhysical(col) == PhysicalInt64 {
			if _, fits := d.Int64(); !fits {
				nw.fail(fmt.Errorf("column %q, row %d of this write: DECIMAL unscaled value %s needs more than 64 bits, "+
					"which this writer's INT64 encoding cannot store", col.Name, nw.rowsSeen, d))
				lb.appendEntry(defLevel, repLevel)
				return
			}
		}
		lb.appendDecimalEntry(lb.maxDefLevel, repLevel, d)
		return
	}
	// Value is present — def level is maxDefLevel.
	lb.appendEntryWithValue(lb.maxDefLevel, repLevel, val)
}

// hasNetworkLiteralForm reports whether a column of this type stores BINARY
// but accepts TEXT on the way in.
func hasNetworkLiteralForm(t TypeID) bool {
	switch t {
	case TypeIPv4, TypeIPv6, TypeMAC, TypeUUID:
		return true
	}
	return false
}

// convertNetworkLiteral turns a text literal into the binary form its column
// is defined to hold: an int64 for IPV4 and MAC, sixteen bytes for IPV6 and
// UUID. It has three outcomes, and the two that are not "it converted" are
// the point.
//
// There used to be one. Every converter answered garbage with a zero value:
// ipv4StringToInt64 and macStringToInt64 returned 0, so "zz" landed in a MAC
// column as 00:00:00:00:00:00, indistinguishable from an address somebody
// meant; ipv6StringToBytes returned nothing; and convertStringToBytes stored
// an unparseable UUID as THE RAW STRING BYTES, so "not-a-uuid" became ten
// bytes in a column whose entries are sixteen. That last one produced a file
// wadjet WROTE that wadjet's own row reader then refused — "UUID is 16 bytes
// per value but row 2 holds 10" — while the native columnar reader read it.
// One file, two paths, two answers, and the row path is the one compaction
// and ANALYZE run on.
//
// PostgreSQL decides what a bad literal means (ADR-0012) and there it is an
// error: `invalid input syntax for type uuid`. So a literal that parses
// converts, and anything else is an error naming the column, the row and the
// literal.
//
// The empty literal is the third outcome: it is an absence, and it is written
// as NULL. "" is the one input for which "a value" has no stable meaning here
// — stored as a value it is a zero-length entry in a fixed-width column,
// which the row reader called an error and the columnar reader called a
// value, and which answers false to IS NULL and equal to the empty string
// when what was meant was that there is no address. The readers hold the
// other end of this contract: a zero-length entry in an IPV6 or UUID column
// reads back as NULL on both paths (reader.go unpackAllPresent /
// unpackWithNulls, scan/columnar_native.go).
func convertNetworkLiteral(colType TypeID, s string) (any, error) {
	if s == "" {
		return nil, nil
	}
	switch colType {
	case TypeIPv4:
		if n, ok := ipv4StringToInt64(s); ok {
			return n, nil
		}
	case TypeMAC:
		if n, ok := macStringToInt64(s); ok {
			return n, nil
		}
	case TypeIPv6:
		if b := ipv6StringToBytes(s); b != nil {
			return b, nil
		}
	case TypeUUID:
		if b := parseUUIDForWrite(s); b != nil {
			return b, nil
		}
	}
	return nil, fmt.Errorf("%q is not a valid %s value", s, colType)
}

// decomposeArray handles ARRAY (LIST) type columns.
func (nw *NativeWriter) decomposeArray(col Column, val any, defLevel, repLevel, repDepth int32, leafIdx *int) {
	elemCol := Column{Name: "element", Type: TypeString}
	if col.ElementType != nil {
		elemCol = *col.ElementType
	}
	// A LIST's element is OPTIONAL by the schema, not by what the caller
	// declared — the same third-site rule decomposeMap spells out for a
	// map's key and value. flattenColumn and buildArraySchemaElements both
	// force it, so this must too: it makes no difference for a leaf element
	// (decomposeLeaf takes a present value's level from the leaf buffer,
	// which flattenColumn already fixed) and all the difference for a
	// container one, where decomposeRow/decomposeMap/decomposeArray derive
	// the inner definition level from Nullable and would stamp an absent
	// field one level below what the footer describes.
	elemCol.Nullable = true

	if val == nil {
		// Entire array is null. Emit null at current def level for all leaves.
		curDef := defLevel // outer group absent
		nw.emitNullForSubtree(elemCol, curDef, repLevel, leafIdx)
		return
	}

	arr, ok := val.([]any)
	if !ok {
		// Try to handle as a typed slice — shouldn't normally happen.
		nw.emitNullForSubtree(elemCol, defLevel, repLevel, leafIdx)
		return
	}

	// Advance defLevel past the outer optional group.
	innerDef := defLevel
	if col.Nullable {
		innerDef++ // outer group is present
	}

	if len(arr) == 0 {
		// Empty array — outer group present but list group has no entries.
		// def = innerDef (list is empty, so repeated group absent).
		nw.emitNullForSubtree(elemCol, innerDef, repLevel, leafIdx)
		return
	}

	// Non-empty array: each element gets def at least innerDef+1 (list group present).
	listDef := innerDef + 1
	// The repetition level a CONTINUING element carries is this list's own
	// depth in the schema, which is repDepth+1 — NOT repLevel+1. The two are
	// different numbers whenever this list is the first entry of an outer
	// repeated group: repLevel is then the OUTER level (or 0 at the top of
	// the row), because that is where the repetition last happened, while
	// the depth is unchanged. Deriving one from the other stamped the second
	// element of a list inside the FIRST entry of a map (or of another list)
	// one level too low, which reads back as a new entry of the OUTER
	// container — {"k": [1, 2]} came back as {"k": [1]}, and [[1, 2]] as
	// [[1], [2]]. flattenColumn already threads the depth correctly; this is
	// the same arithmetic on the value side.
	listRepDepth := repDepth + 1

	for i, elem := range arr {
		elemRep := listRepDepth
		if i == 0 {
			elemRep = repLevel // first element continues the enclosing repetition
		}
		saveLeafIdx := *leafIdx
		nw.decomposeValue(elemCol, elem, listDef, elemRep, listRepDepth, leafIdx)
		// Reset leafIdx for next element — all elements write to the same leaves.
		if i < len(arr)-1 {
			*leafIdx = saveLeafIdx
		}
	}
}

// decomposeMap handles MAP type columns.
func (nw *NativeWriter) decomposeMap(col Column, val any, defLevel, repLevel, repDepth int32, leafIdx *int) {
	if col.ElementType == nil || len(col.ElementType.Fields) != 2 {
		// Malformed MAP — skip leaves.
		return
	}

	// The key/value repetition types are fixed by the MAP schema, not by
	// what the caller declared. This is the THIRD site that has to say so —
	// flattenColumn (which fixes the leaf buffers' maxDefLevel) and
	// buildMapSchemaElements (which fixes the footer's levels) are the other
	// two, and all three must agree or the levels this function stamps land
	// somewhere the footer does not describe. It makes no difference for a
	// leaf value (decomposeLeaf takes its level from the leaf buffer, which
	// flattenColumn already fixed) and all the difference for a nested one:
	// decomposeRow/decomposeArray derive their inner def level from
	// Nullable, so a MAP<K, ROW<..>> whose value was declared non-nullable
	// wrote its fields one level below the footer's.
	keyCol := col.ElementType.Fields[0]
	keyCol.Nullable = false // keys are required
	valCol := col.ElementType.Fields[1]
	valCol.Nullable = true // values are optional

	if val == nil {
		if !col.Nullable {
			// A required MAP has no encoding for NULL: its key leaf's
			// definition levels only run 0..maxDef, and level 0 already
			// means "map present, no entries" — the EMPTY map. Writing a
			// NULL here produced a file whose own schema says the value
			// cannot be null, so the reader is right to read that entry
			// back as {} and the writer is what was wrong. Refuse it at
			// the source rather than emit a file that cannot say what it
			// was asked to store.
			nw.fail(fmt.Errorf("column %q: MAP is not nullable, cannot write a NULL map "+
				"(an empty map is map[string]any{}, which is a different value)", col.Name))
			val = map[string]any{} // keep the leaf buffers in step; the write has already failed
		} else {
			// Entire map is null.
			nw.emitNullForSubtree(keyCol, defLevel, repLevel, leafIdx)
			nw.emitNullForSubtree(valCol, defLevel, repLevel, leafIdx)
			return
		}
	}

	m, ok := val.(map[string]any)
	if !ok {
		// A MAP's STORAGE shape — the []any of {key,value} entry maps that
		// batch.Vector.GetValue (and therefore RecordBatch.RowAt/ToRows)
		// hands back for a MAP column — is not the native map[string]any
		// this function otherwise requires. Any row that passed through
		// RowAt/ToRows before being handed back to WriteRows (UPDATE's and
		// MERGE's re-ingest of a boxed row) carries its MAP columns in that
		// shape, and without this conversion they silently wrote as NULL:
		// the type assertion failed and fell straight to the empty-subtree
		// branch below, with no error to say a value went missing. Found
		// chasing #448/#449's regression tests once ReadFileColumnar could
		// finally reach this path for a table with a MAP column.
		if converted, ok2 := mapFromStorageShapeEntries(val, keyCol.Name, valCol.Name); ok2 {
			m, ok = converted, true
		}
	}
	if !ok {
		nw.emitNullForSubtree(keyCol, defLevel, repLevel, leafIdx)
		nw.emitNullForSubtree(valCol, defLevel, repLevel, leafIdx)
		return
	}

	innerDef := defLevel
	if col.Nullable {
		innerDef++ // outer MAP group is present
	}

	if len(m) == 0 {
		// Empty map.
		nw.emitNullForSubtree(keyCol, innerDef, repLevel, leafIdx)
		nw.emitNullForSubtree(valCol, innerDef, repLevel, leafIdx)
		return
	}

	// Non-empty map: iterate key-value pairs.
	kvDef := innerDef + 1 // key_value group is present
	kvRep := repDepth + 1 // this map's own depth — see decomposeArray

	first := true
	keyLeafIdx := *leafIdx
	// Count leaves for key subtree so we know where value leaves start.
	keyLeafCount := countLeaves(keyCol)
	valLeafIdx := keyLeafIdx + keyLeafCount

	for _, k := range sortedMapKeys(m) {
		v := m[k]
		elemRep := kvRep
		if first {
			elemRep = repLevel
			first = false
		}
		tmpKeyIdx := keyLeafIdx
		tmpValIdx := valLeafIdx
		nw.decomposeValue(keyCol, k, kvDef, elemRep, kvRep, &tmpKeyIdx)
		// kvDef, not kvDef+1: this level is only ever stamped on an ABSENT
		// value (a present one carries the leaf's own maxDefLevel), and
		// "key_value present, value null" IS kvDef. Claiming the value's
		// own level for a value that was never written told the reader to
		// consume one it did not have — the level and value streams then
		// slid apart and the decoder ran off the end of the page.
		nw.decomposeValue(valCol, v, kvDef, elemRep, kvRep, &tmpValIdx)
	}

	// Advance leafIdx past all map leaves.
	*leafIdx = valLeafIdx + countLeaves(valCol)
}

// decomposeRow handles ROW/STRUCT type columns.
func (nw *NativeWriter) decomposeRow(col Column, val any, defLevel, repLevel, repDepth int32, leafIdx *int) {
	innerDef := defLevel
	if col.Nullable {
		innerDef++ // group presence
	}

	if val == nil {
		// Entire struct is null — emit null for each field.
		for _, field := range col.Fields {
			nw.emitNullForSubtree(field, defLevel, repLevel, leafIdx)
		}
		return
	}

	m, ok := val.(map[string]any)
	if !ok {
		for _, field := range col.Fields {
			nw.emitNullForSubtree(field, defLevel, repLevel, leafIdx)
		}
		return
	}

	// Struct present — decompose each field. A struct adds no repetition, so
	// both the level to stamp and the depth pass through unchanged.
	for _, field := range col.Fields {
		fieldVal := m[field.Name]
		nw.decomposeValue(field, fieldVal, innerDef, repLevel, repDepth, leafIdx)
	}
}

// emitNullForSubtree emits a null entry (with the given def/rep) for every
// leaf under the given column subtree.
func (nw *NativeWriter) emitNullForSubtree(col Column, defLevel, repLevel int32, leafIdx *int) {
	switch col.Type {
	case TypeArray:
		elemCol := Column{Name: "element", Type: TypeString}
		if col.ElementType != nil {
			elemCol = *col.ElementType
		}
		nw.emitNullForSubtree(elemCol, defLevel, repLevel, leafIdx)
	case TypeMap:
		if col.ElementType != nil && len(col.ElementType.Fields) == 2 {
			nw.emitNullForSubtree(col.ElementType.Fields[0], defLevel, repLevel, leafIdx)
			nw.emitNullForSubtree(col.ElementType.Fields[1], defLevel, repLevel, leafIdx)
		}
	case TypeRow:
		for _, field := range col.Fields {
			nw.emitNullForSubtree(field, defLevel, repLevel, leafIdx)
		}
	default:
		lb := &nw.leafBufs[*leafIdx]
		lb.appendEntry(defLevel, repLevel)
		*leafIdx++
	}
}

// sortedMapKeys returns m's keys in byte order.
//
// Go map iteration is randomized, so ranging over the map wrote the same
// MAP value's entries in a different order on every call, and the file was
// therefore not a function of its input: two writes of identical rows
// produced different bytes. Nothing that compares files can work against
// that — no golden file, no content hash, no byte-for-byte check that a
// rewrite changed nothing. (Row-group min/max survive it, being
// commutative; the bytes and the entry order do not.)
//
// The order is also observable downstream: it is the order the entries are
// laid out in, and the vector side turns exactly that into the order
// GetValue hands back. batch.mapEntryRows sorts on the same rule, because
// this writer and that vector are the two ways the same map reaches disk
// and they have to agree.
// mapFromStorageShapeEntries converts a MAP's storage-shape value — []any of
// {keyName: k, valName: v} entry maps, the shape batch.Vector.GetValue's
// TypeMap arm produces (and batch.mapEntryRows builds from a native map on
// the way in) — back into the native map[string]any this writer expects.
// Returns ok=false for anything else, so the caller's existing
// malformed-input handling is unchanged.
//
// MAP keys are always Go strings at this boundary (mapKeyValue's own
// comment: "Row-level keys are always strings"), so a non-string key entry
// is exactly as malformed as any other shape val could have been.
func mapFromStorageShapeEntries(val any, keyName, valName string) (map[string]any, bool) {
	entries, ok := val.([]any)
	if !ok {
		return nil, false
	}
	m := make(map[string]any, len(entries))
	for _, e := range entries {
		entry, ok := e.(map[string]any)
		if !ok {
			return nil, false
		}
		k, ok := entry[keyName].(string)
		if !ok {
			return nil, false
		}
		m[k] = entry[valName]
	}
	return m, true
}

func sortedMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// countLeaves returns the number of leaf columns in a column subtree.
func countLeaves(col Column) int {
	switch col.Type {
	case TypeArray:
		if col.ElementType != nil {
			return countLeaves(*col.ElementType)
		}
		return 1
	case TypeMap:
		if col.ElementType != nil && len(col.ElementType.Fields) == 2 {
			return countLeaves(col.ElementType.Fields[0]) + countLeaves(col.ElementType.Fields[1])
		}
		return 0
	case TypeRow:
		n := 0
		for _, f := range col.Fields {
			n += countLeaves(f)
		}
		return n
	default:
		return 1
	}
}

// Close flushes any remaining rows and writes the file footer.
func (nw *NativeWriter) Close() error {
	if nw.err != nil {
		return nw.err
	}
	// Write magic header if this is the first write.
	if nw.written == 0 {
		if err := nw.writeBytes([]byte("PAR1")); err != nil {
			return err
		}
	}

	// Flush remaining rows.
	if nw.numRows > 0 {
		if err := nw.flushRowGroup(); err != nil {
			return err
		}
	}

	// Build and write footer.
	return nw.writeFooter()
}

func (nw *NativeWriter) writeBytes(b []byte) error {
	n, err := nw.w.Write(b)
	nw.written += int64(n)
	return err
}

func (nw *NativeWriter) flushRowGroup() error {
	// Write magic header on first flush.
	if nw.written == 0 {
		if err := nw.writeBytes([]byte("PAR1")); err != nil {
			return err
		}
	}

	rgOffset := nw.written
	numRows := int64(nw.numRows)
	var totalSize, totalCompressed int64

	columns := make([]ColumnChunk, len(nw.leafBufs))
	for i := range nw.leafBufs {
		lb := &nw.leafBufs[i]
		chunkOffset := nw.written

		uncompressed, compressed, err := nw.writeColumnChunk(lb)
		if err != nil {
			return fmt.Errorf("writing column %s: %w", lb.path, err)
		}
		totalSize += uncompressed
		totalCompressed += compressed

		columns[i] = ColumnChunk{
			FileOffset: chunkOffset,
			MetaData: &ColumnMetaData{
				Type:                  lb.physical,
				Encodings:             []Encoding{EncodingPlain, EncodingRLE},
				PathInSchema:          lb.path,
				Codec:                 nw.codec,
				NumValues:             int64(lb.count),
				TotalUncompressedSize: uncompressed,
				TotalCompressedSize:   compressed,
				DataPageOffset:        chunkOffset,
				Statistics:            lb.buildStats(),
			},
		}

		// Reset leaf buffer for next row group.
		lb.reset()
	}

	nw.rowGroups = append(nw.rowGroups, RowGroup{
		Columns:             columns,
		TotalByteSize:       totalSize,
		NumRows:             numRows,
		FileOffset:          rgOffset,
		TotalCompressedSize: totalCompressed,
	})

	nw.numRows = 0
	return nil
}

// pageRange is one data page's slice of a leaf buffer: entry (row-level)
// range [rowStart,rowEnd) and the matching non-null value range
// [valStart,valEnd).
type pageRange struct {
	rowStart, rowEnd, valStart, valEnd int
}

// pageRowRanges splits a leaf buffer into page-sized entry ranges
// targeting WriterConfig.PageBufferSize of PLAIN-encoded value bytes per
// page (#300 — one page per chunk defeated every page-granular reader
// optimization: sel-decode page skipping, future page-stat pruning, and
// bounded decompression buffers). Splitting is restricted to flat leaves
// of byte-sliceable physical types; everything else — nested (page cuts
// must fall on record boundaries), BOOLEAN (bit-packed values can't
// split on an unaligned value index), INT96 / FIXED_LEN (rare, low
// value) — keeps the single-page layout, byte-identical to the previous
// writer.
func (lb *leafBuffer) pageRowRanges(target int) []pageRange {
	numVals := lb.count - int(lb.numNulls)
	single := []pageRange{{0, lb.count, 0, numVals}}
	if target <= 0 || lb.maxRepLevel > 0 || lb.count == 0 {
		return single
	}
	var width int
	switch lb.physical {
	case PhysicalInt32, PhysicalFloat:
		width = 4
	case PhysicalInt64, PhysicalDouble:
		width = 8
	case PhysicalByteArray:
		width = 0 // sized from offsets below
	default:
		// FIXED_LEN_BYTE_ARRAY falls here too — a wide DECIMAL(p>18) column
		// (decimalFLBABytes, 16 bytes/value) stays single-page per row group,
		// same as BOOLEAN and INT96, until splitting FLBA is worth adding.
		return single
	}
	var ranges []pageRange
	rowStart, valStart, val, sz := 0, 0, 0, 0
	for i := 0; i < lb.count; i++ {
		isVal := lb.maxDefLevel == 0 || lb.defLevels[i] == lb.maxDefLevel
		if isVal {
			if lb.physical == PhysicalByteArray {
				end := uint32(len(lb.packed))
				if val+1 < len(lb.offsets) {
					end = lb.offsets[val+1]
				}
				sz += 4 + int(end-lb.offsets[val])
			} else {
				sz += width
			}
			val++
		}
		sz++ // per-entry definition-level estimate
		if sz >= target {
			ranges = append(ranges, pageRange{rowStart, i + 1, valStart, val})
			rowStart, valStart, sz = i+1, val, 0
		}
	}
	if rowStart < lb.count || len(ranges) == 0 {
		ranges = append(ranges, pageRange{rowStart, lb.count, valStart, val})
	}
	return ranges
}

// pagePlainData returns the PLAIN-encoded value bytes for one page range.
func (lb *leafBuffer) pagePlainData(pr pageRange, full bool) []byte {
	switch lb.physical {
	case PhysicalBoolean:
		return lb.boolBuf // never split (pageRowRanges)
	case PhysicalByteArray:
		// PLAIN byte array: each value prefixed with 4-byte LE length.
		var buf bytes.Buffer
		for i := pr.valStart; i < pr.valEnd; i++ {
			start := lb.offsets[i]
			end := uint32(len(lb.packed))
			if i+1 < len(lb.offsets) {
				end = lb.offsets[i+1]
			}
			val := lb.packed[start:end]
			var lenBuf [4]byte
			binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(val)))
			buf.Write(lenBuf[:])
			buf.Write(val)
		}
		return buf.Bytes()
	default:
		if full {
			return lb.data
		}
		numVals := lb.count - int(lb.numNulls)
		if numVals <= 0 {
			return nil
		}
		width := len(lb.data) / numVals
		return lb.data[pr.valStart*width : pr.valEnd*width]
	}
}

func (nw *NativeWriter) writeColumnChunk(lb *leafBuffer) (uncompressed, compressed int64, err error) {
	ranges := lb.pageRowRanges(nw.config.PageBufferSize)
	single := len(ranges) == 1
	var totU, totC int64
	for _, pr := range ranges {
		u, c, err := nw.writeDataPage(lb, pr, single)
		if err != nil {
			return 0, 0, err
		}
		totU += u
		totC += c
	}
	return totU, totC, nil
}

// writeDataPage emits one PLAIN data page covering pr. A single-page
// chunk is byte-identical to the pre-split writer (chunk-level stats in
// the page header included); multi-page chunks omit per-page stats — the
// chunk ColumnMetaData carries the authoritative statistics either way,
// and per-page stats would otherwise repeat the chunk bounds.
func (nw *NativeWriter) writeDataPage(lb *leafBuffer, pr pageRange, single bool) (uncompressed, compressed int64, err error) {
	// The reader refuses a page header claiming more than MaxPageValues, so
	// the writer must not produce one. pageRowRanges splits by BYTES and
	// declines to split at all for BOOLEAN, INT96, FIXED_LEN and nested
	// leaves, so a large enough RowGroupSize (or a wide enough array) can
	// reach the ceiling on those. Failing here names the knob; writing the
	// file would produce one this package cannot read back.
	if n := pr.rowEnd - pr.rowStart; n > MaxPageValues {
		return 0, 0, fmt.Errorf("column %q: a page would declare %d values, past the %d a page may hold "+
			"— lower WriterConfig.RowGroupSize", lb.col.Name, n, MaxPageValues)
	}
	var pageBuf bytes.Buffer

	// Write repetition levels (RLE encoded with 4-byte LE length prefix).
	if lb.maxRepLevel > 0 {
		bitWidth := bitsRequiredForMax(lb.maxRepLevel)
		repData := encodeLevelsRLE(lb.repLevels[pr.rowStart:pr.rowEnd], pr.rowEnd-pr.rowStart, bitWidth)
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(repData)))
		pageBuf.Write(lenBuf[:])
		pageBuf.Write(repData)
	}

	// Write definition levels (RLE encoded with 4-byte LE length prefix).
	if lb.maxDefLevel > 0 {
		bitWidth := bitsRequiredForMax(lb.maxDefLevel)
		defData := encodeLevelsRLE(lb.defLevels[pr.rowStart:pr.rowEnd], pr.rowEnd-pr.rowStart, bitWidth)
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(defData)))
		pageBuf.Write(lenBuf[:])
		pageBuf.Write(defData)
	}

	// Write PLAIN-encoded values.
	pageBuf.Write(lb.pagePlainData(pr, single))

	uncompressedData := pageBuf.Bytes()
	uncompressedSize := int32(len(uncompressedData))

	// Compress.
	compressedData, err := compressPage(uncompressedData, nw.codec)
	if err != nil {
		return 0, 0, fmt.Errorf("compressing page: %w", err)
	}
	compressedSize := int32(len(compressedData))

	// Build page header.
	dph := &DataPageHeader{
		NumValues:               int32(pr.rowEnd - pr.rowStart),
		Encoding:                EncodingPlain,
		DefinitionLevelEncoding: EncodingRLE,
		RepetitionLevelEncoding: EncodingRLE,
	}
	if single {
		dph.Statistics = lb.buildStats()
	}
	ph := &PageHeader{
		Type:                 PageDataV1,
		UncompressedPageSize: uncompressedSize,
		CompressedPageSize:   compressedSize,
		DataPageHeader:       dph,
	}

	headerBytes := EncodePageHeader(ph)

	// Write header + compressed data.
	if err := nw.writeBytes(headerBytes); err != nil {
		return 0, 0, err
	}
	if err := nw.writeBytes(compressedData); err != nil {
		return 0, 0, err
	}

	totalUncompressed := int64(len(headerBytes)) + int64(uncompressedSize)
	totalCompressed := int64(len(headerBytes)) + int64(compressedSize)
	return totalUncompressed, totalCompressed, nil
}

func (nw *NativeWriter) writeFooter() error {
	totalRows := int64(0)
	for _, rg := range nw.rowGroups {
		totalRows += rg.NumRows
	}

	// Build schema elements (flattened schema tree).
	schemaElements := buildSchemaElements(nw.schema)

	// Stamp the DECLARED schema alongside the parquet one. Nine of the 22
	// types have no parquet annotation that can carry them —
	// buildLeafSchemaElement writes none for IPv4, IPv6, MAC, UUID, Bytes,
	// Port, Protocol or Duration — so TypeIDFromSchemaNode cannot recover
	// them on read and the file comes back as INT64/BYTE_ARRAY. Every
	// consumer that reads its types from the catalog instead was fine; the
	// ones that read them from the file (the DAG worker's scan, the
	// coordinator's scalar extraction, any tool) answered 167772165 for
	// 10.0.0.5 (#396). This side channel makes the file self-describing for
	// all 22 without touching the physical layout: a foreign reader ignores
	// an unknown key and still sees a valid INT64/BYTE_ARRAY column, the
	// way Spark and Iceberg carry their own schemas.
	declared, err := json.Marshal(nw.schema)
	if err != nil {
		return fmt.Errorf("encoding declared schema for the footer: %w", err)
	}

	kv := []KeyValue{
		{Key: "wadjet.version", Value: "0.1.0"},
		{Key: DeclaredSchemaKey, Value: string(declared)},
	}
	// CidrStatsOrderKey is a promise about EVERY CIDR value in the file, not
	// just the current row group's, so it is decided once here from every
	// leaf's cidrKeyFailed rather than per row group. Its absence — an old
	// file, or a value somewhere that did not parse as an address — is what
	// tells a reader the min/max it is holding for a CIDR column, if any,
	// are the column's raw TEXT byte-order extremes and must be withheld
	// rather than trusted for pruning (#523). Written only when the schema
	// actually has a CIDR column, so a file with none is byte-for-byte
	// unchanged.
	hasCidr, cidrStatsInet := false, true
	for i := range nw.leafBufs {
		if nw.leafBufs[i].col.Type == TypeCIDR {
			hasCidr = true
			if nw.leafBufs[i].cidrKeyFailed {
				cidrStatsInet = false
				break
			}
		}
	}
	if hasCidr && cidrStatsInet {
		kv = append(kv, KeyValue{Key: CidrStatsOrderKey, Value: CidrStatsOrderInet})
	}

	md := &FileMetaData{
		Version:          1,
		Schema:           schemaElements,
		NumRows:          totalRows,
		RowGroups:        nw.rowGroups,
		CreatedBy:        "wadjet (native writer)",
		KeyValueMetadata: kv,
	}

	footerBytes := EncodeFileMetaData(md)

	if err := nw.writeBytes(footerBytes); err != nil {
		return err
	}

	// Write footer length (4 bytes LE).
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(footerBytes)))
	if err := nw.writeBytes(lenBuf[:]); err != nil {
		return err
	}

	// Write magic trailer.
	return nw.writeBytes([]byte("PAR1"))
}

// buildSchemaElements creates the flattened schema tree for the footer.
// Handles flat columns and nested types (ARRAY/LIST, MAP, ROW/STRUCT).
func buildSchemaElements(schema Schema) []SchemaElement {
	// Root element (message).
	root := SchemaElement{
		Name:        "wadjet_schema",
		NumChildren: int32(len(schema.Columns)),
	}
	elements := []SchemaElement{root}

	for _, col := range schema.Columns {
		buildColumnSchemaElements(col, &elements)
	}
	return elements
}

// buildColumnSchemaElements recursively emits SchemaElements for a single column.
func buildColumnSchemaElements(col Column, elements *[]SchemaElement) {
	switch col.Type {
	case TypeArray:
		buildArraySchemaElements(col, elements)
	case TypeMap:
		buildMapSchemaElements(col, elements)
	case TypeRow:
		buildRowSchemaElements(col, elements)
	default:
		buildLeafSchemaElement(col, elements)
	}
}

// buildArraySchemaElements emits the standard Parquet LIST schema:
//
//	optional group <name> (LIST) {
//	  repeated group list {
//	    optional <element_type> element
//	  }
//	}
func buildArraySchemaElements(col Column, elements *[]SchemaElement) {
	rep := FieldOptional
	if !col.Nullable {
		rep = FieldRequired
	}
	ct := ConvertedList
	// Outer group with ConvertedType=LIST.
	outer := SchemaElement{
		Name:           col.Name,
		NumChildren:    1,
		RepetitionType: rep,
		ConvertedType:  &ct,
		LogicalType:    &LogicalType{Type: LogicalList},
	}
	*elements = append(*elements, outer)

	// Repeated "list" group.
	listGroup := SchemaElement{
		Name:           "list",
		NumChildren:    1,
		RepetitionType: FieldRepeated,
	}
	*elements = append(*elements, listGroup)

	// Element column.
	elemCol := Column{Name: "element", Type: TypeString, Nullable: true}
	if col.ElementType != nil {
		elemCol = *col.ElementType
		elemCol.Nullable = true // elements are always optional in LIST
	}
	buildColumnSchemaElements(elemCol, elements)
}

// buildMapSchemaElements emits the standard Parquet MAP schema:
//
//	optional group <name> (MAP) {
//	  repeated group key_value (MAP_KEY_VALUE) {
//	    required <key_type> key
//	    optional <value_type> value
//	  }
//	}
func buildMapSchemaElements(col Column, elements *[]SchemaElement) {
	rep := FieldOptional
	if !col.Nullable {
		rep = FieldRequired
	}
	ct := ConvertedMap
	outer := SchemaElement{
		Name:           col.Name,
		NumChildren:    1,
		RepetitionType: rep,
		ConvertedType:  &ct,
		LogicalType:    &LogicalType{Type: LogicalMap},
	}
	*elements = append(*elements, outer)

	kvCt := ConvertedMapKeyValue
	kvGroup := SchemaElement{
		Name:           "key_value",
		NumChildren:    2,
		RepetitionType: FieldRepeated,
		ConvertedType:  &kvCt,
	}
	*elements = append(*elements, kvGroup)

	if col.ElementType != nil && col.ElementType.Type == TypeRow && len(col.ElementType.Fields) == 2 {
		keyCol := col.ElementType.Fields[0]
		keyCol.Nullable = false // keys are required
		buildColumnSchemaElements(keyCol, elements)

		valCol := col.ElementType.Fields[1]
		valCol.Nullable = true // values are optional
		buildColumnSchemaElements(valCol, elements)
	}
}

// buildRowSchemaElements emits the Parquet STRUCT schema:
//
//	optional group <name> {
//	  optional <type> field1
//	  optional <type> field2
//	  ...
//	}
func buildRowSchemaElements(col Column, elements *[]SchemaElement) {
	rep := FieldOptional
	if !col.Nullable {
		rep = FieldRequired
	}
	group := SchemaElement{
		Name:           col.Name,
		NumChildren:    int32(len(col.Fields)),
		RepetitionType: rep,
	}
	*elements = append(*elements, group)

	for _, field := range col.Fields {
		buildColumnSchemaElements(field, elements)
	}
}

// buildLeafSchemaElement emits a single leaf SchemaElement.
func buildLeafSchemaElement(col Column, elements *[]SchemaElement) {
	se := SchemaElement{
		Name: col.Name,
	}
	pt := columnPhysical(col)
	se.Type = &pt

	if col.Nullable {
		se.RepetitionType = FieldOptional
	} else {
		se.RepetitionType = FieldRequired
	}

	// Set converted type and logical type for annotations.
	switch col.Type {
	case TypeString, TypeCIDR:
		ct := ConvertedUTF8
		se.ConvertedType = &ct
		se.LogicalType = &LogicalType{Type: LogicalString}
	case TypeTimestamp:
		ct := ConvertedTimestampMillis
		se.ConvertedType = &ct
		se.LogicalType = &LogicalType{
			Type:            LogicalTimestampMillis,
			IsAdjustedToUTC: true,
		}
	case TypeDate:
		ct := ConvertedDate
		se.ConvertedType = &ct
		se.LogicalType = &LogicalType{Type: LogicalDate}
	case TypeInt32:
		ct := ConvertedInt32
		se.ConvertedType = &ct
		se.LogicalType = &LogicalType{Type: LogicalInteger, BitWidth: 32, IsSigned: true}
	case TypeInt64:
		ct := ConvertedInt64
		se.ConvertedType = &ct
		se.LogicalType = &LogicalType{Type: LogicalInteger, BitWidth: 64, IsSigned: true}
	case TypeDecimal:
		ct := ConvertedDecimal
		se.ConvertedType = &ct
		prec := decimalEffectivePrecision(col.Precision)
		se.Precision = int32(prec)
		se.Scale = int32(col.Scale)
		if pt == PhysicalFixedLenByteArray {
			se.TypeLength = decimalFLBAWidth
		}
		se.LogicalType = &LogicalType{
			Type:      LogicalDecimal,
			Precision: prec,
			Scale:     col.Scale,
		}
	case TypeVector:
		se.TypeLength = int32(col.Dimension * 4) // dim × sizeof(float32)
		se.LogicalType = &LogicalType{
			Type:      LogicalVector,
			Dimension: col.Dimension,
		}
	}

	*elements = append(*elements, se)
}

// --- Leaf buffer methods ---

// appendEntry appends a null/absent entry with the given def/rep levels.
func (lb *leafBuffer) appendEntry(defLevel, repLevel int32) {
	lb.defLevels = append(lb.defLevels, defLevel)
	lb.repLevels = append(lb.repLevels, repLevel)
	lb.numNulls++
	lb.count++
}

// appendEntryWithValue appends a value entry with the given def/rep levels.
func (lb *leafBuffer) appendEntryWithValue(defLevel, repLevel int32, val any) {
	lb.defLevels = append(lb.defLevels, defLevel)
	lb.repLevels = append(lb.repLevels, repLevel)
	lb.count++

	switch lb.physical {
	case PhysicalBoolean:
		b := toBool(val)
		lb.appendBool(b)
	case PhysicalInt32:
		v := toInt32(val, lb.col.Type)
		lb.appendInt32(v)
	case PhysicalInt64:
		lb.appendInt64(toInt64(val, lb.col.Type))
	case PhysicalFloat:
		v := toFloat32(val)
		lb.appendFloat32(v)
	case PhysicalDouble:
		v := toFloat64(val)
		lb.appendFloat64(v)
	case PhysicalByteArray:
		b := toBytes(val, lb.col.Type)
		lb.appendByteArray(b)
	case PhysicalFixedLenByteArray:
		b := toBytes(val, lb.col.Type)
		lb.data = append(lb.data, b...)
	}
}

// appendDecimalEntry appends a DECIMAL value entry.
//
// It takes a Decimal128 rather than an `any`, which is the point: every other
// converter in this file answers a box it does not understand with a zero, and
// for a DECIMAL that zero is a stored number nobody wrote (#647). The box is
// resolved once, in decomposeLeaf, where a failure can name the column and the
// row; by the time a value reaches here it has a value.
func (lb *leafBuffer) appendDecimalEntry(defLevel, repLevel int32, d Decimal128) {
	lb.defLevels = append(lb.defLevels, defLevel)
	lb.repLevels = append(lb.repLevels, repLevel)
	lb.count++

	if lb.physical == PhysicalFixedLenByteArray {
		lb.data = append(lb.data, decimalFLBABytes(d)...)
		return
	}
	// decomposeLeaf refused the unscaled values an INT64 leaf cannot hold, so
	// the narrowing here is exact.
	v, _ := d.Int64()
	lb.appendInt64(v)
}

func (lb *leafBuffer) appendBool(v bool) {
	if lb.boolPos%8 == 0 {
		lb.boolBuf = append(lb.boolBuf, 0)
	}
	if v {
		lb.boolBuf[len(lb.boolBuf)-1] |= 1 << (lb.boolPos % 8)
	}
	lb.boolPos++
}

func (lb *leafBuffer) appendInt32(v int32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(v))
	lb.data = append(lb.data, buf[:]...)
	lb.updateStatsI32(v)
}

func (lb *leafBuffer) appendInt64(v int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	lb.data = append(lb.data, buf[:]...)
	lb.updateStatsI64(v)
}

func (lb *leafBuffer) appendFloat32(v float32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], math.Float32bits(v))
	lb.data = append(lb.data, buf[:]...)
	lb.updateStatsF32(v)
}

func (lb *leafBuffer) appendFloat64(v float64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
	lb.data = append(lb.data, buf[:]...)
	lb.updateStatsF64(v)
}

func (lb *leafBuffer) appendByteArray(b []byte) {
	lb.offsets = append(lb.offsets, uint32(len(lb.packed)))
	lb.packed = append(lb.packed, b...)
	lb.updateStatsBytes(b)
}

func (lb *leafBuffer) reset() {
	lb.data = lb.data[:0]
	lb.offsets = lb.offsets[:0]
	lb.packed = lb.packed[:0]
	lb.boolBuf = lb.boolBuf[:0]
	lb.boolPos = 0
	lb.defLevels = lb.defLevels[:0]
	lb.repLevels = lb.repLevels[:0]
	lb.numNulls = 0
	lb.count = 0
	lb.hasStats = false
	lb.minBytes = nil
	lb.maxBytes = nil
	lb.minCidrKey = ""
	lb.maxCidrKey = ""
	// cidrKeyFailed is deliberately NOT reset here — see its doc.
}

func (lb *leafBuffer) updateStatsI32(v int32) {
	if !lb.hasStats {
		lb.minI32, lb.maxI32 = v, v
		lb.hasStats = true
	} else {
		if v < lb.minI32 {
			lb.minI32 = v
		}
		if v > lb.maxI32 {
			lb.maxI32 = v
		}
	}
}

func (lb *leafBuffer) updateStatsI64(v int64) {
	if !lb.hasStats {
		lb.minI64, lb.maxI64 = v, v
		lb.hasStats = true
	} else {
		if v < lb.minI64 {
			lb.minI64 = v
		}
		if v > lb.maxI64 {
			lb.maxI64 = v
		}
	}
}

func (lb *leafBuffer) updateStatsF32(v float32) {
	if !lb.hasStats || v < lb.minF32 {
		lb.minF32 = v
	}
	if !lb.hasStats || v > lb.maxF32 {
		lb.maxF32 = v
	}
	lb.hasStats = true
}

func (lb *leafBuffer) updateStatsF64(v float64) {
	if !lb.hasStats || v < lb.minF64 {
		lb.minF64 = v
	}
	if !lb.hasStats || v > lb.maxF64 {
		lb.maxF64 = v
	}
	lb.hasStats = true
}

func (lb *leafBuffer) updateStatsBytes(b []byte) {
	if lb.col.Type == TypeCIDR {
		lb.updateStatsCIDR(b)
		return
	}
	if !lb.hasStats {
		lb.minBytes = append([]byte(nil), b...)
		lb.maxBytes = append([]byte(nil), b...)
		lb.hasStats = true
	} else {
		if bytes.Compare(b, lb.minBytes) < 0 {
			lb.minBytes = append(lb.minBytes[:0], b...)
		}
		if bytes.Compare(b, lb.maxBytes) > 0 {
			lb.maxBytes = append(lb.maxBytes[:0], b...)
		}
	}
}

// updateStatsCIDR tracks a CIDR leaf's row-group min/max by PostgreSQL's
// inet order (family, common bits under the smaller mask, mask length, full
// address — kernel.CidrSortKey's ordering) rather than the text's own byte
// order (#523, ADR-0018 §6: the footer previously held the text-order
// extremes, which a reader compares in the WRONG order against the engine's
// inet-ordered literal, and #492 withheld CIDR from pruning entirely rather
// than answer wrong). The stored minBytes/maxBytes stay the WINNING ROWS'
// TEXT — CIDR's physical storage is unchanged — only the comparison used to
// pick them changes; the reader re-derives the sort key from that text when
// writeFooter's CidrStatsOrderKey flag says every value in the file parsed.
//
// A value that fails to parse as an address latches cidrKeyFailed (which
// suppresses that flag for the whole file, see its doc) and falls back to
// the raw byte-order comparison above so the leaf still produces SOME bound
// rather than none — a bound the reader will withhold using anyway, once it
// sees the missing flag, so its exact order does not matter.
func (lb *leafBuffer) updateStatsCIDR(b []byte) {
	key, ok := CidrStatsSortKey(string(b))
	if !ok {
		lb.cidrKeyFailed = true
		if !lb.hasStats {
			lb.minBytes = append([]byte(nil), b...)
			lb.maxBytes = append([]byte(nil), b...)
			lb.hasStats = true
		} else {
			if bytes.Compare(b, lb.minBytes) < 0 {
				lb.minBytes = append(lb.minBytes[:0], b...)
			}
			if bytes.Compare(b, lb.maxBytes) > 0 {
				lb.maxBytes = append(lb.maxBytes[:0], b...)
			}
		}
		return
	}
	if !lb.hasStats {
		lb.minBytes = append([]byte(nil), b...)
		lb.maxBytes = append([]byte(nil), b...)
		lb.minCidrKey = key
		lb.maxCidrKey = key
		lb.hasStats = true
		return
	}
	if key < lb.minCidrKey {
		lb.minBytes = append(lb.minBytes[:0], b...)
		lb.minCidrKey = key
	}
	if key > lb.maxCidrKey {
		lb.maxBytes = append(lb.maxBytes[:0], b...)
		lb.maxCidrKey = key
	}
}

// CidrStatsSortKey is kernel.CidrSortKey, duplicated here rather than
// imported: internal/storage/parquet sits BELOW internal/engine/exec/kernel
// in the import graph (kernel imports internal/engine/batch, which imports
// this package, and since #523 imports this package directly as well), so
// this package cannot import kernel without a cycle. Any change to
// CidrSortKey's encoding must be mirrored here.
//
// Exported ONLY so the duplication can be pinned from the side of the graph
// that can see both: kernel.TestCidrStatsSortKeyMatchesKernel runs
// PostgreSQL's own inet-order fixture (pgInetOrder, derived from a live
// postgres:17-alpine) through both functions and requires byte-identical
// keys. Nothing outside this package should call it to make a decision —
// RowGroupStats' CidrInetBound is the supported way to get a comparable
// CIDR bound, because it also carries the per-file confirmation this
// function cannot make.
func CidrStatsSortKey(s string) (string, bool) {
	t := s
	if !strings.ContainsRune(t, '/') {
		if strings.ContainsRune(t, ':') {
			t += "/128"
		} else {
			t += "/32"
		}
	}
	ip, ipnet, err := net.ParseCIDR(t)
	if err != nil || ipnet == nil {
		return "", false
	}
	ones, bits := ipnet.Mask.Size()
	var full, masked net.IP
	var family byte
	if bits == net.IPv4len*8 {
		family, full, masked = 0x04, ip.To4(), ipnet.IP.To4()
	} else {
		family, full, masked = 0x06, ip.To16(), ipnet.IP.To16()
	}
	if full == nil || masked == nil {
		return "", false
	}
	buf := make([]byte, 0, 2+2*len(full))
	buf = append(buf, family)
	buf = append(buf, masked...)
	buf = append(buf, byte(ones))
	buf = append(buf, full...)
	return string(buf), true
}

func (lb *leafBuffer) buildStats() *Statistics {
	s := &Statistics{NullCount: lb.numNulls}
	if !lb.hasStats {
		return s
	}
	switch lb.physical {
	case PhysicalInt32:
		s.MinValue = make([]byte, 4)
		s.MaxValue = make([]byte, 4)
		binary.LittleEndian.PutUint32(s.MinValue, uint32(lb.minI32))
		binary.LittleEndian.PutUint32(s.MaxValue, uint32(lb.maxI32))
	case PhysicalInt64:
		s.MinValue = make([]byte, 8)
		s.MaxValue = make([]byte, 8)
		binary.LittleEndian.PutUint64(s.MinValue, uint64(lb.minI64))
		binary.LittleEndian.PutUint64(s.MaxValue, uint64(lb.maxI64))
	case PhysicalFloat:
		s.MinValue = make([]byte, 4)
		s.MaxValue = make([]byte, 4)
		binary.LittleEndian.PutUint32(s.MinValue, math.Float32bits(lb.minF32))
		binary.LittleEndian.PutUint32(s.MaxValue, math.Float32bits(lb.maxF32))
	case PhysicalDouble:
		s.MinValue = make([]byte, 8)
		s.MaxValue = make([]byte, 8)
		binary.LittleEndian.PutUint64(s.MinValue, math.Float64bits(lb.minF64))
		binary.LittleEndian.PutUint64(s.MaxValue, math.Float64bits(lb.maxF64))
	case PhysicalByteArray:
		s.MinValue = append([]byte(nil), lb.minBytes...)
		s.MaxValue = append([]byte(nil), lb.maxBytes...)
	}
	return s
}

// --- Type conversion helpers ---

func toBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	default:
		return false
	}
}

func toInt32(v any, colType TypeID) int32 {
	switch t := v.(type) {
	case int:
		return int32(t)
	case int32:
		return t
	case int64:
		return int32(t)
	case float64:
		return int32(t)
	case string:
		if colType == TypeDate {
			return parseDateForWrite(t)
		}
		return 0
	default:
		return 0
	}
}

// decimalFLBABytes renders an unscaled DECIMAL value as the sixteen-byte
// big-endian two's-complement integer a FIXED_LEN_BYTE_ARRAY DECIMAL leaf
// holds — the same layout decimalFromBytesRaw reads back and the one pyarrow
// writes.
//
// The box → unscaled step is DecimalValueFromBox, in decomposeLeaf. It used to
// live here, sharing decimalUnscaledInt64 with the INT64 leaf: an INTEGER box
// was written verbatim (ADR-0018 §4's contract, correct) but a REAL or STRING
// box went through strconv.ParseFloat and int64(math.Round(t*pow)), so a value
// wider than the column wrapped the int64, garbage and NaN stored 0, and every
// value past ~16 significant digits lost its exactness (#647).
func decimalFLBABytes(d Decimal128) []byte {
	// Every byte gets written below regardless of sign — decimalFLBAWidth is
	// exactly 8 bytes of hi plus 8 bytes of lo — so there is no sign-extended
	// prefill to seed first.
	b := make([]byte, decimalFLBAWidth)
	for i := 0; i < 8; i++ {
		b[decimalFLBAWidth-1-i] = byte(d.Lo >> (8 * i))
		b[decimalFLBAWidth-9-i] = byte(uint64(d.Hi) >> (8 * i))
	}
	return b
}

func toInt64(v any, colType TypeID) int64 {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case int64:
		return t
	case float64:
		return int64(t)
	case string:
		return convertStringToInt64(t, colType)
	case time.Time:
		// TypeTimestamp / TypeDuration land here when ingest hands a time.Time
		// directly. The parquet schema declares TypeTimestamp as
		// TimestampMillis (file_writer.go:813), so encode in milliseconds —
		// otherwise the row group stores 0 from the default branch and every
		// query against the column reads zeros (TestTimestampStringComparison
		// surfaced this: `event_time >= '<literal>'` returned 0 rows because
		// every column value was 0). TypeDate has its own pre-converter in
		// writer.go::prepareRows; it never reaches this fall-through.
		return t.UnixMilli()
	default:
		return 0
	}
}

func toFloat32(v any) float32 {
	switch t := v.(type) {
	case float32:
		return t
	case float64:
		return float32(t)
	case int:
		return float32(t)
	default:
		return 0
	}
}

func toFloat64(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	default:
		return 0
	}
}

func toBytes(v any, colType TypeID) []byte {
	switch t := v.(type) {
	case []byte:
		return t
	case string:
		return convertStringToBytes(t, colType)
	case []float32:
		// VECTOR type: encode as little-endian float32 bytes
		buf := make([]byte, len(t)*4)
		for i, f := range t {
			binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
		}
		return buf
	default:
		return nil
	}
}

// convertStringToInt64 handles network type string-to-int64 conversion.
// decomposeLeaf converts and validates these literals before the value ever
// reaches a leaf buffer, so an unparseable one cannot arrive here.
func convertStringToInt64(s string, colType TypeID) int64 {
	switch colType {
	case TypeIPv4:
		n, _ := ipv4StringToInt64(s)
		return n
	case TypeMAC:
		n, _ := macStringToInt64(s)
		return n
	default:
		return 0
	}
}

// convertStringToBytes handles network type string-to-bytes conversion.
func convertStringToBytes(s string, colType TypeID) []byte {
	switch colType {
	case TypeIPv6:
		return ipv6StringToBytes(s)
	case TypeUUID:
		// A literal that does not parse never reaches here: decomposeLeaf
		// converts and refuses first. Returning the RAW STRING BYTES is what
		// it used to do, and that is how a ten-byte value got into a
		// sixteen-byte column.
		return parseUUIDForWrite(s)
	default:
		return []byte(s)
	}
}

// --- RLE encoding for definition and repetition levels ---

// encodeDefLevelsRLE encodes definition levels using RLE/bit-packing hybrid.
// For nullable columns, bitWidth=1: 0=null, 1=present.
// This is a backward-compatible wrapper around encodeLevelsRLE for flat schemas.
func encodeDefLevelsRLE(nulls []bool, count int) []byte {
	levels := make([]int32, count)
	for i := 0; i < count; i++ {
		if i >= len(nulls) || !nulls[i] {
			levels[i] = 1 // present
		}
		// else levels[i] = 0 (null)
	}
	return encodeLevelsRLE(levels, count, 1)
}

// encodeLevelsRLE encodes int32 levels using RLE/bit-packing hybrid encoding
// at the given bit width. Works for any max level (not just 0/1).
func encodeLevelsRLE(levels []int32, count int, bitWidth int) []byte {
	if count == 0 {
		return nil
	}

	var buf []byte
	valueBytes := (bitWidth + 7) / 8

	i := 0
	for i < count {
		val := int32(0)
		if i < len(levels) {
			val = levels[i]
		}

		// Count consecutive same values.
		runLen := 1
		for i+runLen < count {
			nextVal := int32(0)
			if i+runLen < len(levels) {
				nextVal = levels[i+runLen]
			}
			if nextVal != val {
				break
			}
			runLen++
		}

		// RLE header: count << 1 (LSB=0 for RLE mode).
		buf = appendVarint(buf, uint64(runLen)<<1)
		// Value: ceil(bitWidth/8) bytes, little-endian.
		for b := 0; b < valueBytes; b++ {
			buf = append(buf, byte(val>>(uint(b)*8)))
		}
		i += runLen
	}

	return buf
}

// bitsRequiredForMax returns the number of bits needed to represent values 0..maxVal.
func bitsRequiredForMax(maxVal int32) int {
	if maxVal <= 0 {
		return 0
	}
	bits := 0
	v := maxVal
	for v > 0 {
		bits++
		v >>= 1
	}
	return bits
}

func appendVarint(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	return append(buf, byte(v))
}

// --- Compression ---

func compressPage(data []byte, codec CompressionCodec) ([]byte, error) {
	switch codec {
	case CodecNone:
		return data, nil
	case CodecSnappy:
		return snappy.Encode(nil, data), nil
	case CodecZstd:
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			return nil, fmt.Errorf("creating zstd encoder: %w", err)
		}
		defer enc.Close()
		return enc.EncodeAll(data, nil), nil
	case CodecGzip:
		var buf bytes.Buffer
		w, err := gzip.NewWriterLevel(&buf, gzip.DefaultCompression)
		if err != nil {
			return nil, fmt.Errorf("creating gzip writer: %w", err)
		}
		if _, err := w.Write(data); err != nil {
			return nil, fmt.Errorf("gzip write: %w", err)
		}
		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("gzip close: %w", err)
		}
		return buf.Bytes(), nil
	default:
		return data, nil
	}
}

// Network type conversion helpers (reuse writer.go functions where possible).
//
// Both report whether the literal parsed. They used to answer garbage with 0,
// which is also the answer for "0.0.0.0" and for 00:00:00:00:00:00 — so the
// caller could not tell a parsed address from a rejected one, and "zz" was
// stored as a real MAC. See convertNetworkLiteral.
func ipv4StringToInt64(s string) (int64, bool) {
	// Simple IPv4 parser — avoid net.ParseIP allocation.
	var ip [4]byte
	idx := 0
	octet := 0
	digits := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			if digits == 0 || idx >= 3 {
				return 0, false
			}
			ip[idx] = byte(octet)
			idx++
			octet = 0
			digits = 0
		} else if s[i] >= '0' && s[i] <= '9' {
			octet = octet*10 + int(s[i]-'0')
			digits++
			if octet > 255 || digits > 3 {
				return 0, false
			}
		} else {
			return 0, false
		}
	}
	if idx == 3 && digits > 0 {
		ip[idx] = byte(octet)
		return int64(binary.BigEndian.Uint32(ip[:])), true
	}
	return 0, false
}

func macStringToInt64(s string) (int64, bool) {
	// Parse "00:11:22:33:44:55" format.
	if len(s) != 17 {
		return 0, false
	}
	var n uint64
	for i := 0; i < 6; i++ {
		if i > 0 && s[i*3-1] != ':' {
			return 0, false
		}
		hi := unhex(s[i*3])
		lo := unhex(s[i*3+1])
		if hi == 0xFF || lo == 0xFF {
			return 0, false
		}
		n = (n << 8) | uint64(hi<<4|lo)
	}
	return int64(n), true
}

// ipv6StringToBytes parses an IPv6 literal into the 16-byte storage form an
// IPV6 column is defined to hold — the same conversion Writer.prepareRows
// does ahead of WriteRows, and the same one batch.Vector.SetValue does for a
// string handed to an IPV6 vector.
//
// It used to store the TEXT instead, on the reasoning that prepareRows had
// already converted anything real. Whatever came through the NativeWriter's
// direct API therefore landed as 11 bytes where the contract is exactly 16,
// and Vector.GetValue renders any other length as the EMPTY STRING — the
// same silent shape as #395. Invisible until the declared type was honoured
// on read (#396); now the column reads back as IPV6 and shows it.
//
// A literal that does not parse returns nil, and nil is a refusal rather than
// a value: decomposeLeaf turns it into an error naming the column and the row
// (convertNetworkLiteral). It used to be stored as no bytes at all, which
// read back as "" — an address-shaped hole that IS NULL answered false to.
func ipv6StringToBytes(s string) []byte {
	ip := net.ParseIP(s)
	if ip == nil {
		return nil
	}
	return ip.To16()
}
