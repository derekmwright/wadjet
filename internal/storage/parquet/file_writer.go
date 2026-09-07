package parquet

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"

	"github.com/derekmwright/wadjet/internal/sqlerr"
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

	// closed latches the FINALIZATION, which err cannot express: a Close
	// that succeeded leaves err nil, and the writer then looked exactly like
	// a fresh one to WriteMapRows. See ErrWriterClosed.
	//
	// Atomic, and claimed with CompareAndSwap, for one reason: two goroutines
	// racing to Close must not BOTH finalize. Everything else in this struct
	// is unsynchronized and the writer is not safe for concurrent use (see
	// ErrWriterClosed); this one field is, because the damage a lost race does
	// here is a second footer written over a complete file rather than a
	// merely garbled one (round-1 N1).
	closed atomic.Bool
	// closeMu serialises the finalization itself, so a second Close waits for
	// the first rather than racing it for nw.err (round-2 P). It is held only
	// by Close; checkWritable reads the atomic latch and never blocks.
	closeMu sync.Mutex
}

// ErrWriterClosed is returned by every call on a writer whose file has
// already been finalized.
//
// A parquet file ends with its footer, a four-byte footer length and the
// magic trailer, so the last byte Close writes is the end of the artifact.
// Nothing can be appended to it and nothing can be taken back. Before this,
// neither Writer nor NativeWriter recorded that Close had run, and a later
// WriteRows/WriteMapRows returned nil in both of the two shapes the row-group
// size selects (#972, measured at f415faba on a one-INT64-column file):
//
//   - the row fitted the open row group, so it was buffered, nothing reached
//     the output, and the accepted row was silently LOST;
//   - the row crossed RowGroupSize, so a whole column chunk was appended
//     AFTER the trailer — 292 bytes became 347 and both wadjet's reader and
//     pyarrow then refused the file ("invalid magic", "Parquet magic bytes
//     not found in footer"). A finalized, readable file became unreadable
//     because of a call that returned success.
//
// Close was not idempotent either: a second Close wrote a second footer and
// trailer over the first, which is what a `defer w.Close()` beside an
// explicit one would have done.
//
// The rule is therefore the simplest one that has no such shapes: a closed
// writer is closed. The first Close latches it — whether it succeeded or
// failed — and every later WriteRows, WriteMapRows and Close returns a loud
// error having touched neither the leaf buffers nor the output. A writer
// whose Close FAILED keeps returning that failure instead, because it is the
// more specific answer and it is what #888's latch already promised.
//
// A writer is NOT safe for concurrent use: its leaf buffers, its error latch
// and its byte count are all unsynchronized, and two goroutines writing rows
// to one writer will corrupt the file. The single exception is this latch,
// which is claimed atomically, so two goroutines racing to Close cannot both
// finalize — exactly one writes the footer and the other is told the file is
// already finalized.
var ErrWriterClosed = errors.New("parquet: writer is closed (the file was already finalized)")

// checkWritable is the one gate every write door asks before it accepts
// anything. Writer.WriteRows asks it BEFORE prepareRows, which rewrites the
// caller's own maps in place: a refused write must not touch those either.
func (nw *NativeWriter) checkWritable() error {
	if nw.err != nil {
		return nw.err
	}
	if nw.closed.Load() {
		return ErrWriterClosed
	}
	return nil
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
	hasStats bool
	// hasFloatBound is the float leaves' own "a min/max exists" flag, kept
	// separate from hasStats because a NaN is a value the leaf HAS but which
	// the Parquet spec excludes from min/max: a float column of only NaN (and
	// nulls) must write null_count but NO float bound. hasStats still records
	// that the leaf saw a value (so null_count is unaffected), and buildStats
	// gates the float MinValue/MaxValue on THIS flag instead (#928).
	hasFloatBound      bool
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
//
// The writer takes a DEEP COPY of schema and uses only that copy afterwards, so
// a caller amending a reusable Schema while a writer is alive changes nothing
// about the file. Before this, Columns, nested Fields and ElementType were all
// still the caller's memory, and the writer reads the schema at two separate
// moments: leaf buffers and leaf PATHS are frozen at construction, and the
// footer's schema tree is built again at Close. A mutation between the two made
// them disagree, with WriteRows and Close both returning nil (#973, measured at
// f415faba):
//
//	Columns[0].Name = "b"     -> "row group 0 column 0 carries path [a] but
//	                             schema leaf 0 is [b]"; unreadable here, and
//	                             pyarrow reads the file as column "b".
//	Fields[0].Name = "b"      -> the same, one level down ([r a] vs [r b]).
//	Columns[0].Type -> FLOAT64 -> opens, then "FLOAT64 cannot be decoded from
//	                             an INT64 page"; pyarrow refuses the file.
//	ElementType.Type -> STRING -> pyarrow OPENS it as list<element: string>
//	                             and hands back the INT64 bytes as strings.
func NewNativeWriter(w io.Writer, schema Schema, cfg WriterConfig) *NativeWriter {
	if cfg.RowGroupSize <= 0 {
		cfg.RowGroupSize = 128 * 1024
	}
	schema = schema.Clone()

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
	// ONE validation, both constructors. NewWriter has always run this;
	// NewNativeWriter — exported, documented with a usage example, and the
	// door every test in this package uses — did not, so the malformed-MAP
	// corruption ValidateWriteSchema exists to refuse was still reachable
	// through it: WriteMapRows nil, Close nil, 623 bytes, and neither this
	// package ("row group 0 column 0 carries path [a] but schema leaf 0 is
	// [m key_value a]") nor pyarrow ("Malformed schema: not enough elements")
	// could open the result (#970).
	//
	// The constructor's signature cannot report it, so the error is latched
	// the way a decomposition failure is: the first WriteMapRows or Close
	// returns it, before any output.
	if err := ValidateWriteSchema(schema); err != nil {
		// Validate FIRST, allocate after: a schema this writer has just
		// refused is not one to flatten into leaf buffers, and every write
		// door returns the latched error before it could read them anyway
		// (round-1 N2).
		nw.fail(err)
		return nw
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
		if _, err := vectorTypeLength(c.Dimension); err != nil {
			return fmt.Errorf("column %q: %w", path, err)
		}
		return nil
	case TypeDecimal:
		if _, _, err := checkDecimalDeclaration(c.Precision, c.Scale); err != nil {
			return fmt.Errorf("column %q: %w", path, err)
		}
		return nil
	}
	return nil
}

// MaxVectorDimension is the widest VECTOR this format can carry: a leaf's
// FIXED_LEN_BYTE_ARRAY type_length is a signed int32 and a component is four
// bytes, so 2^31-1 bytes is 536,870,911 components.
//
// It is ONE constant for the whole engine on purpose. The writer's bound and
// the DDL's bound disagreed at 0a3da4ff — `CREATE TABLE t (v
// VECTOR(4611686018427387905))` was accepted by ResolveColumn and produced a
// file whose declared width had wrapped — and a declaration a door accepts
// that the writer cannot store is a table that fails at its first flush at
// best, and a file nobody can read at worst.
const MaxVectorDimension = math.MaxInt32 / 4

// vectorTypeLength is the FIXED_LEN_BYTE_ARRAY width a VECTOR(dimension) leaf
// declares — and the only way to obtain one.
//
// ValidateWriteSchema used to require only Dimension > 0 and
// buildLeafSchemaElement then wrote `int32(col.Dimension * 4)`, an unchecked
// narrowing (of an int multiplication that itself overflows on a 32-bit
// build). Measured at f415faba with NewWriter and Close both returning nil
// (#971):
//
//	Dimension 536870911 (MaxInt32/4) -> type_length 2147483644; pyarrow opens it
//	Dimension 536870912              -> type_length -2147483648; pyarrow refuses
//	                                    it: "Invalid FIXED_LEN_BYTE_ARRAY
//	                                    length: 0"
//	Dimension 2147483647             -> the same refusal
//
// A fixed-width leaf carries no per-value length — the chunk is one run of
// bytes cut every type_length bytes on the way back (ADR-0018 §10, Width) — so
// a wrong width is not a wrong number in one field, it is the boundary of
// every value in the column.
func vectorTypeLength(dimension int) (int32, error) {
	if dimension <= 0 {
		return 0, fmt.Errorf("a VECTOR needs a positive Dimension, got %d", dimension)
	}
	// The OPERAND is bounded, not the product. Widening to int64 first was the
	// round-0 fix and it fixed nothing: on a 64-bit build `int` IS int64, so
	// `int64(dimension) * 4` overflows before any check on the product can see
	// it. Measured at 0a3da4ff, all with NewWriter and Close returning nil:
	// 2^61 and 2^62 wrote type_length 0 (pyarrow: "Invalid
	// FIXED_LEN_BYTE_ARRAY length: 0"), MaxInt wrote -4, and 2^62+1 wrote 4 —
	// a file pyarrow OPENS as fixed_size_binary[4] and wadjet reads back as
	// VECTOR(1). Dividing the ceiling instead of multiplying the operand
	// cannot overflow for any int (round-1 B1).
	if dimension > math.MaxInt32/4 {
		return 0, fmt.Errorf("a VECTOR of %d float32 components is 4x that many bytes wide, past "+
			"the %d a FIXED_LEN_BYTE_ARRAY type_length can carry (at most %d components)",
			dimension, int64(math.MaxInt32), MaxVectorDimension)
	}
	return int32(dimension * 4), nil
}

// checkDecimalDeclaration returns the (precision, scale) a DECIMAL column's
// footer annotation will carry, or the reason the column cannot be written.
//
// `Precision <= 0` is this package's documented "unconstrained" sentinel and
// becomes 38 (decimalEffectivePrecision), so the FILE's precision — not the
// field — is what the scale is measured against, and it is what a foreign
// reader will apply. Measured at f415faba, all reached through NewWriter with
// Close returning nil (#969):
//
//	DECIMAL(9,-1)  a row of "1.25" read back as 0, and pyarrow refuses the
//	               file: "Scale must be a non-negative integer that does not
//	               exceed precision for Decimal logical type"
//	DECIMAL(4,9)   an EMPTY file still carries the annotation, and pyarrow
//	               refuses it the same way
//	DECIMAL(0,40)  the file declares decimal(38,40); pyarrow refuses it
//	DECIMAL(50,2)  the file declares decimal(38,2) — wadjet reads back its own
//	               output as DECIMAL(38,2), not the DECIMAL(50,2) asked for
//	DECIMAL(-3,2)  the same silent re-declaration
//
// ParseDecimalParams enforces 1 <= precision <= 38 and 0 <= scale <= precision
// for DDL; a Column built in Go bypassed it entirely. Scale == precision is
// legal and stays legal (pyarrow opens DECIMAL(38,38)).
func checkDecimalDeclaration(precision, scale int) (int32, int32, error) {
	if precision > MaxDecimalDigits {
		return 0, 0, fmt.Errorf("a DECIMAL precision of %d is past the %d digits a 128-bit unscaled "+
			"carrier holds; the file would declare DECIMAL(%d,%d) instead",
			precision, MaxDecimalDigits, decimalEffectivePrecision(precision), scale)
	}
	if precision < 0 {
		return 0, 0, fmt.Errorf("a DECIMAL precision of %d is not a precision; the file would declare "+
			"DECIMAL(%d,%d) instead (0 is the unconstrained sentinel and means %d)",
			precision, decimalEffectivePrecision(precision), scale, MaxDecimalDigits)
	}
	if scale < 0 {
		return 0, 0, fmt.Errorf("a DECIMAL scale of %d is negative; a file annotated that way is one "+
			"the reference implementation refuses to open", scale)
	}
	eff := decimalEffectivePrecision(precision)
	if scale > eff {
		return 0, 0, fmt.Errorf("a DECIMAL scale of %d is past the precision %d the file will declare; "+
			"a file annotated that way is one the reference implementation refuses to open", scale, eff)
	}
	return int32(eff), int32(scale), nil
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
	if err := nw.checkWritable(); err != nil {
		return err
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
		nw.appendAbsentLeaf(lb, col, defLevel, repLevel)
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
			// The empty literal is an absence, and absence is NULL — which a
			// REQUIRED column cannot express, so it takes the same refusal a
			// literal nil does.
			nw.appendAbsentLeaf(lb, col, defLevel, repLevel)
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
	//
	// A time.Time for a DATE column, and a string or a time.Duration for the
	// other two temporal types, are normalised HERE for the same reason and
	// by the same rule: every one of them is a box ingest.checkType DECLARES
	// acceptable, and every one of them used to reach toInt32/toInt64's
	// default arm and store ZERO — 1970-01-01 for a DATE, 1970-01-01T00:00Z
	// for a TIMESTAMP, a zero interval for a DURATION — with no error
	// anywhere. That is how `INSERT INTO t VALUES (1, '2020-01-01')` stored
	// the epoch while ingest.Ingest with the same text stored the date: the
	// SQL path boxes a DATE as time.Time and the programmatic one boxes it as
	// a string, and only the string had a converter (#673).
	//
	// The accept-set is the boxes checkType admits, so the two boundaries
	// agree by construction, and what they cannot convert FAILS the write
	// rather than storing a wrong instant.
	if norm, ok, err := normalizeTemporalBox(col.Type, val); err != nil {
		nw.fail(fmt.Errorf("column %q, row %d of this write: %w", col.Name, nw.rowsSeen, err))
		lb.appendEntry(defLevel, repLevel)
		return
	} else if ok {
		val = norm
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
	//
	// An INT32 or INT64 leaf's box is resolved inside, and a value it cannot
	// hold fails the write HERE, where the column and the row are still known
	// — the same rule and the same shape as the DECIMAL block above. Before
	// this, an out-of-range integer WRAPPED into the file and a NaN wrote
	// whatever Go's implementation-defined float→int conversion produced.
	if err := lb.appendEntryWithValue(lb.maxDefLevel, repLevel, val); err != nil {
		nw.fail(fmt.Errorf("column %q, row %d of this write: %w", col.Name, nw.rowsSeen, err))
		lb.appendEntry(defLevel, repLevel)
	}
}

// appendAbsentLeaf records that this leaf has no value at this position, or
// refuses when the position cannot say so.
//
// A definition level says how many of a leaf's optional ancestors are present;
// the leaf's own maxDefLevel is the level at which the VALUE itself is
// present. A REQUIRED leaf has no level below that to spend on absence, so
// appending one for a nil wrote the PRESENT level and advanced the count with
// nothing behind it: every later value in that column shifted by one. For a
// required BOOLEAN, `[nil, true]` read back as `[true, false]` — the bit
// padding hid the mismatch — and for a required INT64, `[nil, 42]` produced a
// file the decoder could not finish, two values declared over eight data bytes
// (#887).
//
// The test is the LEVEL, not the column's Nullable flag, and that is what makes
// it right at depth: a required field of a PRESENT optional struct has
// defLevel == maxDefLevel here and is refused, while the same field under an
// ABSENT optional ancestor never reaches this function at all (its subtree goes
// through emitNullForSubtree at the ancestor's own lower level), which is
// exactly the case that must stay legal.
//
// The SQLSTATE is PostgreSQL's 23502 not_null_violation, the one
// ingest.validateRow already raises for a missing non-nullable column.
func (nw *NativeWriter) appendAbsentLeaf(lb *leafBuffer, col Column, defLevel, repLevel int32) {
	if defLevel >= lb.maxDefLevel {
		nw.fail(sqlerr.New("23502",
			"column %q, row %d of this write: null value in column %q violates not-null constraint",
			col.Name, nw.rowsSeen, col.Name))
	}
	lb.appendEntry(defLevel, repLevel)
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
		if !col.Nullable {
			// A required ARRAY has no encoding for NULL, exactly as a
			// required MAP has none: level 0 already means "list present, no
			// entries" — the EMPTY array — so writing a null here produced a
			// file whose own schema says the value cannot be null and whose
			// reader is right to read {} back. Refuse it at the source (#887).
			nw.fail(sqlerr.New("23502",
				"column %q, row %d of this write: ARRAY is not nullable, cannot write a NULL array "+
					"(an empty array is []any{}, which is a different value)", col.Name, nw.rowsSeen))
		}
		// Entire array is null. Emit null at current def level for all leaves.
		curDef := defLevel // outer group absent
		nw.emitNullForSubtree(elemCol, curDef, repLevel, leafIdx)
		return
	}

	arr, err := arrayElements(col, val)
	if err != nil {
		// A box with no reading as an array used to become an absent
		// subtree — a NULL nobody wrote (#889).
		nw.fail(fmt.Errorf("row %d of this write: %w", nw.rowsSeen, err))
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
		// Neither the native map nor the storage shape. A string-keyed map of
		// some other value type is the same value spelled differently and is
		// normalised; anything else has no reading as a map at all and fails
		// the write rather than becoming a NULL (#889).
		conv, err := rowFields(col, val, "a MAP")
		if err != nil {
			nw.fail(fmt.Errorf("row %d of this write: %w", nw.rowsSeen, err))
			nw.emitNullForSubtree(keyCol, defLevel, repLevel, leafIdx)
			nw.emitNullForSubtree(valCol, defLevel, repLevel, leafIdx)
			return
		}
		m = conv
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
		if !col.Nullable {
			// A required ROW has no encoding for NULL either: with no
			// optional group of its own, defLevel is already the level at
			// which the struct is present, so the fields' null entries land
			// on a level the footer says means "present" (#887).
			nw.fail(sqlerr.New("23502",
				"column %q, row %d of this write: ROW is not nullable, cannot write a NULL struct",
				col.Name, nw.rowsSeen))
		}
		// Entire struct is null — emit null for each field.
		for _, field := range col.Fields {
			nw.emitNullForSubtree(field, defLevel, repLevel, leafIdx)
		}
		return
	}

	m, err := rowFields(col, val, "a ROW")
	if err != nil {
		nw.fail(fmt.Errorf("row %d of this write: %w", nw.rowsSeen, err))
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
//
// It is terminal: see ErrWriterClosed. A second Close appends nothing.
func (nw *NativeWriter) Close() error {
	// Claimed BEFORE the first byte of the finalization goes out, so a Close
	// that dies half way through the footer cannot be retried into a second
	// one — and, because the claim is a CAS, so that two goroutines racing to
	// Close cannot both write a footer: exactly one wins and the other is told
	// the file is already finalized.
	// The finalization runs under closeMu, and a second caller waits for it
	// rather than racing it.
	//
	// The round-1 shape claimed the latch with a CompareAndSwap and let the
	// loser read nw.err to decide what to report — a data race with the
	// winner's own nw.fail(), measured under -race against a failing output
	// stream in 5 of 10 runs. Returning ErrWriterClosed unconditionally there
	// removes the race but also removes an answer #888 promised: "the first
	// output or flush failure is latched and every later WriteRows and the
	// Close return it" (ADR-0018 §10). Holding the mutex keeps both — the
	// second caller observes a FINISHED finalization, so nw.err is visible to
	// it through the lock and it reports the same failure the first Close did
	// (round-2 P).
	nw.closeMu.Lock()
	defer nw.closeMu.Unlock()
	if nw.closed.Load() {
		// A writer whose finalization FAILED keeps returning that failure: it
		// is the more specific answer and it is what #888's latch promised.
		if err := nw.err; err != nil {
			return err
		}
		return ErrWriterClosed
	}
	// Claimed BEFORE the first byte of the finalization goes out, so a Close
	// that dies half way through the footer cannot be retried into a second
	// one, and so checkWritable's lock-free read sees the file as finished
	// from this point on.
	nw.closed.Store(true)
	if nw.err != nil {
		// A failed writer stays failed, and stays closed: the finalization
		// is over either way, so no later call may write.
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
	if err := nw.writeFooter(); err != nil {
		nw.fail(err)
		return nw.err
	}
	return nil
}

// writeBytes is the ONE seam every byte of the file leaves through, so it is
// where an output failure is LATCHED.
//
// A row group is emitted column by column and each leaf buffer is reset the
// moment its chunk is written, so a failure on the fourth Write of a two-column
// group leaves the first column's values already discarded and numRows still
// standing. Nothing can rebuild that. Before this, neither the failure nor the
// flush that carried it reached nw.err: WriteRows reported the I/O error,
// Close then RETRIED the inconsistent state, wrote a footer over it and
// returned nil, and the reader opened a perfectly valid file with one column
// simply gone (#888).
func (nw *NativeWriter) writeBytes(b []byte) error {
	if nw.err != nil {
		return nw.err
	}
	n, err := nw.w.Write(b)
	nw.written += int64(n)
	if err != nil {
		nw.fail(fmt.Errorf("writing to the output stream: %w", err))
		return nw.err
	}
	// A conforming io.Writer that returns nil MUST have consumed all of b; the
	// Go docs say a short write should carry a non-nil error, but many writers
	// do not, so the caller defends the boundary. A partial write with a nil
	// error dropped the tail of b out of the file's middle and left the leaf
	// buffers already reset — exactly as unrecoverable as an errored write —
	// so it is latched the same way (#926).
	if n != len(b) {
		nw.fail(fmt.Errorf("writing to the output stream: wrote %d of %d bytes: %w", n, len(b), io.ErrShortWrite))
		return nw.err
	}
	return nil
}

func (nw *NativeWriter) flushRowGroup() error {
	if nw.err != nil {
		return nw.err
	}
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
			// Latched, not merely returned: the leaves written before this
			// one have already been reset, so this row group can never be
			// completed and no later call may act as though it could.
			nw.fail(fmt.Errorf("writing column %s: %w", lb.path, err))
			return nw.err
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

// checkPageSize refuses a page body that cannot be described by the Thrift
// page header's int32 UncompressedPageSize/CompressedPageSize fields. The
// Parquet spec caps a page at 2^31-1 bytes; without this guard the int32 cast
// below wrapped a >=2GB page to a negative/wrong size and the file was
// finalized as success — silent corruption any reader would then read past
// (#929). A page reaches this size only from a single value in the 2GB-4GB
// range (a BYTE_ARRAY/FLBA/VECTOR value, whose own length prefix is uint32);
// decomposition refuses such a value first, so the message there names the
// row, and this is the structural backstop at the cast itself. ADR-0018 §10.
func checkPageSize(colName string, n int) error {
	if n > math.MaxInt32 {
		return fmt.Errorf("column %q: page body is %d bytes, exceeds the %d-byte "+
			"parquet page limit (int32 page-size header field)", colName, n, math.MaxInt32)
	}
	return nil
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
	if err := checkPageSize(lb.col.Name, len(uncompressedData)); err != nil {
		return 0, 0, err
	}
	uncompressedSize := int32(len(uncompressedData))

	// Compress.
	compressedData, err := compressPage(uncompressedData, nw.codec)
	if err != nil {
		return 0, 0, fmt.Errorf("compressing page: %w", err)
	}
	if err := checkPageSize(lb.col.Name, len(compressedData)); err != nil {
		return 0, 0, err
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

// footerTrailerLength is the value a file's four-byte trailer carries for a
// footer of n bytes — and the ONLY way to obtain one, so the narrowing cannot
// happen anywhere else.
//
// The trailer is a fixed four-byte unsigned length, so a footer past
// math.MaxUint32 has no honest position to point at. writeFooter used to
// encode the metadata, WRITE IT, and only then narrow the length with a bare
// uint32() conversion: 2^32 bytes of footer became a trailer of 0 and 2^32+4
// became 4, so Close appended PAR1 over a location pointing into the data and
// returned nil. Readers seek there and interpret whatever they find as
// metadata. It is reachable from row-group metadata alone — one RowGroup plus
// one ColumnChunk per column accumulates per flush and is retained to Close —
// not only from huge values (#974).
//
// Two bounds, in the order that names the failure most precisely:
//
//   - The FORMAT's width. Structural, not policy: nothing can carry it.
//   - This package's own READ ceiling, footerMaxSize. ADR-0018 §2's corollary
//     binds the writer to the reader's ceilings, and a 64 MiB footer is a file
//     wadjet itself refuses to open — writing one produces an artifact that is
//     unreadable here and merely pathological elsewhere. Refusing at Close
//     says so while the caller still has the rows.
func footerTrailerLength(n int64) (uint32, error) {
	if n > math.MaxUint32 {
		return 0, fmt.Errorf("parquet: refusing to finalize the file: its footer is %d bytes, which the "+
			"format's 4-byte trailer cannot address (max %d)", n, int64(math.MaxUint32))
	}
	if n > footerMaxSize {
		return 0, fmt.Errorf("parquet: refusing to finalize the file: its footer is %d bytes, past the "+
			"%d-byte ceiling this package will read back", n, int64(footerMaxSize))
	}
	if n <= 0 {
		return 0, fmt.Errorf("parquet: refusing to finalize the file: its footer encoded to %d bytes", n)
	}
	return uint32(n), nil
}

func (nw *NativeWriter) writeFooter() error {
	totalRows := int64(0)
	for _, rg := range nw.rowGroups {
		totalRows += rg.NumRows
	}

	// Build schema elements (flattened schema tree).
	schemaElements, err := buildSchemaElements(nw.schema)
	if err != nil {
		return err
	}

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
		CreatedBy:        CreatedBy(),
		KeyValueMetadata: kv,
	}

	footerBytes := EncodeFileMetaData(md)
	if len(footerBytes) == 0 {
		// The encoder refused a field it could not state honestly rather than
		// substituting a placeholder for it (round-2 N). Nothing has been
		// written yet, so the file is simply not finalized.
		return fmt.Errorf("refusing to finalize the file: its footer could not be encoded")
	}

	// BEFORE the first footer byte goes out: a footer whose length the
	// trailer cannot carry has no honest file to be part of, and once the
	// bytes are on the stream nothing can take them back (#974).
	trailerLen, err := footerTrailerLength(int64(len(footerBytes)))
	if err != nil {
		return err
	}

	if err := nw.writeBytes(footerBytes); err != nil {
		return err
	}

	// Write footer length (4 bytes LE).
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], trailerLen)
	if err := nw.writeBytes(lenBuf[:]); err != nil {
		return err
	}

	// Write magic trailer.
	return nw.writeBytes([]byte("PAR1"))
}

// buildSchemaElements creates the flattened schema tree for the footer.
// Handles flat columns and nested types (ARRAY/LIST, MAP, ROW/STRUCT).
//
// It returns an error rather than emitting a field it had to narrow: this is
// the structural backstop at the cast, in the same position as checkPageSize
// (#929). ValidateWriteSchema refuses the same shapes at construction, where
// the caller still has its rows; if the two ever disagree the file is not
// written at all rather than written with a wrapped width (#971).
func buildSchemaElements(schema Schema) ([]SchemaElement, error) {
	// Root element (message).
	root := SchemaElement{
		Name:        "wadjet_schema",
		NumChildren: int32(len(schema.Columns)),
	}
	elements := []SchemaElement{root}

	for _, col := range schema.Columns {
		if err := buildColumnSchemaElements(col, &elements); err != nil {
			return nil, err
		}
	}
	return elements, nil
}

// buildColumnSchemaElements recursively emits SchemaElements for a single column.
func buildColumnSchemaElements(col Column, elements *[]SchemaElement) error {
	switch col.Type {
	case TypeArray:
		return buildArraySchemaElements(col, elements)
	case TypeMap:
		return buildMapSchemaElements(col, elements)
	case TypeRow:
		return buildRowSchemaElements(col, elements)
	default:
		return buildLeafSchemaElement(col, elements)
	}
}

// buildArraySchemaElements emits the standard Parquet LIST schema:
//
//	optional group <name> (LIST) {
//	  repeated group list {
//	    optional <element_type> element
//	  }
//	}
func buildArraySchemaElements(col Column, elements *[]SchemaElement) error {
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
	// The backstop, not a default. Substituting a STRING element for a missing
	// ElementType emitted a LIST whose element does not describe what the leaf
	// buffers hold; ValidateWriteSchema refuses the shape at construction, and
	// if the two ever disagree the file is not written at all (round-1 P1).
	if col.ElementType == nil {
		return fmt.Errorf("column %q: an ARRAY needs an ElementType; the footer's schema tree "+
			"cannot be built without it", col.Name)
	}
	elemCol := *col.ElementType
	elemCol.Nullable = true // elements are always optional in LIST
	return buildColumnSchemaElements(elemCol, elements)
}

// buildMapSchemaElements emits the standard Parquet MAP schema:
//
//	optional group <name> (MAP) {
//	  repeated group key_value (MAP_KEY_VALUE) {
//	    required <key_type> key
//	    optional <value_type> value
//	  }
//	}
func buildMapSchemaElements(col Column, elements *[]SchemaElement) error {
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

	// The backstop the ADR claims. Returning nil here emitted a "key_value"
	// group promising two children and giving none, which BuildSchemaTree reads
	// back as a group borrowing the next two TOP-LEVEL columns — the #970
	// corruption, still finalizable through this path with the validator
	// removed (a 424-byte unreadable file, measured by the round-0 review).
	// A truncated tree is never emitted now (round-1 P1).
	if col.ElementType == nil || col.ElementType.Type != TypeRow || len(col.ElementType.Fields) != 2 {
		return fmt.Errorf("column %q: a MAP needs ElementType = ROW with exactly two fields "+
			"(key, value); the footer's key_value group cannot be built without them", col.Name)
	}
	keyCol := col.ElementType.Fields[0]
	keyCol.Nullable = false // keys are required
	if err := buildColumnSchemaElements(keyCol, elements); err != nil {
		return err
	}

	valCol := col.ElementType.Fields[1]
	valCol.Nullable = true // values are optional
	return buildColumnSchemaElements(valCol, elements)
}

// buildRowSchemaElements emits the Parquet STRUCT schema:
//
//	optional group <name> {
//	  optional <type> field1
//	  optional <type> field2
//	  ...
//	}
func buildRowSchemaElements(col Column, elements *[]SchemaElement) error {
	// Uniform with the MAP and ARRAY backstops above: the validator refuses a
	// field-less ROW, and the builder does not emit a group the validator
	// would not have allowed (round-1 P1). This one is self-consistent rather
	// than truncated — NumChildren would be 0 and 0 children follow — but the
	// two walks disagreeing is the failure class, not the shape of the damage.
	if len(col.Fields) == 0 {
		return fmt.Errorf("column %q: a ROW needs at least one field; the footer's schema tree "+
			"cannot describe an empty group", col.Name)
	}
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
		if err := buildColumnSchemaElements(field, elements); err != nil {
			return err
		}
	}
	return nil
}

// buildLeafSchemaElement emits a single leaf SchemaElement.
func buildLeafSchemaElement(col Column, elements *[]SchemaElement) error {
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
		prec, scale, err := checkDecimalDeclaration(col.Precision, col.Scale)
		if err != nil {
			return fmt.Errorf("column %q: %w", col.Name, err)
		}
		se.Precision = prec
		se.Scale = scale
		if pt == PhysicalFixedLenByteArray {
			se.TypeLength = decimalFLBAWidth
		}
		se.LogicalType = &LogicalType{
			Type:      LogicalDecimal,
			Precision: int(prec),
			Scale:     int(scale),
		}
	case TypeVector:
		width, err := vectorTypeLength(col.Dimension) // dim × sizeof(float32)
		if err != nil {
			return fmt.Errorf("column %q: %w", col.Name, err)
		}
		se.TypeLength = width
		se.LogicalType = &LogicalType{
			Type:      LogicalVector,
			Dimension: col.Dimension,
		}
	}

	*elements = append(*elements, se)
	return nil
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
//
// It returns an error rather than storing a value the leaf cannot hold, and
// the integer leaves resolve their box FIRST, before a single level is
// appended: a refused value must leave the buffer exactly as it found it so
// the caller can append the NULL entry that keeps this leaf aligned with its
// siblings.
func (lb *leafBuffer) appendEntryWithValue(defLevel, repLevel int32, val any) error {
	var (
		b    bool
		i32  int32
		i64  int64
		f32  float32
		f64  float64
		raw  []byte
		err  error
		phys = lb.physical
	)
	switch phys {
	case PhysicalBoolean:
		b, err = boolLeafValue(lb.col.Type, val)
	case PhysicalInt32:
		i32, err = int32LeafValue(lb.col.Type, val)
	case PhysicalInt64:
		// DECIMAL is the other INT64 leaf and never arrives here:
		// decomposeLeaf resolves it through DecimalValueFromBox and returns.
		i64, err = int64LeafValue(lb.col.Type, val)
	case PhysicalFloat:
		f32, err = float32LeafValue(lb.col.Type, val)
	case PhysicalDouble:
		f64, err = float64LeafValue(lb.col.Type, val)
	case PhysicalByteArray:
		raw, err = bytesLeafValue(lb.col, val)
	case PhysicalFixedLenByteArray:
		raw, err = vectorLeafValue(lb.col, val)
	}
	if err != nil {
		return err
	}
	// A single BYTE_ARRAY/FLBA/VECTOR value in the 2GB-4GB range (its own
	// length prefix is uint32) alone overflows a page's int32 size field. Refuse
	// it HERE, where the caller still names the column and the row, rather than
	// let it reach checkPageSize with only the page to point at (#929).
	if len(raw) > math.MaxInt32 {
		return fmt.Errorf("value is %d bytes, exceeds the %d-byte parquet page limit",
			len(raw), math.MaxInt32)
	}
	// The row group's byte-array RUN is addressed by a uint32 offset table
	// (leafBuffer.offsets), so the SUM has a ceiling the individual value
	// check above cannot see: appendByteArray narrows len(lb.packed) with no
	// bound, and past 4 GiB in one row group every offset after the wrap
	// points at the wrong value — a silently corrupt column, not a short one.
	// Found by the round-1 sweep for unchecked narrowings that B1 asked for;
	// refused here, where the row can still be named, and reachable only with
	// a RowGroupSize large enough to accumulate 4 GiB of one column.
	if phys == PhysicalByteArray && int64(len(lb.packed))+int64(len(raw)) > math.MaxUint32 {
		return fmt.Errorf("this row group already holds %d bytes for this column and the value adds "+
			"%d, past the %d a leaf's uint32 offset table can address; lower RowGroupSize",
			len(lb.packed), len(raw), int64(math.MaxUint32))
	}

	lb.defLevels = append(lb.defLevels, defLevel)
	lb.repLevels = append(lb.repLevels, repLevel)
	lb.count++

	switch phys {
	case PhysicalBoolean:
		lb.appendBool(b)
	case PhysicalInt32:
		lb.appendInt32(i32)
	case PhysicalInt64:
		lb.appendInt64(i64)
	case PhysicalFloat:
		lb.appendFloat32(f32)
	case PhysicalDouble:
		lb.appendFloat64(f64)
	case PhysicalByteArray:
		lb.appendByteArray(raw)
	case PhysicalFixedLenByteArray:
		lb.data = append(lb.data, raw...)
	}
	return nil
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
	lb.hasFloatBound = false
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
	// NaN is excluded from min/max (Parquet spec): it seeds neither bound and
	// never displaces one. hasStats still records the value so null_count is
	// unaffected; a leaf of only NaN leaves hasFloatBound false and writes no
	// float min/max (#928).
	lb.hasStats = true
	if math.IsNaN(float64(v)) {
		return
	}
	if !lb.hasFloatBound {
		lb.minF32, lb.maxF32 = v, v
		lb.hasFloatBound = true
		return
	}
	// The ±0.0 spec rule: -0.0 and +0.0 compare equal, so a bare `<`/`>` never
	// moves the bound between them. min prefers -0.0, max prefers +0.0.
	if v < lb.minF32 || (v == lb.minF32 && math.Signbit(float64(v)) && !math.Signbit(float64(lb.minF32))) {
		lb.minF32 = v
	}
	if v > lb.maxF32 || (v == lb.maxF32 && !math.Signbit(float64(v)) && math.Signbit(float64(lb.maxF32))) {
		lb.maxF32 = v
	}
}

func (lb *leafBuffer) updateStatsF64(v float64) {
	lb.hasStats = true
	if math.IsNaN(v) {
		return
	}
	if !lb.hasFloatBound {
		lb.minF64, lb.maxF64 = v, v
		lb.hasFloatBound = true
		return
	}
	if v < lb.minF64 || (v == lb.minF64 && math.Signbit(v) && !math.Signbit(lb.minF64)) {
		lb.minF64 = v
	}
	if v > lb.maxF64 || (v == lb.maxF64 && !math.Signbit(v) && math.Signbit(lb.maxF64)) {
		lb.maxF64 = v
	}
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
		// PostgreSQL's abbreviated v4 forms in its INET grammar, read by the
		// ONE parser this package and kernel share (#627). Mirrors
		// kernel.CidrSortKey's own fallback exactly, which is what the
		// duplication gate requires.
		a, ones, pok := PgIPv4Pton(s)
		if !pok {
			return "", false
		}
		full := net.IP(a[:])
		masked := full.Mask(net.CIDRMask(ones, 32))
		buf := make([]byte, 0, 2+2*net.IPv4len)
		buf = append(buf, 0x04)
		buf = append(buf, masked...)
		buf = append(buf, byte(ones))
		buf = append(buf, full...)
		return string(buf), true
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
		// No float bound means every value was NaN (or null): the spec says
		// such a column carries null_count but no min/max, so leave them unset.
		if !lb.hasFloatBound {
			return s
		}
		s.MinValue = make([]byte, 4)
		s.MaxValue = make([]byte, 4)
		binary.LittleEndian.PutUint32(s.MinValue, math.Float32bits(lb.minF32))
		binary.LittleEndian.PutUint32(s.MaxValue, math.Float32bits(lb.maxF32))
	case PhysicalDouble:
		if !lb.hasFloatBound {
			return s
		}
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

// toBool, toInt32, toInt64, toFloat32, toFloat64, toBytes and
// convertStringToInt64 used to live in this file. Each narrowed or rendered
// with a bare Go conversion and answered every box it did not name with a ZERO
// VALUE — false, 0, an empty byte slice, 0.0.0.0. Every leaf now resolves its
// box through leaf_value.go, which refuses what it cannot store, so the last
// four were left with no callers at all; ADR-0018 §10 says "there is no
// `default:` arm that produces a value", and four dead functions one call away
// from making that untrue is not the same as it being true (round-1 review N1).

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
