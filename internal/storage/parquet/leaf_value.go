package parquet

import (
	"encoding/binary"
	"math"
	"net"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// int32LeafValue and int64LeafValue resolve a caller's box into the value an
// INT32 / INT64 leaf stores, or refuse it.
//
// Their predecessors (toInt32, toInt64) narrowed with a bare Go conversion and
// had no way to say no. Measured on the tree before this file existed, writing
// one row through NativeWriter.WriteMapRows and reading it back:
//
//	INT32 leaf, int64(3000000000)   stored -1294967296   wrapped
//	INT32 leaf, int64(-2147483649)  stored  2147483647   wrapped
//	INT32 leaf, int(3000000000)     stored -1294967296   wrapped
//	INT32 leaf, float64(1e10)       stored -2147483648   implementation-defined
//	INT32 leaf, math.NaN()          stored -2147483648   implementation-defined
//	INT32 leaf, float64(2.5)        stored  2            truncated
//	INT32 leaf, int8/int16/uint8/uint16/uint32   stored 0
//	INT64 leaf, uint64(5), uint8(6)              stored 0
//	INT64 leaf, math.NaN(), float64(1e30)        stored -9223372036854775808
//
// The last two rows of each block are the ones no reviewer expects: those five
// integer boxes are ADMITTED by ingest.checkType for an INT32 / PORT /
// PROTOCOL column and uint64 is admitted for an INT64 one, and every one of
// them fell through a `default: return 0` arm and stored a zero nobody wrote.
// Go's float→int conversion is IMPLEMENTATION-DEFINED outside the
// destination's range and for a NaN, which is why the check has to happen on
// the FLOAT and not on whatever integer the conversion happened to produce.
//
// PostgreSQL raises 22003 for the same assignment — `INSERT INTO t(int4col)
// VALUES (3000000000)`, `'NaN'::float8::int4` and `1e10::float8::int4` are all
// `22003 integer out of range` on postgres:17-alpine — and batch's
// IntegerRangeError is the engine's carrier for it. This package cannot use
// that type (batch imports parquet, not the other way round), so it raises the
// same SQLSTATE through sqlerr, the way DecimalRescale already does.
//
// A FRACTIONAL float is refused rather than rounded. PostgreSQL rounds at the
// CAST (float8→int4 half-to-even, numeric→int4 half-away-from-zero) and
// wadjet's DML door implements both rules in assignIntegerValue; by the time a
// value reaches a leaf the assignment cast has already happened, so a float
// with a fraction here means nobody applied one — and storing 2 for 2.5 is
// exactly the "reads back as a different number" ADR-0018 forbids.
func int32LeafValue(colType TypeID, v any) (int32, error) {
	if n, isInt, err := integerBoxValue(colType, v); isInt {
		if err != nil {
			return 0, err
		}
		if n < math.MinInt32 || n > math.MaxInt32 {
			return 0, leafRangeError(colType, v)
		}
		return int32(n), nil
	}
	switch t := v.(type) {
	case float32:
		return floatToInt32Leaf(colType, float64(t), v)
	case float64:
		return floatToInt32Leaf(colType, t, v)
	}
	// A DATE text literal, a time.Time and a time.Duration are converted by
	// normalizeTemporalBox before a leaf ever sees them, and ParseDateDays is
	// the one accept-set for a date (#560). Anything still wearing another box
	// here has no rule at all, and the zero its predecessor stored for it was
	// a number nobody wrote.
	return 0, leafBoxError(colType, v)
}

// CheckInt32LeafValue is int32LeafValue's refusal without its value, for the
// one caller that has a TypeID and no Column: Writer.prepareRows, which must
// not mangle a PORT/PROTOCOL box on the way past. Everything with a Column asks
// CheckLeafBox.
func CheckInt32LeafValue(colType TypeID, v any) error {
	_, err := int32LeafValue(colType, v)
	return err
}

// CheckLeafBox reports whether this writer can store this box in this column,
// without writing anything. It is decomposeLeaf's own sequence of questions,
// in decomposeLeaf's own order, so the answer is the answer the write will
// give.
//
// It exists so the boundary ABOVE the writer can ask ONE question instead of
// reimplementing the list. ingest.checkType used to ask six of its own, and
// they were narrower in one direction and wider in the other: it refused
// `uint(42)` and `int8(42)` for numeric columns (values the leaf holds
// exactly), and it ADMITTED a malformed address or timestamp text for
// IPv4/IPv6/MAC/UUID/TIMESTAMP/DURATION because it checked only the Go TYPE.
// The second half is the one that costs: a bad literal admitted at the door
// fails at the FLUSH instead, which happens per BUFFER, so one bad row takes a
// batch of good ones with it and reports against a partition rather than
// against the row that carried it — the argument #647 already made for DECIMAL
// and #560 for DATE (round-1 review P2).
//
// A nil is not this function's business: presence is the schema's rule and
// decomposeLeaf's, and the caller checks it first.
func CheckLeafBox(col Column, v any) error {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok && hasNetworkLiteralForm(col.Type) {
		_, err := convertNetworkLiteral(col.Type, s)
		return err
	}
	// One call, not two: this runs per value per column on the ingest path, and
	// a TIMESTAMP or DATE literal is parsed inside it.
	if _, ok, err := normalizeTemporalBox(col.Type, v); err != nil {
		return err
	} else if ok {
		// A normalised temporal box is stored as the int32/int64 the
		// normalisation produced; there is nothing left for the leaf to
		// refuse.
		return nil
	}
	switch col.Type {
	case TypeDecimal:
		d, err := DecimalValueFromBox(v, col.Precision, col.Scale)
		if err != nil {
			return err
		}
		if columnPhysical(col) == PhysicalInt64 {
			if _, fits := d.Int64(); !fits {
				return sqlerr.New("22003",
					"DECIMAL unscaled value %s needs more than 64 bits, "+
						"which this writer's INT64 encoding cannot store", d)
			}
		}
		return nil
	case TypeVector:
		_, err := vectorLeafValue(col, v)
		return err
	}
	switch columnPhysical(col) {
	case PhysicalBoolean:
		_, err := boolLeafValue(col.Type, v)
		return err
	case PhysicalInt32:
		_, err := int32LeafValue(col.Type, v)
		return err
	case PhysicalInt64:
		_, err := int64LeafValue(col.Type, v)
		return err
	case PhysicalFloat:
		_, err := float32LeafValue(col.Type, v)
		return err
	case PhysicalDouble:
		_, err := float64LeafValue(col.Type, v)
		return err
	case PhysicalByteArray:
		_, err := bytesLeafValue(col, v)
		return err
	}
	return nil
}

func int64LeafValue(colType TypeID, v any) (int64, error) {
	if n, isInt, err := integerBoxValue(colType, v); isInt {
		if err != nil {
			return 0, err
		}
		return n, nil
	}
	switch t := v.(type) {
	case float32:
		return floatToInt64Leaf(colType, float64(t), v)
	case float64:
		return floatToInt64Leaf(colType, t, v)
	case time.Time:
		// TypeTimestamp ONLY, when ingest hands a time.Time directly. The
		// parquet schema declares TypeTimestamp as TimestampMillis, so encode
		// in milliseconds — otherwise the row group stores 0 and every query
		// against the column reads zeros (TestTimestampStringComparison
		// surfaced this).
		//
		// The colType test is what keeps the two boundaries agreeing: INT64,
		// IPV4, MAC and DURATION are INT64 leaves too, and this arm used to
		// take a time.Time for any of them and store a millisecond count
		// nobody asked for. ingest.checkType admits a time.Time for TIMESTAMP
		// and DATE and for nothing else, and it is right to — a BIGINT column
		// is a number, not an instant (round-2 review, the agreement gate's
		// last numeric cell).
		if colType != TypeTimestamp {
			return 0, leafBoxError(colType, v)
		}
		return t.UnixMilli(), nil
	case string:
		// IPv4 and MAC text is converted by convertNetworkLiteral before a
		// leaf sees it, so this is the belt to that braces: text that names no
		// address fails the write instead of storing 0.0.0.0.
		switch colType {
		case TypeIPv4:
			if n, ok := ipv4StringToInt64(t); ok {
				return n, nil
			}
		case TypeMAC:
			if n, ok := macStringToInt64(t); ok {
				return n, nil
			}
		}
		return 0, leafBoxError(colType, v)
	case net.IP:
		return networkBytesToInt64(colType, t, v)
	case net.HardwareAddr:
		return networkBytesToInt64(colType, t, v)
	case []byte:
		return networkBytesToInt64(colType, t, v)
	}
	return 0, leafBoxError(colType, v)
}

// networkBytesToInt64 stores an IPv4 or MAC address handed over in its BINARY
// form — the box `net.ParseIP(…).To4()` and `net.ParseMAC` produce, which the
// public writer's callers do hand it and which used to fall through to a zero:
// TestSchemaAsRestoresTypesAFileCannotDeclare wrote net.ParseIP("10.0.0.5") and
// stored 0.0.0.0, and asserted only the column's TYPE, so nothing saw it.
func networkBytesToInt64(colType TypeID, b []byte, box any) (int64, error) {
	switch colType {
	case TypeIPv4:
		if ip := net.IP(b).To4(); ip != nil {
			return int64(uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])), nil
		}
	case TypeMAC:
		if len(b) == 6 {
			var n uint64
			for _, c := range b {
				n = n<<8 | uint64(c)
			}
			return int64(n), nil
		}
	}
	return 0, leafBoxError(colType, box)
}

// integerBoxValue widens an integer box to an int64 without narrowing
// anything. isInt=false means the box is not an integer at all and the caller's
// other rules apply; a non-nil error means it IS an integer and no int64 holds
// it, which only the two unsigned 64-bit-wide boxes can be.
//
// The list is the union of the accept-sets the two boundaries above this one
// declare: ingest.checkType takes int, int8, int16, int32, int64, uint8,
// uint16 and uint32 for an INT32 / PORT / PROTOCOL column and adds uint64 for
// an INT64 one. `uint` is here because a caller of the public NativeWriter can
// hand one and its width is the platform's.
func integerBoxValue(colType TypeID, v any) (int64, bool, error) {
	switch t := v.(type) {
	case int:
		return int64(t), true, nil
	case int8:
		return int64(t), true, nil
	case int16:
		return int64(t), true, nil
	case int32:
		return int64(t), true, nil
	case int64:
		return t, true, nil
	case uint8:
		return int64(t), true, nil
	case uint16:
		return int64(t), true, nil
	case uint32:
		return int64(t), true, nil
	case uint:
		if uint64(t) > math.MaxInt64 {
			return 0, true, leafRangeError(colType, v)
		}
		return int64(t), true, nil
	case uint64:
		if t > math.MaxInt64 {
			return 0, true, leafRangeError(colType, v)
		}
		return int64(t), true, nil
	}
	return 0, false, nil
}

func floatToInt32Leaf(colType TypeID, f float64, box any) (int32, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) || f < math.MinInt32 || f > math.MaxInt32 {
		return 0, leafRangeError(colType, box)
	}
	if f != math.Trunc(f) {
		return 0, leafInexactError(colType, box)
	}
	return int32(f), nil
}

// maxInt64AsFloat is 2^63 — the first float64 ABOVE math.MaxInt64, since
// MaxInt64 itself has no float64. The bound is therefore half-open on the
// positive side and closed on the negative one, where -2^63 is exact.
const maxInt64AsFloat = 9223372036854775808.0

func floatToInt64Leaf(colType TypeID, f float64, box any) (int64, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) || f < -maxInt64AsFloat || f >= maxInt64AsFloat {
		return 0, leafRangeError(colType, box)
	}
	if f != math.Trunc(f) {
		return 0, leafInexactError(colType, box)
	}
	return int64(f), nil
}

// boolLeafValue, float32LeafValue, float64LeafValue, bytesLeafValue and
// vectorLeafValue are the rest of the same boundary. Their predecessors
// (toBool, toFloat32, toFloat64, toBytes) answered every box they did not name
// with a zero value — false, 0, or an EMPTY byte slice — and #885 measured
// four of those zeros against boxes ingest.checkType explicitly admits:
//
//	FLOAT32 leaf, int64(42)   stored 0     (toFloat32 named only float32/float64/int)
//	FLOAT64 leaf, int32(42)   stored 0     (toFloat64 named only float64/float32/int/int64)
//	INT32   leaf, int8(42)    stored 0
//	INT64   leaf, uint32(42)  stored 0
//
// ADR-0018's position, amended by this arc: the writer VALIDATES every box at
// the decomposition boundary — type, width, presence, range — and any
// violation is an error returned from WriteRows and latched into Close. An
// upstream check is not the guarantee; the exported writer is.
func boolLeafValue(colType TypeID, v any) (bool, error) {
	if b, ok := v.(bool); ok {
		return b, nil
	}
	return false, leafBoxError(colType, v)
}

func float32LeafValue(colType TypeID, v any) (float32, error) {
	f, err := float64LeafValue(colType, v)
	if err != nil {
		return 0, err
	}
	// PostgreSQL raises 22003 for `1e40::float4` ("value out of range:
	// overflow"), so a finite double with no float32 is refused rather than
	// stored as the ±Inf Go's conversion produces. A double that IS infinite
	// stays infinite: `'Infinity'::float4` is legal there.
	out := float32(f)
	if math.IsInf(float64(out), 0) && !math.IsInf(f, 0) {
		return 0, leafRangeError(colType, v)
	}
	return out, nil
}

func float64LeafValue(colType TypeID, v any) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case float32:
		return float64(t), nil
	}
	// Every integer box the boundaries admit widens to a double the way
	// PostgreSQL's int→float8 assignment does: legally, and lossily past 2^53
	// without an error, which is what PostgreSQL itself does.
	if n, isInt, err := integerBoxValue(colType, v); isInt {
		if err != nil {
			return 0, err
		}
		return float64(n), nil
	}
	return 0, leafBoxError(colType, v)
}

// bytesLeafValue resolves a BYTE_ARRAY leaf's box. A box it does not name used
// to become an EMPTY value — a zero-length string or an all-zero address — with
// no error.
func bytesLeafValue(col Column, v any) ([]byte, error) {
	var b []byte
	switch t := v.(type) {
	case []byte:
		b = t
	case net.IP:
		b = t
	case net.HardwareAddr:
		b = t
	case string:
		b = convertStringToBytes(t, col.Type)
		if b == nil {
			return nil, sqlerr.New("22P02", "%q is not a valid %s value", t, col.Type)
		}
	default:
		return nil, leafBoxError(col.Type, v)
	}
	// IPv6 and UUID are FIXED sixteen-byte values inside a variable-length
	// leaf, so a short one is not caught by the leaf's own framing the way a
	// FIXED_LEN_BYTE_ARRAY's would be — it just reads back as a different
	// address. (parseUUIDForWrite's comment already records how a ten-byte
	// value once got into a sixteen-byte column.)
	//
	// A ZERO-length entry is the exception and is kept: it is the legacy
	// absence form these columns carry in files this project has already
	// written (TestAnalyzeTableWithZeroLengthUUIDValues and
	// TestCompactTable_ZeroLengthUUIDValues are built from it), and refusing
	// it here would make a shape the reader must go on handling unwritable.
	if col.Type == TypeIPv6 || col.Type == TypeUUID {
		if len(b) != 16 && len(b) != 0 {
			return nil, sqlerr.New("22023",
				"column %q is %s, which is 16 bytes; the value is %d bytes",
				col.Name, col.Type, len(b))
		}
	}
	return b, nil
}

// vectorLeafValue resolves the FIXED_LEN_BYTE_ARRAY leaf, which for this
// writer is VECTOR(N) and nothing else — a DECIMAL wide enough to need an FLBA
// is resolved in decomposeLeaf and appended through appendDecimalEntry.
//
// The WIDTH is the whole point (#886). An FLBA leaf carries no per-value
// length: the column chunk is one run of bytes cut every TypeLength bytes on
// the way back. Appending a value of the wrong width therefore does not
// produce a short value, it MOVES THE BOUNDARY for every value after it — a
// VECTOR(2) fed [1] and then [2,3,4] read back as [1,2] and [3,4], with write,
// Close and read all returning nil, because the two errors cancelled in the
// byte total. No read-side length check can see that, which is why the width
// is enforced here, on the way in.
func vectorLeafValue(col Column, v any) ([]byte, error) {
	if col.Dimension <= 0 {
		return nil, sqlerr.New("22023",
			"column %q declares VECTOR with no dimension, which has no fixed width", col.Name)
	}
	switch t := v.(type) {
	case []float32:
		if len(t) != col.Dimension {
			return nil, vectorWidthError(col, len(t))
		}
		buf := make([]byte, len(t)*4)
		for i, f := range t {
			binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
		}
		return buf, nil
	case []float64:
		if len(t) != col.Dimension {
			return nil, vectorWidthError(col, len(t))
		}
		buf := make([]byte, len(t)*4)
		for i, f := range t {
			buf32, err := float32LeafValue(col.Type, f)
			if err != nil {
				return nil, err
			}
			binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(buf32))
		}
		return buf, nil
	case []byte:
		if len(t) != col.Dimension*4 {
			return nil, sqlerr.New("22023",
				"column %q is VECTOR(%d), which is %d bytes; the value is %d bytes",
				col.Name, col.Dimension, col.Dimension*4, len(t))
		}
		return t, nil
	}
	return nil, leafBoxError(col.Type, v)
}

func vectorWidthError(col Column, got int) error {
	return sqlerr.New("22023",
		"column %q is VECTOR(%d); the value has %d components",
		col.Name, col.Dimension, got)
}

// leafRangeError is PostgreSQL's numeric_value_out_of_range, the SQLSTATE it
// raises for the same assignment and the one batch.IntegerRangeError carries
// for the engine's own narrowing seam.
func leafRangeError(colType TypeID, v any) error {
	return sqlerr.New("22003", "%s value %v out of range", colType, v)
}

func leafInexactError(colType TypeID, v any) error {
	return sqlerr.New("22003", "%s value %v is not a whole number", colType, v)
}

// leafBoxError is PostgreSQL's datatype_mismatch: the value is not out of
// range, it is not a number this column can hold at all.
func leafBoxError(colType TypeID, v any) error {
	return sqlerr.New("42804", "%s column cannot store a %T value", colType, v)
}
