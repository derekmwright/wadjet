package expr

import (
	"unicode/utf8"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// Offsets-shape evaluation.
//
// A consumer of a variable-length column's SHAPE rather than its CONTENTS
// can answer off the offsets array with zero byte access: LENGTH() /
// octet_length() / bit_length(), IS NULL / IS NOT NULL, and = '' / <> ''
// comparisons. The generic paths all funnel through ColRef.Eval, which
// for a TypeString column calls Vector.GetString -> BytesColumn.StringValue
// -> string(bc.Value(i)) — a full copy of every value, discarded one line
// later. ClickBench Q28 (AVG(LENGTH(URL)) ... GROUP BY CounterID) copies
// ~9 GB of URL bytes to compute lengths that are offsets[i+1]-offsets[i].
//
// SEMANTICS NOTE (verified, deliberately preserved): our length() is a
// BYTE count, not PostgreSQL's character count. fnLength is
// float64(len(toString(v))) and vecLength was already an offsets
// subtraction. octet_length is therefore an exact alias, bit_length is
// 8x, and char_length/character_length keep the rune-counting
// implementation (fnCharLength) — those must decode bytes and get no
// offsets fast path.
//
// Every node here carries a generic fallback and takes it whenever the
// resolved column is not a flat byte-array column, so semantics stay
// bit-identical with the path it replaces.

// byteArrayShaped reports whether v's values are stored as a plain
// BytesColumn whose byte length equals the length of the value ColRef.Eval
// would produce. TypeString boxes BytesColumn.StringValue and TypeBytes
// boxes BytesColumn.Value, so both match. TypeIPv6/TypeCIDR/TypeUUID also
// use BytesData but ColRef.Eval renders them (net.IP.String(), etc.), so
// their offsets length is NOT the string length — excluded.
func byteArrayShaped(v *batch.Vector) bool {
	return v.Type == batch.TypeString || v.Type == batch.TypeBytes
}

// --- scalar (boxed) implementations ---

// fnOctetLength returns the number of bytes, the same count our length()
// has always returned.
func fnOctetLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return int32(len(toString(args[0])))
}

// fnBitLength returns the number of bits (8 x octet_length).
func fnBitLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return int32(8 * len(toString(args[0])))
}

// --- vectorized kernels ---

// vecOctetLength writes byte lengths straight off the offsets array.
func vecOctetLength(args []*batch.Vector, out *batch.Vector, n int) {
	vecShapeLenScaled(args, out, n, 1)
}

// vecBitLength writes bit lengths (8 x byte length) off the offsets array.
func vecBitLength(args []*batch.Vector, out *batch.Vector, n int) {
	vecShapeLenScaled(args, out, n, 8)
}

// vecShapeLenScaled is the shared offsets-only length kernel. mul is 1 for
// length/octet_length and 8 for bit_length.
//
// The length family declares int4 (RetInt32), matching PostgreSQL, so its
// projection allocates an Int32 output vector and this kernel writes
// Int32Data (#530). It used to declare and compute float8 — a JDBC/pgx client
// reading LENGTH as an Integer got a Double.
func vecShapeLenScaled(args []*batch.Vector, out *batch.Vector, n int, mul int) {
	src := args[0]
	if len(out.Int32Data) < n {
		// The output vector cannot hold what this kernel writes — the
		// planner declared a non-int32 type for the projection. Writing
		// anyway indexed off the end of a zero-length slice and panicked the
		// whole server process: `SELECT LENGTH(c) FROM t` killed every
		// connection, not just the one that asked. Degrade to a typed write
		// so the query answers instead of the server dying. FuncCall.EvalVec
		// now keeps a mismatched vector away from every kernel (the output
		// type comes from the registry's declared return type, #310); this
		// guard stays because ColShapeLen calls straight in.
		vecShapeLenAny(src, out, n, mul)
		return
	}
	if !byteArrayShaped(src) || len(src.BytesData.Offsets) <= n {
		// Non-byte-array input (or an offsets array too short to cover the
		// batch): fall back to the boxed per-row definition rather than
		// indexing off the end. length(int_col) reached this kernel with a
		// nil Offsets slice before.
		vecShapeLenBoxed(src, out, n, mul)
		return
	}
	hasNulls := src.Nulls.HasNulls()
	offs := src.BytesData.Offsets
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		l := 0
		if offs[i+1] >= offs[i] {
			l = int(offs[i+1] - offs[i])
		}
		out.Int32Data[i] = int32(mul * l)
	}
}

// vecShapeLenBoxed is the non-byte-array fallback: it reproduces
// fnOctetLength/fnBitLength over GetValue exactly.
func vecShapeLenBoxed(src *batch.Vector, out *batch.Vector, n int, mul int) {
	if len(out.Int32Data) < n {
		vecShapeLenAny(src, out, n, mul)
		return
	}
	for i := 0; i < n; i++ {
		v := src.GetValue(i)
		if v == nil {
			out.Nulls.SetNull(i)
			continue
		}
		out.Int32Data[i] = int32(mul * len(toString(v)))
	}
}

// vecShapeLenAny writes lengths through the output vector's own typed setter,
// for the case where that vector is not int32-backed. Slower than either
// fast path and never taken when the projection is typed correctly — it exists
// so a type mismatch is a slow answer rather than a process-wide panic.
func vecShapeLenAny(src *batch.Vector, out *batch.Vector, n int, mul int) {
	for i := 0; i < n; i++ {
		v := src.GetValue(i)
		if v == nil {
			out.Nulls.SetNull(i)
			continue
		}
		out.SetValue(i, int32(mul*len(toString(v))))
	}
}

// vecCharLength counts runes. This is the one member of the length family
// that genuinely needs the bytes — no offsets fast path exists for it.
func vecCharLength(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	if len(out.Int32Data) < n {
		// Non-int32 output vector: see vecShapeLenScaled.
		for i := 0; i < n; i++ {
			v := src.GetValue(i)
			if v == nil {
				out.Nulls.SetNull(i)
				continue
			}
			out.SetValue(i, int32(utf8.RuneCountInString(toString(v))))
		}
		return
	}
	if !byteArrayShaped(src) || len(src.BytesData.Offsets) <= n {
		for i := 0; i < n; i++ {
			v := src.GetValue(i)
			if v == nil {
				out.Nulls.SetNull(i)
				continue
			}
			out.Int32Data[i] = int32(utf8.RuneCountInString(toString(v)))
		}
		return
	}
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Int32Data[i] = int32(utf8.RuneCount(src.BytesData.Value(i)))
	}
}

// --- typed nodes (per-row paths) ---

// ColShapeLen evaluates length()/octet_length()/bit_length() over a bare
// column reference by subtracting offsets, never materializing the value.
// Any column whose stored bytes are not the value ColRef.Eval would box
// (numeric, temporal, network-rendered, ROW field access) delegates to the
// generic FuncCall it replaced, so results are unchanged.
type ColShapeLen struct {
	Col      *ColRef
	Mul      int // 1 for length/octet_length, 8 for bit_length
	Fallback *FuncCall
}

func (e *ColShapeLen) Eval(b *batch.RecordBatch, row int) any {
	v, ok := e.EvalFloat64(b, row)
	if !ok {
		return nil
	}
	// int4, matching fnLength/fnOctetLength/fnBitLength and PostgreSQL (#530).
	// The boxed value must carry the same Go type as the generic FuncCall it
	// replaces, or a consumer that type-asserts it disagrees by path.
	return int32(v)
}

func (e *ColShapeLen) EvalFloat64(b *batch.RecordBatch, row int) (float64, bool) {
	if v, ok := e.fastCol(b); ok {
		if v.Nulls.IsNullFast(row) {
			return 0, false
		}
		return float64(e.Mul * v.BytesData.LengthAt(row)), true
	}
	g := e.Fallback.Eval(b, row)
	if g == nil {
		return 0, false
	}
	return ToFloat64(g), true
}

func (e *ColShapeLen) EvalInt64(b *batch.RecordBatch, row int) (int64, bool) {
	f, ok := e.EvalFloat64(b, row)
	if !ok {
		return 0, false
	}
	return int64(f), true
}

// EvalVec fills out for the whole batch. Mirrors FuncCall.EvalVec's
// contract: writes Int32Data (the length family is int4, #530), marking
// nulls in out.Nulls.
func (e *ColShapeLen) EvalVec(b *batch.RecordBatch, out *batch.Vector, n int) {
	v, ok := e.fastCol(b)
	if !ok || out.Int32Data == nil {
		e.Fallback.EvalVec(b, out, n)
		return
	}
	vecShapeLenScaled([]*batch.Vector{v}, out, n, e.Mul)
}

// EvalFloat64Vec fills dst for rows [0, n), reporting whether any row was
// null (the VecFloat64Expr contract: the caller re-runs EvalFloat64 per row
// to set the null bits when this returns true).
func (e *ColShapeLen) EvalFloat64Vec(b *batch.RecordBatch, dst []float64, n int) bool {
	v, ok := e.fastCol(b)
	if !ok || len(dst) < n {
		anyNull := false
		for i := 0; i < n; i++ {
			f, valid := e.EvalFloat64(b, i)
			if !valid {
				anyNull = true
				f = 0
			}
			if i < len(dst) {
				dst[i] = f
			}
		}
		return anyNull
	}
	offs := v.BytesData.Offsets
	hasNulls := v.Nulls.HasNulls()
	anyNull := false
	for i := 0; i < n; i++ {
		if hasNulls && v.Nulls.IsNullFast(i) {
			dst[i] = 0
			anyNull = true
			continue
		}
		l := 0
		if offs[i+1] >= offs[i] {
			l = int(offs[i+1] - offs[i])
		}
		dst[i] = float64(e.Mul * l)
	}
	return anyNull
}

// fastCol resolves the column and reports whether the offsets fast path is
// valid for it.
func (e *ColShapeLen) fastCol(b *batch.RecordBatch) (*batch.Vector, bool) {
	e.Col.resolve(b)
	if e.Col.idx < 0 || e.Col.idx >= len(b.Columns) || e.Col.structField != "" {
		return nil, false
	}
	v := b.Columns[e.Col.idx]
	if !byteArrayShaped(v) || len(v.BytesData.Offsets) <= b.Len {
		return nil, false
	}
	return v, true
}

// ColEmptyStr evaluates a column compared for equality or inequality
// against the empty string literal as a zero-length offsets test.
// Restricted to TypeString: that is the only type for which the
// generic Cmp path compares ColRef.Eval's boxed string against "" (a
// TypeBytes column boxes []byte, which compare() handles through a
// different branch, and the network/UUID types render their bytes).
//
// NULL handling matches Cmp exactly: a NULL operand makes the comparison
// UNKNOWN for BOTH = and <> — nil on the boxed path, excluded by EvalBool.
type ColEmptyStr struct {
	Col      *ColRef
	Not      bool // true for <>
	Fallback *Cmp
}

func (e *ColEmptyStr) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *ColEmptyStr) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

func (e *ColEmptyStr) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	e.Col.resolve(b)
	if e.Col.idx < 0 || e.Col.idx >= len(b.Columns) || e.Col.structField != "" {
		return e.Fallback.EvalBoolNull(b, row)
	}
	v := b.Columns[e.Col.idx]
	if v.Type != batch.TypeString || len(v.BytesData.Offsets) <= b.Len {
		return e.Fallback.EvalBoolNull(b, row)
	}
	if v.Nulls.IsNullFast(row) {
		return false, true
	}
	empty := v.BytesData.LengthAt(row) == 0
	if e.Not {
		return !empty, false
	}
	return empty, false
}

// ColIsNull evaluates `col IS [NOT] NULL` off the null bitmap for a
// byte-array column, where the generic IsNull node boxes (and for
// TypeString copies) the value only to test it against nil.
//
// Scoped to TypeString/TypeBytes deliberately: ColRef.Eval returns nil for
// exactly the null rows of those two types (TypeString via GetString's
// ok flag, TypeBytes via GetValue's leading null check), so the rewrite is
// value-identical. Other types keep the generic node.
type ColIsNull struct {
	Col      *ColRef
	Not      bool
	Fallback *IsNull
}

func (e *ColIsNull) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *ColIsNull) EvalBool(b *batch.RecordBatch, row int) bool {
	e.Col.resolve(b)
	if e.Col.idx < 0 || e.Col.idx >= len(b.Columns) || e.Col.structField != "" {
		return e.Fallback.EvalBool(b, row)
	}
	v := b.Columns[e.Col.idx]
	if !byteArrayShaped(v) {
		return e.Fallback.EvalBool(b, row)
	}
	isNull := v.Nulls.IsNullFast(row)
	if e.Not {
		return !isNull
	}
	return isNull
}

// EvalBoolNull: IS [NOT] NULL never answers UNKNOWN.
func (e *ColIsNull) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	return e.EvalBool(b, row), false
}
