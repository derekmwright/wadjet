package kernel

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// CompareOp represents a comparison operation.
type CompareOp int

const (
	OpEq CompareOp = iota
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
)

// --- Generic typed comparison ---

func resolveCompare[T Ordered](op CompareOp) func(a, b T) bool {
	switch op {
	case OpEq:
		return func(a, b T) bool { return a == b }
	case OpNe:
		return func(a, b T) bool { return a != b }
	case OpLt:
		return func(a, b T) bool { return a < b }
	case OpLe:
		return func(a, b T) bool { return a <= b }
	case OpGt:
		return func(a, b T) bool { return a > b }
	case OpGe:
		return func(a, b T) bool { return a >= b }
	default:
		return func(a, b T) bool { return false }
	}
}

// compareFilterImpl creates a FilterKernel that compares typed column data
// against a pre-resolved constant. The generic is monomorphized at compile time.
func compareFilterImpl[T Ordered](getData func(v *batch.Vector) []T, val T, op CompareOp) FilterKernel {
	cmpFn := resolveCompare[T](op)
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		data := getData(vec)
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		if sel != nil {
			if hasNulls {
				for _, idx := range sel {
					if !vec.Nulls.IsNullFast(int(idx)) && cmpFn(data[idx], val) {
						out = append(out, idx)
					}
				}
			} else {
				for _, idx := range sel {
					if cmpFn(data[idx], val) {
						out = append(out, idx)
					}
				}
			}
		} else {
			if hasNulls {
				for i := 0; i < vecLen; i++ {
					if !vec.Nulls.IsNullFast(i) && cmpFn(data[i], val) {
						out = append(out, uint32(i))
					}
				}
			} else {
				for i := 0; i < vecLen; i++ {
					if cmpFn(data[i], val) {
						out = append(out, uint32(i))
					}
				}
			}
		}
		return out
	}
}

// Data accessor functions for each vector type.
func getInt64Data(v *batch.Vector) []int64     { return v.Int64Data }
func getInt32Data(v *batch.Vector) []int32     { return v.Int32Data }
func getFloat64Data(v *batch.Vector) []float64 { return v.Float64Data }
func getFloat32Data(v *batch.Vector) []float32 { return v.Float32Data }

// compareFilterBool creates a FilterKernel for boolean columns.
// Only Eq and Ne are meaningful for booleans.
func compareFilterBool(op CompareOp, val bool) FilterKernel {
	wantTrue := (op == OpEq && val) || (op == OpNe && !val)
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		data := vec.BoolData
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		if sel != nil {
			if hasNulls {
				for _, idx := range sel {
					if !vec.Nulls.IsNullFast(int(idx)) && data[idx] == wantTrue {
						out = append(out, idx)
					}
				}
			} else {
				for _, idx := range sel {
					if data[idx] == wantTrue {
						out = append(out, idx)
					}
				}
			}
		} else {
			if hasNulls {
				for i := 0; i < vecLen; i++ {
					if !vec.Nulls.IsNullFast(i) && data[i] == wantTrue {
						out = append(out, uint32(i))
					}
				}
			} else {
				for i := 0; i < vecLen; i++ {
					if data[i] == wantTrue {
						out = append(out, uint32(i))
					}
				}
			}
		}
		return out
	}
}

func toBool(v any) bool {
	switch tv := v.(type) {
	case bool:
		return tv
	default:
		return false
	}
}

// ResolveFilterKernel creates a FilterKernel for comparing a column of the
// given type against a constant value. The type dispatch happens once here;
// the returned function has no type switches in its inner loop.
func ResolveFilterKernel(typ batch.TypeID, op CompareOp, value any) FilterKernel {
	if value == nil {
		// A nil constant is NOT the type's zero. Every coercion below reads
		// it as one — toInt64(nil) is 0, toString(nil) is "", toBool(nil) is
		// false — so `WHERE c_i64 = NULL` answered the rows holding 0 and
		// `WHERE c_str = NULL` the rows holding "" (#450). A comparison
		// against NULL is UNKNOWN for every row and a WHERE admits only
		// TRUE, so the kernel selects nothing.
		//
		// The planner does not send one here any more (it lowers the shape to
		// exec.MatchNothingFilter); this is the guard that makes "no caller
		// passes nil" a local fact rather than an invariant spread across
		// every entry point that can bind a constant.
		return matchNothingKernel
	}
	switch typ {
	case batch.TypeBool:
		return compareFilterBool(op, toBool(value))
	case batch.TypeInt64, batch.TypeTimestamp:
		return compareFilterImpl(getInt64Data, toInt64(value), op)
	case batch.TypeInt32:
		return compareFilterImpl(getInt32Data, int32(toInt64(value)), op)
	case batch.TypeFloat64:
		return compareFilterImpl(getFloat64Data, toFloat64(value), op)
	case batch.TypeFloat32:
		return compareFilterImpl(getFloat32Data, float32(toFloat64(value)), op)
	case batch.TypeString:
		return compareFilterString(op, toString(value))
	case batch.TypeIPv4:
		return compareFilterImpl(getInt64Data, parseIPv4ToInt64(toString(value)), op)
	case batch.TypeMAC:
		return compareFilterImpl(getInt64Data, parseMACToInt64(toString(value)), op)
	case batch.TypeIPv6:
		return compareFilterString(op, parseIPv6ToRawString(toString(value)))
	case batch.TypeCIDR:
		return compareFilterString(op, toString(value))
	case batch.TypePort, batch.TypeProtocol:
		return compareFilterImpl(getInt32Data, int32(toInt64(value)), op)
	case batch.TypeDuration:
		return compareFilterImpl(getInt64Data, toInt64(value), op)
	case batch.TypeUUID:
		return compareFilterString(op, parseUUIDToRawString(toString(value)))
	case batch.TypeDate:
		return compareFilterImpl(getInt32Data, toDateInt32(value), op)
	case batch.TypeBytes:
		// BYTES compares by bytes, which is what the string kernel does —
		// it reads the same arena. The constant arrives as a string from a
		// SQL literal and as []byte from a parameter.
		return compareFilterString(op, toBytesString(value))
	case batch.TypeDecimal:
		return compareFilterDecimal(op, value)
	default:
		return nil
	}
}

// matchNothingKernel selects no rows. See ResolveFilterKernel's nil guard.
func matchNothingKernel(_ *batch.Vector, _ []uint32, _ int, outSel []uint32) []uint32 {
	return outSel[:0]
}

// toBytesString renders a BYTES filter constant as the raw byte string the
// column stores.
func toBytesString(v any) string {
	switch tv := v.(type) {
	case string:
		return tv
	case []byte:
		return string(tv)
	default:
		return ""
	}
}

// compareFilterDecimal compares a DECIMAL column against a constant.
//
// Without this arm ResolveFilterKernel returned nil for DECIMAL, and every
// caller read that as "the column does not exist": `WHERE dec_col <> 5.0005`
// failed the query with `filter column "c_dec" does not exist in the input
// schema` whenever the predicate reached the operator-level filter instead of
// the scan (#401).
//
// The comparison is EXACT, per ADR-0012 — PostgreSQL compares numeric against
// a numeric literal at full precision. The column's values live at its own
// scale, so the constant is truncated to that scale and the truncation's
// RESIDUAL is carried: with a DECIMAL(18,4) column, `> 2499.5074494849528`
// must still exclude the row holding exactly 2499.5074, which comparing
// against the truncated constant alone would admit.
//
// The literal is resolved per CALL, not per row and not once at resolve time:
// the vector carries the scale, and a kernel resolved from one batch can be
// handed the next one.
func compareFilterDecimal(op CompareOp, value any) FilterKernel {
	text, ok := DecimalConstText(value)
	if !ok {
		// Not a number: there is no comparison to make. The caller turns a
		// nil kernel into the query error PostgreSQL raises (#463).
		return nil
	}
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		lit := decimalLiteralAt(text, vec.DecimalData.Scale)
		data := vec.DecimalData.Data
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		keep := func(i int) bool {
			return applyCompareOp(lit.Order(data[i]), op)
		}
		if sel != nil {
			for _, idx := range sel {
				if hasNulls && vec.Nulls.IsNullFast(int(idx)) {
					continue
				}
				if keep(int(idx)) {
					out = append(out, idx)
				}
			}
			return out
		}
		for i := 0; i < vecLen; i++ {
			if hasNulls && vec.Nulls.IsNullFast(i) {
				continue
			}
			if keep(i) {
				out = append(out, uint32(i))
			}
		}
		return out
	}
}

// compareInt128 is defined in sort.go (same package) — both the sort kernels
// and the DECIMAL filter/equality kernels below compare on the same Int128
// representation and share the one function.
//
// The DECIMAL-against-constant order is batch.ScaledDecimal.Order: the
// constant resolved into the column's domain, ordered against a stored cell,
// residual and out-of-range saturation included.

func applyCompareOp(c int, op CompareOp) bool {
	switch op {
	case OpEq:
		return c == 0
	case OpNe:
		return c != 0
	case OpLt:
		return c < 0
	case OpLe:
		return c <= 0
	case OpGt:
		return c > 0
	case OpGe:
		return c >= 0
	}
	return false
}

// DecimalConstText renders a comparison constant as the decimal text a
// DECIMAL column's domain is reached through, and reports whether the
// constant IS a number.
//
// The second result is not decoration. A constant nobody can read used to
// resolve to the value ZERO and match every stored zero (#463) — the worst
// shape a failure can take, because it neither errors nor returns nothing.
// PostgreSQL refuses the query instead ("invalid input syntax for type
// numeric"), and ADR-0012 makes PostgreSQL the authority on error-versus-not,
// so a false here is a query error at the caller, not a value.
//
// Exponent form is passed through untouched: batch.DecimalTextAt folds the
// exponent into the scaling exactly, where expanding it through a float64
// first is what lost 1e400 entirely.
func DecimalConstText(v any) (string, bool) {
	switch tv := v.(type) {
	case string:
		return tv, isDecimalText(tv)
	case []byte:
		return string(tv), isDecimalText(string(tv))
	case float64:
		// NaN and the infinities have no numeric text and no place in a
		// DECIMAL's order; they are not values this column type holds.
		if math.IsNaN(tv) || math.IsInf(tv, 0) {
			return "", false
		}
		return strconv.FormatFloat(tv, 'f', -1, 64), true
	case float32:
		f := float64(tv)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return "", false
		}
		return strconv.FormatFloat(f, 'f', -1, 32), true
	case int64:
		return strconv.FormatInt(tv, 10), true
	case int32:
		return strconv.FormatInt(int64(tv), 10), true
	case int:
		return strconv.FormatInt(int64(tv), 10), true
	default:
		return "", false
	}
}

// isDecimalText reports whether text names a number. Numeric shape does not
// depend on the scale it will later be resolved at, so scale 0 answers it.
func isDecimalText(s string) bool {
	_, ok := batch.DecimalTextAt(s, 0)
	return ok
}

// decimalLiteralText is DecimalConstText for the callers that have already
// established the constant is a number.
func decimalLiteralText(v any) string {
	text, _ := DecimalConstText(v)
	return text
}

// decimalLiteralAt resolves decimal text into a column's domain at `scale`:
// the unscaled value, the residual of the digits the scale could not hold,
// and the saturation of a magnitude wider than Int128.
func decimalLiteralAt(text string, scale int) batch.ScaledDecimal {
	d, _ := batch.DecimalTextAt(text, scale)
	return d
}

// compareFilterString handles string column comparison.
// Uses UnsafeStringValue for zero-copy comparisons — the string is consumed
// within the comparison function and never stored.
func compareFilterString(op CompareOp, val string) FilterKernel {
	// Offsets-shape: comparing against the empty string is a zero-length
	// test on the offsets array, not a byte compare. This is the dominant
	// string filter in the ClickBench corpus (16 of 43 queries carry a
	// `col <> ''` conjunct) and it is what makes a column filtered that way
	// still eligible for the lengths-only scan decode — the kernel never
	// touches the value arena.
	if val == "" && (op == OpEq || op == OpNe) {
		return emptyStringFilter(op == OpNe)
	}
	cmpFn := resolveCompare[string](op)
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		if sel != nil {
			if hasNulls {
				for _, idx := range sel {
					if !vec.Nulls.IsNullFast(int(idx)) && cmpFn(vec.BytesData.UnsafeStringValue(int(idx)), val) {
						out = append(out, idx)
					}
				}
			} else {
				for _, idx := range sel {
					if cmpFn(vec.BytesData.UnsafeStringValue(int(idx)), val) {
						out = append(out, idx)
					}
				}
			}
		} else {
			if hasNulls {
				for i := 0; i < vecLen; i++ {
					if !vec.Nulls.IsNullFast(i) && cmpFn(vec.BytesData.UnsafeStringValue(i), val) {
						out = append(out, uint32(i))
					}
				}
			} else {
				for i := 0; i < vecLen; i++ {
					if cmpFn(vec.BytesData.UnsafeStringValue(i), val) {
						out = append(out, uint32(i))
					}
				}
			}
		}
		return out
	}
}

// emptyStringFilter selects rows whose value is (nonEmpty=false) or is not
// (nonEmpty=true) the empty string, reading only offsets and the null mask.
// NULL rows never pass, matching the byte-compare kernel it replaces.
func emptyStringFilter(nonEmpty bool) FilterKernel {
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		bd := &vec.BytesData
		if sel != nil {
			for _, idx := range sel {
				i := int(idx)
				if hasNulls && vec.Nulls.IsNullFast(i) {
					continue
				}
				if (bd.LengthAt(i) != 0) == nonEmpty {
					out = append(out, idx)
				}
			}
			return out
		}
		for i := 0; i < vecLen; i++ {
			if hasNulls && vec.Nulls.IsNullFast(i) {
				continue
			}
			if (bd.LengthAt(i) != 0) == nonEmpty {
				out = append(out, uint32(i))
			}
		}
		return out
	}
}

// --- IN filter kernels ---

// ResolveInFilterKernel creates a FilterKernel that checks set membership.
// The set is built once; the inner loop does a hash lookup per element.
func ResolveInFilterKernel(typ batch.TypeID, values []any, negate bool) FilterKernel {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp:
		set := make(map[int64]struct{}, len(values))
		for _, v := range values {
			set[toInt64(v)] = struct{}{}
		}
		return inFilterInt64(getInt64Data, set, negate)
	case batch.TypeDate:
		set := make(map[int32]struct{}, len(values))
		for _, v := range values {
			set[toDateInt32(v)] = struct{}{}
		}
		return inFilterInt32(getInt32Data, set, negate)
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol:
		set := make(map[int32]struct{}, len(values))
		for _, v := range values {
			set[int32(toInt64(v))] = struct{}{}
		}
		return inFilterInt32(getInt32Data, set, negate)
	case batch.TypeFloat64:
		set := make(map[float64]struct{}, len(values))
		for _, v := range values {
			set[toFloat64(v)] = struct{}{}
		}
		return inFilterFloat64(set, negate)
	case batch.TypeString, batch.TypeCIDR:
		set := make(map[string]struct{}, len(values))
		for _, v := range values {
			set[toString(v)] = struct{}{}
		}
		return inFilterString(set, negate)
	case batch.TypeUUID:
		// UUID stores 16 RAW bytes, not the 36-character text.
		set := make(map[string]struct{}, len(values))
		for _, v := range values {
			set[parseUUIDToRawString(toString(v))] = struct{}{}
		}
		return inFilterString(set, negate)
	case batch.TypeIPv6:
		// IPv6 stores the 16 RAW bytes; the literal is text. The scalar
		// comparison kernel already parses it (parseIPv6ToRawString) and the
		// IN path did not, so `WHERE ipv6_col IN ('2001:db8::1')` could not
		// have matched even once the type had an arm.
		set := make(map[string]struct{}, len(values))
		for _, v := range values {
			set[parseIPv6ToRawString(toString(v))] = struct{}{}
		}
		return inFilterString(set, negate)
	case batch.TypeBytes:
		set := make(map[string]struct{}, len(values))
		for _, v := range values {
			set[toBytesString(v)] = struct{}{}
		}
		return inFilterString(set, negate)
	case batch.TypeIPv4:
		set := make(map[int64]struct{}, len(values))
		for _, v := range values {
			set[parseIPv4ToInt64(toString(v))] = struct{}{}
		}
		return inFilterInt64(getInt64Data, set, negate)
	case batch.TypeMAC:
		set := make(map[int64]struct{}, len(values))
		for _, v := range values {
			set[parseMACToInt64(toString(v))] = struct{}{}
		}
		return inFilterInt64(getInt64Data, set, negate)
	case batch.TypeDuration:
		set := make(map[int64]struct{}, len(values))
		for _, v := range values {
			set[toInt64(v)] = struct{}{}
		}
		return inFilterInt64(getInt64Data, set, negate)
	case batch.TypeFloat32:
		// The probe widens to float64, which is exact for every float32, so
		// the set is built the same way and 1.5 in the list matches 1.5 in
		// the column.
		set := make(map[float64]struct{}, len(values))
		for _, v := range values {
			set[toFloat64(v)] = struct{}{}
		}
		return inFilterFloat32(set, negate)
	case batch.TypeBool:
		return inFilterBool(values, negate)
	case batch.TypeDecimal:
		return inFilterDecimal(values, negate)
	default:
		return nil
	}
}

// inFilterFloat32 probes a FLOAT32 column against a float64 set.
func inFilterFloat32(set map[float64]struct{}, negate bool) FilterKernel {
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		keep := func(i int) bool {
			_, found := set[float64(vec.Float32Data[i])]
			return found != negate
		}
		if sel != nil {
			for _, idx := range sel {
				if hasNulls && vec.Nulls.IsNullFast(int(idx)) {
					continue
				}
				if keep(int(idx)) {
					out = append(out, idx)
				}
			}
			return out
		}
		for i := 0; i < vecLen; i++ {
			if hasNulls && vec.Nulls.IsNullFast(i) {
				continue
			}
			if keep(i) {
				out = append(out, uint32(i))
			}
		}
		return out
	}
}

// inFilterBool collapses the set to at most two members before the loop.
func inFilterBool(values []any, negate bool) FilterKernel {
	var wantTrue, wantFalse bool
	for _, v := range values {
		if toBool(v) {
			wantTrue = true
		} else {
			wantFalse = true
		}
	}
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		keep := func(i int) bool {
			found := wantTrue
			if !vec.BoolData[i] {
				found = wantFalse
			}
			return found != negate
		}
		if sel != nil {
			for _, idx := range sel {
				if hasNulls && vec.Nulls.IsNullFast(int(idx)) {
					continue
				}
				if keep(int(idx)) {
					out = append(out, idx)
				}
			}
			return out
		}
		for i := 0; i < vecLen; i++ {
			if hasNulls && vec.Nulls.IsNullFast(i) {
				continue
			}
			if keep(i) {
				out = append(out, uint32(i))
			}
		}
		return out
	}
}

// inFilterDecimal probes a DECIMAL column against the literals scaled to the
// column's own scale, with the truncation residual carried the same way
// compareFilterDecimal carries it: a literal that does not fit the scale
// exactly cannot equal any stored value, so it drops out of the set.
func inFilterDecimal(values []any, negate bool) FilterKernel {
	texts := make([]string, 0, len(values))
	for _, v := range values {
		text, ok := DecimalConstText(v)
		if !ok {
			return nil // see compareFilterDecimal: a query error, not a value
		}
		texts = append(texts, text)
	}
	// The set is a pure function of the column's SCALE, and a column's scale
	// does not change between batches, so it is built once rather than
	// re-parsed per batch. Keyed by scale, not cached unconditionally:
	// nothing here promises one kernel only ever sees one column.
	//
	// Unsynchronized deliberately. ResolveInFilterKernel is called from
	// InFilter.Execute, and InFilter.Clone builds a FRESH InFilter that
	// resolves its own kernel, so no two workers share this closure —
	// unlike ColumnCompare's predicate, which Filter.Clone does share and
	// which is why that one carries no mutable state at all.
	var memoSet map[batch.Int128]struct{}
	memoScale := -1
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		scale := vec.DecimalData.Scale
		set := memoSet
		if set == nil || memoScale != scale {
			set = make(map[batch.Int128]struct{}, len(texts))
			for _, t := range texts {
				lit := decimalLiteralAt(t, scale)
				if lit.Residual != 0 || lit.Sat != 0 {
					// Not representable at this scale, or outside the
					// carrier's range entirely: equals nothing.
					continue
				}
				set[lit.Unscaled] = struct{}{}
			}
			memoSet, memoScale = set, scale
		}
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		keep := func(i int) bool {
			_, found := set[vec.DecimalData.Data[i]]
			return found != negate
		}
		if sel != nil {
			for _, idx := range sel {
				if hasNulls && vec.Nulls.IsNullFast(int(idx)) {
					continue
				}
				if keep(int(idx)) {
					out = append(out, idx)
				}
			}
			return out
		}
		for i := 0; i < vecLen; i++ {
			if hasNulls && vec.Nulls.IsNullFast(i) {
				continue
			}
			if keep(i) {
				out = append(out, uint32(i))
			}
		}
		return out
	}
}

func inFilterInt64(getData func(v *batch.Vector) []int64, set map[int64]struct{}, negate bool) FilterKernel {
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		data := getData(vec)
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		if sel != nil {
			if hasNulls {
				for _, idx := range sel {
					if !vec.Nulls.IsNullFast(int(idx)) {
						_, found := set[data[idx]]
						if found != negate {
							out = append(out, idx)
						}
					}
				}
			} else {
				for _, idx := range sel {
					_, found := set[data[idx]]
					if found != negate {
						out = append(out, idx)
					}
				}
			}
		} else {
			if hasNulls {
				for i := 0; i < vecLen; i++ {
					if !vec.Nulls.IsNullFast(i) {
						_, found := set[data[i]]
						if found != negate {
							out = append(out, uint32(i))
						}
					}
				}
			} else {
				for i := 0; i < vecLen; i++ {
					_, found := set[data[i]]
					if found != negate {
						out = append(out, uint32(i))
					}
				}
			}
		}
		return out
	}
}

func inFilterInt32(getData func(v *batch.Vector) []int32, set map[int32]struct{}, negate bool) FilterKernel {
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		data := getData(vec)
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		if sel != nil {
			if hasNulls {
				for _, idx := range sel {
					if !vec.Nulls.IsNullFast(int(idx)) {
						_, found := set[data[idx]]
						if found != negate {
							out = append(out, idx)
						}
					}
				}
			} else {
				for _, idx := range sel {
					_, found := set[data[idx]]
					if found != negate {
						out = append(out, idx)
					}
				}
			}
		} else {
			if hasNulls {
				for i := 0; i < vecLen; i++ {
					if !vec.Nulls.IsNullFast(i) {
						_, found := set[data[i]]
						if found != negate {
							out = append(out, uint32(i))
						}
					}
				}
			} else {
				for i := 0; i < vecLen; i++ {
					_, found := set[data[i]]
					if found != negate {
						out = append(out, uint32(i))
					}
				}
			}
		}
		return out
	}
}

func inFilterFloat64(set map[float64]struct{}, negate bool) FilterKernel {
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		data := vec.Float64Data
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		if sel != nil {
			for _, idx := range sel {
				if hasNulls && vec.Nulls.IsNullFast(int(idx)) {
					continue
				}
				_, found := set[data[idx]]
				if found != negate {
					out = append(out, idx)
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				if hasNulls && vec.Nulls.IsNullFast(i) {
					continue
				}
				_, found := set[data[i]]
				if found != negate {
					out = append(out, uint32(i))
				}
			}
		}
		return out
	}
}

func inFilterString(set map[string]struct{}, negate bool) FilterKernel {
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		if sel != nil {
			for _, idx := range sel {
				if hasNulls && vec.Nulls.IsNullFast(int(idx)) {
					continue
				}
				_, found := set[vec.BytesData.UnsafeStringValue(int(idx))]
				if found != negate {
					out = append(out, idx)
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				if hasNulls && vec.Nulls.IsNullFast(i) {
					continue
				}
				_, found := set[vec.BytesData.UnsafeStringValue(i)]
				if found != negate {
					out = append(out, uint32(i))
				}
			}
		}
		return out
	}
}

// --- LIKE filter kernel ---

// ResolveLikeFilterKernel creates a FilterKernel for SQL LIKE pattern matching.
// Converts SQL LIKE patterns (% and _) to optimized matching functions.
func ResolveLikeFilterKernel(pattern string, negate bool) FilterKernel {
	matcher := compileLikePattern(pattern)
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		if sel != nil {
			for _, idx := range sel {
				if hasNulls && vec.Nulls.IsNullFast(int(idx)) {
					continue
				}
				if matcher(vec.BytesData.UnsafeStringValue(int(idx))) != negate {
					out = append(out, idx)
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				if hasNulls && vec.Nulls.IsNullFast(i) {
					continue
				}
				if matcher(vec.BytesData.UnsafeStringValue(i)) != negate {
					out = append(out, uint32(i))
				}
			}
		}
		return out
	}
}

// compileLikePattern converts a SQL LIKE pattern to an optimized match function.
// Handles common cases with specialized string operations instead of regex.
func compileLikePattern(pattern string) func(string) bool {
	// Optimize common patterns:
	// '%suffix' → strings.HasSuffix
	// 'prefix%' → strings.HasPrefix
	// '%contains%' → strings.Contains
	// 'prefix%suffix' → HasPrefix + HasSuffix + len check
	hasLeadingWild := len(pattern) > 0 && pattern[0] == '%'
	hasTrailingWild := len(pattern) > 0 && pattern[len(pattern)-1] == '%'

	// Check if the pattern has any special chars beyond leading/trailing %
	inner := pattern
	if hasLeadingWild {
		inner = inner[1:]
	}
	if hasTrailingWild && len(inner) > 0 {
		inner = inner[:len(inner)-1]
	}

	// If inner has no wildcards, use fast string operations
	hasInnerWild := false
	for _, c := range inner {
		if c == '%' || c == '_' {
			hasInnerWild = true
			break
		}
	}

	if !hasInnerWild {
		switch {
		case hasLeadingWild && hasTrailingWild:
			// %contains%
			return func(s string) bool {
				return len(s) >= len(inner) && containsStr(s, inner)
			}
		case hasLeadingWild:
			// %suffix
			return func(s string) bool {
				return len(s) >= len(inner) && s[len(s)-len(inner):] == inner
			}
		case hasTrailingWild:
			// prefix%
			return func(s string) bool {
				return len(s) >= len(inner) && s[:len(inner)] == inner
			}
		default:
			// exact match
			return func(s string) bool { return s == inner }
		}
	}

	// Fallback: recursive matching for complex patterns
	return func(s string) bool { return matchLike(s, pattern) }
}

// containsStr is a simple string contains check (avoids strings import in kernel).
// containsStr delegates to strings.Contains: the stdlib uses the SIMD
// bytealg index (AVX2 on amd64) plus Rabin-Karp fallback. The previous
// hand-rolled positional scan called memeq at every offset — 27% flat of
// ClickBench Q23 (LIKE '%Google%' over 100M titles/URLs).
func containsStr(s, substr string) bool {
	return strings.Contains(s, substr)
}

// matchLike implements recursive SQL LIKE pattern matching.
func matchLike(s, pattern string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '%':
			// Skip consecutive %
			for len(pattern) > 0 && pattern[0] == '%' {
				pattern = pattern[1:]
			}
			if len(pattern) == 0 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if matchLike(s[i:], pattern) {
					return true
				}
			}
			return false
		case '_':
			if len(s) == 0 {
				return false
			}
			s = s[1:]
			pattern = pattern[1:]
		default:
			if len(s) == 0 || s[0] != pattern[0] {
				return false
			}
			s = s[1:]
			pattern = pattern[1:]
		}
	}
	return len(s) == 0
}

// --- Type conversion helpers ---

func toInt64(v any) int64 {
	switch tv := v.(type) {
	case int64:
		return tv
	case int:
		return int64(tv)
	case int32:
		return int64(tv)
	case float64:
		return int64(tv)
	case string:
		return parseTimestampString(tv)
	default:
		return 0
	}
}

// toDateInt32 converts a value to days-since-epoch for DATE column comparison.
// DATE columns store int32 days since 1970-01-01, not milliseconds.
func toDateInt32(v any) int32 {
	switch tv := v.(type) {
	case int32:
		return tv
	case int64:
		return int32(tv)
	case int:
		return int32(tv)
	case string:
		return parseDateToDays(tv)
	default:
		return 0
	}
}

// parseDateToDays converts a date string to days since 1970-01-01.
//
// KNOWN CORRECTNESS LIMIT (#451): dates outside 1677-09-22 .. 2262-04-11 are
// SATURATED, not refused. t.Sub returns a time.Duration — int64 NANOSECONDS —
// and its contract is to return the maximum (minimum) Duration rather than
// report an overflow, and ±math.MaxInt64 ns is ±106751.99 days, so every date
// past the upper edge answers 106751 (2262-04-11) and every date past the
// lower edge answers -106751 (1677-09-22). The int32 return is not the
// constraint: it holds ~5.8 million years. Nothing reports the clamp — the
// caller gets an ordinary-looking day count, so `d = '9999-12-31'` (the SCD
// end-of-time sentinel) filters on 2262-04-11, and StatsDomainValue hands the
// same clamped count to the prune layer as a row-group bound. The answer is
// wrong rows, not an error.
func parseDateToDays(s string) int32 {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		// Try with timestamp formats, truncate to date
		for _, layout := range []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			time.RFC3339,
		} {
			if t, err = time.Parse(layout, s); err == nil {
				break
			}
		}
		if err != nil {
			return 0
		}
	}
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	return int32(t.Sub(epoch).Hours() / 24)
}

// parseTimestampString parses common timestamp formats into epoch milliseconds.
// Used for implicit string-to-timestamp casting in comparisons.
func parseTimestampString(s string) int64 {
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMilli()
		}
	}
	return 0
}

func toFloat64(v any) float64 {
	switch tv := v.(type) {
	case float64:
		return tv
	case float32:
		return float64(tv)
	case int64:
		return float64(tv)
	case int:
		return float64(tv)
	default:
		return 0
	}
}

func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// parseIPv4ToInt64 converts a string IPv4 address to its int64 representation.
func parseIPv4ToInt64(s string) int64 {
	ip := net.ParseIP(s)
	if ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			return int64(binary.BigEndian.Uint32(ip4))
		}
	}
	return 0
}

// parseMACToInt64 converts a string MAC address to its int64 representation.
func parseMACToInt64(s string) int64 {
	hw, err := net.ParseMAC(s)
	if err != nil || len(hw) != 6 {
		return 0
	}
	var n uint64
	for _, b := range hw {
		n = (n << 8) | uint64(b)
	}
	return int64(n)
}

// parseIPv6ToRawString converts a string IPv6 address to its raw 16-byte string form.
func parseIPv6ToRawString(s string) string {
	ip := net.ParseIP(s)
	if ip != nil {
		return string(ip.To16())
	}
	return ""
}

// parseUUIDToRawString converts a UUID literal to the 16 RAW bytes a UUID
// column stores. Comparing the 36-character text against those bytes — which
// is what the string kernel did before — can never match, so
// `WHERE uuid_col = '…'` silently returned no rows. Same shape as IPv6's, and
// the same reason parseIPv6ToRawString exists.
//
// A 16-byte input is already raw and passes through: internal callers that
// build a predicate from a value they read out of a vector hand it over in
// that form, and no UUID TEXT is 16 characters long (32 without dashes, 36
// with), so the two cannot be confused.
//
// A literal that is neither yields "", which matches nothing: a stored value
// is always 16 bytes.
func parseUUIDToRawString(s string) string {
	if len(s) == 16 {
		return s
	}
	clean := make([]byte, 0, 32)
	for i := 0; i < len(s); i++ {
		if s[i] != '-' {
			clean = append(clean, s[i])
		}
	}
	if len(clean) != 32 {
		return ""
	}
	raw := make([]byte, 16)
	if _, err := hex.Decode(raw, clean); err != nil {
		return ""
	}
	return string(raw)
}

// UUIDLiteralToRaw is parseUUIDToRawString for the row-at-a-time predicate in
// package exec, so the two comparison paths convert the literal identically.
func UUIDLiteralToRaw(s string) string { return parseUUIDToRawString(s) }

// ColColFilterKernel compares two columns element-wise, returning matching row indices.
type ColColFilterKernel func(left, right *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32

// colColFilterImpl creates a ColColFilterKernel for comparing two typed columns.
func colColFilterImpl[T Ordered](getData func(v *batch.Vector) []T, op CompareOp) ColColFilterKernel {
	cmpFn := resolveCompare[T](op)
	return func(left, right *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		ld := getData(left)
		rd := getData(right)
		out := outSel[:0]
		lNulls := left.Nulls.HasNulls()
		rNulls := right.Nulls.HasNulls()
		hasNulls := lNulls || rNulls
		if sel != nil {
			if hasNulls {
				for _, idx := range sel {
					i := int(idx)
					if !left.Nulls.IsNullFast(i) && !right.Nulls.IsNullFast(i) && cmpFn(ld[idx], rd[idx]) {
						out = append(out, idx)
					}
				}
			} else {
				for _, idx := range sel {
					if cmpFn(ld[idx], rd[idx]) {
						out = append(out, idx)
					}
				}
			}
		} else {
			if hasNulls {
				for i := 0; i < vecLen; i++ {
					if !left.Nulls.IsNullFast(i) && !right.Nulls.IsNullFast(i) && cmpFn(ld[i], rd[i]) {
						out = append(out, uint32(i))
					}
				}
			} else {
				for i := 0; i < vecLen; i++ {
					if cmpFn(ld[i], rd[i]) {
						out = append(out, uint32(i))
					}
				}
			}
		}
		return out
	}
}

// colColFilterString creates a ColColFilterKernel for comparing two string columns.
// Uses UnsafeStringValue for zero-copy comparisons — strings are consumed
// within the comparison function and never stored.
func colColFilterString(op CompareOp) ColColFilterKernel {
	cmpFn := resolveCompare[string](op)
	return func(left, right *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		out := outSel[:0]
		hasNulls := left.Nulls.HasNulls() || right.Nulls.HasNulls()
		if sel != nil {
			if hasNulls {
				for _, idx := range sel {
					i := int(idx)
					if !left.Nulls.IsNullFast(i) && !right.Nulls.IsNullFast(i) && cmpFn(left.BytesData.UnsafeStringValue(i), right.BytesData.UnsafeStringValue(i)) {
						out = append(out, idx)
					}
				}
			} else {
				for _, idx := range sel {
					i := int(idx)
					if cmpFn(left.BytesData.UnsafeStringValue(i), right.BytesData.UnsafeStringValue(i)) {
						out = append(out, idx)
					}
				}
			}
		} else {
			if hasNulls {
				for i := 0; i < vecLen; i++ {
					if !left.Nulls.IsNullFast(i) && !right.Nulls.IsNullFast(i) && cmpFn(left.BytesData.UnsafeStringValue(i), right.BytesData.UnsafeStringValue(i)) {
						out = append(out, uint32(i))
					}
				}
			} else {
				for i := 0; i < vecLen; i++ {
					if cmpFn(left.BytesData.UnsafeStringValue(i), right.BytesData.UnsafeStringValue(i)) {
						out = append(out, uint32(i))
					}
				}
			}
		}
		return out
	}
}

// ResolveColColFilterKernel creates a ColColFilterKernel for comparing two columns
// of the given type. Returns nil if the type is not supported.
func ResolveColColFilterKernel(typ batch.TypeID, op CompareOp) ColColFilterKernel {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return colColFilterImpl(getInt64Data, op)
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return colColFilterImpl(getInt32Data, op)
	case batch.TypeFloat64:
		return colColFilterImpl(getFloat64Data, op)
	case batch.TypeFloat32:
		return colColFilterImpl(getFloat32Data, op)
	case batch.TypeString, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		return colColFilterString(op)
	default:
		return nil
	}
}
