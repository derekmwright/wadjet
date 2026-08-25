package kernel

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
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

// compareFilterFloat is compareFilterImpl for FLOAT32/FLOAT64: same loop
// shape, but the per-row test comes from kernel/float_order.go so the column
// is compared in PostgreSQL's total order rather than Go's IEEE754 one. The
// operator and the constant's NaN-ness are resolved once, here — and the
// constant's NaN-ness also picks WHICH shape the per-row test takes. A NaN
// constant is rare on the query side and its answer depends only on the
// row's own NaN-ness, so it keeps resolveFloatConstPred's one-argument
// capturing closure. The common, non-NaN case uses resolveFloatConstPred2's
// non-capturing two-argument form instead, with val carried as a plain
// loop-invariant argument: the capturing form here measured +28% on
// FilterColumnCompare, because calling a closure that captured the constant
// was a genuine indirect call per row rather than the near-free dispatch the
// equivalent integer path (resolveCompare, above) gets.
func compareFilterFloat[T FloatOrdered](getData func(v *batch.Vector) []T, val T, op CompareOp) FilterKernel {
	if val != val {
		keep := resolveFloatConstPred(op, val)
		return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
			data := getData(vec)
			out := outSel[:0]
			hasNulls := vec.Nulls.HasNulls()
			if sel != nil {
				if hasNulls {
					for _, idx := range sel {
						if !vec.Nulls.IsNullFast(int(idx)) && keep(data[idx]) {
							out = append(out, idx)
						}
					}
				} else {
					for _, idx := range sel {
						if keep(data[idx]) {
							out = append(out, idx)
						}
					}
				}
			} else {
				if hasNulls {
					for i := 0; i < vecLen; i++ {
						if !vec.Nulls.IsNullFast(i) && keep(data[i]) {
							out = append(out, uint32(i))
						}
					}
				} else {
					for i := 0; i < vecLen; i++ {
						if keep(data[i]) {
							out = append(out, uint32(i))
						}
					}
				}
			}
			return out
		}
	}
	cmpFn := resolveFloatConstPred2[T](op)
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
		// Not compareFilterImpl: FLOAT compares in PostgreSQL's total order,
		// where NaN is the greatest value and equal to itself, so `> 1e300`
		// admits a NaN row and `= f` keeps one (#459, ADR-0012 item 8).
		return compareFilterFloat(getFloat64Data, toFloat64(value), op)
	case batch.TypeFloat32:
		return compareFilterFloat(getFloat32Data, float32(toFloat64(value)), op)
	case batch.TypeString:
		return compareFilterString(op, toString(value))
	case batch.TypeIPv4:
		// ok is false when the literal names no address at all. Returning a
		// kernel keyed on 0 used to answer that as the address 0.0.0.0,
		// MATCHING every row holding it (#519, the CIDR/IPv6 shape #492
		// already fixed) — a nil kernel is how this package asks the caller
		// to raise 22P02 instead (exec.networkConstError).
		if n, ok := parseIPv4ToInt64(toString(value)); ok {
			return compareFilterImpl(getInt64Data, n, op)
		}
		return nil
	case batch.TypeMAC:
		if n, ok := parseMACToInt64(toString(value)); ok {
			return compareFilterImpl(getInt64Data, n, op)
		}
		return nil
	case batch.TypeIPv6:
		// IPv6LitKey, not a plain net.ParseIP: a v4-shaped literal against a
		// v6 column is a FAMILY comparison in PostgreSQL, not a v4-mapped
		// address in the middle of the v6 range (#492 follow-up). An
		// unparseable literal returns no kernel at all — see the CIDR arm.
		if key, ok := IPv6LitKey(toString(value)); ok {
			return compareFilterString(op, key)
		}
		return nil
	case batch.TypeCIDR:
		// The column stores plain TEXT (parquet/schema.go), so comparing the
		// literal against it directly is a byte comparison of that text —
		// LEXICAL, not PostgreSQL's inet order (family, common bits under the
		// smaller mask, mask length, full address): "10.0.0.0/24" sorts below
		// "9.0.0.0/8" as text even though 10.x is the larger address (#492).
		// compareFilterCIDR re-keys both the literal and every row's own
		// text into that order before comparing. A bare address parses as a
		// /32 or /128 host route, as PostgreSQL's inet reads the same input.
		if key, ok := CidrSortKey(toString(value)); ok {
			return compareFilterCIDR(op, key)
		}
		// The literal is not an address at all. Returning a match-nothing
		// kernel here made `c_cidr <> 'garbage'` answer ZERO rows where every
		// row is a legitimate answer, and it is not what the other network
		// arms do either: the pre-#519 parseIPv4ToInt64 answered 0 for an
		// unparseable literal, which MATCHED the rows holding 0.0.0.0, and
		// the IPv6 arm's plain net.ParseIP answered "" before IPv6LitKey
		// replaced it. All three were silent wrong answers to a query that
		// cannot mean anything; PostgreSQL raises 22P02 for it, and
		// ADR-0012 item 1 makes that the answer. A nil kernel is how this
		// package asks the caller to raise — see compareFilterDecimal.
		return nil
	case batch.TypePort, batch.TypeProtocol:
		return compareFilterImpl(getInt32Data, int32(toInt64(value)), op)
	case batch.TypeDuration:
		return compareFilterImpl(getInt64Data, toInt64(value), op)
	case batch.TypeUUID:
		// ok is false when the literal is 36/32 characters of something
		// other than hex, or any other length: not a UUID at all. The old
		// "" sentinel matched nothing for `=` and MATCHED EVERY ROW for
		// `<>` (#519) — a nil kernel raises 22P02 instead.
		if raw, ok := parseUUIDToRawString(toString(value)); ok {
			return compareFilterString(op, raw)
		}
		return nil
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

// CidrSortKey re-keys a CIDR/inet TEXT value ("192.168.1.0/24", "10.0.0.1/8",
// or a bare "10.0.0.1") into PostgreSQL's `inet` order — network_cmp — as a
// byte string two keys compare LEXICALLY in exactly that order.
//
// PostgreSQL's network_cmp_internal compares, in this sequence:
//
//  1. the address FAMILY (v4 before v6),
//  2. the common bits under the SMALLER of the two prefix lengths,
//  3. the prefix length itself,
//  4. the FULL, UNMASKED address.
//
// The key is [family][address masked to its own prefix, full width][prefix
// length][full unmasked address], which reproduces that order exactly. Step 2
// needs both operands and no single-value key can hold it directly, but the
// masked address is equivalent: if the first min(len) bits differ, both keys
// retain the differing bit and compare the same way; if they agree, the
// shorter prefix's key has zeros where the longer one may have ones, so it
// sorts first — which is step 3's answer — and when those bits are zero too
// the keys tie and the explicit prefix-length byte decides. The trailing full
// address is step 4.
//
// Verified against live PostgreSQL 17 over host-bearing and canonical values,
// v4 and v6, at mixed prefix lengths — the whole table is
// TestCidrSortKeyMatchesPostgresInetOrder's fixture. Three of its consequences
// are worth naming because a simpler key gets them wrong:
//
//	'9.255.255.255/32' < '10.0.0.0/8'   — common bits decide before the mask
//	'192.168.1.5/24'   < '192.168.1.0/32' — the MASK outranks the address
//	'10.0.0.0/8'       < '10.0.0.1/8'   — host bits are kept, and ordered last
//
// That last one is why the key cannot be built from net.ParseCIDR's MASKED
// network alone, which is what this function did when #492 introduced it:
// keying only ipnet.IP threw the host bits away, so '10.0.0.1/8' and
// '10.0.0.0/8' became the SAME value and `= '10.0.0.1/8'` answered rows
// holding a different address. Wadjet's CIDR column is unvalidated text
// (internal/storage/ingest), and host-bearing prefixes are ordinary in the
// network data this type exists for, so those are not edge values.
//
// A BARE address with no "/" is a /32 (v4) or /128 (v6), which is what
// PostgreSQL's inet does with the same input — `'10.0.0.1'::inet =
// '10.0.0.1/32'::inet` is true. A v4-MAPPED v6 address ("::ffff:10.0.0.2")
// keeps the v6 family, also matching PostgreSQL (`family()` answers 6).
//
// ok is false when s is not an address at all. Callers must turn that into a
// query ERROR, never a match-nothing kernel: see ResolveFilterKernel's
// TypeCIDR arm.
//
// Exported — unlike this file's other literal parse helpers
// (parseIPv4ToInt64, parseMACToInt64), which internal/engine/expr duplicates
// locally rather than importing — because this one is not a trivial
// re-encode: expr.CmpNetworkLit's CIDR literal and this kernel's per-row CIDR
// key MUST agree bit for bit, and two structural parsers maintained
// separately is exactly the shape #492 already is (the kernel path numeric,
// the expr path lexical). One implementation, shared, is what keeps them from
// drifting apart again.
func CidrSortKey(s string) (string, bool) {
	t := s
	if !strings.ContainsRune(t, '/') {
		// A bare address is a host route. ':' is present in every IPv6 text
		// form and in no IPv4 one, which is the same split net.ParseCIDR
		// itself makes (it tries the dotted-quad parse first and falls back
		// to the v6 parser).
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

// IPv6LitKey re-keys an IPv6 filter literal into the form a TypeIPv6 column's
// rows compare against: the address's raw 16 bytes, which a byte comparison
// orders exactly as the address's own big-endian numeric value.
//
// A v4-shaped literal is not that, and is not a v4-MAPPED v6 address either.
// PostgreSQL's inet compares the FAMILY first and puts every v4 address below
// every v6 one (`'255.255.255.255'::inet < '::'::inet` is true), including
// below a v4-mapped v6 address, which it still calls family 6
// (`family('::ffff:10.0.0.2'::inet)` answers 6). The key for a v4 literal is
// therefore the EMPTY string: it is shorter than, and a prefix of, every
// 16-byte row value, so it compares strictly below all of them and equals
// none — PostgreSQL's family rule, with no per-row re-keying.
//
// Reading a v4 literal as its v4-mapped 16 bytes instead — which is what
// the TypeIPv6 kernel arm used to do, through a plain net.ParseIP —
// placed it in the MIDDLE of the v6 range (below 2001:db8:: and above ::1),
// while the row-at-a-time path fell through to a lexical text comparison
// entirely: two paths, two orders, neither PostgreSQL's.
//
// ok is false for a literal that is no address at all; the caller raises the
// query error, the same as CidrSortKey's.
func IPv6LitKey(s string) (key string, ok bool) {
	ip := net.ParseIP(s)
	if ip == nil {
		return "", false
	}
	if ip.To4() != nil && !strings.ContainsRune(s, ':') {
		return "", true // a v4 literal: below every v6 row, by family
	}
	return string(ip.To16()), true
}

// compareFilterCIDR orders a CIDR column against a literal in PostgreSQL's
// inet order (CidrSortKey), never by the column's raw stored text (#492).
// litKey is the literal's key, precomputed once; each row's own key is
// recomputed from its text every time, since the column has no compact byte
// encoding to read instead (parquet/schema.go stores CIDR as plain text).
//
// A stored value that is not an address matches NOTHING, for every operator
// including `<>`: the column is unvalidated text, and a value with no place
// in the order has no defined comparison, which is UNKNOWN — the same answer
// a NULL row gets. expr.CmpNetworkLit.evalCIDR answers that row the same way,
// deliberately: it used to fall through to a LEXICAL text comparison there,
// so one malformed row made the two paths disagree.
func compareFilterCIDR(op CompareOp, litKey string) FilterKernel {
	cmpFn := resolveCompare[string](op)
	match := func(vec *batch.Vector, i int) bool {
		key, ok := CidrSortKey(vec.BytesData.UnsafeStringValue(i))
		return ok && cmpFn(key, litKey)
	}
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		if sel != nil {
			for _, idx := range sel {
				if (!hasNulls || !vec.Nulls.IsNullFast(int(idx))) && match(vec, int(idx)) {
					out = append(out, idx)
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				if (!hasNulls || !vec.Nulls.IsNullFast(i)) && match(vec, i) {
					out = append(out, uint32(i))
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
		set, hasNaN := floatInSet(values)
		return inFilterFloat64(set, hasNaN, negate)
	case batch.TypeString:
		set := make(map[string]struct{}, len(values))
		for _, v := range values {
			set[toString(v)] = struct{}{}
		}
		return inFilterString(set, negate)
	case batch.TypeCIDR:
		// CIDR shares TypeString's storage but not its equality: two spellings
		// of one network are one value in PostgreSQL's inet and two strings
		// here, so the set holds CidrSortKey keys and every row is re-keyed to
		// match — the same key `=` compares through, because
		// `c_cidr = 'X'` and `c_cidr IN ('X')` answering differently would be
		// the two-kernel version of the two-path defect #492 closed.
		set := make(map[string]struct{}, len(values))
		for _, v := range values {
			key, ok := CidrSortKey(toString(v))
			if !ok {
				return nil // not an address: the caller raises 22P02
			}
			set[key] = struct{}{}
		}
		return inFilterKeyed(set, CidrSortKey, negate)
	case batch.TypeUUID:
		// UUID stores 16 RAW bytes, not the 36-character text.
		set := make(map[string]struct{}, len(values))
		for _, v := range values {
			raw, ok := parseUUIDToRawString(toString(v))
			if !ok {
				return nil // not a UUID: the caller raises 22P02
			}
			set[raw] = struct{}{}
		}
		return inFilterString(set, negate)
	case batch.TypeIPv6:
		// IPv6 stores the 16 RAW bytes; the literal is text. The scalar
		// comparison kernel already parses it (IPv6LitKey) and the IN path did
		// not, so `WHERE ipv6_col IN ('2001:db8::1')` could not have matched
		// even once the type had an arm. A v4 literal keys to the empty
		// string, which no 16-byte row equals — PostgreSQL's family rule, the
		// same answer the scalar arm gives.
		set := make(map[string]struct{}, len(values))
		for _, v := range values {
			key, ok := IPv6LitKey(toString(v))
			if !ok {
				return nil // not an address: the caller raises 22P02
			}
			set[key] = struct{}{}
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
			n, ok := parseIPv4ToInt64(toString(v))
			if !ok {
				return nil // not an address: the caller raises 22P02
			}
			set[n] = struct{}{}
		}
		return inFilterInt64(getInt64Data, set, negate)
	case batch.TypeMAC:
		set := make(map[int64]struct{}, len(values))
		for _, v := range values {
			n, ok := parseMACToInt64(toString(v))
			if !ok {
				return nil // not an address: the caller raises 22P02
			}
			set[n] = struct{}{}
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
		set, hasNaN := floatInSet(values)
		return inFilterFloat32(set, hasNaN, negate)
	case batch.TypeBool:
		return inFilterBool(values, negate)
	case batch.TypeDecimal:
		return inFilterDecimal(values, negate)
	default:
		return nil
	}
}

// floatInSet builds an IN list's float64 membership set, and reports
// separately whether the list held a NaN.
//
// The flag is not redundant with the map. A Go map's keys compare with `==`,
// which is IEEE754: a NaN key is unreachable by lookup — inserting one and
// probing with a NaN both miss — so a NaN in the list could never have matched
// a NaN in the column, though PostgreSQL says it does (NaN = NaN is TRUE, so
// `f IN ('NaN')` selects the NaN rows). `==` also makes -0.0 and +0.0 the SAME
// key, which is the answer PostgreSQL gives and is a property of the language
// rather than of the runtime's hashing, so no fold is needed for that pair.
func floatInSet(values []any) (map[float64]struct{}, bool) {
	set := make(map[float64]struct{}, len(values))
	hasNaN := false
	for _, v := range values {
		f := toFloat64(v)
		if f != f {
			hasNaN = true
			continue
		}
		set[f] = struct{}{}
	}
	return set, hasNaN
}

// inFilterFloat32 probes a FLOAT32 column against a float64 set.
func inFilterFloat32(set map[float64]struct{}, hasNaN, negate bool) FilterKernel {
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		keep := func(i int) bool {
			f := vec.Float32Data[i]
			if f != f {
				return hasNaN != negate
			}
			_, found := set[float64(f)]
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

func inFilterFloat64(set map[float64]struct{}, hasNaN, negate bool) FilterKernel {
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		data := vec.Float64Data
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		member := func(f float64) bool {
			if f != f {
				// A NaN column value: the Go map cannot answer this (see
				// floatInSet), and PostgreSQL says NaN = NaN.
				return hasNaN
			}
			_, found := set[f]
			return found
		}
		if sel != nil {
			for _, idx := range sel {
				if hasNulls && vec.Nulls.IsNullFast(int(idx)) {
					continue
				}
				if member(data[idx]) != negate {
					out = append(out, idx)
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				if hasNulls && vec.Nulls.IsNullFast(i) {
					continue
				}
				if member(data[i]) != negate {
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

// inFilterKeyed is inFilterString for a column whose stored TEXT is not the
// value's comparison key: each row is re-keyed with the same function the set
// members went through. A row the key function rejects matches nothing, for
// either polarity — the value is not an address, so membership is UNKNOWN
// rather than false, which is what a NULL row does here too.
func inFilterKeyed(set map[string]struct{}, keyOf func(string) (string, bool), negate bool) FilterKernel {
	member := func(vec *batch.Vector, i int) bool {
		key, ok := keyOf(vec.BytesData.UnsafeStringValue(i))
		if !ok {
			return false
		}
		_, found := set[key]
		return found != negate
	}
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		if sel != nil {
			for _, idx := range sel {
				if (!hasNulls || !vec.Nulls.IsNullFast(int(idx))) && member(vec, int(idx)) {
					out = append(out, idx)
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				if (!hasNulls || !vec.Nulls.IsNullFast(i)) && member(vec, i) {
					out = append(out, uint32(i))
				}
			}
		}
		return out
	}
}

// --- LIKE filter kernel ---

// ResolveLikeFilterKernel creates a FilterKernel for SQL LIKE pattern
// matching against a column of the given type. Converts SQL LIKE patterns
// (% and _) to optimized matching functions.
//
// The column's underlying storage is not always TEXT in BytesData: TypeIPv4/
// TypeMAC/TypePort/TypeProtocol box as Int64Data/Int32Data, and TypeIPv6/
// TypeUUID box as BytesData but hold the address's RAW binary form, not the
// human-readable text a LIKE pattern is written against. This used to be a
// single BytesData.UnsafeStringValue call with no type check at all —
// indexing an empty backing store for the Int64Data/Int32Data types (a
// process-killing panic, since it is not the one deliberate FatalEvalPanic
// shape recover() converts back into a query error) and matching nothing for
// IPv6/UUID (their raw bytes never contain the pattern's text) (#497).
// likeTextRenderer resolves the row->text function once per column, the same
// per-type-dispatch-once discipline ResolveFilterKernel already follows, so
// the inner loop has no per-row type switch.
func ResolveLikeFilterKernel(typ batch.TypeID, pattern string, negate bool) FilterKernel {
	matcher := compileLikePattern(pattern)
	render := likeTextRenderer(typ)
	return func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		out := outSel[:0]
		hasNulls := vec.Nulls.HasNulls()
		if sel != nil {
			for _, idx := range sel {
				if hasNulls && vec.Nulls.IsNullFast(int(idx)) {
					continue
				}
				if matcher(render(vec, int(idx))) != negate {
					out = append(out, idx)
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				if hasNulls && vec.Nulls.IsNullFast(i) {
					continue
				}
				if matcher(render(vec, i)) != negate {
					out = append(out, uint32(i))
				}
			}
		}
		return out
	}
}

// likeTextRenderer resolves, once per column, the row->text function LIKE
// matches a pattern against.
//
// Wadjet renders every SIX network-native types and UUID as human-readable
// text for CAST AS STRING and scalar function arguments (#484) — LIKE follows
// the same convention rather than refusing outright the way PostgreSQL does
// for inet/cidr/macaddr (verified live: `'10.0.0.1'::inet LIKE '10.%'` raises
// "operator does not exist: inet ~~ unknown"). ADR-0012 item 11 records the
// decision and its reasons. TypeCIDR is already TEXT in its own storage
// (parquet/schema.go), so it falls through to the same BytesData path
// TypeString/TypeBytes use.
//
// That CAST-agreement claim is scoped to those SEVEN types and no further.
// It is false for DATE, whose CAST AS STRING answers the epoch DAY (15007)
// while this renderer, the projection and PostgreSQL's own `date::text` all
// answer 2011-02-02 — a separate defect in CAST's string family (#521), not
// a contract this function is part of.
//
// The default arm covers every OTHER type (Int64/Float64/Bool/Decimal/Date/
// the containers) with the row's own boxed value — fmt.Sprint on whatever
// Vector.GetValue returns — never indexing BytesData on a column that does
// not have it, which is the one invariant this function exists to restore
// regardless of what LIKE against a given type is decided to MEAN.
//
// This rendering is the DEFINITION of what LIKE matches, so the
// row-at-a-time path has to reproduce it rather than the other way round:
// expr.likeOperand reads Vector.GetValue for the four types ColRef.Eval boxes
// differently (IPv4, MAC, DATE, FLOAT32), and
// wadjet.TestLikeAnswersTheSameAtBothSites sweeps every flat type through
// both sites. Changing a per-type arm here without checking that sweep
// re-opens the divergence it exists to catch.
func likeTextRenderer(typ batch.TypeID) func(*batch.Vector, int) string {
	switch typ {
	case batch.TypeString, batch.TypeBytes, batch.TypeCIDR:
		// Already TEXT in BytesData: the original zero-copy path.
		return func(v *batch.Vector, i int) string { return v.BytesData.UnsafeStringValue(i) }
	case batch.TypeIPv4:
		return func(v *batch.Vector, i int) string { return likeFormatIPv4(uint32(v.Int64Data[i])) }
	case batch.TypeMAC:
		return func(v *batch.Vector, i int) string { return likeFormatMAC(uint64(v.Int64Data[i])) }
	case batch.TypePort, batch.TypeProtocol:
		return func(v *batch.Vector, i int) string { return strconv.Itoa(int(v.Int32Data[i])) }
	case batch.TypeIPv6:
		return func(v *batch.Vector, i int) string {
			raw := v.BytesData.Value(i)
			if len(raw) != 16 {
				return ""
			}
			return net.IP(raw).String()
		}
	case batch.TypeUUID:
		return func(v *batch.Vector, i int) string {
			raw := v.BytesData.Value(i)
			if len(raw) != 16 {
				return ""
			}
			return likeFormatUUID(raw)
		}
	default:
		return func(v *batch.Vector, i int) string { return fmt.Sprint(v.GetValue(i)) }
	}
}

// likeFormatIPv4 and likeFormatMAC render the raw int64 encodings
// TypeIPv4/TypeMAC columns store into the address text a LIKE pattern is
// written against — duplicated locally rather than imported from batch (this
// package's parseIPv4ToInt64/parseMACToInt64 already duplicate the reverse
// direction for the same reason: these are trivial, one-way re-encodes, not
// the shared-implementation case CidrSortKey's export comment explains).
func likeFormatIPv4(v uint32) string {
	ip := make(net.IP, net.IPv4len)
	binary.BigEndian.PutUint32(ip, v)
	return ip.String()
}

func likeFormatMAC(v uint64) string {
	b := make(net.HardwareAddr, 6)
	for i := 5; i >= 0; i-- {
		b[i] = byte(v)
		v >>= 8
	}
	return b.String()
}

// likeFormatUUID renders 16 raw bytes as "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
func likeFormatUUID(b []byte) string {
	var buf [36]byte
	hex.Encode(buf[0:8], b[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], b[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], b[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], b[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], b[10:16])
	return string(buf[:])
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

// toString renders a filter constant as the text a TEXT-storage column is
// compared against.
//
// It used to answer the EMPTY STRING for every non-string box, which made
// `WHERE s = 1.5` over a STRING column a comparison against "" — nothing
// equals it, everything is greater than it, so `=` selected no rows, `>`
// selected ALL of them including the row holding "1.5", and `<` selected
// none. That is not a lexical answer and not a numeric one; it is the
// type's zero standing in for a value nobody wrote, the same defect class
// #450 fixed for a nil constant one arm over.
//
// A number is rendered as a number now. The exact SOURCE TEXT is better
// still, and the caller substitutes it when it has one (exec.litValueForType,
// ADR-0012 item 6: a numeric literal's carrier is its text) — this is the
// floor for a constant that reached here without one, such as a folded
// expression's value.
func toString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case int64:
		return strconv.FormatInt(s, 10)
	case int32:
		return strconv.FormatInt(int64(s), 10)
	case int:
		return strconv.FormatInt(int64(s), 10)
	case float64:
		return strconv.FormatFloat(s, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(s), 'g', -1, 32)
	case bool:
		// "true"/"false", which is both PostgreSQL's own `boolean::text`
		// (verified live — the single-letter 't' is psql's DISPLAY, not the
		// cast) and what the row-at-a-time path's toString produces through
		// fmt.Sprint. The two have to agree: PostgreSQL refuses `text =
		// boolean` outright (42883), so nothing decides this pair for us
		// except the requirement that one predicate get one answer, and the
		// empty string this used to return made the kernel disagree with the
		// row path on every row (#504 review, non-blocker c).
		return strconv.FormatBool(s)
	}
	return ""
}

// parseIPv4ToInt64 converts a string IPv4 address to its int64
// representation. ok is false when s names no address at all — callers must
// turn that into a query ERROR (see ResolveFilterKernel's TypeIPv4 arm and
// exec.networkConstError), never read it as the address 0.0.0.0 the way this
// used to (#519: `WHERE c_ipv4 = 'garbage'` matched every row holding
// 0.0.0.0, the CIDR/IPv6 shape #492 already fixed one type over).
func parseIPv4ToInt64(s string) (int64, bool) {
	ip := net.ParseIP(s)
	if ip == nil {
		return 0, false
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return 0, false
	}
	return int64(binary.BigEndian.Uint32(ip4)), true
}

// IPv4LitKey exports parseIPv4ToInt64 for exec.networkConstError, which needs
// to know whether a filter literal named an address at all — not just its
// encoded value — the same way kernel.IPv6LitKey and kernel.CidrSortKey
// already do for their two types.
func IPv4LitKey(s string) (int64, bool) { return parseIPv4ToInt64(s) }

// parseMACToInt64 converts a string MAC address to its int64 representation.
// ok is false when s names no address at all (#519; see parseIPv4ToInt64).
func parseMACToInt64(s string) (int64, bool) {
	hw, err := net.ParseMAC(s)
	if err != nil || len(hw) != 6 {
		return 0, false
	}
	var n uint64
	for _, b := range hw {
		n = (n << 8) | uint64(b)
	}
	return int64(n), true
}

// MACLitKey exports parseMACToInt64 for exec.networkConstError; see
// IPv4LitKey.
func MACLitKey(s string) (int64, bool) { return parseMACToInt64(s) }

// parseUUIDToRawString converts a UUID literal to the 16 RAW bytes a UUID
// column stores. Comparing the 36-character text against those bytes — which
// is what the string kernel did before — can never match, so
// `WHERE uuid_col = '…'` silently returned no rows. Same shape as IPv6's, and
// the same reason IPv6LitKey exists.
//
// A 16-byte input is already raw and passes through: internal callers that
// build a predicate from a value they read out of a vector hand it over in
// that form, and no UUID TEXT is 16 characters long (32 without dashes, 36
// with), so the two cannot be confused.
//
// ok is false when s names no address at all — callers must turn that into a
// query ERROR rather than the historical match-nothing/match-everything
// sentinel of "" (#519; see parseIPv4ToInt64).
func parseUUIDToRawString(s string) (string, bool) {
	if len(s) == 16 {
		return s, true
	}
	clean := make([]byte, 0, 32)
	for i := 0; i < len(s); i++ {
		if s[i] != '-' {
			clean = append(clean, s[i])
		}
	}
	if len(clean) != 32 {
		return "", false
	}
	raw := make([]byte, 16)
	if _, err := hex.Decode(raw, clean); err != nil {
		return "", false
	}
	return string(raw), true
}

// UUIDLiteralToRaw is parseUUIDToRawString for the row-at-a-time predicate in
// package exec, so the two comparison paths convert the literal identically.
func UUIDLiteralToRaw(s string) (string, bool) { return parseUUIDToRawString(s) }

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

// colColFilterFloat is colColFilterImpl for FLOAT32/FLOAT64, with the
// per-row test taken from PostgreSQL's float total order — the same
// substitution compareFilterFloat makes on the constant side, and the reason
// `WHERE f = f` no longer drops the NaN rows.
func colColFilterFloat[T FloatOrdered](getData func(v *batch.Vector) []T, op CompareOp) ColColFilterKernel {
	cmpFn := resolveFloatColColPred[T](op)
	return func(left, right *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		ld := getData(left)
		rd := getData(right)
		out := outSel[:0]
		hasNulls := left.Nulls.HasNulls() || right.Nulls.HasNulls()
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

// ResolveColColFilterKernel creates a ColColFilterKernel for comparing two columns
// of the given type. Returns nil if the type is not supported.
func ResolveColColFilterKernel(typ batch.TypeID, op CompareOp) ColColFilterKernel {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return colColFilterImpl(getInt64Data, op)
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return colColFilterImpl(getInt32Data, op)
	case batch.TypeFloat64:
		// PostgreSQL's float order, not Go's — `WHERE f = f` is TRUE for a
		// NaN row and `WHERE a > b` is TRUE when a is NaN and b is not.
		return colColFilterFloat(getFloat64Data, op)
	case batch.TypeFloat32:
		return colColFilterFloat(getFloat32Data, op)
	case batch.TypeString, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		return colColFilterString(op)
	case batch.TypeDecimal:
		return colColFilterDecimal(op)
	default:
		return nil
	}
}

// colColFilterDecimal compares two DECIMAL columns by NUMERIC value.
//
// Without this arm the resolver returned nil, and ColColFilter had nothing to
// fall back to: its row-at-a-time fallback is attached only when the two
// column TYPES differ (#375's mixed-type guard), and two DECIMALs share a
// TypeID. `WHERE d_2 = d_4` therefore failed the query outright — "could not
// resolve kernel for d_2 0 d_4" — for every operator (#477).
//
// The two columns' SCALES need not agree, and the comparison is exact at any
// pair of them: CompareDecimalValues rescales the smaller-scale operand up and
// falls back to big.Int only for the pair whose rescale overflows. Which of
// the two paths applies is decided ONCE per batch, because a scale is a
// property of the column, not of the row.
func colColFilterDecimal(op CompareOp) ColColFilterKernel {
	return func(left, right *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32 {
		ld, rd := left.DecimalData.Data, right.DecimalData.Data
		ls, rs := left.DecimalData.Scale, right.DecimalData.Scale
		keep := func(i int) bool { return applyCompareOp(compareInt128(ld[i], rd[i]), op) }
		if ls != rs {
			keep = func(i int) bool {
				return applyCompareOp(CompareDecimalValues(ld[i], ls, rd[i], rs), op)
			}
		}
		out := outSel[:0]
		hasNulls := left.Nulls.HasNulls() || right.Nulls.HasNulls()
		null := func(i int) bool {
			return hasNulls && (left.Nulls.IsNullFast(i) || right.Nulls.IsNullFast(i))
		}
		if sel != nil {
			for _, idx := range sel {
				if !null(int(idx)) && keep(int(idx)) {
					out = append(out, idx)
				}
			}
			return out
		}
		for i := 0; i < vecLen; i++ {
			if !null(i) && keep(i) {
				out = append(out, uint32(i))
			}
		}
		return out
	}
}
