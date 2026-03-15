package kernel

import (
	"encoding/binary"
	"net"

	"github.com/derekmwright/caelum/internal/engine/batch"
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
	return func(vec *batch.Vector, sel []uint16, vecLen int, outSel []uint16) []uint16 {
		data := getData(vec)
		out := outSel[:0]
		if sel != nil {
			for _, idx := range sel {
				if !vec.Nulls.IsNullFast(int(idx)) && cmpFn(data[idx], val) {
					out = append(out, idx)
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				if !vec.Nulls.IsNullFast(i) && cmpFn(data[i], val) {
					out = append(out, uint16(i))
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

// ResolveFilterKernel creates a FilterKernel for comparing a column of the
// given type against a constant value. The type dispatch happens once here;
// the returned function has no type switches in its inner loop.
func ResolveFilterKernel(typ batch.TypeID, op CompareOp, value any) FilterKernel {
	switch typ {
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
		return compareFilterString(op, toString(value))
	case batch.TypeDate:
		return compareFilterImpl(getInt32Data, int32(toInt64(value)), op)
	default:
		return nil
	}
}

// compareFilterString handles string column comparison.
func compareFilterString(op CompareOp, val string) FilterKernel {
	cmpFn := resolveCompare[string](op)
	return func(vec *batch.Vector, sel []uint16, vecLen int, outSel []uint16) []uint16 {
		out := outSel[:0]
		if sel != nil {
			for _, idx := range sel {
				if !vec.Nulls.IsNullFast(int(idx)) && cmpFn(vec.BytesData.StringValue(int(idx)), val) {
					out = append(out, idx)
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				if !vec.Nulls.IsNullFast(i) && cmpFn(vec.BytesData.StringValue(i), val) {
					out = append(out, uint16(i))
				}
			}
		}
		return out
	}
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
	default:
		return 0
	}
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
