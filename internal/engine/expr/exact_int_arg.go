package expr

import (
	"strconv"
	"strings"
)

// The registry's INTEGER argument read (#1031).
//
// Five call sites took an integer argument as `int64(ToFloat64(arg))`, which
// is a round trip through a double: past 2^53 a double cannot hold a 64-bit
// integer's low bits, so `PARSE_BYTES('9007199254740993')` answered
// 9007199254740992 and `HUMAN_READABLE_SECONDS(9007199254740993)` ended in 32
// seconds rather than 33. It is the same loss the bitwise family had before
// #966 and the same rule closes it: an integer box is taken as itself, and
// only a value that is genuinely not an integer goes near a float.
//
// The two PARSE sites need one more step, because their argument is TEXT that
// may carry a fraction and a unit ('1.5 GiB'). They read the number exactly
// when it is spelled as an integer and the unit's multiplier is one — every
// multiplier but 'BPS' is — and multiply on the checked int64 path, so a
// product with no int64 is 22003 rather than whatever `int64(float64)`
// happens to produce for an out-of-range double. A fractional spelling keeps
// the float product it always had: there is no exact integer read of "1.5",
// and the values that reach it are far below the boundary.

// exactIntArg reads an integer argument as the 64-bit value it is.
//
// An integer box is taken as itself. Anything else — a float, a numeric
// string — keeps the conversion these functions have always applied, so no
// shape that answered before starts refusing.
func exactIntArg(v any) int64 {
	if i, ok := toInt64Safe(v); ok {
		return i
	}
	if s, ok := v.(string); ok {
		if n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
			return n
		}
	}
	return int64(ToFloat64(v))
}

// exactScaledInt reads numStr as an integer and multiplies it by mult
// exactly, answering ok=false when either step has no exact integer form —
// a fractional spelling, a fractional multiplier, or text that is not a
// number. The caller then keeps its float product.
//
// The multiply is the CHECKED one, so `PARSE_BYTES('99999999999999 EB')` is
// 22003 `bigint out of range` instead of whatever `int64(1e35)` produces,
// which is not defined by the Go specification at all.
func exactScaledInt(numStr string, mult float64) (int64, bool) {
	n, err := strconv.ParseInt(strings.TrimSpace(numStr), 10, 64)
	if err != nil {
		return 0, false
	}
	m := int64(mult)
	if float64(m) != mult {
		return 0, false // a fractional multiplier: 'BPS' is 0.125
	}
	return mulInt64Checked(n, m), true
}
