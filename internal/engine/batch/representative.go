// SPDX-License-Identifier: MIT

package batch

import "math"

// The representative order (ADR-0024, 2026-11-24 amendment; #1713).
//
// SQL equality decides which values are ONE group, one DISTINCT row, one
// set-operation member. It does not decide which of their stored
// representations the answer prints: 7 and 7.00 are one NUMERIC value, -0
// and 0 one float. Where two such representations meet — a GROUP BY or
// DISTINCT hit, a clone merge, a spill run merge, a set operation — the
// engine keeps the SMALLER under the one fixed total order below, so the
// survivor is a function of the values alone and never of execution timing,
// worker count, clone merge order, spill order or transport order. The
// minimum under a total order is commutative, associative and idempotent,
// which is what makes every one of those meets agree.
//
// The order is defined for the types whose SQL-equal values can carry
// different stored representations in this engine (ADR-0024's eligibility
// table):
//
//   - DECIMAL: the LOWER effective display scale first. The effective display
//     scale of a value is the number of fraction digits its one printer
//     (DecimalColumn.Text, unmarked, which is the group key's box) prints:
//     an explicit code d prints d digits when the carrier's digits past d
//     are zero (invariant I1) and S otherwise; DScaleCarrier prints S;
//     DScaleUnknown prints the carrier trimmed, S less its trailing zeros.
//     Two SQL-equal values with one effective display scale print the same
//     text, so the order is total on what can coexist; the text compared
//     bytewise is the stated tie-break and never separates two of them.
//   - FLOAT32 / FLOAT64: by IEEE-754 bits with the SIGN bit first (set before
//     clear), then the bits ascending: -0 before 0, and the NaN payloads,
//     which the key folds onto one NaN, in one fixed order.
//   - CIDR: the stored text is the representation and the key is
//     kernel.CidrOrderKey's (`10.0.0.1` and `10.0.0.1/32` are one value,
//     #546): the SHORTER text first, then the text bytewise.
//
// Every other type's SQL-equal values share one stored representation and
// compare equal here (RepCompareBox answers 0).

// RepCompareFloat64 orders two SQL-equal float64 representations: negative
// for a before b, positive for b before a, 0 for the same bits.
func RepCompareFloat64(a, b float64) int {
	return repCompareBits(math.Float64bits(a), math.Float64bits(b), 63)
}

// RepCompareFloat32 is RepCompareFloat64 for float32.
func RepCompareFloat32(a, b float32) int {
	return repCompareBits(uint64(math.Float32bits(a)), uint64(math.Float32bits(b)), 31)
}

func repCompareBits(ka, kb uint64, signBit uint) int {
	if ka == kb {
		return 0
	}
	sa, sb := ka>>signBit&1, kb>>signBit&1
	if sa != sb {
		if sa == 1 {
			return -1
		}
		return 1
	}
	if ka < kb {
		return -1
	}
	return 1
}

// DecimalTextFractionDigits is the effective display scale of a printed
// DECIMAL box: the digits after its point, 0 without one.
func DecimalTextFractionDigits(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '.' {
			return len(s) - 1 - i
		}
	}
	return 0
}

// RepCompareDecimalText orders two SQL-equal printed DECIMAL boxes: the lower
// effective display scale first, then the text bytewise.
func RepCompareDecimalText(a, b string) int {
	if a == b {
		return 0
	}
	da, db := DecimalTextFractionDigits(a), DecimalTextFractionDigits(b)
	if da != db {
		if da < db {
			return -1
		}
		return 1
	}
	if a < b {
		return -1
	}
	return 1
}

// KeyDisplayScale is the effective display scale of row i: exactly the
// fraction digits Text(i, false) prints, computed without printing.
func (c *DecimalColumn) KeyDisplayScale(i int) int {
	s := c.Scale
	if s <= 0 {
		return 0
	}
	switch code := c.DScaleCode(i); code {
	case DScaleCarrier:
		return s
	case DScaleUnknown:
		return s - decimalTrailingZeros(c.Data[i], s)
	default:
		d := int(code)
		if d >= s || !decimalZeroPast(c.Data[i], s-d) {
			return s
		}
		return d
	}
}

// decimalZeroPast reports whether x's last k decimal digits are zero:
// invariant I1 for an explicit display-scale code, checked with one
// division where the carrier fits int64.
func decimalZeroPast(x Int128, k int) bool {
	if k <= 0 {
		return true
	}
	if x.FitsInt64() && k < len(pow10u64) {
		return x.ToInt64()%int64(pow10u64[k]) == 0
	}
	return decimalTrailingZeros(x, k) >= k
}

// decimalTrailingZeros counts x's trailing decimal zeros, at most max; a zero
// carrier counts max (it trims to "0").
func decimalTrailingZeros(x Int128, max int) int {
	if x.IsZero() {
		return max
	}
	n := 0
	if x.FitsInt64() {
		v := x.ToInt64()
		for n < max && v%10 == 0 {
			v /= 10
			n++
		}
		return n
	}
	ten := Int128{Lo: 10}
	for n < max {
		q, r, ok := x.QuoRem(ten)
		if !ok || !r.IsZero() {
			return n
		}
		x = q
		n++
	}
	return n
}

// RepCompareDecimalRow orders row i of a DECIMAL vector against a stored
// printed box of an SQL-equal value, the way RepCompareDecimalText orders
// the row's own box against it, without boxing the row unless the effective
// display scales tie.
func RepCompareDecimalRow(v *Vector, i int, stored string) int {
	for v.Base != nil {
		i = int(v.Indices[i])
		v = v.Base
	}
	ds := DecimalTextFractionDigits(stored)
	c := &v.DecimalData
	if code := c.DScaleCode(i); code != DScaleCarrier && code != DScaleUnknown && int(code) > ds && int(code) < c.Scale {
		// An explicit code d prints d or S digits, both above the stored
		// box's: the row is the larger without testing I1.
		return 1
	}
	dr := c.KeyDisplayScale(i)
	if dr != ds {
		if dr < ds {
			return -1
		}
		return 1
	}
	return 0
}

// RepEligibleType reports whether SQL-equal values of typ can carry
// different stored representations, i.e. whether the representative order
// can tell two members of one group apart.
func RepEligibleType(typ TypeID) bool {
	switch typ {
	case TypeDecimal, TypeFloat64, TypeFloat32, TypeCIDR:
		return true
	}
	return false
}

// RepCompareText orders two SQL-equal CIDR texts: the shorter first, then
// bytewise.
func RepCompareText(a, b string) int {
	switch {
	case a == b:
		return 0
	case len(a) != len(b):
		if len(a) < len(b) {
			return -1
		}
		return 1
	case a < b:
		return -1
	}
	return 1
}

// RepCompareBox orders two boxes of SQL-equal values of typ (the boxes
// GetValueOf(i, false) produces). Types outside RepEligibleType, and a nil
// on either side (a NULL, which equals only a NULL), compare 0.
func RepCompareBox(typ TypeID, a, b any) int {
	if a == nil || b == nil {
		return 0
	}
	switch typ {
	case TypeDecimal:
		sa, ok1 := a.(string)
		sb, ok2 := b.(string)
		if ok1 && ok2 {
			return RepCompareDecimalText(sa, sb)
		}
	case TypeFloat64:
		fa, ok1 := a.(float64)
		fb, ok2 := b.(float64)
		if ok1 && ok2 {
			return RepCompareFloat64(fa, fb)
		}
	case TypeFloat32:
		fa, ok1 := a.(float32)
		fb, ok2 := b.(float32)
		if ok1 && ok2 {
			return RepCompareFloat32(fa, fb)
		}
	case TypeCIDR:
		sa, ok1 := a.(string)
		sb, ok2 := b.(string)
		if ok1 && ok2 {
			return RepCompareText(sa, sb)
		}
	}
	return 0
}
