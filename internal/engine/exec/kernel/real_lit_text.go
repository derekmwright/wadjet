package kernel

import (
	"errors"
	"math"
	"strconv"
	"strings"
)

// RealOverflowText renders a numeric literal's source text the way
// PostgreSQL's `numeric` output does — expanded, never in exponent form — for
// the SQLSTATE 22003 message raised when that literal will not fit a `real`.
//
// The message is part of the answer, not decoration. PostgreSQL raises
//
//	ERROR:  "10000000000000000000000000000000000000000" is out of range for type real
//
// for `real IN (1e40, 3.1)`, and it prints the same forty-one digits whether
// the query spelled the literal 1e40, 1e+40, 1.0e40 or in full: the cast that
// fails is numeric->real, and a numeric's text is its digits. Wadjet used to
// print whatever Go's %v gave the boxed float64 ("1e+40"), so the two
// evaluation paths could not even produce the same message for the same query,
// and neither matched PostgreSQL (ADR-0012 item 1 makes the SQLSTATE and its
// text PostgreSQL's to decide).
//
// Text that is not a number at all is returned unchanged: this function
// renders, it does not validate — the caller has already established that the
// value overflows.
func RealOverflowText(text string) string {
	s := strings.TrimSpace(text)
	if s == "" {
		return text
	}
	sign := ""
	if s[0] == '+' || s[0] == '-' {
		if s[0] == '-' {
			sign = "-"
		}
		s = s[1:]
	}
	mant, exp, ok := splitExponent(s)
	if !ok {
		return text
	}
	intPart, fracPart, ok := splitPoint(mant)
	if !ok {
		return text
	}
	digits := intPart + fracPart
	// point is where the decimal point sits among digits once the exponent has
	// been folded in; it can fall outside the digit string on either side.
	point := len(intPart) + exp
	switch {
	case point >= len(digits):
		digits += strings.Repeat("0", point-len(digits))
		return sign + trimLeadingZeros(digits)
	case point <= 0:
		return sign + "0." + strings.Repeat("0", -point) + digits
	default:
		return sign + trimLeadingZeros(digits[:point]) + "." + digits[point:]
	}
}

// splitExponent splits "12.5e-3" into its mantissa and integer exponent.
func splitExponent(s string) (mant string, exp int, ok bool) {
	i := strings.IndexAny(s, "eE")
	if i < 0 {
		return s, 0, true
	}
	mant, es := s[:i], s[i+1:]
	if es == "" {
		return "", 0, false
	}
	neg := false
	if es[0] == '+' || es[0] == '-' {
		neg = es[0] == '-'
		es = es[1:]
	}
	if es == "" {
		return "", 0, false
	}
	for _, c := range es {
		if c < '0' || c > '9' {
			return "", 0, false
		}
		// Clamped rather than allowed to overflow: an exponent past this is
		// already outside every type's range, and the zero-padding below must
		// not be asked for a string that size.
		if exp < 100000 {
			exp = exp*10 + int(c-'0')
		}
	}
	if neg {
		exp = -exp
	}
	return mant, exp, true
}

// splitPoint splits a mantissa into its integer and fractional digits,
// rejecting anything that is not digits and at most one point.
func splitPoint(mant string) (intPart, fracPart string, ok bool) {
	intPart = mant
	if i := strings.IndexByte(mant, '.'); i >= 0 {
		intPart, fracPart = mant[:i], mant[i+1:]
	}
	if intPart == "" && fracPart == "" {
		return "", "", false
	}
	for _, part := range [2]string{intPart, fracPart} {
		for _, c := range part {
			if c < '0' || c > '9' {
				return "", "", false
			}
		}
	}
	return intPart, fracPart, true
}

// trimLeadingZeros drops insignificant leading zeros, keeping one digit.
func trimLeadingZeros(s string) string {
	i := 0
	for i < len(s)-1 && s[i] == '0' {
		i++
	}
	return s[i:]
}

// RealLitTextUnrepresentable reports whether literal TEXT names a number a
// `real` cannot carry, in either direction — the condition that makes
// PostgreSQL's cast of an IN list to real[] fail with 22003. Both directions:
// 1e40 overflows to +Inf and 1e-46 underflows to 0.0, and each would match
// rows the predicate must not (see kernel.Float32Fit).
//
// Text, not a float64 box: the box has already been through the compiler's
// numeric conversion, and a literal past float64's OWN range (1e400) arrives
// there as +Inf, which is a legal real value and would be waved through.
// Reading the digits keeps "the user wrote a number too big for a real" apart
// from "the user wrote infinity", which PostgreSQL also keeps apart (#549's
// Float32FitOf draws the same distinction for a boxed value).
func RealLitTextUnrepresentable(text string) bool {
	s := strings.TrimSpace(text)
	if s == "" {
		return false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		// Past float64's own range parses as ErrRange with f set to ±Inf,
		// which for THIS question is the right answer: a literal past float64
		// is past real too. Any other parse failure is not a number at all.
		var ne *strconv.NumError
		if !errors.As(err, &ne) || !errors.Is(ne.Err, strconv.ErrRange) {
			return false
		}
	}
	if math.IsInf(f, 0) && !litTextIsInfinity(s) {
		return true
	}
	// BELOW float64's own range there is no error to read: ParseFloat answers
	// a plain 0 for 1e-400, so the box says "zero" — a legal real — where the
	// digits say a non-zero number no float can carry. PostgreSQL refuses
	// `real IN (1e-400, 3.1)` with the same underflow message it gives 1e-46,
	// naming all four hundred digits, so the digits decide here too. This is
	// the small-magnitude mirror of the infinity check above.
	if f == 0 && !litTextIsZero(s) {
		return true
	}
	return Float32FitOf(f) != Float32Fits
}

// litTextIsZero reports whether a numeric literal's own digits are all zeros —
// "0", "0.000", "-0e10" — as opposed to naming a number too small for the
// float64 the text was parsed into.
func litTextIsZero(s string) bool {
	mant, _, ok := splitExponent(strings.TrimLeft(s, "+-"))
	if !ok {
		return false
	}
	return strings.IndexAny(mant, "123456789") < 0
}

// litTextIsInfinity reports whether the text SPELLS infinity, as opposed to
// naming a finite number too large to hold. PostgreSQL accepts 'Infinity' as a
// real and rejects 1e40 as one.
func litTextIsInfinity(s string) bool {
	t := strings.ToLower(strings.TrimLeft(s, "+-"))
	return t == "inf" || t == "infinity"
}
