package parquet

import (
	"math"
	"math/bits"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The DECIMAL text grammar, and the file writer's checked conversion from a
// boxed value to the UNSCALED integer a DECIMAL leaf holds.
//
// The grammar lives HERE, in the lowest package of the two that need it, for
// the reason ParseDateDays does: `internal/engine/batch` imports this package,
// so this is the only place a single accept-set can sit. batch.DecimalTextAt
// and batch.DecimalSpecialText read through the functions below, so the
// comparison path and the write path cannot disagree about what text names a
// number (ADR-0024 items 4 and 6).
//
// The write path used to be strconv.ParseFloat followed by
// int64(math.Round(t*pow)): a literal wider than the column wrapped the int64
// (99999999999999999999.99 into a DECIMAL(9,2) stored -92233720368547758.08),
// unparseable text and every NaN/Infinity stored 0, ' 3.50 ' stored 0 because
// ParseFloat refuses the surrounding space, and anything past float64's ~16
// significant digits lost its exactness on the way in (#647). ADR-0018's rule
// is that a value this package cannot represent fails the WRITE, where the
// column and the row are still known.

// decimalSpaceCutset is the whitespace PostgreSQL's numeric input function
// strips around a value: C `isspace` in the C locale, not Unicode's set.
// Trimming Unicode space here would accept input PostgreSQL refuses — a
// no-break space before a constant is 22P02 there.
const decimalSpaceCutset = " \t\n\v\f\r"

// maxDecimalExponent bounds the power of ten a literal's exponent contributes.
// Anything at this magnitude already saturates (or truncates to zero) at every
// scale a DECIMAL can declare, so clamping changes no answer and keeps the
// arithmetic below in range.
const maxDecimalExponent = 1 << 30

// maxDecimal128Digits is the widest base-10 magnitude a 128-bit two's
// complement integer can hold: 2^127-1 has 39 digits, so 40 never fits and 39
// has to be checked.
const maxDecimal128Digits = 39

// decimalFLBAMaxWidth is the widest FIXED_LEN_BYTE_ARRAY entry a DECIMAL leaf
// this package reads may carry: sixteen bytes is exactly the two's-complement
// width of the Decimal128 every reader decodes into. A wider entry belongs to
// a precision past 38, which this carrier cannot hold (ADR-0024 item 1).
const decimalFLBAMaxWidth = 16

// DecimalSpecialKind names one of the three values PostgreSQL's `numeric` has
// and this carrier does not: NaN and, since PostgreSQL 14, ±Infinity. A
// 128-bit integer at a fixed scale has no bit pattern for any of them and the
// parquet DECIMAL annotation has none either (ADR-0024 items 1 and 6).
//
// The constants ARE their rank in PostgreSQL's numeric order, which is a total
// order rather than IEEE754's: -Infinity below every finite value, Infinity
// above every finite value, and NaN above Infinity and equal only to itself.
type DecimalSpecialKind int8

const (
	DecimalNegInf DecimalSpecialKind = -1
	DecimalFinite DecimalSpecialKind = 0
	DecimalPosInf DecimalSpecialKind = 1
	DecimalNaN    DecimalSpecialKind = 2
)

// DecimalSpecialText reads PostgreSQL's numeric input grammar for the three
// values above, and returns DecimalFinite for everything else — including text
// that names no number at all, which is DecimalTextParts's question, not this
// one's.
//
// The accept-set is PostgreSQL 17.11's, verified live on postgres:17-alpine:
// surrounding whitespace is stripped; `NaN` is case-insensitive and takes NO
// sign (`+NaN` and `-NaN` are both 22P02 there); `Infinity` and its short form
// `Inf` are case-insensitive and take an optional immediately-adjacent `+` or
// `-`. Nothing else — a prefix (`Infin`, `infinit`), a sign separated by a
// space (`- inf`) and any trailing character (`NaN0`) are all refused.
func DecimalSpecialText(text string) DecimalSpecialKind {
	// batch.CompareDecimalTexts calls this per ROW on the boxed path, where
	// every operand is an ordinary number, so the answer for "not one of the
	// three" has to cost a byte rather than three case-folded comparisons:
	// only 'n', 'i' and their upper-case forms can begin one, after the
	// whitespace and the optional sign.
	if !mayBeDecimalSpecial(text) {
		return DecimalFinite
	}
	s := strings.Trim(text, decimalSpaceCutset)
	signed, neg := false, false
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		signed, neg, s = true, s[0] == '-', s[1:]
	}
	switch {
	case strings.EqualFold(s, "nan"):
		if signed {
			return DecimalFinite // PostgreSQL refuses '+NaN' and '-NaN' outright
		}
		return DecimalNaN
	case strings.EqualFold(s, "infinity"), strings.EqualFold(s, "inf"):
		if neg {
			return DecimalNegInf
		}
		return DecimalPosInf
	}
	return DecimalFinite
}

// mayBeDecimalSpecial is DecimalSpecialText's rejection fast path: it walks
// past leading whitespace and one optional sign and reports whether what
// follows could begin "nan", "inf" or "infinity" in any case. Every digit, '.'
// and every ordinary word answers false on one byte.
func mayBeDecimalSpecial(text string) bool {
	i := 0
	for i < len(text) && isDecimalSpace(text[i]) {
		i++
	}
	if i < len(text) && (text[i] == '+' || text[i] == '-') {
		i++
	}
	if i >= len(text) {
		return false
	}
	switch text[i] {
	case 'n', 'N', 'i', 'I':
		return true
	}
	return false
}

// isDecimalSpace is decimalSpaceCutset by byte, for the fast path above. The
// two must name the same set.
func isDecimalSpace(c byte) bool {
	return strings.IndexByte(decimalSpaceCutset, c) >= 0
}

// DecimalSpecialValueError is the refusal a NaN/±Infinity spelling earns when
// it reaches a caller producing a stored VALUE, and nil for every other text.
//
// The code is 22003 numeric_value_out_of_range, not 22P02: PostgreSQL reads
// all three as `numeric` VALUES, so the text is not an input-syntax error — it
// names a value this carrier has no bit pattern for (ADR-0024 item 6).
// PostgreSQL raises exactly this for the infinities against a constrained
// column ("a field with precision 18, scale 4 cannot hold an infinite value",
// verified live on postgres:17-alpine); NaN it stores, and wadjet refusing it
// is the divergence item 6 records.
func DecimalSpecialValueError(s string) error {
	if DecimalSpecialText(s) == DecimalFinite {
		return nil
	}
	return sqlerr.New("22003",
		"numeric field overflow: %q has no DECIMAL value — PostgreSQL's numeric has NaN and "+
			"±Infinity, and wadjet's DECIMAL is a finite 128-bit unscaled integer with no bit "+
			"pattern for either, so they are COMPARISON literals only and never stored values "+
			"(ADR-0024 item 6)", s)
}

// DecimalTextParts splits numeric TEXT — plain or exponent form — into its
// sign, its digits with the decimal point removed, and the power of ten those
// digits must be multiplied by, exactly and without ever going through a
// float64: the value is `(-1)^neg * digits * 10^exp`.
//
// The exponent is read as an INTEGER and folded into the power of ten, never
// expanded through a float64. Expanding through strconv.ParseFloat is what
// made `1e400` unreadable — ParseFloat reports ErrRange, the old expansion
// gave up and handed the untouched "1e400" to a parser with no exponent
// handling, and that returned the value ZERO, which matched every row holding
// zero (#463). Here 1e400 is simply a number with a large exponent: it
// resolves, saturates for a comparison (#462) and is 22003 for a value.
//
// ok=false means the text names no number. It is deliberately NOT reported as
// the value zero: a constant nobody can read used to compare EQUAL to every
// stored zero (#463), and on the write path it used to be STORED as zero
// (#647), which is the same failure one layer down.
func DecimalTextParts(s string) (neg bool, digits string, exp int, ok bool) {
	// decimalSpaceCutset, never strings.TrimSpace: PostgreSQL's numeric input
	// skips C isspace() only, so a NO-BREAK SPACE (U+00A0) before the digits
	// is a non-whitespace byte it refuses with 22P02. TrimSpace strips it and
	// would have answered the row.
	s = strings.Trim(s, decimalSpaceCutset)
	if s == "" {
		return false, "", 0, false
	}
	switch s[0] {
	case '-':
		neg, s = true, s[1:]
	case '+':
		s = s[1:]
	}
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.Atoi(s[i+1:])
		if err != nil && !isAtoiRangeError(err) {
			return false, "", 0, false
		}
		// A range error keeps Atoi's clamped magnitude, which is already far
		// past anything that changes the answer; clamping again keeps
		// exp+scale from overflowing an int in the caller.
		exp = min(max(e, -maxDecimalExponent), maxDecimalExponent)
		s = s[:i]
	}
	intPart, fracPart, _ := strings.Cut(s, ".")
	if !allDecimalDigits(intPart) || !allDecimalDigits(fracPart) || intPart+fracPart == "" {
		return false, "", 0, false
	}
	return neg, intPart + fracPart, exp - len(fracPart), true
}

// isAtoiRangeError reports the strconv.ErrRange an exponent past an int can
// return. It is spelled out rather than compared with errors.Is so this file
// keeps the same dependency surface as the rest of the package.
func isAtoiRangeError(err error) bool {
	ne, ok := err.(*strconv.NumError)
	return ok && ne.Err == strconv.ErrRange
}

// allDecimalDigits reports whether every byte is 0-9. The EMPTY string is all
// digits — "5." and ".5" are both numbers, and each has one empty part.
// (date_parse.go's allDigits answers false for the empty string, which is the
// right answer for a date field and the wrong one here.)
func allDecimalDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// decimalEffectivePrecision is the bound a DECIMAL column's declared precision
// actually places on a stored value.
//
// `Precision <= 0` is the codebase's "unconstrained" sentinel and a precision
// past 38 cannot be honoured by a 128-bit carrier, so both become 38 — the
// widest bound this package can enforce. Skipping the check for either was the
// older behaviour and it is wrong in the one direction that matters: the
// values a skip admits are exactly the ones with no carrier. DDL refuses a
// precision past 38 outright (ParseDecimalParams); this is the backstop for a
// Column built in Go.
func decimalEffectivePrecision(precision int) int {
	if precision <= 0 || precision > MaxDecimalDigits {
		return MaxDecimalDigits
	}
	return precision
}

// MaxDecimalDigits is the widest DECIMAL precision a 128-bit unscaled carrier
// can hold: 10^38 < 2^127, 10^39 is not. It is batch.MaxDecimalPrecision seen
// from the storage side; the two are asserted equal by
// TestDecimalGrammarMatchesBatch.
const MaxDecimalDigits = 38

// decimalOverflow is PostgreSQL's numeric field overflow, with the DETAIL it
// prints folded into the message (wadjet carries one string, not two).
// PostgreSQL's exponent is precision-scale, and it says "must ROUND to",
// because the scale reduction happens before the bound is applied.
func decimalOverflow(precision, scale int) error {
	return sqlerr.New("22003",
		"numeric field overflow: a field with precision %d, scale %d must round to an "+
			"absolute value less than 10^%d", precision, scale, precision-scale)
}

// DecimalValueFromText resolves numeric TEXT into the UNSCALED value a
// DECIMAL(precision, scale) column stores, exactly and without a float64
// anywhere.
//
// Assignment semantics are PostgreSQL's, verified live on postgres:17-alpine
// against `numeric(9,2)`: surrounding C whitespace is stripped; a literal
// finer than the column's scale ROUNDS half away from zero (1.239 -> 1.24,
// -1.235 -> -1.24) rather than erroring, which is where a value STORE parts
// company with a COMPARISON (batch.DecimalTextAt keeps the dropped digits as a
// residual so a finer literal still orders correctly); the rounded value is
// then held to the declared precision, so 9999999.999 into a DECIMAL(9,2)
// rounds to 10000000.00 and overflows. Text naming no number is 22P02 and NaN
// / ±Infinity are 22003 (ADR-0024 item 6).
func DecimalValueFromText(s string, precision, scale int) (Decimal128, error) {
	if err := DecimalSpecialValueError(s); err != nil {
		return Decimal128{}, err
	}
	neg, digits, exp, ok := DecimalTextParts(s)
	if !ok {
		return Decimal128{}, sqlerr.New("22P02", "invalid input syntax for type numeric: %q", s)
	}
	p := decimalEffectivePrecision(precision)
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return Decimal128{}, nil // zero, at every scale
	}

	// The unscaled value at `scale` is digits x 10^(exp+scale), rounded half
	// away from zero at the point where the digits run out.
	shift := exp + scale
	var kept string
	roundUp := false
	switch {
	case shift >= 0:
		if len(digits)+shift > maxDecimal128Digits {
			return Decimal128{}, decimalOverflow(p, scale)
		}
		kept = digits + strings.Repeat("0", shift)
	default:
		keptLen := len(digits) + shift
		switch {
		case keptLen < 0:
			// Below half a unit at this scale however the digits read: the
			// leading digit is at least one place right of the tenths.
			return Decimal128{}, nil
		case keptLen == 0:
			// 0.d1d2... of a unit: it rounds to one unit iff d1 >= 5.
			kept = ""
			roundUp = digits[0] >= '5'
		default:
			kept, roundUp = digits[:keptLen], digits[keptLen] >= '5'
		}
	}

	hi, lo, ok := decimalMagFromDigits(kept)
	if !ok {
		return Decimal128{}, decimalOverflow(p, scale)
	}
	if roundUp {
		var carry uint64
		lo, carry = bits.Add64(lo, 1, 0)
		hi, carry = bits.Add64(hi, 0, carry)
		if carry != 0 || hi > math.MaxInt64 {
			return Decimal128{}, decimalOverflow(p, scale)
		}
	}
	if !decimalMagFitsPrecision(hi, lo, p) {
		return Decimal128{}, decimalOverflow(p, scale)
	}
	return decimalSigned(hi, lo, neg), nil
}

// DecimalValueFromFloat converts a REAL box through its SHORTEST round-trip
// text and then through DecimalValueFromText, so a float and the literal a
// user typed for it land on the same unscaled integer and are held to the same
// declared precision. Going through math.Round(f * 10^scale) instead lost the
// exactness of everything past ~16 significant digits and wrapped the int64
// past 2^63 with no error at all (#647).
//
// NaN and the infinities are 22003: a DECIMAL column has no bit pattern for
// them (ADR-0024 item 6). They used to store 0.
func DecimalValueFromFloat(f float64, precision, scale int) (Decimal128, error) {
	return decimalValueFromFloatBits(f, 64, precision, scale)
}

// decimalValueFromFloatBits is DecimalValueFromFloat with the width of the box
// the float ARRIVED in. bitSize picks the float32 or the float64 spelling, so
// a REAL holding 0.1 stores as 0.1 and not as the 0.10000000149011612 its
// widening to float64 makes exact — the same rule batch.setCheckedDecimalFloat
// follows for the row-to-batch side of the same conversion.
func decimalValueFromFloatBits(f float64, bitSize, precision, scale int) (Decimal128, error) {
	if math.IsNaN(f) {
		return Decimal128{}, DecimalSpecialValueError("NaN")
	}
	if math.IsInf(f, 1) {
		return Decimal128{}, DecimalSpecialValueError("Infinity")
	}
	if math.IsInf(f, -1) {
		return Decimal128{}, DecimalSpecialValueError("-Infinity")
	}
	return DecimalValueFromText(strconv.FormatFloat(f, 'g', -1, bitSize), precision, scale)
}

// DecimalValueFromUnscaled holds an ALREADY-UNSCALED box to the column's
// declared precision. ADR-0018 §4: an integer box (int, int32, int64,
// Decimal128) is the unscaled value at the column's scale — 3.25 in a
// DECIMAL(9,2) is the int64 325 — because that is what the format stores, what
// Reader.ReadRows hands back and what the engine's decimal vector holds. The
// scale therefore does not enter here; the precision still does, since a
// stored value violating its own column's precision is the assumption the
// set-operation mover relies on when it skips the fit check.
func DecimalValueFromUnscaled(d Decimal128, precision, scale int) (Decimal128, error) {
	hi, lo, ok := decimalMagnitude(d)
	if !ok || !decimalMagFitsPrecision(hi, lo, decimalEffectivePrecision(precision)) {
		return Decimal128{}, decimalOverflow(decimalEffectivePrecision(precision), scale)
	}
	return d, nil
}

// DecimalValueFromBox is the one door every DECIMAL box takes on the way into
// a leaf: it decides which boxes are already unscaled and which carry a
// decimal point, and it reports rather than storing a number the column cannot
// hold.
func DecimalValueFromBox(v any, precision, scale int) (Decimal128, error) {
	switch t := v.(type) {
	case Decimal128:
		return DecimalValueFromUnscaled(t, precision, scale)
	case int:
		return DecimalValueFromUnscaled(Decimal128From(int64(t)), precision, scale)
	case int8:
		return DecimalValueFromUnscaled(Decimal128From(int64(t)), precision, scale)
	case int16:
		return DecimalValueFromUnscaled(Decimal128From(int64(t)), precision, scale)
	case int32:
		return DecimalValueFromUnscaled(Decimal128From(int64(t)), precision, scale)
	case int64:
		return DecimalValueFromUnscaled(Decimal128From(t), precision, scale)
	case uint8:
		return DecimalValueFromUnscaled(Decimal128From(int64(t)), precision, scale)
	case uint16:
		return DecimalValueFromUnscaled(Decimal128From(int64(t)), precision, scale)
	case uint32:
		return DecimalValueFromUnscaled(Decimal128From(int64(t)), precision, scale)
	case float64:
		return DecimalValueFromFloat(t, precision, scale)
	case float32:
		return decimalValueFromFloatBits(float64(t), 32, precision, scale)
	case string:
		return DecimalValueFromText(t, precision, scale)
	default:
		return Decimal128{}, sqlerr.New("22P02",
			"cannot store %T in a DECIMAL column: a DECIMAL value is numeric text, a float, "+
				"or the already-unscaled integer ADR-0018 §4 names", v)
	}
}

// decimalMagFromDigits reads a base-10 MAGNITUDE (no sign, leading zeros
// allowed) into the two words of a 128-bit unsigned value, reporting false
// when it does not fit the 127 bits a signed carrier leaves for it.
func decimalMagFromDigits(digits string) (hi, lo uint64, ok bool) {
	if len(digits) > maxDecimal128Digits {
		return 0, 0, false
	}
	for i := 0; i < len(digits); i++ {
		// (hi:lo) = (hi:lo)*10 + d, refusing anything that leaves the 127
		// bits a two's-complement magnitude has.
		carry, low := bits.Mul64(lo, 10)
		hiHi, hiLo := bits.Mul64(hi, 10)
		if hiHi != 0 {
			return 0, 0, false
		}
		newHi, c1 := bits.Add64(hiLo, carry, 0)
		if c1 != 0 {
			return 0, 0, false
		}
		newLo, c2 := bits.Add64(low, uint64(digits[i]-'0'), 0)
		newHi, c3 := bits.Add64(newHi, 0, c2)
		if c3 != 0 || newHi > math.MaxInt64 {
			return 0, 0, false
		}
		hi, lo = newHi, newLo
	}
	return hi, lo, true
}

// decimalMagnitude returns |d| as two unsigned words, ok=false for -2^127,
// whose magnitude has no 128-bit signed form.
func decimalMagnitude(d Decimal128) (hi, lo uint64, ok bool) {
	if d.Hi >= 0 {
		return uint64(d.Hi), d.Lo, true
	}
	nlo, borrow := bits.Sub64(0, d.Lo, 0)
	nhi, _ := bits.Sub64(0, uint64(d.Hi), borrow)
	if nhi > math.MaxInt64 {
		return 0, 0, false
	}
	return nhi, nlo, true
}

// decimalSigned assembles a magnitude and a sign into two's complement.
func decimalSigned(hi, lo uint64, neg bool) Decimal128 {
	if !neg {
		return Decimal128{Hi: int64(hi), Lo: lo}
	}
	nlo, borrow := bits.Sub64(0, lo, 0)
	nhi, _ := bits.Sub64(0, hi, borrow)
	return Decimal128{Hi: int64(nhi), Lo: nlo}
}

// decimalPow10 is 10^0 .. 10^38 as unsigned word pairs — the EXCLUSIVE bound
// on the unscaled magnitude a DECIMAL(p, s) column may hold.
var decimalPow10 = func() [MaxDecimalDigits + 1][2]uint64 {
	var t [MaxDecimalDigits + 1][2]uint64
	hi, lo := uint64(0), uint64(1)
	for i := range t {
		t[i] = [2]uint64{hi, lo}
		carry, low := bits.Mul64(lo, 10)
		_, hiLo := bits.Mul64(hi, 10)
		hi, lo = hiLo+carry, low
	}
	return t
}()

// decimalMagFitsPrecision reports whether an unscaled MAGNITUDE is below
// 10^precision.
func decimalMagFitsPrecision(hi, lo uint64, precision int) bool {
	if precision <= 0 || precision > MaxDecimalDigits {
		precision = MaxDecimalDigits
	}
	lim := decimalPow10[precision]
	if hi != lim[0] {
		return hi < lim[0]
	}
	return lo < lim[1]
}
