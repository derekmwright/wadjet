package batch

import (
	"encoding/binary"
	"errors"
	"math"
	"math/big"
	"math/bits"
	"strconv"
	"strings"
)

// Int128 is a 128-bit signed integer used for DECIMAL storage.
// Values are stored as scaled integers: DECIMAL(10,2) value 123.45 → 12345.
type Int128 struct {
	Hi int64  // upper 64 bits (signed)
	Lo uint64 // lower 64 bits (unsigned)
}

// Int128From creates an Int128 from an int64 value.
func Int128From(v int64) Int128 {
	hi := int64(0)
	if v < 0 {
		hi = -1
	}
	return Int128{Hi: hi, Lo: uint64(v)}
}

// Int128FromFloat64 converts a float64 to Int128 with the given scale.
// For example, Int128FromFloat64(123.45, 2) → Int128 representing 12345.
func Int128FromFloat64(f float64, scale int) Int128 {
	scaled := f * math.Pow10(scale)
	// Round to nearest integer
	if scaled >= 0 {
		scaled += 0.5
	} else {
		scaled -= 0.5
	}
	return Int128From(int64(scaled))
}

// ToFloat64 converts an Int128 decimal value to float64 using the given scale.
func (d Int128) ToFloat64(scale int) float64 {
	// For values that fit in int64 (Hi is 0 or -1 sign extension)
	if d.Hi == 0 && d.Lo <= math.MaxInt64 {
		return float64(int64(d.Lo)) / math.Pow10(scale)
	}
	if d.Hi == -1 && d.Lo > math.MaxInt64 {
		return float64(int64(d.Lo)) / math.Pow10(scale)
	}
	// Large values: combine hi and lo
	f := float64(d.Hi)*math.Exp2(64) + float64(d.Lo)
	return f / math.Pow10(scale)
}

// ToInt64 returns the low 64 bits of the unscaled value as an int64. It is
// only the value when the value FITS: Hi is not consulted, so a wider Int128
// comes back truncated and, past 2^63, with the wrong sign. Callers that may
// see a wide value want String, FormatDecimal or BigInt instead — dropping Hi
// here is what made every DECIMAL past 64 bits render as its low half (#434).
func (d Int128) ToInt64() int64 {
	return int64(d.Lo)
}

// FitsInt64 reports whether the value is exactly an int64, i.e. whether Hi is
// nothing but ToInt64's sign extension.
func (d Int128) FitsInt64() bool {
	return (d.Hi == 0 && d.Lo <= math.MaxInt64) || (d.Hi == -1 && d.Lo > math.MaxInt64)
}

// String renders the unscaled value in base 10, exactly, at any width.
func (d Int128) String() string {
	if d.FitsInt64() {
		return strconv.FormatInt(int64(d.Lo), 10)
	}
	return d.BigInt().String()
}

// absDigits returns the base-10 digits of the value's MAGNITUDE, with no sign.
//
// The wide arm goes through big.Int rather than a 128-bit divmod-by-10^19
// loop: this is the row-materialization path, not a kernel, and the narrow
// arm below covers every value a DECIMAL(p <= 18) column can hold — which is
// every value wadjet itself wrote before #429 and all of TPC-H and ClickBench.
// A divmod loop here would be a hundred lines of carry arithmetic to save an
// allocation on the values that are already the rare ones.
func (d Int128) absDigits() string {
	if d.Hi == 0 {
		return strconv.FormatUint(d.Lo, 10)
	}
	// Abs on the big.Int, not Neg on the Int128: -2^127 negates to itself,
	// so its magnitude has no Int128 to be rendered from.
	return new(big.Int).Abs(d.BigInt()).String()
}

// IsZero returns true if the value is zero.
func (d Int128) IsZero() bool {
	return d.Hi == 0 && d.Lo == 0
}

// Neg returns the negation of the value.
func (d Int128) Neg() Int128 {
	lo := ^d.Lo + 1
	hi := ^d.Hi
	if lo == 0 {
		hi++
	}
	return Int128{Hi: hi, Lo: lo}
}

// IsNegative returns true if the value is negative.
func (d Int128) IsNegative() bool {
	return d.Hi < 0
}

// Add returns d + other.
func (d Int128) Add(other Int128) Int128 {
	lo := d.Lo + other.Lo
	hi := d.Hi + other.Hi
	if lo < d.Lo { // carry
		hi++
	}
	return Int128{Hi: hi, Lo: lo}
}

// AddChecked returns d + other and reports whether the EXACT sum fits in an
// Int128. A false second result means the first is the WRAPPED value and must
// not be shown to anyone: it is a different number, silently.
//
// Two's-complement signed addition overflows exactly when the operands share a
// sign and the result does not — the standard rule, and the only one needed
// here because Add is a plain 128-bit add with carry.
//
// SUM over a DECIMAL column accumulates through this (kernel.Accumulator,
// agg_scatter's flat arrays): the aggregate's carrier is 128 bits wide, so a
// DECIMAL(38) column holding values near 10^38 overflows after two rows, and
// the wrapped answer looks like an ordinary number. See docs/adr/0012,
// item 9 (exact numeric aggregates).
func (d Int128) AddChecked(other Int128) (Int128, bool) {
	sum := d.Add(other)
	if (d.Hi < 0) == (other.Hi < 0) && (sum.Hi < 0) != (d.Hi < 0) {
		return sum, false
	}
	return sum, true
}

// Sub returns d - other.
func (d Int128) Sub(other Int128) Int128 {
	return d.Add(other.Neg())
}

// Less returns true if d < other (signed comparison).
func (d Int128) Less(other Int128) bool {
	if d.Hi != other.Hi {
		return d.Hi < other.Hi
	}
	return d.Lo < other.Lo
}

// Equal returns true if d == other.
func (d Int128) Equal(other Int128) bool {
	return d.Hi == other.Hi && d.Lo == other.Lo
}

// Cmp orders two values: -1, 0 or +1 as d is less than, equal to or greater
// than other. Every DECIMAL comparison in the engine bottoms out here.
func (d Int128) Cmp(other Int128) int {
	if d.Less(other) {
		return -1
	}
	if other.Less(d) {
		return 1
	}
	return 0
}

// Int128Max and Int128Min are the widest values the carrier holds. A DECIMAL
// column never reaches them — 10^38 < 2^127 — but a LITERAL in a predicate is
// under no such limit, and the two are what a literal outside the range
// saturates to.
var (
	Int128Max = Int128{Hi: math.MaxInt64, Lo: math.MaxUint64}
	Int128Min = Int128{Hi: math.MinInt64, Lo: 0}
)

// pow10u64 holds the powers of ten that fit in a uint64 (10^19 < 2^64).
var pow10u64 = [20]uint64{
	1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
	1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
}

// MulPow10 returns d x 10^n and reports whether the EXACT product fits in
// Int128. It never returns an approximation: a false second result means the
// caller must take a wider path, not that the first result is close.
//
// Rescaling one operand up to the other's scale is how two DECIMALs of
// different scale are compared exactly (kernel.CompareDecimalAt), which
// matters at a sort-merge join key where the comparator decides EQUALITY:
// float64 rescaling makes 9007199254740993 and 9007199254740992.0 the same
// number and emits a join row for a pair that does not match.
func (d Int128) MulPow10(n int) (Int128, bool) {
	if n < 0 {
		return Int128{}, false
	}
	if n == 0 || d.IsZero() {
		return d, true
	}
	neg := d.IsNegative()
	mag := d
	if neg {
		mag = d.Neg()
		if mag.IsNegative() {
			// -2^127 negates to itself; its magnitude has no Int128, so
			// any multiple of it certainly has none.
			return Int128{}, false
		}
	}
	hi, lo := uint64(mag.Hi), mag.Lo
	for n > 0 {
		k := n
		if k > 19 {
			k = 19
		}
		m := pow10u64[k]
		carryHi, newLo := bits.Mul64(lo, m)
		topHi, midHi := bits.Mul64(hi, m)
		if topHi != 0 {
			return Int128{}, false // product needs more than 128 bits
		}
		newHi, carry := bits.Add64(carryHi, midHi, 0)
		if carry != 0 {
			return Int128{}, false
		}
		hi, lo = newHi, newLo
		n -= k
	}
	if hi&(1<<63) != 0 {
		// Magnitude >= 2^127: outside the signed range (conservatively so
		// for exactly -2^127, whose callers take the wide path instead).
		return Int128{}, false
	}
	res := Int128{Hi: int64(hi), Lo: lo}
	if neg {
		res = res.Neg()
	}
	return res, true
}

// BigInt returns the value as a big.Int: Hi x 2^64 + Lo, exactly. Used on the
// paths where an Int128 is too narrow to hold an intermediate result.
func (d Int128) BigInt() *big.Int {
	b := new(big.Int).SetInt64(d.Hi)
	b.Lsh(b, 64)
	return b.Add(b, new(big.Int).SetUint64(d.Lo))
}

// MaxDecimalScale and MaxDecimalPrecision are the widest DECIMAL an Int128
// carrier can hold: 10^38 < 2^127, 10^39 is not.
const (
	MaxDecimalPrecision = 38
	MaxDecimalScale     = 38
)

// AvgScaleIncrement is how many fractional digits AVG(DECIMAL) adds to its
// input's scale.
//
// The contract (ADR-0012 item 9, the AVG bullet): AVG over a DECIMAL is
// EXACT numeric division rounded half-away-from-zero at scale+4, the rule
// Spark and SQL Server use.
// PostgreSQL instead picks a scale giving at least 16 significant digits (and
// never below the dividend's own scale), so the two agree to at least
// min(both scales) and differ only in how many digits past that they keep.
// A fixed increment is the honest choice for an engine whose numeric carrier
// is 128 bits wide: the digits kept do not depend on the magnitude of the
// answer, so the same query over more rows cannot silently change the scale
// of its own output column.
const AvgScaleIncrement = 4

// AvgScale returns the scale AVG emits over a DECIMAL input of this scale.
func AvgScale(inScale int) int {
	s := inScale + AvgScaleIncrement
	if s > MaxDecimalScale {
		s = MaxDecimalScale
	}
	if s < 0 {
		s = 0
	}
	return s
}

// DecimalAvg divides an exact DECIMAL sum by a row count, returning the
// unscaled quotient at scale+addScale, rounded half away from zero (which is
// what PostgreSQL's numeric rounding does, and what DECIMAL casts here do).
//
// ok=false means the exact quotient has no Int128 — the caller must report an
// error rather than an approximation. That is reachable even when the SUM
// itself fit: scaling by 10^addScale is a multiplication, so a sum near the
// top of the range with a small count has no representable average.
func DecimalAvg(sum Int128, count int64, addScale int) (Int128, bool) {
	if count <= 0 || addScale < 0 {
		return Int128{}, false
	}
	// Fast path: the scaled dividend fits an int64, so the whole division is
	// two machine instructions. count < 2^62 keeps the rounding comparison
	// (2*rem vs count) inside int64 — a row count past that is not reachable.
	if scaled, ok := sum.MulPow10(addScale); ok && scaled.FitsInt64() && count < 1<<62 {
		n := scaled.ToInt64()
		q, rem := n/count, n%count
		if rem < 0 {
			rem = -rem
		}
		if rem*2 >= count {
			if n < 0 {
				q--
			} else {
				q++
			}
		}
		return Int128From(q), true
	}
	// Wide path: exact big.Int division, then the same half-away-from-zero
	// rounding. Reached only by DECIMALs past 18 digits, which is where a
	// float64 quotient would have lost the digits this whole path exists for.
	num := sum.BigInt()
	num.Mul(num, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(addScale)), nil))
	den := big.NewInt(count)
	q, rem := new(big.Int).QuoRem(num, den, new(big.Int))
	rem.Abs(rem)
	if rem.Lsh(rem, 1).Cmp(den) >= 0 {
		if num.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	if !fitsInt128(q) {
		return Int128{}, false
	}
	return int128FromBig(q), true
}

// fitsInt128 reports whether b is representable as a signed 128-bit integer.
func fitsInt128(b *big.Int) bool {
	return b.BitLen() < 128 || (b.Sign() < 0 && b.BitLen() == 128 &&
		b.CmpAbs(new(big.Int).Lsh(big.NewInt(1), 127)) == 0)
}

// FormatDecimal renders the unscaled value as a decimal string at the given
// scale — the text form of a DECIMAL column, and so what GetValue hands the
// row map, ToRows, the JSON encoder and the pgwire text protocol.
//
// The fraction is EXACTLY scale digits, never fewer. It used to be trimmed of
// trailing zeros, so a numeric(9,2) holding -24.50 reached a client as
// "-24.5" and a numeric(38,10) zero as "0.0" (#453). PostgreSQL renders a
// numeric(p,s) at its DECLARED scale always, and ADR-0012 makes PostgreSQL
// the authority — but the deeper reason is that a DECIMAL column exists
// BECAUSE its scale is part of the value's meaning. A currency column that
// spells itself "-24.5" is one a BI tool displays wrong, and any client that
// string-compares or formats from the text gets a different answer than it
// gets from PostgreSQL. The trim also reached the wire's binary form, whose
// dscale header pgNumericDigits counts off this very string.
//
// scale <= 0 renders no point at all — "12345", not "12345." — which is
// PostgreSQL's numeric(p,0) too.
//
// It formats the whole 128 bits. It used to read only Lo, through
// `v := int64(abs.Lo)` and an int64 divmod, which was wrong twice over: a
// value past 64 bits rendered as its low half (Int128{Hi:5, Lo:0x112210f4-
// 7de98115} at scale 10 came out 123456789.0123456789 instead of
// 9346828825.8671214869), and any magnitude with Lo >= 2^63 made that int64
// negative, so the sign leaked into both halves and produced text that is not
// a number at all — "--922337203.-6854775808" for unscaled Int64Min (#434).
//
// Splitting the digit STRING at the scale is also what makes the result exact
// for every scale: math.Pow10 is a float64, and 10^23 has no exact one.
func (d Int128) FormatDecimal(scale int) string {
	if scale <= 0 {
		return d.String()
	}

	digits := d.absDigits()
	intPart, fracStr := "0", digits
	if len(digits) > scale {
		intPart, fracStr = digits[:len(digits)-scale], digits[len(digits)-scale:]
	} else {
		fracStr = strings.Repeat("0", scale-len(digits)) + digits
	}

	if d.IsNegative() {
		return "-" + intPart + "." + fracStr
	}
	return intPart + "." + fracStr
}

// DecimalColumn stores an array of Int128 values for DECIMAL vectors.
type DecimalColumn struct {
	Data  []Int128
	Scale int // number of decimal places
}

// NewDecimalColumn creates a new decimal column with the given capacity and scale.
func NewDecimalColumn(capacity, scale int) DecimalColumn {
	return DecimalColumn{
		Data:  make([]Int128, capacity),
		Scale: scale,
	}
}

// ScaledDecimal is a numeric value resolved into a DECIMAL column's domain:
// the unscaled integer at that column's scale, plus everything the domain
// could not hold. It is the single carrier every DECIMAL comparison converts
// a constant through — the vectorized kernel, the row-at-a-time expression
// and the row-group prune — so that one predicate cannot be read three ways.
type ScaledDecimal struct {
	// Unscaled is the value at the column's scale, truncated toward zero.
	Unscaled Int128
	// Residual is the SIGN of what the truncation dropped: +1 when the true
	// value is strictly above Unscaled, -1 when strictly below, 0 when the
	// conversion was exact. It is what keeps a literal finer than the
	// column's scale in its rational place in the order (ADR-0012 item 6).
	Residual int
	// Sat is 0 when the true value HAS an Int128 at this scale, and +1 / -1
	// when it lies above Int128Max / below Int128Min.
	//
	// A value outside the range orders above (or below) every value the
	// column can hold, which is what it actually is. Narrowing it by
	// two's-complement wraparound instead landed it back INSIDE the ordinary
	// range, so `WHERE d < 1e39` — true of every row — selected none of them
	// (#462).
	Sat int
}

// Order returns -1, 0 or +1 as an unscaled column value at the SAME scale is
// less than, equal to, or greater than this value.
func (s ScaledDecimal) Order(cell Int128) int {
	// Sat is -1, 0 or +1 (see its doc comment), so negating it is the same
	// answer as the two-armed switch this replaced — every representable
	// value is below a saturated maximum (Sat>0 → -1) and above a saturated
	// minimum (Sat<0 → +1) — at a size the compiler will actually inline:
	// the switch cost 83 against Go's 80 inlining budget, so Order stopped
	// inlining into the vectorized DECIMAL filter kernel (measured +39%).
	if s.Sat != 0 {
		return -s.Sat
	}
	if c := cell.Cmp(s.Unscaled); c != 0 {
		return c
	}
	// Equal to what the truncation kept: a positive residual means the true
	// constant is larger than that, so the column's value is smaller.
	return -s.Residual
}

// maxInt128Digits is the widest base-10 magnitude an Int128 can hold: 2^127-1
// has 39 digits, so 40 digits never fits and 39 has to be checked.
const maxInt128Digits = 39

// saturatedDecimal is the ScaledDecimal for a magnitude outside the Int128
// range, on the given side of it.
func saturatedDecimal(neg bool) ScaledDecimal {
	if neg {
		return ScaledDecimal{Unscaled: Int128Min, Sat: -1}
	}
	return ScaledDecimal{Unscaled: Int128Max, Sat: 1}
}

// DecimalTextAt resolves numeric TEXT into a DECIMAL column's domain at
// `scale`, exactly and without ever going through a float64.
//
// ok=false means the text is not a number. It is deliberately NOT reported as
// the value zero: a constant nobody can read used to compare EQUAL to every
// stored zero (#463), which is the most dangerous shape a parse failure can
// take because it neither errors nor returns nothing.
func DecimalTextAt(text string, scale int) (ScaledDecimal, bool) {
	neg, digits, exp, ok := decimalParts(text)
	if !ok {
		return ScaledDecimal{}, false
	}
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return ScaledDecimal{}, true // zero, at every scale
	}
	sign := 1
	if neg {
		sign = -1
	}
	// The unscaled value at `scale` is digits x 10^(exp+scale).
	shift := exp + scale
	residual := 0
	var kept string
	switch {
	case shift >= 0:
		if len(digits)+shift > maxInt128Digits {
			return saturatedDecimal(neg), true
		}
		kept = digits + strings.Repeat("0", shift)
	default:
		drop := -shift
		if drop >= len(digits) {
			// Smaller in magnitude than one unit at this scale: it truncates
			// to zero but is not zero, and the residual is what keeps
			// `> 0.0001` from admitting the row holding 0.00.
			return ScaledDecimal{Residual: sign}, true
		}
		kept, digits = digits[:len(digits)-drop], digits[len(digits)-drop:]
		if strings.Trim(digits, "0") != "" {
			residual = sign
		}
		if len(kept) > maxInt128Digits {
			return saturatedDecimal(neg), true
		}
	}
	v, ok := int128FromDigits(kept, neg)
	if !ok {
		return saturatedDecimal(neg), true
	}
	return ScaledDecimal{Unscaled: v, Residual: residual}, true
}

// CompareDecimalTexts orders two numeric TEXTS as the exact numbers they name,
// returning -1, 0 or +1, and ok=false when either is not a number.
//
// No scale, no carrier, no float: this is the comparison for two values whose
// only lossless form is their text — a DECIMAL column rendered by
// FormatDecimal against a literal too wide for the float64 box the compiler
// built for it (ADR-0012 item 6). It compares the power of ten of the leading
// digit first and the digit strings after, so it is exact at any width and
// allocates nothing.
func CompareDecimalTexts(a, b string) (int, bool) {
	aNeg, aDigits, aExp, aOK := decimalParts(a)
	bNeg, bDigits, bExp, bOK := decimalParts(b)
	if !aOK || !bOK {
		return 0, false
	}
	aDigits, aExp = trimDecimalDigits(aDigits, aExp)
	bDigits, bExp = trimDecimalDigits(bDigits, bExp)
	switch {
	case aDigits == "" && bDigits == "":
		return 0, true // both zero, whatever their spelling or sign
	case aDigits == "":
		if bNeg {
			return 1, true
		}
		return -1, true
	case bDigits == "":
		if aNeg {
			return -1, true
		}
		return 1, true
	case aNeg != bNeg:
		if aNeg {
			return -1, true
		}
		return 1, true
	}
	c := compareDecimalMagnitudes(aDigits, aExp, bDigits, bExp)
	if aNeg {
		c = -c
	}
	return c, true
}

// trimDecimalDigits strips a magnitude's leading zeros and folds its trailing
// zeros into the exponent, so "1.50" and "1.5000" reach the comparison in the
// same shape.
func trimDecimalDigits(digits string, exp int) (string, int) {
	digits = strings.TrimLeft(digits, "0")
	for len(digits) > 0 && digits[len(digits)-1] == '0' {
		digits = digits[:len(digits)-1]
		exp++
	}
	return digits, exp
}

// compareDecimalMagnitudes orders two NON-ZERO magnitudes, each given as
// leading-and-trailing-zero-free digits times 10^exp.
func compareDecimalMagnitudes(aDigits string, aExp int, bDigits string, bExp int) int {
	// The power of ten of the most significant digit decides first.
	if ah, bh := len(aDigits)+aExp, len(bDigits)+bExp; ah != bh {
		if ah < bh {
			return -1
		}
		return 1
	}
	n := max(len(aDigits), len(bDigits))
	for i := 0; i < n; i++ {
		x, y := byte('0'), byte('0')
		if i < len(aDigits) {
			x = aDigits[i]
		}
		if i < len(bDigits) {
			y = bDigits[i]
		}
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}

// decimalParts splits numeric text — plain or exponent form — into its sign,
// its digits with the decimal point removed, and the power of ten those
// digits must be multiplied by. ok=false for anything that is not a number.
//
// The exponent is read as an INTEGER and folded into the power of ten, never
// expanded through a float64. Expanding through strconv.ParseFloat is what
// made `1e400` unreadable — ParseFloat reports ErrRange, the old expansion
// gave up and handed the untouched "1e400" to a parser with no exponent
// handling, and that returned the value ZERO, which matched every row holding
// zero (#463). Here 1e400 is simply a number with a large exponent: it
// resolves, saturates, and orders above everything (#462).
func decimalParts(s string) (neg bool, digits string, exp int, ok bool) {
	s = strings.TrimSpace(s)
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
		if err != nil && !errors.Is(err, strconv.ErrRange) {
			return false, "", 0, false
		}
		// ErrRange keeps Atoi's clamped magnitude, which is already far past
		// anything that changes the answer; clamping again keeps exp+scale
		// from overflowing an int in the caller.
		exp = min(max(e, -maxDecimalExponent), maxDecimalExponent)
		s = s[:i]
	}
	intPart, fracPart, _ := strings.Cut(s, ".")
	if !allDigits(intPart) || !allDigits(fracPart) || intPart+fracPart == "" {
		return false, "", 0, false
	}
	return neg, intPart + fracPart, exp - len(fracPart), true
}

// maxDecimalExponent bounds the power of ten a literal's exponent contributes.
// Anything at this magnitude already saturates (or truncates to zero) at every
// scale a DECIMAL can declare, so clamping changes no answer and keeps the
// arithmetic below in range.
const maxDecimalExponent = 1 << 30

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// int128FromDigits reads a base-10 MAGNITUDE (no sign, no leading zeros
// required) into a signed Int128, reporting false when it does not fit.
func int128FromDigits(digits string, neg bool) (Int128, bool) {
	// 19 digits is the widest magnitude that certainly fits a uint64, and it
	// covers every DECIMAL(p <= 18) column.
	if len(digits) <= 19 {
		u, err := strconv.ParseUint(digits, 10, 64)
		if err != nil {
			return Int128{}, false
		}
		v := Int128{Lo: u}
		if neg {
			v = v.Neg()
		}
		return v, true
	}
	b, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return Int128{}, false
	}
	if neg {
		b.Neg(b)
	}
	if !fitsInt128(b) {
		return Int128{}, false
	}
	return int128FromBig(b), true
}

// ParseDecimalString parses a decimal string like "123.45" into an Int128 at
// the given scale, truncating toward zero. Text that is not a number reads as
// zero here — callers that must tell "not a number" from "the value zero"
// take DecimalTextAt, which is the same conversion with that answer kept.
func ParseDecimalString(s string, scale int) Int128 {
	d, ok := DecimalTextAt(s, scale)
	if !ok {
		return Int128{}
	}
	return d.Unscaled
}

// int128FromBig narrows a big.Int to Int128, SATURATING at the range's ends.
//
// It used to wrap two's complement, on the argument that no DECIMAL(38) value
// can overflow — true of a column's values, false of a literal compared
// against one, and a wrapped literal reappears inside the ordinary range as a
// perfectly plausible number of the wrong sign (#462). Saturation keeps it on
// the correct side of every value the column holds.
func int128FromBig(b *big.Int) Int128 {
	if !fitsInt128(b) {
		if b.Sign() < 0 {
			return Int128Min
		}
		return Int128Max
	}
	neg := b.Sign() < 0
	mag := new(big.Int).Abs(b)
	lo := new(big.Int).And(mag, new(big.Int).SetUint64(^uint64(0))).Uint64()
	hi := new(big.Int).Rsh(mag, 64).Uint64()
	out := Int128{Hi: int64(hi), Lo: lo}
	if neg {
		out = out.Neg()
	}
	return out
}

// --- Canonical DECIMAL key encoding ---
//
// A DECIMAL group / DISTINCT / join / bloom key used to be
// `math.Float64bits(v.ToFloat64(scale))`. A float64 carries ~16 significant
// decimal digits and a DECIMAL(38,10) carries 38, so every pair of values that
// agrees to 16 digits shared one key: GROUP BY collapsed them into one group,
// COUNT(DISTINCT) counted them once, and a hash join matched each against the
// other (#474).
//
// Keying on the raw 16 bytes of the unscaled Int128 is the wrong repair: it
// makes the key depend on the SCALE, so 12.75 stored in a DECIMAL(9,2)
// (unscaled 1275) would stop matching 12.75 stored in a DECIMAL(18,4)
// (unscaled 127500) — and a join between two tables that declare the same
// quantity at different scales is exactly the shape that breaks. The
// comparator (kernel.CompareDecimalAt) calls those two equal, and ADR-0012
// item 8's invariant is that two values the comparator calls equal must also
// SERIALIZE alike.
//
// So the key is the value's canonical form: the unique (unscaled, scale) pair
// with scale >= 0 minimal, i.e. trailing zero digits stripped from the
// fraction. 12.75 is (1275, 2) from either column; 12.7500 normalizes to it;
// 1200 at scale 2 (unscaled 120000) and 1200 at scale 0 both normalize to
// (1200, 0); zero is (0, 0) at every scale. That form is unique per VALUE, so
// the encoding is injective by construction — different values cannot collide,
// whatever their declared precision.

// decimalKeyNegative is the sign bit in a canonical DECIMAL key's second byte.
const decimalKeyNegative = 0x80

// MaxDecimalKeyLen is the widest AppendDecimalKey output: a scale byte, a
// sign/length byte, and up to 16 magnitude bytes.
const MaxDecimalKeyLen = 18

// AppendDecimalKey appends the canonical, scale-normalized binary key for the
// DECIMAL value `d` held at `scale`, and returns the extended buffer.
//
// The encoding is
//
//	[normalized scale : 1 byte][sign | magnitude length : 1 byte][magnitude : N bytes]
//
// with the magnitude in minimal-width BIG-endian bytes (no leading zero byte)
// and the sign in the high bit of the length byte. It is self-delimiting — the
// length byte says how many follow — which is what lets it sit inside a
// multi-column key or a nested container element without a separator, exactly
// like the fixed-width arms it joins.
//
// Minimal width rather than a flat 16 bytes because these keys are stored per
// group and per build row: a DECIMAL(9,2) price keys in 4 bytes, so the whole
// key of a single-DECIMAL GROUP BY still fits the 8-byte compact path.
func AppendDecimalKey(buf []byte, d Int128, scale int) []byte {
	if scale < 0 {
		scale = 0
	}
	hi, lo, neg := d.magnitude()
	if hi == 0 && lo == 0 {
		// Zero is one value at every scale, so it is one key.
		return append(buf, 0, 0)
	}
	// Strip trailing zero digits so the scale is minimal. The narrow arm is
	// every value a DECIMAL(p <= 19) column can hold and needs no 128-bit
	// division; both arms exit on the first non-zero last digit, which is the
	// common case for a value that is not padded.
	if hi == 0 {
		for scale > 0 && lo%10 == 0 {
			lo /= 10
			scale--
		}
	} else {
		for scale > 0 {
			qhi := hi / 10
			qlo, rem := bits.Div64(hi%10, lo, 10)
			if rem != 0 {
				break
			}
			hi, lo = qhi, qlo
			scale--
		}
	}

	var mag [16]byte
	binary.BigEndian.PutUint64(mag[0:8], hi)
	binary.BigEndian.PutUint64(mag[8:16], lo)
	first := 0
	for mag[first] == 0 {
		first++ // terminates: the value is not zero
	}
	sl := byte(16 - first)
	if neg {
		sl |= decimalKeyNegative
	}
	buf = append(buf, byte(scale), sl)
	return append(buf, mag[first:]...)
}

// magnitude returns |d| as an unsigned 128-bit pair plus the sign. Two's
// complement negation of -2^127 wraps to itself, whose BITS are the correct
// unsigned magnitude 2^127 — so reading the negated halves as unsigned is
// right for every Int128 including that one, where Neg alone is not.
func (d Int128) magnitude() (hi, lo uint64, neg bool) {
	if d.Hi < 0 {
		n := d.Neg()
		return uint64(n.Hi), n.Lo, true
	}
	return uint64(d.Hi), d.Lo, false
}
