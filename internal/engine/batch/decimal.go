package batch

import (
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

// FormatDecimal renders the unscaled value as a decimal string at the given
// scale — the text form of a DECIMAL column, and so what GetValue hands the
// row map, ToRows, the JSON encoder and the pgwire text protocol.
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

	// Trailing zeros are trimmed, but never all of them: 3.00 at scale 2 is
	// "3.0", not "3." — long-standing behavior the wire format depends on.
	fracStr = strings.TrimRight(fracStr, "0")
	if fracStr == "" {
		fracStr = "0"
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

// ParseDecimalString parses a decimal string like "123.45" into an Int128 with the given scale.
func ParseDecimalString(s string, scale int) Int128 {
	s = strings.TrimSpace(s)
	neg := false
	if len(s) > 0 && s[0] == '-' {
		neg = true
		s = s[1:]
	} else if len(s) > 0 && s[0] == '+' {
		s = s[1:]
	}

	parts := strings.SplitN(s, ".", 2)
	intStr := parts[0]
	fracStr := ""
	if len(parts) == 2 {
		fracStr = parts[1]
	}

	// Pad or truncate fractional part to match scale
	if len(fracStr) < scale {
		fracStr += strings.Repeat("0", scale-len(fracStr))
	} else if len(fracStr) > scale {
		fracStr = fracStr[:scale]
	}

	combined := intStr + fracStr
	if combined == "" {
		return Int128{}
	}

	// The narrow arm is every value a DECIMAL(p <= 18) column can hold. The
	// wide one is why this is not a Sscanf into an int64 any more: that
	// returned an error nobody read and left the value at whatever it had
	// parsed, so a 20-digit unscaled value — which #429 made wadjet write
	// itself — round-tripped through FormatDecimal as zero or as its prefix.
	if v, err := strconv.ParseInt(combined, 10, 64); err == nil {
		result := Int128From(v)
		if neg {
			result = result.Neg()
		}
		return result
	}
	b, ok := new(big.Int).SetString(combined, 10)
	if !ok {
		return Int128{}
	}
	if neg {
		b.Neg(b)
	}
	return int128FromBig(b)
}

// int128FromBig narrows a big.Int to Int128, wrapping (two's complement) if it
// does not fit — which no DECIMAL(38) value can do, since 10^38 < 2^127.
func int128FromBig(b *big.Int) Int128 {
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
