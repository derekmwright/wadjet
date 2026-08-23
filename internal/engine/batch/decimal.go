package batch

import (
	"fmt"
	"math"
	"math/big"
	"math/bits"
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

// ToInt64 returns the unscaled int64 value. Only valid if the value fits.
func (d Int128) ToInt64() int64 {
	return int64(d.Lo)
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

// FormatDecimal formats the Int128 as a decimal string with the given scale.
func (d Int128) FormatDecimal(scale int) string {
	if scale <= 0 {
		return fmt.Sprintf("%d", d.ToInt64())
	}

	neg := d.IsNegative()
	abs := d
	if neg {
		abs = d.Neg()
	}

	// For values that fit in int64
	v := int64(abs.Lo)
	divisor := int64(math.Pow10(scale))

	intPart := v / divisor
	fracPart := v % divisor

	fracStr := fmt.Sprintf("%0*d", scale, fracPart)
	// Trim trailing zeros
	fracStr = strings.TrimRight(fracStr, "0")
	if fracStr == "" {
		fracStr = "0"
	}

	prefix := ""
	if neg {
		prefix = "-"
	}

	return fmt.Sprintf("%s%d.%s", prefix, intPart, fracStr)
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
	var v int64
	fmt.Sscanf(combined, "%d", &v)

	result := Int128From(v)
	if neg {
		result = result.Neg()
	}
	return result
}
