package batch

import (
	"fmt"
	"math"
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
