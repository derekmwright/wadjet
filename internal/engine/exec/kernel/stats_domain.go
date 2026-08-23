package kernel

import (
	"math"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// StatsDomainValue converts a SQL literal into the representation a column's
// parquet STATISTICS and DICTIONARY entries are in, and reports whether the
// conversion exists.
//
// It is the producer half of the rule the prune layer cannot enforce for
// itself: `scan.CanPruneRowGroup` compares two `any` values by their Go kind
// and has no idea what either MEANS, so a raw file bound and an engine literal
// that both land in the same kind get compared as if they agreed. Three
// columns did exactly that (#442, and #438 which is the same defect seen
// through a DECIMAL):
//
//	DECIMAL(18,4)  stats hold the UNSCALED integer (1500.15 -> 15001500)
//	               and the literal arrives as float64(1500.15), so every row
//	               group whose unscaled bound exceeds the literal is pruned.
//	IPV6, UUID     stats hold the RAW 16 bytes and the literal arrives as
//	               text, and '2' (0x32) sorts above every byte of a
//	               2001:db8:: address, so every row group is pruned.
//
// The engine's own order for those columns is the stored one — the filter
// kernel converts the LITERAL (parseIPv6ToRawString, and decimalLiteralAt
// against the vector's scale) rather than rendering the column — so this
// function is that same conversion, hoisted to where the planner still knows
// the column's type and scale. Rendering the bounds the other way would be
// wrong for IPv6: text order is not address order ('2001:db8::10' sorts below
// '2001:db8::5').
//
// A false second result means "no conversion" and the caller must WITHHOLD the
// predicate from the prune layer entirely. Every type is listed explicitly and
// there is no pass-through default, because a new type that silently inherited
// "compare it raw" is precisely how this class arrives.
func StatsDomainValue(typ batch.TypeID, scale int, v any) (any, bool) {
	if v == nil {
		return nil, false
	}
	switch typ {
	// Already in the stats domain: the physical value IS the engine value and
	// the literal already arrives as one. (TIMESTAMP's bounds are rescaled to
	// engine milliseconds by the reader, at the one place that still holds the
	// leaf's unit — see parquet.RowGroupStats.)
	case batch.TypeBool, batch.TypeInt32, batch.TypeInt64,
		batch.TypeFloat32, batch.TypeFloat64,
		batch.TypeString, batch.TypeCIDR,
		batch.TypePort, batch.TypeProtocol,
		batch.TypeDuration, batch.TypeTimestamp:
		return v, true

	// BYTES compares by bytes and a []byte literal has to become the string
	// the stats decode to.
	case batch.TypeBytes:
		switch tv := v.(type) {
		case string:
			return tv, true
		case []byte:
			return string(tv), true
		}
		return nil, false

	// DATE stores days since epoch; the literal is usually text.
	case batch.TypeDate:
		switch tv := v.(type) {
		case int64:
			return tv, true
		case int32:
			return int64(tv), true
		case int:
			return int64(tv), true
		case string:
			d := parseDateToDays(tv)
			if d == 0 && tv != "1970-01-01" {
				return nil, false
			}
			return int64(d), true
		}
		return nil, false

	// IPV4 and MAC store an integer; the literal is text. These two never
	// produced a WRONG answer (an int64 bound against a text literal is
	// refused by the prune layer), but they never pruned either.
	case batch.TypeIPv4:
		s, ok := v.(string)
		if !ok {
			return v, true // already numeric
		}
		n := parseIPv4ToInt64(s)
		if n == 0 && s != "0.0.0.0" {
			return nil, false
		}
		return n, true
	case batch.TypeMAC:
		s, ok := v.(string)
		if !ok {
			return v, true
		}
		n := parseMACToInt64(s)
		if n == 0 && s != "00:00:00:00:00:00" {
			return nil, false
		}
		return n, true

	// IPV6 and UUID store the raw 16 bytes.
	case batch.TypeIPv6:
		s, ok := v.(string)
		if !ok {
			return nil, false
		}
		raw := parseIPv6ToRawString(s)
		if raw == "" {
			return nil, false
		}
		return raw, true
	case batch.TypeUUID:
		s, ok := v.(string)
		if !ok {
			return nil, false
		}
		raw := parseUUIDToRawString(s)
		if raw == "" {
			return nil, false
		}
		return raw, true

	// DECIMAL stores the unscaled integer at the column's scale.
	case batch.TypeDecimal:
		return decimalStatsValue(v, scale)

	// No scalar bound: a container's statistics belong to its LEAVES, which
	// are not this column.
	case batch.TypeArray, batch.TypeRow, batch.TypeMap, batch.TypeVector:
		return nil, false
	}
	return nil, false
}

// decimalStatsValue renders a DECIMAL literal as the unscaled integer the
// column's statistics and dictionary hold, and only when that integer is
// EXACT.
//
// Exactness is the whole soundness argument. A literal the column's scale
// cannot represent (0.005 in a DECIMAL(9,2)) has no unscaled integer, and any
// rounding of it would be a bound the row group's real values can sit on the
// wrong side of. Withholding costs a prune; guessing costs rows.
//
// The reading of an INTEGER literal is the KERNEL's, not the writer's:
// `WHERE v = 3` on a DECIMAL(9,2) means the number three, unscaled 300 — the
// same thing decimalLiteralText says, and deliberately not ADR-0018 §4's
// writer rule, where an integer BOX is already unscaled.
func decimalStatsValue(v any, scale int) (any, bool) {
	if scale < 0 {
		return nil, false
	}
	var text string
	switch tv := v.(type) {
	case int64:
		text = strconv.FormatInt(tv, 10)
	case int32:
		text = strconv.FormatInt(int64(tv), 10)
	case int:
		text = strconv.FormatInt(int64(tv), 10)
	case float64:
		if math.IsNaN(tv) || math.IsInf(tv, 0) {
			return nil, false
		}
		text = strconv.FormatFloat(tv, 'f', -1, 64)
	case float32:
		if math.IsNaN(float64(tv)) || math.IsInf(float64(tv), 0) {
			return nil, false
		}
		text = strconv.FormatFloat(float64(tv), 'f', -1, 32)
	case string:
		text = strings.TrimSpace(tv)
	default:
		return nil, false
	}

	neg := false
	switch {
	case strings.HasPrefix(text, "-"):
		neg, text = true, text[1:]
	case strings.HasPrefix(text, "+"):
		text = text[1:]
	}
	intPart, fracPart, _ := strings.Cut(text, ".")
	if intPart == "" && fracPart == "" {
		return nil, false
	}
	for _, part := range []string{intPart, fracPart} {
		for i := 0; i < len(part); i++ {
			if part[i] < '0' || part[i] > '9' {
				return nil, false // exponent form, or not a number at all
			}
		}
	}
	switch {
	case len(fracPart) < scale:
		fracPart += strings.Repeat("0", scale-len(fracPart))
	case len(fracPart) > scale:
		if strings.Trim(fracPart[scale:], "0") != "" {
			return nil, false // more decimals than the column can hold
		}
		fracPart = fracPart[:scale]
	}
	unscaled, err := strconv.ParseInt(intPart+fracPart, 10, 64)
	if err != nil {
		// Past int64 the bound is a FIXED_LEN_BYTE_ARRAY the writer does not
		// emit statistics for, so there is nothing to compare against anyway.
		return nil, false
	}
	if neg {
		unscaled = -unscaled
	}
	return unscaled, true
}
