package kernel

import (
	"math"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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
// kernel converts the LITERAL (IPv6LitKey, and decimalLiteralAt
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
	case batch.TypeString, batch.TypeTimestamp:
		return v, true

	// The numeric families take one more step for a QUOTED literal, which is
	// TEXT here and a number to the filter: PostgreSQL coerces an unknown-typed
	// literal to the column's own type, so the prune must read it with the same
	// grammar and at the same width the kernel does (#646, and ADR-0018's "a
	// prune must not read the predicate differently from the filter"). A real
	// column's literal is NARROWED before it is widened back for the bound
	// comparison, so the prune keeps the filter's answer bit for bit; a literal
	// the type refuses withholds, which costs a prune and cannot cost a row —
	// and the kernel raises 22P02/22003 for that same literal, which is what
	// lets the refusal actually run.
	//
	// Before this, a quoted literal reached scan.compareValuesOK as a Go string
	// against a numeric bound, which that function declines: safe, but no prune
	// at all for `WHERE f > '3.1'`.
	case batch.TypeInt32, batch.TypeInt64,
		batch.TypePort, batch.TypeProtocol, batch.TypeDuration:
		text, quoted := QuotedConstText(v)
		if !quoted {
			return v, true
		}
		n, st := parseIntText(text)
		if st != NumConstOK {
			return nil, false
		}
		if typ != batch.TypeInt64 && typ != batch.TypeDuration &&
			(n < math.MinInt32 || n > math.MaxInt32) {
			return nil, false
		}
		return n, true
	case batch.TypeFloat32, batch.TypeFloat64:
		text, quoted := QuotedConstText(v)
		if !quoted {
			return v, true
		}
		bits := 64
		if typ == batch.TypeFloat32 {
			bits = 32
		}
		f, st := FloatLitText(text, bits)
		if st != NumConstOK {
			return nil, false
		}
		if bits == 32 {
			return float64(float32(f)), true
		}
		return f, true

	// BOOL stats bounds are Go bools; a SQL text literal has to be read
	// through PostgreSQL's boolean input grammar before it can be compared
	// against them, exactly as the per-row kernel does (#574). Comparing the
	// raw string against a bool bound would prune on garbage. A literal that
	// names no boolean withholds — pruning declines rather than guesses, and
	// the kernel (ResolveFilterKernel) refuses the same literal with 22P02,
	// which is what lets that refusal actually run.
	case batch.TypeBool:
		switch tv := v.(type) {
		case string:
			if b, ok := ParseBoolText(tv); ok {
				return b, true
			}
			return nil, false
		case []byte:
			if b, ok := ParseBoolText(string(tv)); ok {
				return b, true
			}
			return nil, false
		}
		return v, true

	// CIDR converts to its sort key, boxed as parquet.CidrInetBound rather
	// than a plain string (#523): parquet.RowGroupStats hands back a bound
	// in that SAME boxed type — the row-group's inet-order min/max,
	// re-keyed from the TEXT the writer chose using that order — for a file
	// whose footer promises every CIDR value parsed (parquet.
	// CidrStatsOrderKey). An older file, or one this reader cannot even
	// confirm is CIDR, leaves MinValue/MaxValue as plain strings (raw TEXT,
	// exactly as before #523) or withholds them entirely, and
	// scan.compareValuesOK refuses to compare a CidrInetBound against a
	// plain string. That refusal — not a per-file check here, which this
	// function has no way to make — is what lets this conversion return
	// ok=true unconditionally for any address literal: pruning safely
	// declines wherever the bound's box does not prove it is comparable,
	// the same way a NULL-typed comparison declines rather than guesses
	// (ADR-0018 §6).
	//
	// A literal that is not an address at all withholds exactly as before —
	// pruning does not get to decide that; the query does (exec.
	// networkConstError raises 22P02 for the same literal).
	case batch.TypeCIDR:
		s, ok := v.(string)
		if !ok {
			return nil, false
		}
		key, ok := CidrSortKey(s)
		if !ok {
			return nil, false
		}
		// Text stays empty: this is a predicate CONSTANT, not a row, and a
		// literal-side bound is only ever compared, never persisted.
		return parquet.CidrInetBound{Key: key}, true

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
			d, err := parseDateToDays(tv)
			if err != nil {
				// Out of the DATE column's int32 day range (#451): there is
				// no in-range bound to prune with, and using the pre-fix
				// clamped value deleted row groups the filter kept.
				// Withholding is safe — the per-row kernel
				// (ResolveFilterKernel) refuses this same literal with the
				// same SQLSTATE, and no row group being excluded by
				// pruning is what lets that refusal actually run.
				return nil, false
			}
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
		n, ok := parseIPv4ToInt64(s)
		if !ok {
			return nil, false
		}
		return n, true
	case batch.TypeMAC:
		s, ok := v.(string)
		if !ok {
			return v, true
		}
		n, ok := parseMACToInt64(s)
		if !ok {
			return nil, false
		}
		return n, true

	// IPV6 and UUID store the raw 16 bytes. That IS the engine's order for
	// IPv6 — a fixed-width big-endian address, so byte order is address order
	// — which is why this one converts rather than being withheld with CIDR.
	//
	// A v4-SHAPED literal is the exception. IPv6LitKey keys it below every
	// stored address (PostgreSQL compares the FAMILY first), and no 16-byte
	// bound can express "below all of them" the way the empty key does
	// against a value; converting it to its v4-mapped 16 bytes instead —
	// a plain net.ParseIP does — puts it in the MIDDLE of the range,
	// so the prune and the filter would read the same predicate differently.
	// Withheld, which costs a prune and cannot cost a row.
	case batch.TypeIPv6:
		s, ok := v.(string)
		if !ok {
			return nil, false
		}
		key, keyed := IPv6LitKey(s)
		if !keyed || key == "" {
			return nil, false
		}
		return key, true
	case batch.TypeUUID:
		s, ok := v.(string)
		if !ok {
			return nil, false
		}
		raw, ok := parseUUIDToRawString(s)
		if !ok {
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
		// pgIntWhitespace, not strings.TrimSpace: the prune and the filter
		// must read one predicate the same way (ADR-0012 item 6), and the
		// filter's reader (batch.decimalParts) strips PostgreSQL's C
		// whitespace only — a NBSP-prefixed constant is 22P02 there.
		text = strings.Trim(tv, pgIntWhitespace)
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
	// The SIGN goes back on before the parse, not after it. Two's complement
	// is asymmetric — -9223372036854775808 is an int64 and its magnitude is
	// not — so parsing the digits alone and negating withholds exactly the
	// most negative unscaled value a column can hold, which is where a
	// DECIMAL's minimum bound sits.
	digits := intPart + fracPart
	if neg {
		digits = "-" + digits
	}
	unscaled, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		// Past int64 the bound is a FIXED_LEN_BYTE_ARRAY the writer does not
		// emit statistics for, so there is nothing to compare against anyway.
		return nil, false
	}
	return unscaled, true
}
