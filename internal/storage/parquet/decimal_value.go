package parquet

import (
	"errors"
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
// The accept-set is PostgreSQL 17.11's for these three spellings, verified
// live on postgres:17-alpine: surrounding whitespace is stripped; `NaN` is
// case-insensitive and takes NO sign (`+NaN` and `-NaN` are both 22P02
// there); `Infinity` and its short form `Inf` are case-insensitive and take an
// optional immediately-adjacent `+` or `-`. Nothing else — a prefix (`Infin`,
// `infinit`), a sign separated by a space (`- inf`) and any trailing character
// (`NaN0`) are all refused.
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

// decimalTextBytes is the two spellings of numeric text this file parses: a
// STRING for a literal a user typed, and a []byte for the shortest round-trip
// rendering of a float box, which is built into a stack buffer and must not
// have to become a string to be read (#647 review). The parser is written once
// over both rather than twice.
type decimalTextBytes interface{ ~string | ~[]byte }

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
//
// The grammar is PostgreSQL's numeric input MINUS digit separators and radix
// prefixes. PostgreSQL 16 added both to numeric_in, so 17.11 accepts `1_000`,
// `1_0.5`, `0x10`, `0b101` and `0o17` (verified live) where this refuses all
// five with 22P02. That gap is #634 and is deferred, not decided here; it is a
// REFUSAL of input PostgreSQL takes, never a different value for input both
// accept, so nothing silently disagrees while it is open.
//
// The digit string is the only allocation in this file's parse, and it happens
// only when a value HAS both an integer and a fraction part; the value builder
// below never asks for it at all (decimalTextSplit).
func DecimalTextParts(s string) (neg bool, digits string, exp int, ok bool) {
	neg, ip, fp, exp, ok := decimalTextSplit(s)
	switch {
	case !ok:
		return false, "", 0, false
	case fp == "":
		return neg, ip, exp, true
	case ip == "":
		return neg, fp, exp, true
	}
	return neg, ip + fp, exp, true
}

// decimalTextSplit is DecimalTextParts with the two digit runs kept apart, so
// nothing has to be concatenated to read them. The logical digit string is
// `ip + fp`, and digitAt indexes it without building it.
func decimalTextSplit[T decimalTextBytes](s T) (neg bool, ip, fp T, exp int, ok bool) {
	// C isspace() only, never unicode.IsSpace: PostgreSQL's numeric input
	// skips exactly this set, so a NO-BREAK SPACE (U+00A0) before the digits
	// is a non-whitespace byte it refuses with 22P02. Trimming the Unicode
	// set would have answered the row.
	i, j := 0, len(s)
	for i < j && isDecimalSpace(s[i]) {
		i++
	}
	for j > i && isDecimalSpace(s[j-1]) {
		j--
	}
	s = s[i:j]
	if len(s) == 0 {
		return false, ip, fp, 0, false
	}
	switch s[0] {
	case '-':
		neg, s = true, s[1:]
	case '+':
		s = s[1:]
	}
	for k := 0; k < len(s); k++ {
		if s[k] == 'e' || s[k] == 'E' {
			e, eok := decimalExponent(s[k+1:])
			if !eok {
				return false, ip, fp, 0, false
			}
			exp, s = e, s[:k]
			break
		}
	}
	dot := -1
	for k := 0; k < len(s); k++ {
		if s[k] == '.' {
			dot = k
			break
		}
	}
	if dot < 0 {
		ip = s
	} else {
		ip, fp = s[:dot], s[dot+1:]
	}
	if !allDecimalDigits(ip) || !allDecimalDigits(fp) || len(ip)+len(fp) == 0 {
		return false, ip, fp, 0, false
	}
	return neg, ip, fp, exp - len(fp), true
}

// decimalExponent reads the integer after an `e`/`E`, clamped to
// ±maxDecimalExponent. It replaces strconv.Atoi so the []byte spelling costs no
// allocation; the accept-set is Atoi's — an optional sign then at least one
// digit, no underscores — and a magnitude past the clamp keeps the clamp,
// which is what Atoi's ErrRange arm did.
func decimalExponent[T decimalTextBytes](s T) (int, bool) {
	i, neg := 0, false
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		neg, i = s[0] == '-', 1
	}
	if i >= len(s) {
		return 0, false
	}
	v := 0
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		if v <= maxDecimalExponent {
			v = v*10 + int(c-'0')
		}
	}
	if v > maxDecimalExponent {
		v = maxDecimalExponent
	}
	if neg {
		v = -v
	}
	return v, true
}

// allDecimalDigits reports whether every byte is 0-9. The EMPTY string is all
// digits — "5." and ".5" are both numbers, and each has one empty part.
// (date_parse.go's allDigits answers false for the empty string, which is the
// right answer for a date field and the wrong one here.)
func allDecimalDigits[T decimalTextBytes](s T) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// digitAt indexes the logical digit string `ip + fp` without building it.
func digitAt[T decimalTextBytes](ip, fp T, i int) byte {
	if i < len(ip) {
		return ip[i]
	}
	return fp[i-len(ip)]
}

// DecimalEntryBytes narrows a big-endian two's-complement DECIMAL entry to the
// sixteen bytes this carrier holds, and reports false when the excess carries
// MAGNITUDE rather than sign.
//
// A foreign writer is free to store a DECIMAL in a wider fixed-length array
// than its values need — the format fixes the width per COLUMN, not per value,
// so a DECIMAL(20,4) leaf declared 32 bytes wide holds 32-byte entries whose
// top sixteen are nothing but the sign repeated. Refusing those outright would
// refuse files whose every value fits (#647 review); refusing only the ones
// that actually need the width is the check the format supports. The excess
// must be all-0x00 over a positive value or all-0xFF over a negative one, and
// the kept bytes must carry the SAME sign, or a byte that was dropped is the
// one expressing it.
func DecimalEntryBytes(b []byte) ([]byte, bool) {
	if len(b) <= decimalFLBAMaxWidth {
		return b, true
	}
	excess := len(b) - decimalFLBAMaxWidth
	pad := byte(0)
	if b[0]&0x80 != 0 {
		pad = 0xff
	}
	for i := 0; i < excess; i++ {
		if b[i] != pad {
			return nil, false
		}
	}
	if (b[excess]&0x80 != 0) != (pad == 0xff) {
		return nil, false
	}
	return b[excess:], true
}

// decimalEntryTooWide is the read refusal an entry earns when its excess bytes
// carry magnitude. It names the width and the bound, and leaves the column and
// the row to the caller that knows them.
func decimalEntryTooWide(n int) error {
	return sqlerr.New("22003",
		"a DECIMAL entry of %d bytes needs more than the %d-byte unscaled integer wadjet's "+
			"DECIMAL is (at most %d digits, ADR-0024 item 1); its leading bytes are not sign "+
			"extension, so reading it would silently drop them",
		n, decimalFLBAMaxWidth, MaxDecimalDigits)
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
	d, err := decimalValueFromText(s, precision, scale)
	if errors.Is(err, errDecimalSyntax) {
		return Decimal128{}, sqlerr.New("22P02", "invalid input syntax for type numeric: %q", s)
	}
	return d, err
}

// errDecimalSyntax is the resolver's "this text names no number", turned into
// PostgreSQL's 22P02 by whichever entry point knows how to SPELL the input.
//
// It is a sentinel rather than the finished message because building that
// message inside the generic resolver puts the input through an `any`, which
// makes it escape — and the float arm's input is a stack buffer that must not
// (#647 review). The refusal is the same one either way.
var errDecimalSyntax = errors.New("numeric input syntax")

// decimalValueFromText is DecimalValueFromText over either spelling, and
// WITHOUT the NaN/±Infinity check: the float arm has already refused those
// three by their bit patterns, and a rendering strconv produced can be nothing
// else. It allocates nothing on the success path.
func decimalValueFromText[T decimalTextBytes](s T, precision, scale int) (Decimal128, error) {
	neg, ip, fp, exp, ok := decimalTextSplit(s)
	if !ok {
		return Decimal128{}, errDecimalSyntax
	}
	p := decimalEffectivePrecision(precision)

	// Skip leading zeros across both runs; what is left is the significant
	// digit string, at index `lead` and `n` digits long.
	total := len(ip) + len(fp)
	lead := 0
	for lead < total && digitAt(ip, fp, lead) == '0' {
		lead++
	}
	n := total - lead
	if n == 0 {
		return Decimal128{}, nil // zero, at every scale
	}

	// The unscaled value at `scale` is those digits x 10^(exp+scale), rounded
	// half away from zero at the point where they run out.
	shift := exp + scale
	keep, zeros, roundUp := n, 0, false
	switch {
	case shift >= 0:
		if n+shift > maxDecimal128Digits {
			return Decimal128{}, decimalOverflow(p, scale)
		}
		zeros = shift
	default:
		keep = n + shift
		switch {
		case keep < 0:
			// Below half a unit at this scale however the digits read: the
			// leading digit is at least one place right of the tenths.
			return Decimal128{}, nil
		case keep == 0:
			// 0.d1d2... of a unit: it rounds to one unit iff d1 >= 5.
			roundUp = digitAt(ip, fp, lead) >= '5'
		default:
			roundUp = digitAt(ip, fp, lead+keep) >= '5'
		}
	}

	hi, lo, ok := decimalMagnitudeOf(ip, fp, lead, keep, zeros)
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
// text and then through the same resolver a literal takes, so a float and the
// literal a user typed for it land on the same unscaled integer and are held
// to the same declared precision. Going through math.Round(f * 10^scale)
// instead lost the exactness of everything past ~16 significant digits and
// wrapped the int64 past 2^63 with no error at all (#647).
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
//
// A RECORDED DIVERGENCE, verified live on postgres:17-alpine: PostgreSQL's
// float8 -> numeric cast renders the float with %.15g, so
// `4611686018427387904::float8::numeric` is 4611686018427390000 there and
// 4611686018427388000 here — wadjet keeps the 17 significant digits that
// identify the float, PostgreSQL keeps 15. Shortest-round-trip is chosen
// deliberately: it is the only rendering that names the float it came from,
// and it is what the row-to-batch twin already does, so the two paths cannot
// disagree about one value. Nothing in SQL reaches this today — a float box
// arrives through the embedded/HTTP API, and `CAST(x AS DECIMAL(p,s))` is
// still ADR-0024 item 6's declared-STRING no-op — so when the CAST evaluator
// lands (#555) it has to decide separately whether the SQL cast follows
// PostgreSQL's %.15g.
//
// The rendering goes into a STACK buffer: strconv.FormatFloat would allocate a
// string per value, and ingest of a float-boxed decimal column is one of these
// per row. 32 bytes covers every shortest 'g' rendering a float64 has (17
// significant digits, a sign, a point and a four-character exponent).
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
	var buf [32]byte
	d, err := decimalValueFromText(strconv.AppendFloat(buf[:0], f, 'g', -1, bitSize), precision, scale)
	if errors.Is(err, errDecimalSyntax) {
		// Unreachable: strconv renders every finite float as a number, and
		// the three that are not finite were refused above. Spelled from the
		// FLOAT rather than from the buffer so the buffer stays on the stack.
		return Decimal128{}, sqlerr.New("22P02", "invalid input syntax for type numeric: %v", f)
	}
	return d, err
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

// decimalMagnitudeOf accumulates the 128-bit unsigned magnitude of `keep`
// digits of the logical string `ip + fp`, starting at index `lead`, followed by
// `zeros` trailing zeros. It reports false when the result leaves the 127 bits
// a two's-complement magnitude has.
//
// It walks the two runs by index rather than taking a digit STRING, because
// building that string was the one allocation on every text box's way into a
// leaf (#647 review).
func decimalMagnitudeOf[T decimalTextBytes](ip, fp T, lead, keep, zeros int) (hi, lo uint64, ok bool) {
	if keep+zeros > maxDecimal128Digits {
		return 0, 0, false
	}
	for i := 0; i < keep+zeros; i++ {
		d := byte('0')
		if i < keep {
			d = digitAt(ip, fp, lead+i)
		}
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
		newLo, c2 := bits.Add64(low, uint64(d-'0'), 0)
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
