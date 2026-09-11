package kernel

import (
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// Quoted unknown literals take the numeric operand's OWN input grammar and
// width at every comparison site, without widening (#646).
// Unquoted numeric literals are distinct: REAL scalar comparisons widen to
// double (#631); quoted REAL comparisons narrow. Keep both kernel paths.
// Literal Go string versus numeric box records source spelling, not a
// value-sniffing order rule (ADR-0012 item 8).
// Unparseable text must classify/refuse, never become a zero sentinel
// (#463, #536, #574).
// See docs/internals/kernel-quoted-numeric-literal-domain.md for the design.

// NumConstStatus classifies a numeric-column filter constant. It is
// IntConstStatus under the name the whole numeric family shares: #536
// introduced the three-way split for the integer types and the float and
// DECIMAL arms need exactly the same three answers, so they are one type
// rather than two that must be kept in step.
//
// NumConstOK: a usable value. NumConstSyntax: the text names no value of the
// type at all (PostgreSQL raises 22P02, invalid_text_representation).
// NumConstRange: it names a number the type cannot carry (22003,
// numeric_value_out_of_range) — a DIFFERENT SQLSTATE with different wording,
// which the WireProtocol oracle checks.
type NumConstStatus = IntConstStatus

const (
	NumConstOK     = IntConstOK
	NumConstSyntax = IntConstSyntax
	NumConstRange  = IntConstRange
)

// QuotedConstText reports a filter constant's TEXT when the constant is a
// QUOTED literal (or a text-shaped parameter), and ok=false for a numeric box.
//
// This is the whole of the quoted-versus-numeric distinction the rule turns
// on, and it is deliberately a test of the BOX rather than of a declaration:
// the planner boxes a quoted string literal as a Go string and an unquoted
// numeric literal as a float64/int64/int (see exec.decimalLitValue, which
// substitutes a numeric literal's source text ONLY for the DECIMAL and STRING
// column types), so "which spelling did the user write" is exactly what the
// box carries here and nothing else does.
func QuotedConstText(v any) (string, bool) {
	switch tv := v.(type) {
	case string:
		return tv, true
	case []byte:
		return string(tv), true
	}
	return "", false
}

// NumericTypeName is the PostgreSQL type name a numeric column's refusal
// message carries. ok=false for every type with no literal rule of this kind.
//
// The wadjet-native PORT/PROTOCOL/DURATION have no PostgreSQL equivalent, so
// they name themselves; the rest use PostgreSQL's own spelling, which the
// pg-oracle's wire arm checks byte-for-byte.
func NumericTypeName(typ batch.TypeID) (string, bool) {
	switch typ {
	case batch.TypeInt64:
		return "bigint", true
	case batch.TypeInt32:
		return "integer", true
	case batch.TypePort, batch.TypeProtocol:
		// The DECLARED wire type, not the internal one. These columns declare
		// OID 23 (int4) since #834, so `port_col = 'abc'` has to say
		// "integer" — the name a client can look up in pg_type and the name
		// PostgreSQL itself uses for the same refusal against an int4 column.
		// It said "port", a type name no client can resolve.
		return "integer", true
	case batch.TypeDuration:
		// OID 20 (int8), nanoseconds — see pgwire.pgTypeOID and ADR-0012.
		return "bigint", true
	case batch.TypeFloat32:
		return "real", true
	case batch.TypeFloat64:
		return "double precision", true
	case batch.TypeDecimal:
		return "numeric", true
	case batch.TypeIPv4, batch.TypeIPv6:
		// PostgreSQL's own name for both: it has one `inet` where this engine
		// has two bare-address types, and a client can look `inet` up.
		return "inet", true
	case batch.TypeBytes:
		return "bytea", true
	case batch.TypeCIDR:
		// `inet`, not `cidr`, and for the same reason the two bare-address
		// types answer `inet`: it is the type PostgreSQL reads the literal
		// with, and its own message says so — `cd = '239'` there is
		// `invalid input syntax for type inet: "239"`. It is also the type a
		// wadjet CIDR column IS: this engine's differential oracle maps it to
		// `inet` because a wadjet CIDR holds host bits under a mask
		// (`'192.168.5.7/24'`), which PostgreSQL's `cidr` refuses outright.
		// Naming `cidr` sent a client to a type whose grammar would not
		// explain the refusal (round-3 review P-E).
		return "inet", true
	case batch.TypeMAC:
		return "macaddr", true
	case batch.TypeUUID:
		return "uuid", true
	}
	return "", false
}

// QuotedLitStatus is THE predicate: does text name a value of the column type
// typ? ok=false means the type has no rule of this kind and the caller must
// not refuse anything.
//
// Every site that can refuse a quoted literal against a numeric column reads
// this one function — the plan-time refusal (physical.refuseLiteralForType via
// expr.RefuseNumericLiteral), the vectorized kernel's arms, the row-at-a-time
// ColumnCompareLit, the boxed sites' refusal masks, and the row-group prune's
// StatsDomainValue — so the accept-set cannot differ between them. A query
// refused at one site and answered at another is the two-path defect class the
// refusal exists to close.
func QuotedLitStatus(typ batch.TypeID, text string) (NumConstStatus, bool) {
	switch typ {
	case batch.TypeInt64, batch.TypeDuration:
		_, st := parseIntText(text)
		return st, true
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol:
		n, st := parseIntText(text)
		if st == NumConstOK && (n < math.MinInt32 || n > math.MaxInt32) {
			return NumConstRange, true
		}
		return st, true
	case batch.TypeFloat64:
		_, st := FloatLitText(text, 64)
		return st, true
	case batch.TypeFloat32:
		_, st := FloatLitText(text, 32)
		return st, true
	case batch.TypeDecimal:
		// The DECIMAL grammar is its own (ADR-0024 item 6): it accepts NaN and
		// the infinities as unsigned/adjacent-signed BOUNDS, refuses a SIGNED
		// NaN that float8 accepts, and never reports a RANGE failure — a
		// literal past the Int128 carrier SATURATES into its place in the
		// order rather than erroring (#462). One reader, so the plan-time and
		// runtime refusals cannot disagree.
		if NewDecimalLiteral(text).Numeric() {
			return NumConstOK, true
		}
		return NumConstSyntax, true
	case batch.TypeIPv4, batch.TypeIPv6:
		// The bare-address types. Their literal has TWO ways of not being a
		// value this column can hold, and they are different answers:
		//
		//   'zzz'      names no address at all               22P02
		//   '10/8'     names a NETWORK, which PostgreSQL accepts and a
		//              bare-address column has no room for  0A000
		//
		// The second is NetworkPrefixLiteral's, not this function's — but the
		// arm has to exist here so a literal reaches a decision at PLAN time
		// at all. Without it the refusal lived in ONE evaluator
		// (exec.networkConstError, reachable only when the vectorized filter
		// declines to build a kernel), so the same query refused in a WHERE,
		// answered inside a CASE, and on the DAG answered a WRONG NUMBER —
		// the widened parser's zero reading. One classification, at typing
		// time, for every arm and every site (#627 round 2, B1).
		if NetworkPrefixLiteral(typ, text) {
			return NumConstOK, true // NOT a syntax error; see RefuseNetworkPrefixLiteral
		}
		if typ == batch.TypeIPv4 {
			if _, ok := IPv4LitKey(text); ok {
				return NumConstOK, true
			}
		} else if _, ok := IPv6LitKey(text); ok {
			return NumConstOK, true
		}
		return NumConstSyntax, true
	case batch.TypeBytes:
		// byteain's accept-set, asked where every other type's is asked. The
		// four sites that read a bytea literal all fell back to its RAW
		// SPELLING when it could not be decoded and none of them raised, so
		// `b = '\x6'` and `b <> '\xzz'` ANSWERED where the server refuses
		// (round 2, P7). One predicate, at plan time, like the rest.
		if _, ok := ByteaLiteral(text); ok {
			return NumConstOK, true
		}
		return NumConstSyntax, true
	case batch.TypeCIDR:
		// The network arm #579 deferred and #627 asks for. It can be wired
		// now for exactly the three types whose accept-set is a SUPERSET of
		// PostgreSQL's — the rule #579 named: a refusal built on a parser
		// stricter than the server's refuses valid input, so the parser has
		// to be widened first. CIDR reads PostgreSQL's abbreviated grammar
		// (pgIPv4Pton), MAC its six spellings (pgMACGroupedHex) and UUID the
		// brace/no-dash/uppercase forms.
		//
		// IPv4 and IPv6 are deliberately absent: their accept-set is not yet a
		// superset, because a prefix narrower than the host width names a
		// NETWORK and those types hold a bare address. Their refusal stays at
		// runtime, where TestPlanTimeNeverRefusesPGValidNetworkLiteral's
		// `{v4, 10/8}` boundary cannot be turned into a plan-time refusal of
		// PostgreSQL-valid text.
		if _, ok := CidrSortKey(text); ok {
			return NumConstOK, true
		}
		return NumConstSyntax, true
	case batch.TypeMAC:
		if _, ok := MACLitKey(text); ok {
			return NumConstOK, true
		}
		return NumConstSyntax, true
	case batch.TypeUUID:
		if _, ok := UUIDLiteralToRaw(text); ok {
			return NumConstOK, true
		}
		return NumConstSyntax, true
	}
	return NumConstOK, false
}

// FloatLitText parses at double width, then uses Float32FitOf for bits=32;
// return float64 in either case, preserving REAL denormals and range errors.
// Trim C whitespace. Reject underscores; accept hex without a binary exponent
// by supplying it. Nonzero underflow to zero is NumConstRange, not zero;
// representable denormals remain values.
// Use FloatSpecialText for float special spellings, including signed NaN;
// DECIMAL has a separate grammar (#534).
// See docs/internals/kernel-float-input-range-classification.md for the design.
func FloatLitText(text string, bits int) (float64, NumConstStatus) {
	s := strings.Trim(text, pgIntWhitespace)
	if s == "" {
		return 0, NumConstSyntax
	}
	// NaN / ±Infinity first: PostgreSQL's own spellings, no prefix matching.
	if f, ok := FloatSpecialText(s); ok {
		return f, NumConstOK
	}
	if strings.IndexByte(s, '_') >= 0 {
		return 0, NumConstSyntax
	}
	f, err := strconv.ParseFloat(hexFloatWithExponent(s), 64)
	if err != nil {
		var ne *strconv.NumError
		if errors.As(err, &ne) && errors.Is(ne.Err, strconv.ErrRange) {
			return 0, NumConstRange
		}
		return 0, NumConstSyntax
	}
	// Go is silent about underflow, so the DIGITS decide: a parse that lands
	// on zero from text naming a non-zero number is PostgreSQL's 22003.
	if f == 0 && !floatTextIsZero(s) {
		return 0, NumConstRange
	}
	if bits == 32 && Float32FitOf(f) != Float32Fits {
		return 0, NumConstRange
	}
	return f, NumConstOK
}

// hexFloatWithExponent supplies the binary exponent Go's hex-float syntax
// requires and C's strtod does not. '0x10' becomes '0x10p0' (16); text that is
// not a hex float, or already carries a 'p', is returned unchanged.
func hexFloatWithExponent(s string) string {
	body := s
	if len(body) > 0 && (body[0] == '+' || body[0] == '-') {
		body = body[1:]
	}
	if len(body) < 3 || body[0] != '0' || (body[1] != 'x' && body[1] != 'X') {
		return s
	}
	if strings.IndexAny(body, "pP") >= 0 {
		return s
	}
	return s + "p0"
}

// floatTextIsZero reports whether float text names the number zero, as opposed
// to a non-zero number too small for a float64 to hold. It reads the DIGITS,
// because the parsed box has already collapsed the distinction: '0.0e-500' and
// '1e-400' both answer 0 there and only the second is PostgreSQL's 22003.
//
// Hex is handled alongside decimal: '0x0p0' is zero and '0x1p-9999' is not.
func floatTextIsZero(s string) bool {
	body := strings.TrimLeft(s, "+-")
	digits := body
	if i := strings.IndexAny(body, "eEpP"); i >= 0 {
		digits = body[:i]
	}
	if len(digits) > 2 && digits[0] == '0' && (digits[1] == 'x' || digits[1] == 'X') {
		digits = digits[2:]
	}
	return strings.IndexAny(digits, "123456789abcdefABCDEF") < 0
}

// Float64FilterConst resolves a FLOAT64 column's filter constant, reading a
// QUOTED literal through PostgreSQL's float8 input grammar rather than as the
// silent 0.0 toFloat64 answered for every string (#646).
func Float64FilterConst(v any) (float64, NumConstStatus) {
	if text, ok := QuotedConstText(v); ok {
		return FloatLitText(text, 64)
	}
	return toFloat64(v), NumConstOK
}

// Float32FilterConst resolves a FLOAT32 (`real`) column's filter constant for
// the QUOTED spelling, which PostgreSQL coerces straight to real — so the
// value comes back NARROWED, and a literal outside real's range is 22003
// rather than a saturating ±Inf or a silent 0.0.
//
// ok=false as the third result means the constant is NOT a quoted literal:
// it is an unquoted numeric one, which takes the opposite rule (#631's
// widening to double, compareFilterFloat32Widen) and must not be narrowed
// here. The two spellings really are two predicates — `r = 3.1` selects no row
// over a column holding real(3.1) and `r = '3.1'` selects it.
func Float32FilterConst(v any) (f float32, st NumConstStatus, quoted bool) {
	text, ok := QuotedConstText(v)
	if !ok {
		return 0, NumConstOK, false
	}
	f64, st := FloatLitText(text, 32)
	if st != NumConstOK {
		return 0, st, true
	}
	return float32(f64), NumConstOK, true
}

// IntLitText reads PostgreSQL's INTEGER input grammar (parseIntText) for
// callers outside this package — the boxed comparison sites in expr, which
// need the VALUE as well as the status QuotedLitStatus reports. One reader for
// the kernel, the row path and the plan-time refusal is the property that
// keeps them from disagreeing about which strings name an integer.
func IntLitText(text string) (int64, NumConstStatus) { return parseIntText(text) }
