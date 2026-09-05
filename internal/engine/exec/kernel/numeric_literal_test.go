package kernel

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// Every expectation in this file is postgres:17-alpine's, taken live with a
// `SELECT '<text>'::<type>` transcript. The three answers a coercion can give
// are all here — a VALUE, 22P02 (invalid_text_representation) and 22003
// (numeric_value_out_of_range) — because the two failures are different
// SQLSTATEs with different wording and the WireProtocol oracle checks which
// one the wire says.

// TestQuotedLitStatusMatchesPostgresInputGrammar is the accept-set, per type.
// The whole point of the table is that the SAME text answers differently for
// different columns: '3.1' is a real and a bigint's 22P02, '1_000' is a
// bigint's 1000 and a real's 22P02, '0x1p3' is a real's 8 and a bigint's
// 22P02.
func TestQuotedLitStatusMatchesPostgresInputGrammar(t *testing.T) {
	const (
		ok  = NumConstOK
		bad = NumConstSyntax
		rng = NumConstRange
	)
	for _, tc := range []struct {
		text                string
		i4, i8, f4, f8, dec NumConstStatus
	}{
		// text          int4  int8  real  float8  numeric
		{"3", ok, ok, ok, ok, ok},
		{"3.1", bad, bad, ok, ok, ok},
		{" 3 ", ok, ok, ok, ok, ok},
		{"+3", ok, ok, ok, ok, ok},
		{"-3", ok, ok, ok, ok, ok},
		{"007", ok, ok, ok, ok, ok},
		{"", bad, bad, bad, bad, bad},
		{"  ", bad, bad, bad, bad, bad},
		{"abc", bad, bad, bad, bad, bad},
		{"3.1x", bad, bad, bad, bad, bad},
		// PostgreSQL 16+ takes underscore separators in its INTEGER and
		// NUMERIC inputs and NOT in its float ones (verified live).
		{"1_000", ok, ok, bad, bad, bad}, // numeric: PG takes it, wadjet defers (#634)
		{"1__000", bad, bad, bad, bad, bad},
		{"_1000", bad, bad, bad, bad, bad},
		{"1000_", bad, bad, bad, bad, bad},
		// Radix prefixes: integer takes 0x/0o/0b; float takes only the C99
		// HEX form, which is a different grammar ('0o17' and '0b101' are
		// 22P02 for a real). numeric takes 0x in PG 16+; wadjet defers.
		{"0x1A", ok, ok, ok, ok, bad},
		{"0X1A", ok, ok, ok, ok, bad},
		{"-0x1A", ok, ok, ok, ok, bad},
		{"0x_1A", ok, ok, bad, bad, bad}, // float: no underscores at all
		{"0x1A_", bad, bad, bad, bad, bad},
		{"0o17", ok, ok, bad, bad, bad},
		{"0b101", ok, ok, bad, bad, bad},
		{"0x", bad, bad, bad, bad, bad},
		{"0b", bad, bad, bad, bad, bad},
		// The C99 hex FLOAT, which strtod reads and Go's ParseFloat needs an
		// exponent for.
		{"0x1p3", bad, bad, ok, ok, bad},
		{"0x.8p1", bad, bad, ok, ok, bad},
		{"0x10", ok, ok, ok, ok, bad},
		// The specials. float8 takes a SIGNED NaN and numeric does not
		// (#534); no integer takes any of them.
		{"NaN", bad, bad, ok, ok, ok},
		{"nan", bad, bad, ok, ok, ok},
		{"+NaN", bad, bad, ok, ok, bad},
		{"-NaN", bad, bad, ok, ok, bad},
		{"Infinity", bad, bad, ok, ok, ok},
		{"-Infinity", bad, bad, ok, ok, ok},
		{"inf", bad, bad, ok, ok, ok},
		{"infinit", bad, bad, bad, bad, bad},
		// RANGE, and it is per type. 1e400 is past double; 1e39 past real
		// only; 3000000000 past int4 only. numeric never reports a range
		// failure — a literal past the carrier SATURATES into its place in
		// the order (#462, ADR-0024 item 6).
		{"1e400", bad, bad, rng, rng, ok},
		{"1e39", bad, bad, rng, ok, ok},
		{"3.4e38", bad, bad, ok, ok, ok},
		{"3.5e38", bad, bad, rng, ok, ok},
		{"3000000000", rng, ok, ok, ok, ok},
		{"9223372036854775807", rng, ok, ok, ok, ok},
		{"-9223372036854775808", rng, ok, ok, ok, ok},
		{"9223372036854775808", rng, rng, ok, ok, ok},
		{"0x8000000000000000", rng, rng, ok, ok, bad},
		// UNDERFLOW is a range failure too, and the boundary is the type's
		// smallest DENORMAL: '1e-45' is a real, '7e-46' rounds to zero.
		{"1e-45", bad, bad, ok, ok, ok},
		{"7e-46", bad, bad, rng, ok, ok},
		{"1e-320", bad, bad, rng, ok, ok},
		{"1e-400", bad, bad, rng, rng, ok},
		// Text whose DIGITS are zero is not underflow, however small the
		// exponent says it is.
		{"0.0e-500", bad, bad, ok, ok, ok},
		{"0", ok, ok, ok, ok, ok},
	} {
		t.Run(tc.text, func(t *testing.T) {
			for _, arm := range []struct {
				typ  batch.TypeID
				want NumConstStatus
			}{
				{batch.TypeInt32, tc.i4},
				{batch.TypeInt64, tc.i8},
				{batch.TypeFloat32, tc.f4},
				{batch.TypeFloat64, tc.f8},
				{batch.TypeDecimal, tc.dec},
			} {
				got, ok := QuotedLitStatus(arm.typ, tc.text)
				if !ok {
					t.Fatalf("%v has no literal rule", arm.typ)
				}
				if got != arm.want {
					t.Errorf("%v(%q) = %v, want %v", arm.typ, tc.text, got, arm.want)
				}
			}
		})
	}
}

// TestQuotedLitStatusHasNoRuleForTheRest is the other direction: a type with
// no rule must answer ok=false, so no site can refuse a literal against it by
// accident.
//
// Every NETWORK type has left this list. CIDR, MAC and UUID left it in #627
// because their parsers became supersets of PostgreSQL's; IPv4 and IPv6 left
// it in round 2, and their arm answers a different question — "does this text
// name a value THIS COLUMN can hold" — which has two failure modes and needs
// both classified in one place:
//
//	'zzz'    names no address at all              22P02, this function
//	'10/8'   names a NETWORK a bare address type
//	         has no room for                      0A000, RefuseNetworkPrefixLiteral
//
// Leaving them off the list is what confined the second answer to ONE
// evaluator, so the same query refused in a WHERE clause, answered inside a
// CASE, and answered a wrong NUMBER on the DAG (B1).
//
// BYTES left it too, for the same shape one type family over: the four sites
// that read a bytea literal fell back to its RAW SPELLING when byteain could
// not decode it and none of them raised, so `b = '\x6'` answered where the
// server refuses (P7).
func TestQuotedLitStatusHasNoRuleForTheRest(t *testing.T) {
	for _, typ := range []batch.TypeID{
		batch.TypeBool, batch.TypeString, batch.TypeTimestamp,
		batch.TypeDate, batch.TypeArray,
		batch.TypeRow, batch.TypeMap, batch.TypeVector,
	} {
		if _, ok := QuotedLitStatus(typ, "definitely not a number"); ok {
			t.Errorf("%v claims a numeric literal rule it must not have", typ)
		}
	}
}

// TestFloatLitTextValues asserts the VALUE, not only the status: a grammar
// that accepts the right strings and reads them as the wrong numbers is the
// defect this replaces, one step over.
func TestFloatLitTextValues(t *testing.T) {
	for _, tc := range []struct {
		text string
		bits int
		want float64
	}{
		{"3.1", 64, 3.1},
		{" 3.1 ", 64, 3.1},
		{"+3.1", 64, 3.1},
		{"-3.1", 64, -3.1},
		{".5", 64, 0.5},
		{"5.", 64, 5},
		{"0x10", 64, 16},
		{"0x1A", 64, 26},
		{"0x1p3", 64, 8},
		{"0x.8p1", 64, 1},
		{"-0x1p3", 64, -8},
		{"1e39", 64, 1e39},
		{"0", 64, 0},
		{"-0", 64, 0},
		// bits=32 changes only the RANGE decision; the parse itself is
		// always at double width and the caller narrows (Float32FilterConst,
		// asserted below).
		{"3.1", 32, 3.1},
		{"16777217", 32, 16777217},
	} {
		t.Run(tc.text, func(t *testing.T) {
			got, st := FloatLitText(tc.text, tc.bits)
			if st != NumConstOK {
				t.Fatalf("FloatLitText(%q, %d) status %v, want OK", tc.text, tc.bits, st)
			}
			if got != tc.want {
				t.Errorf("FloatLitText(%q, %d) = %v, want %v", tc.text, tc.bits, got, tc.want)
			}
		})
	}
	// The specials come back as the IEEE values, so the caller compares them
	// with CompareFloat64 and gets PostgreSQL's float order for free.
	for _, text := range []string{"NaN", "nan", "+NaN", "-NaN"} {
		if f, st := FloatLitText(text, 64); st != NumConstOK || !math.IsNaN(f) {
			t.Errorf("FloatLitText(%q) = %v, %v; want NaN, OK", text, f, st)
		}
	}
	for _, tc := range []struct {
		text string
		sign int
	}{{"Infinity", 1}, {"inf", 1}, {"+inf", 1}, {"-Infinity", -1}, {"-inf", -1}} {
		if f, st := FloatLitText(tc.text, 64); st != NumConstOK || !math.IsInf(f, tc.sign) {
			t.Errorf("FloatLitText(%q) = %v, %v; want %+dInf, OK", tc.text, f, st, tc.sign)
		}
	}
}

// TestFloat32FilterConstNarrows is the half of the real rule that decides a
// row: PostgreSQL coerces a QUOTED literal straight to real, so the value the
// comparison uses is the NARROWED one — which is why `r = '3.1'` finds the row
// that `r = 3.1` (widened to double) misses, and why '16777217' rounds onto a
// column holding 2^24.
//
// quoted=false is the other half: an UNQUOTED numeric box must NOT be narrowed
// here, because that spelling takes #631's widening instead.
func TestFloat32FilterConstNarrows(t *testing.T) {
	for _, tc := range []struct {
		text string
		want float32
	}{
		{"3.1", float32(3.1)},
		{"16777217", 16777216},
		{"1e-45", float32(1e-45)},
		{"0", 0},
		{"-0.25", -0.25},
	} {
		got, st, quoted := Float32FilterConst(tc.text)
		if !quoted || st != NumConstOK || got != tc.want {
			t.Errorf("Float32FilterConst(%q) = %v, %v, quoted=%v; want %v, OK, true",
				tc.text, got, st, quoted, tc.want)
		}
	}
	for _, v := range []any{3.1, float32(3.1), int64(3), 3} {
		if _, _, quoted := Float32FilterConst(v); quoted {
			t.Errorf("Float32FilterConst(%v) reported a QUOTED literal; a numeric box widens (#631)", v)
		}
	}
}

// TestIntLitTextValues is FloatLitTextValues for the integer grammar, whose
// radix and underscore forms are #634's PG-superset gap.
func TestIntLitTextValues(t *testing.T) {
	for _, tc := range []struct {
		text string
		want int64
	}{
		{"3", 3},
		{" 3 ", 3},
		{"\t\n\v\f\r3\r\f\v\n\t", 3},
		{"+7", 7},
		{"-7", -7},
		{"007", 7},   // DECIMAL seven, not octal fifteen
		{"017", 17},  // and not octal either
		{"0x1A", 26}, //
		{"0X1A", 26},
		{"-0x1A", -26},
		{"0x_1A", 26}, // an underscore MAY be first after a radix prefix
		{"0o17", 15},
		{"0b101", 5},
		{"1_000", 1000},
		{"1_0", 10},
		{"0x1_A", 26},
		{"0", 0},
		{"9223372036854775807", math.MaxInt64},
		{"-9223372036854775808", math.MinInt64},
	} {
		t.Run(tc.text, func(t *testing.T) {
			got, st := IntLitText(tc.text)
			if st != NumConstOK {
				t.Fatalf("IntLitText(%q) status %v, want OK", tc.text, st)
			}
			if got != tc.want {
				t.Errorf("IntLitText(%q) = %d, want %d", tc.text, got, tc.want)
			}
		})
	}
}

// TestNumericLitParsingIsUnchangedForOrdinaryInput is the "diff behaviour, not
// just tests" check (docs/design/correctness-fix-protocol.md item 4): a change
// to a shared parser must move NOTHING but the intended class. Every ordinary
// decimal literal here answers exactly what strconv would have — the reader
// this replaced — so the radix/underscore/hex widening cannot have leaked into
// the common case.
func TestNumericLitParsingIsUnchangedForOrdinaryInput(t *testing.T) {
	for _, text := range []string{
		"0", "1", "-1", "42", "-42", "100", "999999", "-999999",
		"2147483647", "-2147483648", "9223372036854775807",
		" 7", "7 ", "+0", "-0", "0000", "12345678901234567",
	} {
		want, err := parseInt10(text)
		if err != nil {
			t.Fatalf("test bug: %q", text)
		}
		got, st := IntLitText(text)
		if st != NumConstOK || got != want {
			t.Errorf("IntLitText(%q) = %d, %v; the base-10 reader said %d", text, got, st, want)
		}
	}
	for _, text := range []string{
		"0", "1.5", "-1.5", "3.1", "1e10", "1e-10", "-2.5e3", "0.0001",
		"123456789.123456789", "1e308", "-1e308", "5e-324",
	} {
		want, err := parseFloat64(text)
		if err != nil {
			t.Fatalf("test bug: %q", text)
		}
		got, st := FloatLitText(text, 64)
		if st != NumConstOK || got != want {
			t.Errorf("FloatLitText(%q) = %v, %v; strconv said %v", text, got, st, want)
		}
	}
}

// parseInt10 and parseFloat64 are the readers this fix REPLACED, written out
// here so the invariance check above compares against them rather than against
// the new code's own answer.
func parseInt10(s string) (int64, error) {
	return strconv.ParseInt(strings.Trim(s, pgIntWhitespace), 10, 64)
}

func parseFloat64(s string) (float64, error) {
	return strconv.ParseFloat(strings.Trim(s, pgIntWhitespace), 64)
}
