package expr

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// THE WHOLE BITWISE FAMILY IS EXACT, AND EVERY INTEGER RESULT IS AN INTEGER.
//
// The first pass of #966 fixed four of the seven bitwise functions and left
// the three shifts, TO_HEX, TO_BASE, BIT_COUNT, FROM_HEX and FROM_BASE reading
// their argument through a float64 — the exact line it had just replaced. A
// right shift by ZERO changed the value. One rule now covers the family: an
// integer argument is read exactly (`bitIntArg` / `toInt64Safe`), and a
// function whose answer is an integer declares one.
//
// The table below is a PostgreSQL 17.11 TRANSCRIPT, taken on the shared oracle
// server with `psql --pset=format=unaligned` over the twelve values, and
// pasted rather than recomputed:
//
//	x                    | x&18 | x|1                  | x#3                  | ~x                   | x<<1                 | x>>1                 | to_hex(x)        | bit_count(x::bit(64))
//	-9223372036854775808 |    0 | -9223372036854775807 | -9223372036854775805 |  9223372036854775807 |                    0 | -4611686018427387904 | 8000000000000000 |  1
//	-4611686018427387904 |    0 | -4611686018427387903 | -4611686018427387901 |  4611686018427387903 | -9223372036854775808 | -2305843009213693952 | c000000000000000 |  2
//	         -2147483648 |    0 |         -2147483647  |         -2147483645  |          2147483647  |          -4294967296 |          -1073741824 | ffffffff80000000 | 33
//	                 -18 |    2 |                 -17  |                 -19  |                  17  |                  -36 |                   -9 | ffffffffffffffee | 62
//	                  -1 |   18 |                  -1  |                  -4  |                   0  |                   -2 |                   -1 | ffffffffffffffff | 64
//	                   0 |    0 |                   1  |                   3  |                  -1  |                    0 |                    0 | 0                |  0
//	                  18 |   18 |                  19  |                  17  |                 -19  |                   36 |                    9 | 12               |  2
//	                 511 |   18 |                 511  |                 508  |                -512  |                 1022 |                  255 | 1ff              |  9
//	          2147483647 |   18 |          2147483647  |          2147483644  |         -2147483648  |           4294967294 |           1073741823 | 7fffffff         | 31
//	 4611686018427387904 |    0 |  4611686018427387905 |  4611686018427387907 | -4611686018427387905 | -9223372036854775808 |  2305843009213693952 | 4000000000000000 |  1
//	 4611686018427387922 |   18 |  4611686018427387923 |  4611686018427387921 | -4611686018427387923 | -9223372036854775772 |  2305843009213693961 | 4000000000000012 |  3
//	 9223372036854775807 |   18 |  9223372036854775807 |  9223372036854775804 | -9223372036854775808 |                   -2 |  4611686018427387903 | 7fffffffffffffff | 63
//
// TWO COLUMNS OF THAT TABLE ARE NOT AN ORACLE FOR EVERY FUNCTION, and saying
// which is the point of writing it out:
//
//   - PostgreSQL's `>>` is an ARITHMETIC (sign-preserving) shift, which is
//     `BITWISE_ARITHMETIC_SHIFT_RIGHT` here. `BITWISE_RIGHT_SHIFT` is Trino's
//     LOGICAL shift — the name comes from Trino, which has both — so its
//     expectation is the unsigned shift, computed beside the transcript and
//     labelled. The two agree for every non-negative value and differ for
//     every negative one, which is what makes the split worth asserting.
//   - `bit_count` has no integer overload in PostgreSQL; `bit_count(bit)` and
//     `bit_count(bytea)` both answer BIGINT, and over `x::bit(64)` they agree
//     with this function value for value. That is why the declaration is
//     INT64 and not a float.
func TestTheWholeBitwiseFamilyIsExact(t *testing.T) {
	and := DefaultRegistry.Lookup("bitwise_and")
	or := DefaultRegistry.Lookup("bitwise_or")
	xor := DefaultRegistry.Lookup("bitwise_xor")
	not := DefaultRegistry.Lookup("bitwise_not")
	shl := DefaultRegistry.Lookup("bitwise_left_shift")
	shr := DefaultRegistry.Lookup("bitwise_right_shift")
	asr := DefaultRegistry.Lookup("bitwise_arithmetic_shift_right")
	toHex := DefaultRegistry.Lookup("to_hex")
	toBase := DefaultRegistry.Lookup("to_base")
	bitCount := DefaultRegistry.Lookup("bit_count")

	for _, tc := range []struct {
		x        int64
		and18    int64
		or1      int64
		xor3     int64
		notX     int64
		shl1     int64
		asr1     int64 // PostgreSQL's x >> 1
		hex      string
		popcount int64
	}{
		{-9223372036854775808, 0, -9223372036854775807, -9223372036854775805, 9223372036854775807, 0, -4611686018427387904, "8000000000000000", 1},
		{-4611686018427387904, 0, -4611686018427387903, -4611686018427387901, 4611686018427387903, -9223372036854775808, -2305843009213693952, "c000000000000000", 2},
		{-2147483648, 0, -2147483647, -2147483645, 2147483647, -4294967296, -1073741824, "ffffffff80000000", 33},
		{-18, 2, -17, -19, 17, -36, -9, "ffffffffffffffee", 62},
		{-1, 18, -1, -4, 0, -2, -1, "ffffffffffffffff", 64},
		{0, 0, 1, 3, -1, 0, 0, "0", 0},
		{18, 18, 19, 17, -19, 36, 9, "12", 2},
		{511, 18, 511, 508, -512, 1022, 255, "1ff", 9},
		{2147483647, 18, 2147483647, 2147483644, -2147483648, 4294967294, 1073741823, "7fffffff", 31},
		{4611686018427387904, 0, 4611686018427387905, 4611686018427387907, -4611686018427387905, -9223372036854775808, 2305843009213693952, "4000000000000000", 1},
		{4611686018427387922, 18, 4611686018427387923, 4611686018427387921, -4611686018427387923, -9223372036854775772, 2305843009213693961, "4000000000000012", 3},
		{9223372036854775807, 18, 9223372036854775807, 9223372036854775804, -9223372036854775808, -2, 4611686018427387903, "7fffffffffffffff", 63},
	} {
		t.Run(fmt.Sprint(tc.x), func(t *testing.T) {
			check := func(name string, got any, want any) {
				t.Helper()
				if got != want {
					t.Errorf("%s over %d = %#v; PostgreSQL 17.11 answers %#v", name, tc.x, got, want)
				}
			}
			check("BITWISE_AND(x,18)", and([]any{tc.x, int64(18)}), tc.and18)
			check("BITWISE_OR(x,1)", or([]any{tc.x, int64(1)}), tc.or1)
			check("BITWISE_XOR(x,3)", xor([]any{tc.x, int64(3)}), tc.xor3)
			check("BITWISE_NOT(x)", not([]any{tc.x}), tc.notX)
			check("BITWISE_LEFT_SHIFT(x,1)", shl([]any{tc.x, int64(1)}), tc.shl1)
			check("BITWISE_ARITHMETIC_SHIFT_RIGHT(x,1)", asr([]any{tc.x, int64(1)}), tc.asr1)
			check("TO_HEX(x)", toHex([]any{tc.x}), tc.hex)
			check("TO_BASE(x,16)", toBase([]any{tc.x, int64(16)}), formatSignedBase16(tc.x))
			check("BIT_COUNT(x)", bitCount([]any{tc.x}), tc.popcount)

			// The LOGICAL right shift, which PostgreSQL's `>>` is not.
			wantLogical := int64(uint64(tc.x) >> 1)
			check("BITWISE_RIGHT_SHIFT(x,1) [Trino's logical shift]",
				shr([]any{tc.x, int64(1)}), wantLogical)

			// A shift by ZERO is the identity, and it was not: the argument
			// came through a double, so `x >> 0` answered a DIFFERENT number
			// for every value past 2^53 (#966 round 2).
			check("BITWISE_RIGHT_SHIFT(x,0)", shr([]any{tc.x, int64(0)}), tc.x)
			check("BITWISE_ARITHMETIC_SHIFT_RIGHT(x,0)", asr([]any{tc.x, int64(0)}), tc.x)
			check("BITWISE_LEFT_SHIFT(x,0)", shl([]any{tc.x, int64(0)}), tc.x)
		})
	}
}

// formatSignedBase16 is TO_BASE's rendering, which is Trino's signed one
// (`-ff`) and NOT PostgreSQL's two's complement — TO_BASE is Trino's function
// and PostgreSQL has no equivalent, while TO_HEX is PostgreSQL's and renders
// the machine word. The two disagreeing on a negative value is deliberate and
// is what this helper makes explicit rather than hiding in an expectation.
//
// It is a RE-DERIVATION, not an oracle, and it is only trustworthy because the
// EXACTNESS half — the digits a double would have lost — is pinned separately
// against literal strings in TestToBaseIsExactOverTheWideValues below. What it
// checks here is that TO_BASE and TO_HEX still disagree on the sign, which is
// the property a future "make them consistent" edit would break silently.
func formatSignedBase16(x int64) string {
	if x < 0 {
		if x == -9223372036854775808 {
			return "-8000000000000000"
		}
		return "-" + fmt.Sprintf("%x", -x)
	}
	return fmt.Sprintf("%x", x)
}

// TO_BASE's exactness, against LITERALS rather than a re-derivation: these are
// the digits a float64 carrier loses. Trino renders a negative value signed,
// which is why they are not the two's complements TO_HEX gives for the same
// numbers.
//
// PostgreSQL has no `to_base`, so the base-36 strings were computed by a
// separate implementation (a short Python conversion) rather than read back
// out of this one — a literal taken from the code under test is not evidence.
func TestToBaseIsExactOverTheWideValues(t *testing.T) {
	toBase := DefaultRegistry.Lookup("to_base")
	for _, tc := range []struct {
		x    int64
		base int64
		want string
	}{
		{1<<62 | 18, 16, "4000000000000012"},
		{1<<62 | 18, 36, "z1ci99jj747m"}, // independently computed, not read back
		{9223372036854775807, 36, "1y2p0ij32e8e7"},
		{9223372036854775807, 16, "7fffffffffffffff"},
		{-9223372036854775808, 16, "-8000000000000000"},
		{-1, 16, "-1"},
		{255, 16, "ff"},
	} {
		if got := toBase([]any{tc.x, tc.base}); got != tc.want {
			t.Errorf("TO_BASE(%d, %d) = %#v, want %q", tc.x, tc.base, got, tc.want)
		}
	}
}

// TO_HEX's WIDTH comes from the argument's own box: an int32 renders eight
// digits and an int64 sixteen, which is how PostgreSQL renders
// `to_hex((-1)::int4)` and `to_hex((-1)::int8)`. Measured 17.11.
//
// THE INT32 ARM IS NOT REACHABLE FROM A COLUMN, and that is measured too
// (coordinator.TestTheTCPFlagFamilyAnswersPostgresBitArithmetic's
// `to_hex_of_an_int32_column_*` cells, five arms): an INT32 column's value
// arrives at a scalar function as an int64 box, so `TO_HEX(int32_col)` over a
// negative renders the sign-extended sixteen-digit word where PostgreSQL
// renders eight. The NUMBER is the same two's complement either way and a
// non-negative argument renders identically; the divergence is recorded in
// ADR-0012 rather than papered over by dropping the arm, which is right
// whenever an int32 does arrive.
func TestToHexRendersTheArgumentsOwnWidth(t *testing.T) {
	toHex := DefaultRegistry.Lookup("to_hex")
	for _, tc := range []struct {
		arg  any
		want string
	}{
		{int32(-1), "ffffffff"},
		{int32(2147483647), "7fffffff"},
		{int32(-2147483648), "80000000"},
		{int32(511), "1ff"},
		{int64(-1), "ffffffffffffffff"},
		{int64(511), "1ff"},
	} {
		if got := toHex([]any{tc.arg}); got != tc.want {
			t.Errorf("TO_HEX(%T %v) = %#v; PostgreSQL 17.11 answers %q", tc.arg, tc.arg, got, tc.want)
		}
	}
}

// Every function of the family that answers an integer DECLARES one. A
// declaration of FLOAT64 over an exact int64 destroys the value on the way out
// — FROM_BASE('4000000000000012', 16) answered 4.611686018427388e+18 — and it
// is the declaration, not the body, that decides.
func TestTheBitwiseFamilyDeclaresIntegers(t *testing.T) {
	for _, name := range []string{
		"bitwise_and", "bitwise_or", "bitwise_xor", "bitwise_not",
		"bitwise_left_shift", "bitwise_right_shift", "bitwise_arithmetic_shift_right",
		"bit_count", "from_hex", "from_base",
	} {
		decl, _ := DefaultRegistry.ReturnType(name).Resolve(0, nil)
		if decl.ID != batch.TypeInt64 {
			t.Errorf("%s declares %v, want INT64", name, decl.ID)
		}
	}
	for _, name := range []string{"to_hex", "to_base"} {
		decl, _ := DefaultRegistry.ReturnType(name).Resolve(0, nil)
		if decl.ID != batch.TypeString {
			t.Errorf("%s declares %v, want STRING", name, decl.ID)
		}
	}
	// The values behind those declarations, past 2^53.
	if got := DefaultRegistry.Lookup("from_hex")([]any{"4000000000000012"}); got != int64(1)<<62|18 {
		t.Errorf("FROM_HEX('4000000000000012') = %#v, want %d", got, int64(1)<<62|18)
	}
	if got := DefaultRegistry.Lookup("from_base")([]any{"4000000000000012", int64(16)}); got != int64(1)<<62|18 {
		t.Errorf("FROM_BASE('4000000000000012', 16) = %#v, want %d", got, int64(1)<<62|18)
	}
}

// AN INTEGER FUNCTION'S VALUE SURVIVES THE ARITHMETIC ABOVE IT.
//
// `numericFuncCall.EvalInt64` is the seam integer arithmetic reads a numeric
// function through, and it converted via a float64. With the family declaring
// INT64 that made two spellings of one value disagree: `BITWISE_OR(f8, 1)`
// answered 4611686018427387923 and `BITWISE_OR(f8, 1) + 0` answered
// 4611686018427387904 — the declaration change opened the seam, so it is
// closed here rather than filed.
func TestAnIntegerFunctionsValueSurvivesTheArithmeticAboveIt(t *testing.T) {
	b := batch.NewRecordBatch(nil, 1)
	b.Len = 1
	const wide = int64(1)<<62 | 18
	fc := &numericFuncCall{&FuncCall{Name: "bitwise_or", Args: []Expr{
		&Lit{Val: wide}, &Lit{Val: int64(1)}}}}
	got, ok := fc.EvalInt64(b, 0)
	if !ok || got != wide|1 {
		t.Errorf("EvalInt64 over BITWISE_OR(2^62|18, 1) = %d (ok=%v), want %d", got, ok, wide|1)
	}
}

// FROM_HEX REQUIRES THE WHOLE STRING, AND ANSWERS WHAT FROM_BASE ANSWERS
// (#966 round 2, N2).
//
// PostgreSQL has no integer-returning from_hex, so two things adjudicate: its
// own hexadecimal decoder, which refuses a bad digit outright
// (`decode('12zz','hex')` is 22023, measured on 17.11), and this engine's own
// FROM_BASE, which returns NULL for `FROM_BASE('12z',16)` because
// strconv.ParseInt requires exhaustion.
//
// `fmt.Sscanf(..., "%x")` did not: it consumed the valid PREFIX and reported
// success, so FROM_HEX('12zz') was 18 — a number derived from text that is not
// one, and the opposite of what the sibling function said about the same
// input. Every cell here is checked against FROM_BASE(s, 16) as well as
// against the literal, because "the two spellings agree" is the property.
func TestFromHexRequiresTheWholeString(t *testing.T) {
	fromHex := DefaultRegistry.Lookup("from_hex")
	fromBase := DefaultRegistry.Lookup("from_base")
	for _, tc := range []struct {
		in   string
		want any
	}{
		{"ff", int64(255)},
		{"FF", int64(255)},
		{"4000000000000012", int64(1)<<62 | 18},
		{"7fffffffffffffff", int64(9223372036854775807)},
		{"0", int64(0)},
		// A valid prefix followed by text that is not hexadecimal. 18 was the
		// old answer for the first two.
		{"12zz", nil},
		{"12 34", nil},
		{"ff!", nil},
		{"", nil},
		{"zz", nil},
		// Sixteen digits with the top bit set does not fit a signed int64.
		{"ffffffffffffffff", nil},
		{"8000000000000000", nil},
	} {
		if got := fromHex([]any{tc.in}); got != tc.want {
			t.Errorf("FROM_HEX(%q) = %#v, want %#v", tc.in, got, tc.want)
		}
		if got := fromBase([]any{tc.in, float64(16)}); got != tc.want {
			t.Errorf("FROM_BASE(%q, 16) = %#v, want %#v — the two spellings of one "+
				"operation must not disagree", tc.in, got, tc.want)
		}
	}
}

// THE EXPRESSION SITES THAT STILL READ AN ARGUMENT THROUGH A DOUBLE
// (#966 round 2, P2).
//
// The bitwise family reads its operands exactly now. The claim that came with
// that fix — that no remaining `int64(ToFloat64(` site in this package can be
// handed a value a double cannot hold — is FALSE, and this is the measurement
// that says so. Three of them take an ordinary callable argument:
//
//	PARSE_BYTES('9007199254740993')          9007199254740992   (a digit lost)
//	PARSE_RATE('9007199254740993')           9007199254740992
//	HUMAN_READABLE_SECONDS(9007199254740993) ends in 32 seconds, not 33
//
// The real bound is narrower and is what this pin states: the remaining sites
// are bounded by their DOMAIN being reached in practice (an epoch, a code
// point, a duration), not by an input guard, and a caller who hands one a
// literal past 2^53 gets the nearest double. The values below are today's, and
// they reproduce unchanged at this arc's base — they are a residual this arc
// declines to widen into, not a regression.
//
// It is a FAIL-ON-CHANGE pin. When these sites are made exact, this test fails
// and deleting it is the proof; it must not be edited to track a new wrong
// answer.
func TestTheSitesThatStillReadAnArgumentThroughADouble(t *testing.T) {
	for _, tc := range []struct {
		fn   string
		args []any
		want any
	}{
		{"parse_bytes", []any{"9007199254740993"}, int64(9007199254740992)},
		{"parse_rate", []any{"9007199254740993"}, int64(9007199254740992)},
		{"human_readable_seconds", []any{int64(9007199254740993)},
			"104249991374 days, 7 hours, 36 minutes, 32 seconds"},
		{"from_unixtime", []any{int64(9007199254740993)}, "285428751-11-12 07:36:32"},
		// And the control: the same functions are exact below 2^53, so the pin
		// is about the carrier and not about the functions being broken.
		{"parse_bytes", []any{"9007199254740992"}, int64(9007199254740992)},
		{"parse_bytes", []any{"4503599627370497"}, int64(4503599627370497)},
		{"human_readable_seconds", []any{int64(4503599627370497)},
			"52124995687 days, 3 hours, 48 minutes, 17 seconds"},
	} {
		got := DefaultRegistry.Lookup(tc.fn)(tc.args)
		if got != tc.want {
			t.Errorf("%s(%v) = %#v, want %#v.\nIf this site became EXACT, delete the "+
				"row — the pin is a record of a residual, not an expectation that it "+
				"stay wrong.", tc.fn, tc.args, got, tc.want)
		}
	}
}
