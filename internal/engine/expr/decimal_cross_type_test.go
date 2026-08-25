package expr

import (
	"math/big"
	"testing"
)

// #476: a DECIMAL column boxes as its RENDERED TEXT (Vector.GetValue), so a
// mixed-type column-column comparison reached compare() as (int64, string).
// The only reading there was the LEXICOGRAPHIC fallback — "9" sorts above
// "10" — so `d_key >= d_2` answered 129 rows where PostgreSQL answers 188.
// `=` and `<>` were right, which is what kept it hidden: only the ORDERING
// operators were wrong.
//
// The reading is selected by the operands' DECLARED kinds now, not by whether
// the string happens to parse as a number (#504), so these assertions go
// through orderByKinds — the rule table boxedPair applies once it has resolved
// both operands — rather than through compare(), which no longer sniffs.

// boxedCmp is what a bound comparison site answers for one operator, given the
// two operands' declared kinds: the declaration-driven rule when one applies,
// and compare()'s ordinary type rules when none does.
func boxedCmp(lk, rk boxKind, lv, rv any, lText, rText string, op CmpOp) bool {
	if c, ok := orderByKinds(lk, rk, lv, rv, lText, rText); ok {
		return cmpOrder(c, op)
	}
	return compare(lv, rv, op)
}

func TestCompareIntegerAgainstDecimalTextIsExact(t *testing.T) {
	for _, tc := range []struct {
		num  int64
		text string
		want int // num against text
	}{
		// The lexicographic order disagrees with the numeric one here, which
		// is the whole defect: "9" > "10" as text.
		{9, "10.0000", -1},
		{10, "9.0000", 1},
		{-25, "-25.00", 0},
		{-25, "-25.01", 1},
		{-25, "-24.99", -1},
		{0, "0.00", 0},
		{0, "-0.00", 0},
		{0, "0.0001", -1},
		{0, "-0.0001", 1},
		{3, "3.0000", 0},
		{3, "3.0001", -1},
		{3, "2.9999", 1},
		// Past float64's significant digits on both sides: exact, not rounded.
		{9007199254740993, "9007199254740992.0000", 1},
		{9007199254740993, "9007199254740993.0000", 0},
		{9007199254740993, "9007199254740994.0000", -1},
		// Past the Int128 carrier: the text saturates and still orders (#462).
		{1, "170141183460469231731687303715884105728", -1},
		{-1, "-170141183460469231731687303715884105729", 1},
	} {
		got, ok := decimalTextOrder(tc.num, tc.text)
		if !ok {
			t.Fatalf("decimalTextOrder(%d, %q) refused a number", tc.num, tc.text)
		}
		if got != tc.want {
			t.Errorf("decimalTextOrder(%d, %q) = %d, want %d", tc.num, tc.text, got, tc.want)
		}
		// And the same answer through compare(), in both operand orders.
		for _, opTc := range []struct {
			op   CmpOp
			want bool
		}{
			{CmpEq, tc.want == 0}, {CmpNe, tc.want != 0},
			{CmpLt, tc.want < 0}, {CmpLe, tc.want <= 0},
			{CmpGt, tc.want > 0}, {CmpGe, tc.want >= 0},
		} {
			if got := boxedCmp(boxNumber, boxDecimal, tc.num, tc.text, "", "", opTc.op); got != opTc.want {
				t.Errorf("boxedCmp(%d, %q, op %d) = %v, want %v", tc.num, tc.text, opTc.op, got, opTc.want)
			}
			// Flipped operands flip the order.
			flipped := map[CmpOp]CmpOp{CmpEq: CmpEq, CmpNe: CmpNe,
				CmpLt: CmpGt, CmpGt: CmpLt, CmpLe: CmpGe, CmpGe: CmpLe}[opTc.op]
			if got := boxedCmp(boxDecimal, boxNumber, tc.text, tc.num, "", "", flipped); got != opTc.want {
				t.Errorf("boxedCmp(%q, %d, op %d) = %v, want %v", tc.text, tc.num, flipped, got, opTc.want)
			}
		}
	}
}

// The exactness is not a spot check: over the fixture shape that produced the
// reported counts, the boxed comparison must agree with exact rational
// arithmetic for every row and every operator.
func TestCompareIntegerAgainstDecimalTextMatchesRationals(t *testing.T) {
	den := big.NewInt(100)
	for i := int64(0); i < 200; i++ {
		text := (&big.Rat{}).SetFrac(big.NewInt((i-100)*25), big.NewInt(100)).FloatString(2)
		want := new(big.Rat).SetFrac(big.NewInt(i), big.NewInt(1)).Cmp(
			new(big.Rat).SetFrac(big.NewInt((i-100)*25), den))
		got, ok := decimalTextOrder(i, text)
		if !ok {
			t.Fatalf("row %d: decimalTextOrder refused %q", i, text)
		}
		if got != want {
			t.Fatalf("row %d: %d against %q = %d, want %d", i, i, text, got, want)
		}
	}
}

// A FLOAT against a DECIMAL is a FLOAT comparison, which is what PostgreSQL
// does with `numeric <op> double precision` — it casts the numeric. Verified
// on live postgres:17-alpine:
// `9007199254740993::numeric = 9007199254740992::float8` is TRUE there.
func TestCompareFloatAgainstDecimalTextFollowsPostgres(t *testing.T) {
	if !boxedCmp(boxNumber, boxDecimal, float64(9007199254740992), "9007199254740993", "", "", CmpEq) {
		t.Error("float8 against numeric is not the float8 comparison PostgreSQL makes it")
	}
	if !boxedCmp(boxNumber, boxDecimal, float64(1.5), "1.50", "", "", CmpEq) {
		t.Error("1.5 <> 1.50")
	}
	if !boxedCmp(boxNumber, boxDecimal, float64(1.5), "1.75", "", "", CmpLt) {
		t.Error("1.5 is not below 1.75")
	}
}

// #504: the SAME two boxes, with the text side declared a STRING column
// instead of a DECIMAL one, must compare AS TEXT — and must do so whichever
// path the query takes, which is the property the sniff broke. A numeric
// literal contributes its exact SOURCE TEXT, so `s = 1.50` and `s = 1.5` are
// different predicates here exactly as `s = '1.50'` and `s = '1.5'` are.
func TestTextColumnAgainstNumericLiteralComparesAsText(t *testing.T) {
	for _, tc := range []struct {
		value string
		text  string
		op    CmpOp
		want  bool
	}{
		// The reported shape: "1.50" is not "1.5", however equal the numbers.
		{"1.50", "1.5", CmpEq, false},
		{"1.50", "1.5", CmpNe, true},
		{"1.5", "1.5", CmpEq, true},
		{"1.50", "1.50", CmpEq, true},
		// Ordering is bytewise, so "10" sorts above "1.5" ('0' > '.') and
		// "9" above both — the numeric reading would say otherwise.
		{"10", "1.5", CmpGt, true},
		{"9", "1.5", CmpGt, true},
		{"abc", "1.5", CmpGt, true},
		{"1.50", "1.5", CmpGt, true},
		{"1.5", "1.5", CmpLt, false},
	} {
		if got := boxedCmp(boxText, boxNumber, tc.value, 1.5, "", tc.text, tc.op); got != tc.want {
			t.Errorf("boxedCmp(text %q, literal %s, op %d) = %v, want %v",
				tc.value, tc.text, tc.op, got, tc.want)
		}
		// Flipped operands flip the order, and the rule does not change.
		flipped := map[CmpOp]CmpOp{CmpEq: CmpEq, CmpNe: CmpNe,
			CmpLt: CmpGt, CmpGt: CmpLt, CmpLe: CmpGe, CmpGe: CmpLe}[tc.op]
		if got := boxedCmp(boxNumber, boxText, 1.5, tc.value, tc.text, "", flipped); got != tc.want {
			t.Errorf("boxedCmp(literal %s, text %q, op %d) = %v, want %v",
				tc.text, tc.value, flipped, got, tc.want)
		}
	}
}

// The branch must not reach an ordinary STRING column: a string that is not a
// number keeps comparing as a string, and a string against a string never
// gets here at all (the both-strings fast path answers first).
func TestCompareLeavesNonNumericTextAlone(t *testing.T) {
	if _, ok := decimalTextOrder(int64(5), "abc"); ok {
		t.Error("decimalTextOrder read a non-number")
	}
	if _, ok := decimalTextOrder(int64(5), ""); ok {
		t.Error("decimalTextOrder read the empty string as a number")
	}
	if _, ok := decimalTextOrder("5", "5"); ok {
		t.Error("decimalTextOrder applied to two strings")
	}
	// Two strings still compare as strings, lexicographically.
	if !compare("10.001", "2.0002", CmpLt) {
		t.Error("two strings no longer compare as strings")
	}
	// And compare() itself no longer reads a numeric-looking string as a
	// number against a number: that guess is what made one predicate answer
	// two different things depending on which path evaluated it (#504). Only
	// a DECLARATION selects the numeric reading now, which boxedCmp supplies
	// above and this call deliberately does not.
	if compare(int64(9), "10.0000", CmpLt) {
		t.Error("compare() still sniffs a numeric-looking string into a numeric comparison")
	}
}
