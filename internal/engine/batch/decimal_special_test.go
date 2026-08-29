package batch

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// #534 / ADR-0024 item 6. PostgreSQL's `numeric` has NaN and, since 14,
// ±Infinity. Wadjet's Int128-at-a-fixed-scale carrier has no bit pattern for
// any of them, so each is a COMPARISON literal and never a stored value: a
// bound past one end of everything the column can hold, which is what
// ScaledDecimal.Sat already expresses for a finite literal too wide for the
// carrier (#462).
//
// Before this, `WHERE d = 'NaN'` was refused with 22P02 — "invalid input
// syntax for type numeric" — for a query PostgreSQL answers with zero rows.

// TestDecimalSpecialTextIsPostgresNumericGrammar is the accept-set, taken from
// a live transcript rather than from memory: every spelling below was fed to
// `$1::numeric` on postgres:17-alpine (17.11) and the answer recorded.
//
// The shape of the grammar is what a hand-rolled reader gets wrong. NaN takes
// NO sign — `+NaN` and `-NaN` are both 22P02 there — while Infinity and its
// short form Inf take an optional IMMEDIATELY-ADJACENT one; surrounding
// whitespace is stripped on both; and nothing is a prefix match, so `Infin`
// and `infinit` are refused where `inf` is accepted.
func TestDecimalSpecialTextIsPostgresNumericGrammar(t *testing.T) {
	for _, tc := range []struct {
		text string
		want DecimalSpecialKind
	}{
		// ACCEPTED by PostgreSQL as NaN.
		{"NaN", DecimalNaN},
		{"nan", DecimalNaN},
		{"NAN", DecimalNaN},
		{"nAn", DecimalNaN},
		{" NaN ", DecimalNaN},
		{"NaN ", DecimalNaN},
		{"  nan", DecimalNaN},
		{"\tnan\n", DecimalNaN},
		// ACCEPTED as Infinity.
		{"Infinity", DecimalPosInf},
		{"infinity", DecimalPosInf},
		{"INFINITY", DecimalPosInf},
		{"+Infinity", DecimalPosInf},
		{"Inf", DecimalPosInf},
		{"inf", DecimalPosInf},
		{"INF", DecimalPosInf},
		{"+inf", DecimalPosInf},
		{"Infinity ", DecimalPosInf},
		// ACCEPTED as -Infinity.
		{"-Infinity", DecimalNegInf},
		{"-infinity", DecimalNegInf},
		{"-inf", DecimalNegInf},
		{"-Inf", DecimalNegInf},
		{" -inf ", DecimalNegInf},

		// REFUSED by PostgreSQL with 22P02, and so not special here. Each of
		// these must fall through to DecimalTextAt, which refuses it too.
		{"+NaN", DecimalFinite},
		{"-NaN", DecimalFinite},
		{"NaN%", DecimalFinite},
		{"NaN0", DecimalFinite},
		{"nan1", DecimalFinite},
		{"1nan", DecimalFinite},
		{"nan.", DecimalFinite},
		{"Infin", DecimalFinite},
		{"infi", DecimalFinite},
		{"infini", DecimalFinite},
		{"infinit", DecimalFinite},
		{"infinityy", DecimalFinite},
		{"inifinity", DecimalFinite},
		{"+ inf", DecimalFinite},
		{"- inf", DecimalFinite},
		{"inf inity", DecimalFinite},
		{"in f", DecimalFinite},
		{"∞", DecimalFinite},
		// Ordinary numbers and ordinary garbage are both DecimalFinite: this
		// function answers "is it one of the three", not "is it a number".
		{"1.5", DecimalFinite},
		{"1e400", DecimalFinite},
		{"abc", DecimalFinite},
		{"", DecimalFinite},
		{"+", DecimalFinite},
		{"-", DecimalFinite},
	} {
		if got := DecimalSpecialText(tc.text); got != tc.want {
			t.Errorf("DecimalSpecialText(%q) = %d, want %d", tc.text, got, tc.want)
		}
	}
}

// TestDecimalSpecialKindsAreTheirRankInPostgresOrder pins the one property the
// constants carry beyond identity: PostgreSQL's numeric order is total —
// -Infinity below every finite value, Infinity above every one, NaN above
// Infinity — and the kinds ARE those positions, so an int comparison of two
// of them is that order.
func TestDecimalSpecialKindsAreTheirRankInPostgresOrder(t *testing.T) {
	if !(DecimalNegInf < DecimalFinite && DecimalFinite < DecimalPosInf && DecimalPosInf < DecimalNaN) {
		t.Fatalf("the kinds are not in PostgreSQL's numeric order: %d %d %d %d",
			DecimalNegInf, DecimalFinite, DecimalPosInf, DecimalNaN)
	}
}

// TestDecimalBoundTextAtSaturatesTheSpecials is the mechanism: each spelling
// resolves to the SAME ScaledDecimal a finite literal past the carrier's range
// resolves to, on the side PostgreSQL's order puts it. NaN and Infinity are
// indistinguishable here on purpose — over a column that can hold neither,
// PostgreSQL's NaN > Infinity is not observable.
func TestDecimalBoundTextAtSaturatesTheSpecials(t *testing.T) {
	for _, tc := range []struct {
		text string
		sat  int
	}{
		{"NaN", 1}, {"nan", 1}, {" NaN ", 1},
		{"Infinity", 1}, {"inf", 1}, {"+Infinity", 1},
		{"-Infinity", -1}, {"-inf", -1}, {" -inf ", -1},
	} {
		for _, scale := range []int{0, 2, 10, 38} {
			got, ok := DecimalBoundTextAt(tc.text, scale)
			if !ok {
				t.Fatalf("DecimalBoundTextAt(%q, %d) refused a comparison literal", tc.text, scale)
			}
			if got.Sat != tc.sat {
				t.Errorf("DecimalBoundTextAt(%q, %d).Sat = %d, want %d", tc.text, scale, got.Sat, tc.sat)
			}
			// Sat decides the order outright, for every stored value.
			for _, cell := range []Int128{Int128From(0), Int128From(-1), Int128Max, Int128Min} {
				if got.Order(cell) != -tc.sat {
					t.Errorf("%q at scale %d does not order past %v", tc.text, scale, cell)
				}
			}
		}
	}

	// It is DecimalTextAt for everything else, including the refusals.
	if _, ok := DecimalBoundTextAt("abc", 2); ok {
		t.Error(`DecimalBoundTextAt("abc") accepted a non-number`)
	}
	if _, ok := DecimalBoundTextAt("+NaN", 2); ok {
		t.Error(`DecimalBoundTextAt("+NaN") accepted a spelling PostgreSQL refuses`)
	}
	if got, ok := DecimalBoundTextAt("12.75", 2); !ok || got.Sat != 0 || got.Unscaled != Int128From(1275) {
		t.Errorf(`DecimalBoundTextAt("12.75", 2) = %+v, %v`, got, ok)
	}
}

// TestDecimalTextAtStillRefusesTheSpecials is the other half of the split: the
// value-producing reader is unchanged, because the widening is a COMPARISON
// rule. A caller that stored what DecimalTextAt read would be storing a number
// the column does not hold.
func TestDecimalTextAtStillRefusesTheSpecials(t *testing.T) {
	for _, text := range []string{"NaN", "nan", "Infinity", "inf", "-Infinity", "-inf"} {
		if _, ok := DecimalTextAt(text, 2); ok {
			t.Errorf("DecimalTextAt(%q) accepted a value with no carrier", text)
		}
	}
}

// TestCompareDecimalTextsOrdersTheSpecials is the boxed comparison path's half
// of the same rule: a simple CASE, IS DISTINCT FROM and GREATEST/LEAST reach a
// DECIMAL column as its RENDERED TEXT and compare it against the literal's
// text here, so this function has to place the three specials too. The finite
// side still has to name a number.
func TestCompareDecimalTextsOrdersTheSpecials(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want int
		ok   bool
	}{
		// A finite value against each special.
		{"12.75", "NaN", -1, true},
		{"NaN", "12.75", 1, true},
		{"-99999999.99", "NaN", -1, true},
		{"12.75", "Infinity", -1, true},
		{"12.75", "-Infinity", 1, true},
		{"-inf", "0", -1, true},
		{"0", "inf", -1, true},
		// PostgreSQL's order among the three themselves: NaN > Infinity, and
		// -Infinity below both. Not reachable from a column, but the rule is
		// stated where the order is.
		{"NaN", "NaN", 0, true},
		{"NaN", "Infinity", 1, true},
		{"Infinity", "NaN", -1, true},
		{"-Infinity", "Infinity", -1, true},
		{"-Infinity", "-inf", 0, true},
		// The finite side that names no number is still refused.
		{"abc", "NaN", 0, false},
		{"NaN", "abc", 0, false},
		{"", "Infinity", 0, false},
		// Unchanged for two finite values.
		{"1.50", "1.5000", 0, true},
		{"10.001", "2.0002", 1, true},
	} {
		got, ok := CompareDecimalTexts(tc.a, tc.b)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("CompareDecimalTexts(%q, %q) = %d, %v; want %d, %v",
				tc.a, tc.b, got, ok, tc.want, tc.ok)
		}
	}
}

// TestParseDecimalStringCheckedRefusesTheSpecialsAs22003 is ADR-0024 item 6's
// VALUE half. A comparison bound is right for a comparison and a lie for a
// stored row (#553's saturation, one class over), so the value-producing
// reader reports rather than saturates — and it reports 22003
// numeric_value_out_of_range, NOT the 22P02 it gave before: PostgreSQL reads
// all three as numeric values, so this is not an input-syntax error but a
// value with no carrier. PostgreSQL raises this same SQLSTATE for the
// infinities against a constrained column ("a field with precision 18, scale 4
// cannot hold an infinite value", verified live on postgres:17-alpine); NaN it
// stores, and wadjet refusing it is the divergence item 6 records.
func TestParseDecimalStringCheckedRefusesTheSpecialsAs22003(t *testing.T) {
	for _, text := range []string{
		"NaN", "nan", " NaN ", "Infinity", "inf", "+Infinity", "-Infinity", "-inf",
		// The spellings strconv.FormatFloat produces, which is how a float box
		// reaches this reader through Vector.setCheckedDecimalFloat.
		strconv.FormatFloat(math.NaN(), 'f', -1, 64),
		strconv.FormatFloat(math.Inf(1), 'f', -1, 64),
		strconv.FormatFloat(math.Inf(-1), 'f', -1, 64),
	} {
		_, err := ParseDecimalStringChecked(text, 2)
		if err == nil {
			t.Errorf("ParseDecimalStringChecked(%q) stored a value the carrier has no bit pattern for", text)
			continue
		}
		if got := sqlerr.StateOf(err); got != "22003" {
			t.Errorf("ParseDecimalStringChecked(%q): SQLSTATE = %q, want 22003; err = %v", text, got, err)
		}
		if !strings.Contains(err.Error(), "ADR-0024 item 6") {
			t.Errorf("ParseDecimalStringChecked(%q) error does not name the record that decided it: %v", text, err)
		}
	}

	// Text that names no number at all is still 22P02, and a finite value is
	// still a value: the accept-set widened by exactly the three specials.
	if _, err := ParseDecimalStringChecked("abc", 2); err == nil {
		t.Error(`ParseDecimalStringChecked("abc") accepted a non-number`)
	} else if got := sqlerr.StateOf(err); got != "22P02" {
		t.Errorf(`ParseDecimalStringChecked("abc"): SQLSTATE = %q, want 22P02`, got)
	}
	if _, err := ParseDecimalStringChecked("+NaN", 2); err == nil {
		t.Error(`ParseDecimalStringChecked("+NaN") accepted a spelling PostgreSQL refuses`)
	} else if got := sqlerr.StateOf(err); got != "22P02" {
		t.Errorf(`ParseDecimalStringChecked("+NaN"): SQLSTATE = %q, want 22P02`, got)
	}
	if got, err := ParseDecimalStringChecked("12.75", 2); err != nil || got != Int128From(1275) {
		t.Errorf(`ParseDecimalStringChecked("12.75", 2) = %v, %v`, got, err)
	}
}

// TestDecimalReadersTrimPostgresWhitespaceOnly is #534's review finding R1.
//
// PostgreSQL's numeric input skips C isspace() — space, tab, newline, vertical
// tab, form feed, carriage return — and nothing else, so a NO-BREAK SPACE
// (U+00A0) before the digits is a non-whitespace byte it refuses with 22P02
// (verified live on postgres:17-alpine, and already pinned for the integer
// types by the wire oracle's IntegerNBSPConstant). decimalParts used
// strings.TrimSpace, which strips the Unicode set: `d = '<NBSP>43219.87'`
// resolved to the number and ANSWERED the row PostgreSQL refuses the query
// for. The three readers built on decimalParts move together.
func TestDecimalReadersTrimPostgresWhitespaceOnly(t *testing.T) {
	const nbsp = "\u00a0"
	for _, text := range []string{
		nbsp + "12.75",
		"12.75" + nbsp,
		"\u2007" + "12.75", // FIGURE SPACE, another Unicode-only space
		"\u3000" + "12.75", // IDEOGRAPHIC SPACE
	} {
		if _, ok := DecimalTextAt(text, 2); ok {
			t.Errorf("DecimalTextAt(%q) accepted a Unicode-only space PostgreSQL refuses", text)
		}
		if _, ok := DecimalBoundTextAt(text, 2); ok {
			t.Errorf("DecimalBoundTextAt(%q) accepted a Unicode-only space PostgreSQL refuses", text)
		}
		if _, ok := CompareDecimalTexts(text, "1.5"); ok {
			t.Errorf("CompareDecimalTexts(%q, ...) accepted a Unicode-only space", text)
		}
	}
	// PostgreSQL's own six bytes are still stripped, at both ends and mixed.
	for _, text := range []string{" 12.75", "12.75 ", "\t12.75\n", "\v\f\r12.75 "} {
		if _, ok := DecimalTextAt(text, 2); !ok {
			t.Errorf("DecimalTextAt(%q) refused PostgreSQL's own whitespace", text)
		}
	}
	// And on the specials, whose reader shares the cutset.
	if DecimalSpecialText(nbsp+"NaN") != DecimalFinite {
		t.Error("DecimalSpecialText accepted a NBSP-prefixed NaN PostgreSQL refuses")
	}
	if DecimalSpecialText(" NaN\t") != DecimalNaN {
		t.Error("DecimalSpecialText refused PostgreSQL's own whitespace around NaN")
	}
}
