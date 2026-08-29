package batch

import "testing"

// TestDecimalCommon pins ADR-0024 item 2's common-type rule: max scale, and a
// precision rebuilt from the widest INTEGER part rather than taken as
// max(precision). The pair (18,2)/(9,4) is the case the two rules disagree on
// — max(precision) declares 18 where the widened values need 20 — and is why
// the single-process set-operation path could declare a type too small for
// its own values (#532/#552).
func TestDecimalCommon(t *testing.T) {
	tests := []struct {
		name string
		in   []DecimalType
		want DecimalType
		ok   bool
	}{
		{"single", []DecimalType{{18, 4}}, DecimalType{18, 4}, true},
		{"same", []DecimalType{{9, 2}, {9, 2}}, DecimalType{9, 2}, true},
		{"wider scale wins", []DecimalType{{9, 2}, {18, 4}}, DecimalType{18, 4}, true},
		{"integer part rebuilt", []DecimalType{{18, 2}, {9, 4}}, DecimalType{20, 4}, true},
		{"int64 operand", []DecimalType{{9, 2}, {Int64DecimalDigits, 0}}, DecimalType{21, 2}, true},
		{"int32 operand", []DecimalType{{9, 2}, {Int32DecimalDigits, 0}}, DecimalType{12, 2}, true},
		{"capped at the carrier", []DecimalType{{38, 0}, {11, 10}}, DecimalType{38, 10}, true},
		{"three arms", []DecimalType{{9, 2}, {18, 4}, {38, 10}}, DecimalType{38, 10}, true},
		{"unconstrained operand declines", []DecimalType{{9, 2}, {0, 0}}, DecimalType{}, false},
		{"empty declines", nil, DecimalType{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := DecimalCommon(tc.in)
			if ok != tc.ok || got != tc.want {
				t.Errorf("DecimalCommon(%v) = %v,%v want %v,%v", tc.in, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// TestDecimalCommonNeverNarrowsAnArm is the property the table above encodes
// case by case: the common type holds every operand's scale, and its integer
// part is at least every operand's — until the 38-digit cap, which is the
// recorded range reduction of ADR-0024 item 7.
func TestDecimalCommonNeverNarrowsAnArm(t *testing.T) {
	widths := []DecimalType{{9, 2}, {18, 4}, {38, 10}, {5, 0}, {19, 0}, {12, 12}}
	for _, a := range widths {
		for _, b := range widths {
			got, ok := DecimalCommon([]DecimalType{a, b})
			if !ok {
				t.Fatalf("DecimalCommon(%v,%v) declined", a, b)
			}
			if got.Scale < a.Scale || got.Scale < b.Scale {
				t.Errorf("DecimalCommon(%v,%v) = %v: scale narrows an arm", a, b, got)
			}
			if got.Precision > MaxDecimalPrecision {
				t.Errorf("DecimalCommon(%v,%v) = %v: past the carrier", a, b, got)
			}
			want := got.Precision - got.Scale
			ideal := max(a.Precision-a.Scale, b.Precision-b.Scale)
			if want > ideal {
				t.Errorf("DecimalCommon(%v,%v) = %v: integer part wider than needed", a, b, got)
			}
		}
	}
}

// TestDecimalTypeOf pins the integer-operand contribution of ADR-0024 item 2.
func TestDecimalTypeOf(t *testing.T) {
	if m, ok := DecimalTypeOf(TypeInt32, DecimalType{}); !ok || m != (DecimalType{Precision: 10}) {
		t.Errorf("INT32 = %v,%v want {10 0},true", m, ok)
	}
	if m, ok := DecimalTypeOf(TypeInt64, DecimalType{}); !ok || m != (DecimalType{Precision: 19}) {
		t.Errorf("INT64 = %v,%v want {19 0},true", m, ok)
	}
	if m, ok := DecimalTypeOf(TypeDecimal, DecimalType{18, 4}); !ok || m != (DecimalType{18, 4}) {
		t.Errorf("DECIMAL = %v,%v want {18 4},true", m, ok)
	}
	if _, ok := DecimalTypeOf(TypeFloat64, DecimalType{}); ok {
		t.Error("FLOAT64 must not contribute to a DECIMAL result type")
	}
	if _, ok := DecimalTypeOf(TypeString, DecimalType{}); ok {
		t.Error("STRING must not contribute to a DECIMAL result type")
	}
}

// TestCanonicalDecimalText pins the scale-independent text key: two
// renderings of one number produce one key, and two different numbers never
// do. It is AppendDecimalKey's rule where the carrier is the text itself,
// which is what a row-at-a-time membership set holds (ADR-0012 item 8).
func TestCanonicalDecimalText(t *testing.T) {
	same := [][]string{
		{"12.75", "12.7500", "0012.75", "+12.75", "1.275e1"},
		{"0", "0.00", "-0", "0e5"},
		{"-0.01", "-0.0100"},
		// The INTEGER spellings. The trailing-zero strip has to run at every
		// exponent, not only at negative ones: stopping at exp >= 0 left
		// "100" keyed 100e0 while "1e2" keyed 1e2, two keys for one number —
		// reachable from `d IN (SELECT s FROM t)` over a STRING column whose
		// text spells an integer either way.
		{"100", "1e2", "1.0e2", "0.1e3", "100.00", "+100"},
		{"1000", "1.000e3", "10e2", "1e3"},
	}
	for _, group := range same {
		var key string
		for i, spelling := range group {
			got, ok := CanonicalDecimalText(spelling)
			if !ok {
				t.Fatalf("CanonicalDecimalText(%q) declined", spelling)
			}
			if i == 0 {
				key = got
				continue
			}
			if got != key {
				t.Errorf("%q keys as %q but %q keys as %q — one number, two keys",
					group[0], key, spelling, got)
			}
		}
	}
	distinct := []string{"12.75", "12.7501", "12.7499", "-12.75", "1275", "0.1275"}
	seen := map[string]string{}
	for _, spelling := range distinct {
		got, ok := CanonicalDecimalText(spelling)
		if !ok {
			t.Fatalf("CanonicalDecimalText(%q) declined", spelling)
		}
		if prev, dup := seen[got]; dup {
			t.Errorf("%q and %q are different numbers with one key %q", prev, spelling, got)
		}
		seen[got] = spelling
	}
	for _, notANumber := range []string{"abc", "", "NaN", "12.7.5"} {
		if _, ok := CanonicalDecimalText(notANumber); ok {
			t.Errorf("CanonicalDecimalText(%q) answered; it names no number", notANumber)
		}
	}
}

// TestCanonicalDecimalTextAgreesWithAppendDecimalKey holds the TEXT key to the
// same grouping as the columnar one. AppendDecimalKey keys a stored (Int128,
// scale) pair and CanonicalDecimalText keys the rendering of that same value,
// and a set operation, a join and an IN subquery can each key one side each
// way — so two values that group together under one must group together under
// the other (ADR-0012 item 8).
func TestCanonicalDecimalTextAgreesWithAppendDecimalKey(t *testing.T) {
	// One value written at four scales, plus its neighbours at each.
	type cell struct {
		unscaled Int128
		scale    int
	}
	cells := []cell{
		{Int128From(1275), 2}, {Int128From(127500), 4}, {Int128From(1275000), 5},
		{Int128From(127501), 4}, {Int128From(127499), 4},
		{Int128From(-1), 2}, {Int128From(-100), 4},
		{Int128From(0), 0}, {Int128From(0), 6},
		{Int128From(100), 0}, {Int128From(10000), 2},
	}
	colKey := map[string][]int{}
	txtKey := map[string][]int{}
	for i, c := range cells {
		ck := string(AppendDecimalKey(nil, c.unscaled, c.scale))
		colKey[ck] = append(colKey[ck], i)
		tk, ok := CanonicalDecimalText(c.unscaled.FormatDecimal(c.scale))
		if !ok {
			t.Fatalf("cell %d: CanonicalDecimalText(%q) declined", i,
				c.unscaled.FormatDecimal(c.scale))
		}
		txtKey[tk] = append(txtKey[tk], i)
	}
	group := func(m map[string][]int) map[int]int {
		out := map[int]int{}
		for _, members := range m {
			for _, i := range members {
				out[i] = members[0]
			}
		}
		return out
	}
	colGroups, txtGroups := group(colKey), group(txtKey)
	for i := range cells {
		if colGroups[i] != txtGroups[i] {
			t.Errorf("cell %d (%s at scale %d): the columnar key groups it with %d, the text key with %d",
				i, cells[i].unscaled.FormatDecimal(cells[i].scale), cells[i].scale,
				colGroups[i], txtGroups[i])
		}
	}
}
