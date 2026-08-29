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
