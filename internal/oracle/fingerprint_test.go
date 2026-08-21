package oracle

import (
	"fmt"
	"testing"
)

func fp(cols []string, rows []map[string]any, ordered bool) Fingerprint {
	return FingerprintOf(&Result{Columns: cols, Rows: rows}, ordered)
}

// TestFingerprintOrderSensitivity: the same rows in a different sequence are
// the same answer when the query did not ask for an order, and a different
// answer when it did. This is the whole of the #313/#316/#320 class.
func TestFingerprintOrderSensitivity(t *testing.T) {
	cols := []string{"k", "v"}
	rows := []map[string]any{
		{"k": "a", "v": 1.0},
		{"k": "b", "v": 2.0},
		{"k": "c", "v": 3.0},
	}
	shuffled := []map[string]any{rows[2], rows[0], rows[1]}

	if ok, _ := fp(cols, rows, false).Match(fp(cols, shuffled, false)); !ok {
		t.Error("unordered fingerprint rejected a reordering; it must be order-insensitive")
	}
	ok, detail := fp(cols, rows, true).Match(fp(cols, shuffled, true))
	if ok {
		t.Error("ordered fingerprint accepted a reordering; a dropped ORDER BY would pass")
	}
	if detail == "" {
		t.Error("mismatch reported no detail")
	}
}

// TestFingerprintCoversStringsAndNulls: the columns a numeric signature
// skips. Each mutation is a bug that shipped green under one.
func TestFingerprintCoversStringsAndNulls(t *testing.T) {
	cols := []string{"name", "revenue"}
	base := []map[string]any{
		{"name": "FRANCE", "revenue": 100.5},
		{"name": "GERMANY", "revenue": 200.25},
	}
	want := fp(cols, base, true)

	cases := []struct {
		name string
		rows []map[string]any
	}{
		{"string column NULLed (#314)", []map[string]any{
			{"name": nil, "revenue": 100.5},
			{"name": nil, "revenue": 200.25},
		}},
		{"string column emptied", []map[string]any{
			{"name": "", "revenue": 100.5},
			{"name": "", "revenue": 200.25},
		}},
		{"string values swapped between rows", []map[string]any{
			{"name": "GERMANY", "revenue": 100.5},
			{"name": "FRANCE", "revenue": 200.25},
		}},
		{"numeric value inflated", []map[string]any{
			{"name": "FRANCE", "revenue": 2512.5},
			{"name": "GERMANY", "revenue": 5006.25},
		}},
		{"one row dropped", base[:1]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if ok, _ := want.Match(fp(cols, tc.rows, true)); ok {
				t.Errorf("fingerprint accepted %s", tc.name)
			}
		})
	}
}

// TestFingerprintNullDistinctFromEmpty pins the rendering the value-signature
// blind spot needed: a NULL and an empty string are different answers.
func TestFingerprintCellNullDistinctFromEmpty(t *testing.T) {
	cols := []string{"c"}
	null := fp(cols, []map[string]any{{"c": nil}}, true)
	empty := fp(cols, []map[string]any{{"c": ""}}, true)
	if ok, _ := null.Match(empty); ok {
		t.Error("NULL and the empty string produced the same fingerprint")
	}
	// And a missing column reads as NULL, not as a silent pass.
	absent := fp(cols, []map[string]any{{}}, true)
	if ok, _ := null.Match(absent); !ok {
		t.Error("an absent cell must render as NULL")
	}
}

// TestFingerprintFloatNoise: summation-order noise between two correct runs
// must not register, while a real value error must.
func TestFingerprintFloatNoise(t *testing.T) {
	cols := []string{"revenue"}
	exact := []map[string]any{{"revenue": 494176258.39}}
	noisy := []map[string]any{{"revenue": 494176258.40}} // ~2e-11 relative
	if ok, detail := fp(cols, exact, true).Match(fp(cols, noisy, true)); !ok {
		t.Errorf("one-ULP-class noise registered as divergence: %s", detail)
	}
	// A boundary hit: 6-digit rendering flips, 4-digit does not, so the
	// coarse arm carries the match.
	a := []map[string]any{{"revenue": 14903.549999}}
	b := []map[string]any{{"revenue": 14903.550001}}
	if ok, detail := fp(cols, a, true).Match(fp(cols, b, true)); !ok {
		t.Errorf("rounding-boundary noise registered as divergence: %s", detail)
	}
	inflated := []map[string]any{{"revenue": 494176258.39 * 25}}
	if ok, _ := fp(cols, exact, true).Match(fp(cols, inflated, true)); ok {
		t.Error("a 25x inflated value passed as noise")
	}
	// 1e-3 relative is far above the coarse quantum and must fail.
	off := []map[string]any{{"revenue": 494176258.39 * 1.001}}
	if ok, _ := fp(cols, exact, true).Match(fp(cols, off, true)); ok {
		t.Error("a 0.1% value error passed as noise")
	}
}

// TestFingerprintIntegerFloatTyping: the reference engine's choice of BIGINT
// vs DOUBLE for a SUM is not a divergence, but two integers that differ in
// their last digit still are — quantization must not swallow them.
func TestFingerprintIntegerFloatTyping(t *testing.T) {
	cols := []string{"n"}
	for _, v := range []any{int64(380456), float64(380456), int32(380456), "380456"} {
		got := fp(cols, []map[string]any{{"n": v}}, true)
		want := fp(cols, []map[string]any{{"n": int64(380456)}}, true)
		if ok, detail := want.Match(got); !ok {
			t.Errorf("%T(%v) fingerprinted differently from int64: %s", v, v, detail)
		}
	}
	// Seven digits is where a 6-significant-digit rendering would collapse
	// two distinct keys into one.
	a := fp(cols, []map[string]any{{"n": int64(1234561)}}, true)
	b := fp(cols, []map[string]any{{"n": int64(1234567)}}, true)
	if ok, _ := a.Match(b); ok {
		t.Error("two 7-digit integers fingerprinted the same")
	}
	c := fp(cols, []map[string]any{{"n": float64(1234567)}}, true)
	if ok, detail := b.Match(c); !ok {
		t.Errorf("int64 and float64 of the same 7-digit integer differ: %s", detail)
	}
}

// TestFingerprintColumnCoverage: a column set or order change is a different
// answer.
func TestFingerprintColumnCoverage(t *testing.T) {
	rows := []map[string]any{{"a": "x", "b": "y"}}
	ab := fp([]string{"a", "b"}, rows, true)
	ba := fp([]string{"b", "a"}, rows, true)
	if ok, _ := ab.Match(ba); ok {
		t.Error("swapping the column order left the fingerprint unchanged")
	}
	only := fp([]string{"a"}, rows, true)
	if ok, _ := ab.Match(only); ok {
		t.Error("dropping a column left the fingerprint unchanged")
	}
}

func TestTextCell(t *testing.T) {
	cases := []struct {
		text string
		want any
	}{
		{"<NULL>", nil},
		{"", ""},               // empty field is the empty string
		{"1234567", "1234567"}, // integer text keeps its spelling
		{"007", "007"},         // ... including a numeric-looking code
		{"380456.0", 380456.0}, // a fraction is a float
		{"505822441.4861013", 505822441.4861013},
		{"-0.5", -0.5},
		{"1995-03-11", "1995-03-11"}, // a date is text
		{"Manufacturer#4", "Manufacturer#4"},
		{"17-281-345-4863", "17-281-345-4863"},
		{"ECONOMY ANODIZED STEEL", "ECONOMY ANODIZED STEEL"},
		{"inf", "inf"},
		{"NaN", "NaN"},
	}
	for _, tc := range cases {
		if got := TextCell(tc.text, "<NULL>"); got != tc.want {
			t.Errorf("TextCell(%q) = %#v, want %#v", tc.text, got, tc.want)
		}
	}
	// Text and the typed value it describes must fingerprint identically.
	cols := []string{"c"}
	for _, pair := range []struct {
		text  string
		typed any
	}{
		{"380456.0", float64(380456)},
		{"380456", int64(380456)},
		{"1234567", int64(1234567)},
		{"505822441.4861013", 505822441.4861013},
		{"<NULL>", nil},
	} {
		a := fp(cols, []map[string]any{{"c": TextCell(pair.text, "<NULL>")}}, true)
		b := fp(cols, []map[string]any{{"c": pair.typed}}, true)
		if ok, detail := b.Match(a); !ok {
			t.Errorf("text %q and %T(%v) fingerprint differently: %s", pair.text, pair.typed, pair.typed, detail)
		}
	}
}

// TestFingerprintSnapsNearIntegers pins the #377 remedy, ported from
// benchmarks/tpch/fingerprint.go's TestSignatureSnapsNearIntegers: a float
// sum that lands exactly on a whole number under one accumulation order and
// an ULP away under another renders as "48051445" versus "4.80514e+07" — a
// difference at EVERY precision, which is the one thing the dual-precision
// policy cannot absorb. It is q09's failure mode at SF1 (two of thirteen
// samples measured).
func TestFingerprintSnapsNearIntegers(t *testing.T) {
	cols := []string{"nation", "o_year", "sum_profit"}
	row := func(v float64) []map[string]any {
		return []map[string]any{{"nation": "ALGERIA", "o_year": "1998", "sum_profit": v}}
	}

	// The same answer, computed in two accumulation orders: one lands on
	// the whole number, the other is 7e-9 below it (1.5e-16 relative).
	onInteger := row(48051445)
	offByAnUlp := row(48051444.999999993)

	onFP := fp(cols, onInteger, true)
	offFP := fp(cols, offByAnUlp, true)
	if ok, detail := onFP.Match(offFP); !ok {
		t.Fatalf("one ULP of accumulation noise around a whole number still breaks the digest: %s", detail)
	}

	// And the remedy must not swallow a real difference: 0.12 on 4.8e7 is
	// 2.5e-9 relative, an order of magnitude past the snap window.
	off := fp(cols, row(48051445.12), true)
	if ok, _ := onFP.Match(off); ok {
		t.Error("snapping swallowed a value that is not the same number")
	}
}

func TestFingerprintEmptyResult(t *testing.T) {
	empty := fp([]string{"a"}, nil, true)
	if empty.Rows != 0 {
		t.Fatalf("empty result reported %d rows", empty.Rows)
	}
	one := fp([]string{"a"}, []map[string]any{{"a": nil}}, true)
	if ok, _ := empty.Match(one); ok {
		t.Error("an empty result matched a one-NULL-row result")
	}
	if s := fmt.Sprint(empty); s == "" {
		t.Error("String() empty")
	}
}
