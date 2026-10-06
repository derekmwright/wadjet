// SPDX-License-Identifier: MIT

package batch

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestDecimalTextDScale(t *testing.T) {
	for _, c := range []struct {
		in string
		d  int
		ok bool
	}{
		{"2.50", 2, true}, {"7", 0, true}, {"-0.125", 3, true}, {"+1.", 0, true}, {".5", 1, true},
		{"1.5e-3", 4, true}, {"1.25e1", 1, true}, {"1e5", 0, true}, {"1.250E+2", 1, true},
		{" 2.50 ", 2, true}, {"", 0, false}, {"abc", 0, false}, {"1.2.3", 0, false}, {"1e", 0, false},
		{"NaN", 0, false}, {"-", 0, false},
	} {
		d, ok := DecimalTextDScale(c.in)
		if d != c.d || ok != c.ok {
			t.Errorf("DecimalTextDScale(%q) = %d, %v; want %d, %v", c.in, d, ok, c.d, c.ok)
		}
	}
}

// TestDecimalColumnTextPrintsTheDisplayScale: one column at carrier scale 4
// holds values written from text of different display scales; each prints its
// own, a value with none prints the carrier (trimmed under the §10 mark), an
// unknown one prints trimmed, and equal numbers keep equal carriers (I5).
func TestDecimalColumnTextPrintsTheDisplayScale(t *testing.T) {
	v := NewColumnVector(parquet.Column{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: 4}, 6)
	for i, s := range []string{"2.50", "2.5", "7", "0.1250"} {
		if err := v.SetValueChecked(i, s); err != nil {
			t.Fatal(err)
		}
	}
	if err := v.SetComputedChecked(4, int64(3)); err != nil {
		t.Fatal(err)
	}
	if err := v.SetValueChecked(5, 1.5); err != nil { // a double has no spelling
		t.Fatal(err)
	}
	want := []string{"2.50", "2.5", "7", "0.1250", "3", "1.5000"}
	for i, w := range want {
		if got := v.GetValueOf(i, false); got != w {
			t.Errorf("row %d prints %v, want %s", i, got, w)
		}
	}
	if got := v.GetValueOf(5, true); got != "1.5" {
		t.Errorf("a carrier value under the unconstrained mark prints %v, want 1.5", got)
	}
	if v.DecimalData.Data[0] != v.DecimalData.Data[1] {
		t.Errorf("2.50 and 2.5 hold different carriers: %v, %v", v.DecimalData.Data[0], v.DecimalData.Data[1])
	}
	if v.GetValue(0) != v.GetValue(1) || v.GetValue(0) != "2.5000" {
		t.Errorf("GetValue is the key box and must not read the display scale: %v, %v", v.GetValue(0), v.GetValue(1))
	}
	v.DecimalData.SetDScaleCode(3, DScaleUnknown)
	if got := v.GetValueOf(3, false); got != "0.125" {
		t.Errorf("an unknown display scale prints %v, want 0.125", got)
	}
}

// TestDecimalColumnUniformCostsNothing: text at the column's own scale, a
// computed carrier and a copy between two such columns never allocate the
// per-row array (invariant I7).
func TestDecimalColumnUniformCostsNothing(t *testing.T) {
	v := NewColumnVector(parquet.Column{Name: "d", Type: parquet.TypeDecimal, Precision: 15, Scale: 2}, 3)
	for i, s := range []string{"1.00", "2.50", "-3.25"} {
		if err := v.SetValueChecked(i, s); err != nil {
			t.Fatal(err)
		}
	}
	w := NewColumnVector(parquet.Column{Name: "d", Type: parquet.TypeDecimal, Precision: 15, Scale: 2}, 3)
	w.DecimalData.CopyRange(0, &v.DecimalData, 0, 3)
	w.DecimalData.Gather(&v.DecimalData, []uint32{2, 1, 0})
	w.DecimalData.CopyRow(1, &v.DecimalData, 0)
	if v.DecimalData.HasDisplayScale() || w.DecimalData.HasDisplayScale() {
		t.Fatalf("a uniform column allocated display scales: %v / %v", v.DecimalData.DScale, w.DecimalData.DScale)
	}
}

// TestDecimalColumnCopyHelpersMoveTheCodes: every helper moves a varying
// column's codes with its carriers, and a reused vector forgets them.
func TestDecimalColumnCopyHelpersMoveTheCodes(t *testing.T) {
	col := parquet.Column{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: 3}
	src := NewColumnVector(col, 4)
	for i, s := range []string{"1.5", "2.25", "3", "4.125"} {
		if err := src.SetValueChecked(i, s); err != nil {
			t.Fatal(err)
		}
	}
	texts := func(v *Vector, n int) string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprint(v.GetValueOf(i, false))
		}
		return strings.Join(out, ",")
	}
	check := func(name, got, want string) {
		t.Helper()
		if got != want {
			t.Errorf("%s: %s, want %s", name, got, want)
		}
	}
	a := NewColumnVector(col, 4)
	a.DecimalData.CopyRange(0, &src.DecimalData, 0, 4)
	check("CopyRange", texts(a, 4), "1.5,2.25,3,4.125")
	g := NewColumnVector(col, 3)
	g.DecimalData.Gather(&src.DecimalData, []uint32{3, 0, 2})
	check("Gather", texts(g, 3), "4.125,1.5,3")
	c := NewColumnVector(col, 2)
	c.CopyValueFrom(0, src, 1)
	c.CopyValueFrom(1, src, 2)
	check("CopyValueFrom", texts(c, 2), "2.25,3")
	ap := NewVectorLike(src)
	for _, i := range []int{2, 0} {
		ap.AppendFrom(src, i)
	}
	check("AppendFrom", texts(ap, 2), "3,1.5")
	view := NewViewVector(src, []uint32{1, 3})
	check("view", texts(view, 2), "2.25,4.125")
	view.Flatten()
	check("Flatten", texts(view, 2), "2.25,4.125")
	b := NewRecordBatch([]parquet.Column{col}, 4)
	b.Columns[0].DecimalData.CopyRange(0, &src.DecimalData, 0, 4)
	b.Reset(4)
	if b.Columns[0].DecimalData.HasDisplayScale() {
		t.Errorf("a reset batch kept its previous values' display scales")
	}
	src.ResetForWrite(4)
	if src.DecimalData.HasDisplayScale() {
		t.Errorf("ResetForWrite kept the previous values' display scales")
	}
}

// TestDisplayScaleNeverCutsADigit is RISKS R2's check: whatever text, scale
// and copy a value goes through, its printed text names exactly the number
// its carrier holds and never more fraction digits than the carrier scale
// (invariant I1) — a display scale can only drop trailing zeros.
func TestDisplayScaleNeverCutsADigit(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for it := 0; it < 20000; it++ {
		scale := rng.Intn(12)
		intDigits := 1 + rng.Intn(8)
		frac := rng.Intn(14)
		var sb strings.Builder
		if rng.Intn(3) == 0 {
			sb.WriteByte('-')
		}
		for k := 0; k < intDigits; k++ {
			sb.WriteByte(byte('0' + rng.Intn(10)))
		}
		if frac > 0 {
			sb.WriteByte('.')
			for k := 0; k < frac; k++ {
				if rng.Intn(3) == 0 {
					sb.WriteByte('0')
				} else {
					sb.WriteByte(byte('0' + rng.Intn(10)))
				}
			}
		}
		text := sb.String()
		v := NewColumnVector(parquet.Column{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: scale}, 2)
		if err := v.SetValueChecked(0, text); err != nil {
			continue
		}
		v.CopyValueFrom(1, v, 0)
		for i := 0; i < 2; i++ {
			got := v.GetValueOf(i, rng.Intn(2) == 0).(string)
			d, err := ParseDecimalStringChecked(got, scale)
			if err != nil || d != v.DecimalData.Data[i] {
				t.Fatalf("%q at scale %d prints %q, which is not its carrier %s", text, scale, got,
					v.DecimalData.Data[i].FormatDecimal(scale))
			}
			if fd, _ := DecimalTextDScale(got); fd > scale {
				t.Fatalf("%q at scale %d prints %q: %d fraction digits past the carrier's", text, scale, got, fd)
			}
		}
	}
}
