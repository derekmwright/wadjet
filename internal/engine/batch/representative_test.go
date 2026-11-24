// SPDX-License-Identifier: MIT

package batch

import (
	"math"
	"math/rand"
	"testing"
)

// The representative order (#1713) must be a TOTAL order on the
// representations one SQL-equal value can take, so that "keep the smaller"
// is a reduction that is commutative, associative and idempotent: the
// survivor of any meet order is the same one.
func TestRepresentativeOrderIsATotalOrder(t *testing.T) {
	type family struct {
		name string
		typ  TypeID
		reps []any
	}
	nan1 := math.Float64frombits(0x7ff8000000000001)
	nan2 := math.Float64frombits(0xfff8000000000000)
	families := []family{
		{"decimal 7", TypeDecimal, []any{"7", "7.0", "7.00", "7.0000000000"}},
		{"decimal -1.5", TypeDecimal, []any{"-1.5", "-1.50", "-1.500"}},
		{"decimal 0", TypeDecimal, []any{"0", "0.00", "0.0000000000"}},
		{"float64 zero", TypeFloat64, []any{math.Copysign(0, -1), 0.0}},
		{"float64 nan", TypeFloat64, []any{math.NaN(), nan1, nan2}},
		{"float32 zero", TypeFloat32, []any{float32(math.Copysign(0, -1)), float32(0)}},
		{"cidr host", TypeCIDR, []any{"10.0.0.1", "10.0.0.1/32"}},
	}
	same := func(a, b any) bool {
		switch x := a.(type) {
		case float64:
			return math.Float64bits(x) == math.Float64bits(b.(float64))
		case float32:
			return math.Float32bits(x) == math.Float32bits(b.(float32))
		}
		return a == b
	}
	for _, f := range families {
		cmp := func(a, b any) int { return RepCompareBox(f.typ, a, b) }
		for _, a := range f.reps {
			if cmp(a, a) != 0 {
				t.Errorf("%s: %v is not equal to itself", f.name, a)
			}
			for _, b := range f.reps {
				ab, ba := cmp(a, b), cmp(b, a)
				if ab != -ba {
					t.Errorf("%s: cmp(%v,%v)=%d but cmp(%v,%v)=%d: not antisymmetric", f.name, a, b, ab, b, a, ba)
				}
				if ab == 0 && !same(a, b) {
					t.Errorf("%s: two different representations %v and %v compare equal: not total", f.name, a, b)
				}
				for _, c := range f.reps {
					if ab < 0 && cmp(b, c) < 0 && cmp(a, c) >= 0 {
						t.Errorf("%s: %v < %v < %v but not %v < %v: not transitive", f.name, a, b, c, a, c)
					}
				}
			}
		}
		// The reduction: every fold order and every tree shape over a
		// multiset of members keeps the same survivor, and min(x, x) = x.
		minOf := func(a, b any) any {
			if cmp(b, a) < 0 {
				return b
			}
			return a
		}
		var want any
		for _, r := range f.reps {
			if want == nil || cmp(r, want) < 0 {
				want = r
			}
		}
		rng := rand.New(rand.NewSource(1713))
		for trial := 0; trial < 200; trial++ {
			n := 1 + rng.Intn(12)
			ms := make([]any, n)
			hasWant := false
			for i := range ms {
				ms[i] = f.reps[rng.Intn(len(f.reps))]
				hasWant = hasWant || same(ms[i], want)
			}
			if !hasWant {
				ms[rng.Intn(n)] = want
			}
			// Left fold in a random permutation.
			perm := rng.Perm(n)
			acc := ms[perm[0]]
			for _, i := range perm[1:] {
				acc = minOf(acc, ms[i])
			}
			// A random tree: pairwise reduction of a shuffled copy.
			level := append([]any(nil), ms...)
			rng.Shuffle(len(level), func(i, j int) { level[i], level[j] = level[j], level[i] })
			for len(level) > 1 {
				var next []any
				for i := 0; i+1 < len(level); i += 2 {
					next = append(next, minOf(level[i], level[i+1]))
				}
				if len(level)%2 == 1 {
					next = append(next, level[len(level)-1])
				}
				level = next
			}
			if !same(acc, want) || !same(level[0], want) {
				t.Fatalf("%s: fold %v, tree %v, want %v over %v", f.name, acc, level[0], want, ms)
			}
			if !same(minOf(acc, acc), acc) {
				t.Fatalf("%s: min(%v,%v) is not %v", f.name, acc, acc, acc)
			}
		}
	}
	if RepCompareBox(TypeFloat64, math.Copysign(0, -1), 0.0) >= 0 {
		t.Error("-0 must order before 0")
	}
	if RepCompareBox(TypeDecimal, "7", "7.00") >= 0 {
		t.Error("7 must order before 7.00 (the lower display scale)")
	}
	if RepCompareBox(TypeString, "a", "b") != 0 {
		t.Error("a type outside the order must compare 0")
	}
}

// KeyDisplayScale must be exactly the fraction digits the group key's box
// prints, for every display-scale code, or the hit path (which reads the
// code) and the merges (which read the box) would order two members
// differently.
func TestKeyDisplayScaleMatchesTheBox(t *testing.T) {
	vals := []int64{0, 7, 70, 700, -7, 15, 150, -150, 123456789, 1}
	for s := 0; s <= 5; s++ {
		pow := int64(1)
		for i := 0; i < s; i++ {
			pow *= 10
		}
		codes := []uint8{DScaleCarrier, DScaleUnknown}
		for d := 0; d <= s+1; d++ {
			codes = append(codes, uint8(d))
		}
		for _, v := range vals {
			for _, scaled := range []bool{false, true} {
				x := v
				if scaled {
					x = v * pow // the carrier of v itself, at scale s
				}
				for _, code := range codes {
					c := &DecimalColumn{Data: []Int128{Int128From(x)}, Scale: s}
					c.setDScaleCode(0, code)
					want := DecimalTextFractionDigits(c.Text(0, false))
					if got := c.KeyDisplayScale(0); got != want {
						t.Errorf("scale %d carrier %d code %d: KeyDisplayScale %d, box %q has %d",
							s, x, code, got, c.Text(0, false), want)
					}
				}
			}
		}
	}
	// A carrier past int64.
	big, _ := Int128From(1).MulPow10(30)
	for _, code := range []uint8{DScaleUnknown, 2, DScaleCarrier} {
		c := &DecimalColumn{Data: []Int128{big}, Scale: 10}
		c.setDScaleCode(0, code)
		if got, want := c.KeyDisplayScale(0), DecimalTextFractionDigits(c.Text(0, false)); got != want {
			t.Errorf("big carrier code %d: %d, box %q", code, got, c.Text(0, false))
		}
	}
}

// The row form of the compare agrees with the box form.
func TestRepCompareDecimalRowAgreesWithTheBox(t *testing.T) {
	v := NewVectorWithScale(TypeDecimal, 4, 4)
	for i, s := range []string{"7", "7.00", "7.0000", "7.0"} {
		v.SetValue(i, s)
	}
	for i := 0; i < 4; i++ {
		for _, stored := range []string{"7", "7.0", "7.00", "7.0000"} {
			got := RepCompareDecimalRow(v, i, stored)
			want := RepCompareDecimalText(v.GetValueOf(i, false).(string), stored)
			if sign(got) != sign(want) {
				t.Errorf("row %d (%v) vs %q: row form %d, box form %d", i, v.GetValueOf(i, false), stored, got, want)
			}
		}
	}
}

// The same agreement over every display-scale code, carriers that keep and
// break invariant I1, and stored boxes at every fraction length.
func TestRepCompareDecimalRowAgreesOverEveryCode(t *testing.T) {
	stored := []string{"7", "7.0", "7.00", "7.000", "0.15", "1.5"}
	for s := 0; s <= 4; s++ {
		for _, x := range []int64{0, 7, 70, 700, 7000, 15, 150, 1500, -70} {
			codes := []uint8{DScaleCarrier, DScaleUnknown}
			for d := 0; d <= s+1; d++ {
				codes = append(codes, uint8(d))
			}
			for _, code := range codes {
				v := NewVectorWithScale(TypeDecimal, 1, s)
				v.DecimalData.Data[0] = Int128From(x)
				v.DecimalData.setDScaleCode(0, code)
				for _, st := range stored {
					got := RepCompareDecimalRow(v, 0, st)
					box := v.DecimalData.Text(0, false)
					want := sign(DecimalTextFractionDigits(box) - DecimalTextFractionDigits(st))
					if sign(got) != want {
						t.Errorf("scale %d carrier %d code %d (box %q) vs %q: row form %d, box form %d",
							s, x, code, box, st, got, want)
					}
				}
			}
		}
	}
}

func sign(x int) int {
	switch {
	case x < 0:
		return -1
	case x > 0:
		return 1
	}
	return 0
}
