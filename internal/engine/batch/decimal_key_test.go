package batch

import (
	"math/big"
	"math/rand"
	"testing"
)

// AppendDecimalKey is the group / DISTINCT / join / bloom / shuffle key for a
// DECIMAL value. Two properties decide whether it is correct, and #474 is what
// happens when the first one fails:
//
//	P1 (injective on values)  two DIFFERENT values never produce the same key
//	P2 (scale-blind)          two representations of the SAME value — 12.75 at
//	                          scale 2 and 12.7500 at scale 4 — produce the SAME
//	                          key, because the comparator calls them equal
//
// The old encoding, math.Float64bits(ToFloat64(scale)), had P2 and lost P1 the
// moment a value needed more than ~16 significant digits.

// decKey is the key for an unscaled big.Int at a scale, as a string so it can
// be compared and used as a map key.
func decKey(t *testing.T, unscaled *big.Int, scale int) string {
	t.Helper()
	return string(AppendDecimalKey(nil, int128FromBig(unscaled), scale))
}

func TestDecimalKeyIsScaleNormalized(t *testing.T) {
	// The same VALUE written at every scale that can hold it exactly. Each
	// row is one value; every entry in it must key identically.
	same := [][]struct {
		unscaled string
		scale    int
	}{
		{{"1275", 2}, {"127500", 4}, {"12750000000", 9}, {"1275000000000", 11}},
		{{"1200", 0}, {"120000", 2}, {"1200000000", 6}},
		{{"0", 0}, {"0", 2}, {"0", 10}, {"0", 38}},
		{{"-1275", 2}, {"-127500", 4}, {"-1275000000000000", 14}},
		{{"7", 0}, {"70", 1}, {"700000000", 8}},
		// A value past 64 bits, so the normalization runs on the wide arm.
		{{"9777777778877777577887713", 10}, {"977777777887777757788771300", 12}},
		{{"-9777777778877777577887713", 10}, {"-97777777788777775778877130", 11}},
	}
	for _, group := range same {
		want := ""
		for i, rep := range group {
			u, ok := new(big.Int).SetString(rep.unscaled, 10)
			if !ok {
				t.Fatalf("bad fixture %q", rep.unscaled)
			}
			got := decKey(t, u, rep.scale)
			if i == 0 {
				want = got
				continue
			}
			if got != want {
				t.Errorf("unscaled %s @ scale %d keyed %x, but %s @ scale %d keyed %x — "+
					"the same value must key alike whatever scale it is stored at",
					rep.unscaled, rep.scale, got, group[0].unscaled, group[0].scale, want)
			}
		}
	}
}

// TestDecimalKeyIsInjective is the #474 property: values a comparator calls
// DIFFERENT must never share a key. The corpus is deliberately full of
// neighbours that agree to more than a float64's ~16 significant digits —
// exactly the pairs the old float64 key merged.
func TestDecimalKeyIsInjective(t *testing.T) {
	type cell struct {
		unscaled *big.Int
		scale    int
	}
	var cells []cell
	add := func(s string, scale int) {
		u, ok := new(big.Int).SetString(s, 10)
		if !ok {
			t.Fatalf("bad fixture %q", s)
		}
		cells = append(cells, cell{u, scale})
	}
	// The issue's own pair: DECIMAL(38,10) values one unit apart in the 25th
	// digit. float64(977777777887777.7577887713) == float64(...714).
	add("9777777778877777577887713", 10)
	add("9777777778877777577887714", 10)
	// Neighbours at the top of the Int128 range, both signs.
	add("99999999999999999999999999999999999999", 10)
	add("99999999999999999999999999999999999998", 10)
	add("-99999999999999999999999999999999999999", 10)
	// Values that differ only in trailing zeros of the UNSCALED integer but
	// not in value, and their true neighbours.
	add("1275", 2)
	add("1276", 2)
	add("12750", 2)
	add("0", 2)
	add("1", 38)
	add("-1", 38)
	// A dense sweep, so a mistake in the length byte or the sign bit shows.
	for i := -40; i <= 40; i++ {
		add(new(big.Int).SetInt64(int64(i)).String(), 3)
		add(new(big.Int).SetInt64(int64(i)*1000).String(), 3)
	}
	// Random wide values at assorted scales.
	rng := rand.New(rand.NewSource(0x474))
	for i := 0; i < 400; i++ {
		u := new(big.Int).Rand(rng, new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil))
		if rng.Intn(2) == 0 {
			u.Neg(u)
		}
		add(u.String(), rng.Intn(20))
	}

	// value(cell) is the exact rational the cell denotes; two cells key alike
	// if and only if those rationals are equal.
	value := func(c cell) *big.Rat {
		den := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(c.scale)), nil)
		return new(big.Rat).SetFrac(new(big.Int).Set(c.unscaled), den)
	}

	byKey := map[string]cell{}
	byValue := map[string]string{} // exact value text -> key
	for _, c := range cells {
		k := decKey(t, c.unscaled, c.scale)
		v := value(c).RatString()
		if prev, ok := byKey[k]; ok {
			if value(prev).Cmp(value(c)) != 0 {
				t.Fatalf("COLLISION: %s@%d and %s@%d are different values (%s vs %s) sharing key %x",
					prev.unscaled, prev.scale, c.unscaled, c.scale,
					value(prev).RatString(), v, k)
			}
		}
		byKey[k] = c
		if prevKey, ok := byValue[v]; ok && prevKey != k {
			t.Fatalf("SPLIT: value %s keyed both %x and %x (%s@%d)", v, prevKey, k, c.unscaled, c.scale)
		}
		byValue[v] = k
	}
}

// TestDecimalKeyExtremes covers the values whose magnitude arithmetic is not
// ordinary: zero at every scale, and -2^127, which negates to itself so its
// magnitude has no Int128 and must be read as unsigned bits.
func TestDecimalKeyExtremes(t *testing.T) {
	zero := string(AppendDecimalKey(nil, Int128{}, 0))
	for _, scale := range []int{0, 1, 2, 10, 38} {
		if got := string(AppendDecimalKey(nil, Int128{}, scale)); got != zero {
			t.Errorf("zero at scale %d keyed %x, want %x — zero is one value at every scale", scale, got, zero)
		}
	}
	if len(zero) != 2 || zero[0] != 0 || zero[1] != 0 {
		t.Errorf("zero key = %x, want the two-byte (scale 0, length 0) form", zero)
	}

	minInt128 := Int128{Hi: -1 << 63, Lo: 0}
	k := AppendDecimalKey(nil, minInt128, 0)
	if len(k) != 2+16 {
		t.Fatalf("-2^127 keyed %d bytes, want 18 (scale, sign|len, 16 magnitude bytes): %x", len(k), k)
	}
	if k[1] != decimalKeyNegative|16 {
		t.Errorf("-2^127 sign/length byte = %#x, want %#x", k[1], decimalKeyNegative|16)
	}
	// Its magnitude is 2^127 exactly, and +2^127 has no Int128 — so nothing
	// else can produce those bytes, which is all injectivity needs here.
	if got := string(AppendDecimalKey(nil, Int128{Hi: -1, Lo: ^uint64(0)}, 0)); got == string(k) {
		t.Error("-2^127 and -1 share a key")
	}
	if got := len(AppendDecimalKey(nil, Int128From(-1), 0)); got != 3 {
		t.Errorf("-1 keyed %d bytes, want 3", got)
	}
	// A negative scale cannot arise from a column, but the encoder must not
	// emit a byte that would desync a multi-column key if one ever did.
	if got := len(AppendDecimalKey(nil, Int128From(5), -3)); got != 3 {
		t.Errorf("negative scale keyed %d bytes, want 3", got)
	}
}

// TestDecimalKeyIsSelfDelimiting: the key sits inside a multi-column group key
// with no separator, so a concatenation of keys must parse back to exactly the
// values that made it. The length byte is what carries that; without it two
// different (a, b) pairs could share bytes and become one group.
func TestDecimalKeyIsSelfDelimiting(t *testing.T) {
	vals := []struct {
		u     int64
		scale int
	}{{0, 0}, {1, 0}, {1, 2}, {-1, 2}, {1275, 2}, {127500, 4}, {1 << 40, 3}, {-(1 << 40), 3}}
	seen := map[string][2]int{}
	for i, a := range vals {
		for j, b := range vals {
			buf := AppendDecimalKey(nil, Int128From(a.u), a.scale)
			buf = AppendDecimalKey(buf, Int128From(b.u), b.scale)
			k := string(buf)
			if prev, ok := seen[k]; ok && prev != [2]int{i, j} {
				// Only legal when both pairs denote the same two values.
				pa, pb := vals[prev[0]], vals[prev[1]]
				sameA := string(AppendDecimalKey(nil, Int128From(pa.u), pa.scale)) ==
					string(AppendDecimalKey(nil, Int128From(a.u), a.scale))
				sameB := string(AppendDecimalKey(nil, Int128From(pb.u), pb.scale)) ==
					string(AppendDecimalKey(nil, Int128From(b.u), b.scale))
				if !sameA || !sameB {
					t.Fatalf("pairs (%v,%v) and (%v,%v) concatenate to the same bytes %x", pa, pb, a, b, k)
				}
			}
			seen[k] = [2]int{i, j}
		}
	}
}

func TestDecimalKeyFitsItsAdvertisedMax(t *testing.T) {
	for _, d := range []Int128{
		{}, Int128From(1), Int128From(-1),
		{Hi: -1 << 63, Lo: 0}, {Hi: 1<<63 - 1, Lo: ^uint64(0)},
	} {
		for _, scale := range []int{0, 7, 38} {
			if got := len(AppendDecimalKey(nil, d, scale)); got > MaxDecimalKeyLen {
				t.Errorf("key for %+v @ %d is %d bytes, past MaxDecimalKeyLen=%d", d, scale, got, MaxDecimalKeyLen)
			}
		}
	}
}

func BenchmarkAppendDecimalKey(b *testing.B) {
	vals := []Int128{Int128From(1275), Int128From(-98765432), Int128From(1000000),
		{Hi: 5, Lo: 0x112210f47de98115}}
	buf := make([]byte, 0, MaxDecimalKeyLen)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = AppendDecimalKey(buf[:0], vals[i&3], 4)
	}
	_ = buf
}
