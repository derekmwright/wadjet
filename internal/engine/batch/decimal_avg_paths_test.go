package batch

import (
	"math/big"
	"math/rand"
	"testing"
)

// DecimalAvg has three paths — an int64 division, an Int128 QuoRem, and a
// big.Int division — and they must be ONE function. The middle one was added
// for the windowed AVG (#586), which divides once per ROW because each row has
// its own frame, so the allocating big.Int path was being paid per row rather
// than per group.
//
// The reference is big.Int arithmetic written out here rather than reused from
// the implementation: a shared helper would let a rounding mistake agree with
// itself.
func decimalAvgReference(sum Int128, count int64, addScale int) (*big.Int, bool) {
	if count <= 0 || addScale < 0 {
		return nil, false
	}
	num := new(big.Int).Mul(sum.BigInt(),
		new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(addScale)), nil))
	den := big.NewInt(count)
	q, rem := new(big.Int).QuoRem(num, den, new(big.Int))
	rem.Abs(rem)
	if rem.Lsh(rem, 1).Cmp(den) >= 0 { // half away from zero
		if num.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	if !fitsInt128(q) {
		return nil, false
	}
	return q, true
}

func TestDecimalAvgPathsAgree(t *testing.T) {
	rng := rand.New(rand.NewSource(586))
	// Sums chosen to land on each path in turn: small enough for the int64
	// division, past int64 but inside the carrier once scaled (the QuoRem
	// path), and past the carrier when scaled (the big.Int path, where the
	// exact quotient may still exist).
	sums := []Int128{
		{}, Int128From(1), Int128From(-1), Int128From(7), Int128From(-7),
		Int128From(1_000_000_007), Int128From(-1_000_000_007),
		Int128From(1 << 62), Int128From(-(1 << 62)),
	}
	for i := 0; i < 400; i++ {
		hi := int64(rng.Uint64())
		lo := rng.Uint64()
		sums = append(sums, Int128{Hi: hi, Lo: lo})
	}
	// A value whose scaled form leaves the carrier, so the big.Int path runs.
	wide, _ := new(big.Int).SetString("99999999999999999999999999999999999999", 10)
	sums = append(sums, int128FromBig(wide), int128FromBig(new(big.Int).Neg(wide)))

	counts := []int64{1, 2, 3, 7, 10, 999, 1 << 20, 1<<62 + 1}
	scales := []int{0, 1, 4, 6, 10}

	for _, sum := range sums {
		for _, count := range counts {
			for _, addScale := range scales {
				got, gotOK := DecimalAvg(sum, count, addScale)
				want, wantOK := decimalAvgReference(sum, count, addScale)
				if gotOK != wantOK {
					t.Fatalf("DecimalAvg(%s, %d, %d) ok = %v, want %v",
						sum.String(), count, addScale, gotOK, wantOK)
				}
				if !gotOK {
					continue
				}
				if got.BigInt().Cmp(want) != 0 {
					t.Fatalf("DecimalAvg(%s, %d, %d) = %s, want %s",
						sum.String(), count, addScale, got.BigInt(), want)
				}
			}
		}
	}
}
