package parquet

import (
	"math"
	"math/big"
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// dec128 builds a carrier from a decimal STRING of the unscaled integer, so a
// case table can name a 39-digit value without arithmetic.
func dec128(t *testing.T, digits string) Decimal128 {
	t.Helper()
	n, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		t.Fatalf("not an integer: %q", digits)
	}
	d, ok := bigToDecimal128(n)
	if !ok {
		t.Fatalf("%s has no 128-bit carrier", digits)
	}
	return d
}

// bigToDecimal128 is the test's own conversion, written from the two's
// complement definition rather than from anything the implementation uses.
func bigToDecimal128(n *big.Int) (Decimal128, bool) {
	lo := new(big.Int).Lsh(big.NewInt(1), 127)
	if n.CmpAbs(lo) > 0 || (n.Sign() >= 0 && n.Cmp(new(big.Int).Sub(lo, big.NewInt(1))) > 0) {
		return Decimal128{}, false
	}
	m := new(big.Int).Set(n)
	if m.Sign() < 0 {
		m.Add(m, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	mask := new(big.Int).SetUint64(math.MaxUint64)
	low := new(big.Int).And(m, mask)
	high := new(big.Int).Rsh(m, 64)
	return Decimal128{Hi: int64(high.Uint64()), Lo: low.Uint64()}, true
}

// decimal128ToBig is the inverse, also written independently.
func decimal128ToBig(d Decimal128) *big.Int {
	n := new(big.Int).SetInt64(d.Hi)
	n.Lsh(n, 64)
	return n.Or(n, new(big.Int).SetUint64(d.Lo))
}

// TestDecimalRescaleFollowsPostgresAssignment holds the reconciliation to the
// PostgreSQL 17.11 transcript ROUND0 recorded. Every want below was measured
// live on postgres:17-alpine; the rule is the ASSIGNMENT cast, which rounds
// half AWAY FROM ZERO on the way down and is exact on the way up.
//
//	12.7567::numeric(15,2)    -> 12.76
//	12.7550::numeric(15,2)    -> 12.76
//	(-12.7550)::numeric(15,2) -> -12.76
//	12.7549::numeric(15,2)    -> 12.75
//	12.75::numeric(15,4)      -> 12.7500
//	123456789012.3456::numeric(9,2) -> 22003
func TestDecimalRescaleFollowsPostgresAssignment(t *testing.T) {
	cases := []struct {
		name                string
		unscaled            string
		from, to, precision int
		want                string // unscaled at `to`, "" when it must fail
		state               string
	}{
		{name: "identity", unscaled: "1275", from: 2, to: 2, precision: 15, want: "1275"},
		{name: "widen 2->4", unscaled: "1275", from: 2, to: 4, precision: 15, want: "127500"},
		{name: "narrow 4->2 exact", unscaled: "127500", from: 4, to: 2, precision: 15, want: "1275"},
		{name: "narrow 4->2 rounds down", unscaled: "127549", from: 4, to: 2, precision: 15, want: "1275"},
		{name: "narrow 4->2 rounds up", unscaled: "127567", from: 4, to: 2, precision: 15, want: "1276"},
		{name: "narrow 4->2 half away from zero", unscaled: "127550", from: 4, to: 2, precision: 15, want: "1276"},
		{name: "negative half away from zero", unscaled: "-127550", from: 4, to: 2, precision: 15, want: "-1276"},
		{name: "negative rounds toward zero below half", unscaled: "-127549", from: 4, to: 2, precision: 15, want: "-1275"},
		{name: "zero at every scale", unscaled: "0", from: 9, to: 0, precision: 15, want: "0"},
		{name: "narrow past the whole value", unscaled: "49", from: 4, to: 0, precision: 15, want: "0"},
		{name: "narrow to one unit", unscaled: "5000", from: 4, to: 0, precision: 15, want: "1"},
		{name: "narrow to one unit, negative", unscaled: "-5000", from: 4, to: 0, precision: 15, want: "-1"},
		// The band. 123456789012.3456 at scale 4 is 1234567890123456; moving
		// it to scale 2 is 12345678901235, which is 14 digits and a
		// DECIMAL(9,2) holds an absolute value below 10^7.
		{name: "past the declared precision", unscaled: "1234567890123456",
			from: 4, to: 2, precision: 9, state: "22003"},
		// Widening past the CARRIER, not merely past the band.
		{name: "widen past the carrier", unscaled: "170141183460469231731687303715884105727",
			from: 0, to: 1, precision: 38, state: "22003"},
		// A file whose scale no carrier represents is refused BY NAME rather
		// than read as though the scale were zero.
		{name: "negative file scale", unscaled: "1", from: -1, to: 2, precision: 15, state: "22003"},
		{name: "file scale past the carrier", unscaled: "1", from: 39, to: 2, precision: 15, state: "22003"},
		// The identity call still holds the value to the declared precision:
		// a file may carry what its own (p, s) admits and the catalog's does
		// not.
		{name: "identity past the declared precision", unscaled: "1000000000",
			from: 2, to: 2, precision: 9, state: "22003"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecimalRescale(dec128(t, tc.unscaled), tc.from, tc.to, tc.precision)
			if tc.state != "" {
				if err == nil {
					t.Fatalf("rescale(%s, %d->%d, p=%d) = %s, want SQLSTATE %s",
						tc.unscaled, tc.from, tc.to, tc.precision, got.String(), tc.state)
				}
				if s := sqlerr.StateOf(err); s != tc.state {
					t.Fatalf("SQLSTATE %s, want %s (%v)", s, tc.state, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("rescale(%s, %d->%d, p=%d): %v",
					tc.unscaled, tc.from, tc.to, tc.precision, err)
			}
			if got.String() != tc.want {
				t.Errorf("rescale(%s, %d->%d) = %s, want %s (live PostgreSQL 17.11)",
					tc.unscaled, tc.from, tc.to, got.String(), tc.want)
			}
		})
	}
}

// bigRescaleOracle is the rescaling rule written from scratch against
// math/big: no constant, no helper and no branch is shared with
// DecimalRescale, which is the whole point (correctness-fix protocol method
// 5). ok=false means the exact result has no 128-bit carrier.
func bigRescaleOracle(n *big.Int, from, to int) (*big.Int, bool) {
	if to >= from {
		p := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(to-from)), nil)
		out := new(big.Int).Mul(n, p)
		if _, ok := bigToDecimal128(out); !ok {
			return nil, false
		}
		return out, true
	}
	div := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(from-to)), nil)
	q, r := new(big.Int).QuoRem(n, div, new(big.Int))
	// Half away from zero: |r| * 2 >= div rounds the magnitude up.
	twice := new(big.Int).Abs(r)
	twice.Lsh(twice, 1)
	if twice.Cmp(div) >= 0 {
		if n.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	if _, ok := bigToDecimal128(q); !ok {
		return nil, false
	}
	return q, true
}

// TestDecimalRescaleMatchesBigRat is the independent-oracle property test the
// protocol asks for on arithmetic: random carriers over every ordered pair of
// scales in 0..38, compared against a from-scratch math/big rescaling, with the
// boundaries (0, ±1, ±5 at the rounding point, ±2^127-1, powers of ten) forced
// into the corpus rather than left to chance.
func TestDecimalRescaleMatchesBigRat(t *testing.T) {
	rng := rand.New(rand.NewSource(20260903))
	var corpus []*big.Int
	for _, s := range []string{
		"0", "1", "-1", "5", "-5", "4", "-4", "9", "-9", "10", "-10", "50", "-50",
		"99", "-99", "100", "-100", "12345", "-12345",
		"170141183460469231731687303715884105727",  // 2^127-1
		"-170141183460469231731687303715884105728", // -2^127
		"99999999999999999999999999999999999999",   // 10^38-1
		"-99999999999999999999999999999999999999",
	} {
		n, _ := new(big.Int).SetString(s, 10)
		corpus = append(corpus, n)
	}
	for i := 0; i < 400; i++ {
		bitsWide := 1 + rng.Intn(127)
		n := new(big.Int).Rand(rng, new(big.Int).Lsh(big.NewInt(1), uint(bitsWide)))
		if rng.Intn(2) == 0 {
			n.Neg(n)
		}
		corpus = append(corpus, n)
	}
	for _, n := range corpus {
		d, ok := bigToDecimal128(n)
		if !ok {
			continue
		}
		for from := 0; from <= MaxDecimalDigits; from++ {
			for to := 0; to <= MaxDecimalDigits; to++ {
				// precision 38 is the widest bound the carrier can enforce, so
				// this arm isolates the SCALING rule from the band.
				got, err := DecimalRescale(d, from, to, MaxDecimalDigits)
				want, ok := bigRescaleOracle(n, from, to)
				if !ok {
					// No carrier for the exact result — but the value may
					// still be in the 10^38 band's shadow, so the only claim
					// is that it must not come back as a number.
					if err == nil && got.String() == "0" && n.Sign() != 0 && to < from {
						continue // rounded away to zero, which the oracle also does
					}
					if err == nil {
						t.Fatalf("rescale(%s, %d->%d) = %s but the exact result has no carrier",
							n, from, to, got.String())
					}
					continue
				}
				// The oracle has a carrier. DecimalRescale may still refuse it
				// for the BAND (|v| >= 10^38), which the oracle does not model.
				if err != nil {
					lim := new(big.Int).Exp(big.NewInt(10), big.NewInt(MaxDecimalDigits), nil)
					if want.CmpAbs(lim) >= 0 {
						continue
					}
					t.Fatalf("rescale(%s, %d->%d) refused a value in band: %v", n, from, to, err)
				}
				if decimal128ToBig(got).Cmp(want) != 0 {
					t.Fatalf("rescale(%s, %d->%d) = %s, want %s (math/big oracle)",
						n, from, to, got.String(), want)
				}
			}
		}
	}
}

// TestDecimalRescaleIsMonotone is what makes rescaling a row group's MIN/MAX
// bounds exact rather than approximate (ReconcileRowGroupStats): if x <= y then
// rescale(x) <= rescale(y), so the rescaled minimum is the minimum of the
// rescaled values. A rounding rule that broke this would let the prune drop a
// row group holding a matching row.
func TestDecimalRescaleIsMonotone(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for _, pair := range [][2]int{{4, 2}, {6, 0}, {2, 4}, {0, 6}, {9, 8}, {38, 0}} {
		from, to := pair[0], pair[1]
		prev := new(big.Int)
		var prevOut *big.Int
		vals := make([]*big.Int, 0, 300)
		for i := 0; i < 300; i++ {
			vals = append(vals, big.NewInt(int64(rng.Intn(4_000_000)-2_000_000)))
		}
		// Sorted, so consecutive pairs are the ordered pairs to check.
		for i := 0; i < len(vals); i++ {
			for j := i + 1; j < len(vals); j++ {
				if vals[j].Cmp(vals[i]) < 0 {
					vals[i], vals[j] = vals[j], vals[i]
				}
			}
		}
		prevOut = nil
		for _, v := range vals {
			d, _ := bigToDecimal128(v)
			out, err := DecimalRescale(d, from, to, MaxDecimalDigits)
			if err != nil {
				t.Fatalf("rescale(%s, %d->%d): %v", v, from, to, err)
			}
			o := decimal128ToBig(out)
			if prevOut != nil && o.Cmp(prevOut) < 0 {
				t.Fatalf("rescale is not monotone at %d->%d: %s -> %s after %s -> %s",
					from, to, prev, o, prev, prevOut)
			}
			prev, prevOut = v, o
		}
	}
}

// TestDecimalRescalePlanNamesItsBoundary attempts the boundary from BOTH sides
// (protocol rules 10 and 11): the shapes the reconciliation acts on, and the
// shapes just outside it that it must decline.
func TestDecimalRescalePlanNamesItsBoundary(t *testing.T) {
	decLeaf := func(scale int32) *SchemaNode {
		p := PhysicalInt64
		lt := &LogicalType{Type: LogicalDecimal, Precision: 15, Scale: int(scale)}
		return &SchemaNode{Name: "a", Type: &p, LogicalType: lt, Precision: 15, Scale: scale}
	}
	intLeaf := func() *SchemaNode {
		p := PhysicalInt64
		return &SchemaNode{Name: "a", Type: &p}
	}
	group := func() *SchemaNode { return &SchemaNode{Name: "g"} }
	want := Column{Name: "a", Type: TypeDecimal, Precision: 15, Scale: 2}

	cases := []struct {
		name string
		leaf *SchemaNode
		col  Column
		from int
		need bool
	}{
		{"file finer than the catalog", decLeaf(4), want, 4, true},
		{"file coarser than the catalog", decLeaf(0), want, 0, true},
		{"file agrees", decLeaf(2), want, 0, false},
		// Just outside: the destination is not a DECIMAL at all.
		{"destination is not a decimal", decLeaf(4),
			Column{Name: "a", Type: TypeInt64}, 0, false},
		// Just outside: the LEAF is not a decimal, so the file states no
		// scale and ADR-0018 §4's already-unscaled rule stands.
		{"file leaf carries no decimal annotation", intLeaf(), want, 0, false},
		{"leaf is a group", group(), want, 0, false},
		{"no leaf at all", nil, want, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			from, need := DecimalRescalePlan(tc.leaf, tc.col)
			if need != tc.need || from != tc.from {
				t.Errorf("plan = (%d, %v), want (%d, %v)", from, need, tc.from, tc.need)
			}
		})
	}
}

// TestDecimalFileScaleReadsTheAnnotation checks the half of the plan that
// reads the FILE, including the case an unannotated leaf produces.
func TestDecimalFileScaleReadsTheAnnotation(t *testing.T) {
	p := PhysicalInt64
	lt := &LogicalType{Type: LogicalDecimal, Precision: 9, Scale: 3}
	leaf := &SchemaNode{Name: "a", Type: &p, LogicalType: lt, Precision: 9, Scale: 3}
	if s, ok := DecimalFileScale(leaf); !ok || s != 3 {
		t.Errorf("DecimalFileScale = (%d, %v), want (3, true)", s, ok)
	}
	bare := &SchemaNode{Name: "a", Type: &p}
	if s, ok := DecimalFileScale(bare); ok {
		t.Errorf("an unannotated INT64 leaf reported scale %d; the file states none", s)
	}
	if _, ok := DecimalFileScale(nil); ok {
		t.Error("a nil leaf reported a scale")
	}
}
