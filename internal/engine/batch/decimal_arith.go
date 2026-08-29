package batch

import (
	"math/big"
	"math/bits"
	"strings"
)

// Exact fixed-point arithmetic over the Int128 carrier.
//
// The contract is docs/adr/0024: DECIMAL is a finite 128-bit fixed-point type,
// a value with no exact carrier at its declared type is an error and never a
// saturated, wrapped or float-narrowed answer, and scale reduction rounds HALF
// AWAY FROM ZERO (PostgreSQL's numeric rounding).
//
// Every function here answers the same two questions: the EXACT value the two
// operands name at the requested output scale, and whether that value has an
// Int128. A status other than DecimalOK is never "close enough" — it means
// the caller must raise the SQLSTATE the status names rather than show anyone
// the first return.
//
// **DecimalOK means the value fits the CARRIER, not the declared (p,s).** The
// two bounds are different: 9 + (10^38-1), whose result type by ADR-0024 item
// 3 is DECIMAL(38,0), is 10^38+8 — an Int128 the carrier holds happily and a
// value DECIMAL(38,0) cannot declare. A wiring site that must honour item 4
// ("a value with no exact carrier AT ITS DECLARED TYPE is a 22003 error")
// wants the DecimalAddAt / SubAt / MulAt / DivAt / ModAt wrappers below, which
// fold DecimalFitsPrecision in; the bare ops leave that bound to the caller
// because some callers (an intermediate, an unconstrained result) have none.
//
// Shape of each op: an Int128 fast path (no allocation, no math/big), and an
// exact big.Int fallback taken only when an INTERMEDIATE overflows the carrier.
// The fallback is not a second opinion — it is the same rule computed at a
// width the intermediate needs, so `a*b` at a large product scale still answers
// when the result at outScale fits. Both paths round once, at outScale.

// DecimalStatus is why a fixed-point operation did or did not produce a value.
// It exists because "no answer" has two causes that PostgreSQL reports as two
// different conditions, and a single bool made them one: a caller writing
// `if !ok { raise 22003 }` reports a numeric overflow for `x / 0`.
type DecimalStatus uint8

const (
	// DecimalOK: the first return is the exact value at the requested scale.
	// It fits the Int128 carrier; see the package note above on why that is
	// not the same as fitting a declared (p,s).
	DecimalOK DecimalStatus = iota
	// DecimalOverflow: the exact value has no Int128 (or, from the ...At
	// wrappers, no value at the declared precision). SQLSTATE 22003,
	// numeric_value_out_of_range.
	DecimalOverflow
	// DecimalDivByZero: the divisor is zero, for both / and %. SQLSTATE
	// 22012, division_by_zero.
	DecimalDivByZero
	// DecimalInvalidScale: a negative scale was asked for. Not a user-visible
	// condition — a column's scale is non-negative by DDL — so a caller that
	// sees this has a planner defect to report as an internal error, not a
	// numeric one.
	DecimalInvalidScale
)

// String names the status, for the error messages the wiring sites build.
func (s DecimalStatus) String() string {
	switch s {
	case DecimalOK:
		return "ok"
	case DecimalOverflow:
		return "numeric value out of range"
	case DecimalDivByZero:
		return "division by zero"
	case DecimalInvalidScale:
		return "invalid scale"
	}
	return "unknown decimal status"
}

// Mul returns d * other and reports whether the EXACT product fits an Int128.
//
// The intermediate is the full 256-bit product of the two magnitudes, built
// from four bits.Mul64 partial products — never big.Int, because this is the
// kernel a DECIMAL multiply runs per row. A false second result means the
// first is zero and carries no information: the product is outside the range,
// full stop.
func (d Int128) Mul(other Int128) (Int128, bool) {
	aHi, aLo, aNeg := d.magnitude()
	bHi, bLo, bNeg := other.magnitude()
	neg := aNeg != bNeg

	r3, r2, r1, r0 := mulMag256(aHi, aLo, bHi, bLo)
	if r3 != 0 || r2 != 0 {
		return Int128{}, false // magnitude needs more than 128 bits
	}
	if r1&(1<<63) != 0 {
		// Magnitude >= 2^127. The signed range holds exactly one such value,
		// -2^127, and only on the negative side.
		if !neg || r1 != 1<<63 || r0 != 0 {
			return Int128{}, false
		}
		return Int128Min, true
	}
	res := Int128{Hi: int64(r1), Lo: r0}
	if neg {
		res = res.Neg()
	}
	return res, true
}

// mulMag256 returns the exact 256-bit product of two unsigned 128-bit
// magnitudes, most significant word first.
func mulMag256(aHi, aLo, bHi, bLo uint64) (r3, r2, r1, r0 uint64) {
	h00, l00 := bits.Mul64(aLo, bLo)
	h01, l01 := bits.Mul64(aLo, bHi)
	h10, l10 := bits.Mul64(aHi, bLo)
	h11, l11 := bits.Mul64(aHi, bHi)

	r0 = l00
	var c1, c2, c uint64
	r1, c1 = bits.Add64(h00, l01, 0)
	r1, c2 = bits.Add64(r1, l10, 0)

	var carry uint64
	r2, c = bits.Add64(h01, h10, 0)
	carry = c
	r2, c = bits.Add64(r2, l11, 0)
	carry += c
	r2, c = bits.Add64(r2, c1+c2, 0)
	carry += c

	// (2^128-1)^2 < 2^256, so the top word cannot itself carry out.
	r3 = h11 + carry
	return r3, r2, r1, r0
}

// SubChecked returns d - other and reports whether the EXACT difference fits
// an Int128. A false second result means the first is the WRAPPED value.
//
// The twin of AddChecked, and needed for the same reason: a sliding window
// frame retracts a row from a running DECIMAL sum by subtracting it, and a
// wrapped difference there is a plausible-looking number that is not the
// answer. Sub alone cannot report it, and negating `other` first does not
// work — -2^127 negates to itself.
func (d Int128) SubChecked(other Int128) (Int128, bool) {
	diff := d.Sub(other)
	// Signed subtraction overflows exactly when the operands' signs DIFFER
	// and the result takes the subtrahend's sign.
	if (d.Hi < 0) != (other.Hi < 0) && (diff.Hi < 0) != (d.Hi < 0) {
		return diff, false
	}
	return diff, true
}

// QuoRem returns the truncated quotient and the remainder of d / other, with
// Go's (and PostgreSQL's, and C's) sign rule: the quotient truncates TOWARD
// ZERO and the remainder takes the sign of the dividend, so
// d == q*other + r always.
//
// ok=false for the two divisions that have no answer in the carrier: a zero
// divisor, and -2^127 / -1 whose quotient is 2^127.
func (d Int128) QuoRem(other Int128) (q, r Int128, ok bool) {
	if other.IsZero() {
		return Int128{}, Int128{}, false
	}
	if d.Equal(Int128Min) && other.Equal(Int128From(-1)) {
		return Int128{}, Int128{}, false
	}
	aHi, aLo, aNeg := d.magnitude()
	bHi, bLo, bNeg := other.magnitude()
	qHi, qLo, rHi, rLo := divMag(aHi, aLo, bHi, bLo)

	// The quotient's magnitude reaches 2^127 only for Int128Min / 1, whose
	// bits are already Int128Min and whose sign is negative — so the negate
	// below is a no-op on exactly the value that needs one.
	q = Int128{Hi: int64(qHi), Lo: qLo}
	r = Int128{Hi: int64(rHi), Lo: rLo}
	if aNeg != bNeg {
		q = q.Neg()
	}
	if aNeg {
		r = r.Neg()
	}
	return q, r, true
}

// divMag divides one unsigned 128-bit magnitude by another, returning the
// quotient and the remainder. The divisor must not be zero.
func divMag(nHi, nLo, dHi, dLo uint64) (qHi, qLo, rHi, rLo uint64) {
	if dHi == 0 {
		// bits.Div64 requires its high word to be below the divisor, which
		// is why the wide dividend is split into two 64-bit divisions.
		if nHi < dLo {
			q, rem := bits.Div64(nHi, nLo, dLo)
			return 0, q, 0, rem
		}
		hi, rem := bits.Div64(0, nHi, dLo)
		lo, rem2 := bits.Div64(rem, nLo, dLo)
		return hi, lo, 0, rem2
	}
	// The divisor is at least 2^64 and the dividend below 2^128, so the
	// quotient is below 2^64.
	if cmpMag(nHi, nLo, dHi, dLo) < 0 {
		return 0, 0, nHi, nLo
	}
	sh := uint(bits.LeadingZeros64(dHi))
	if sh == 0 {
		// The divisor's top bit is set, so the quotient is 1 (it is at least
		// 1 by the comparison above, and 2*d does not fit 128 bits).
		hi, lo := subMag(nHi, nLo, dHi, dLo)
		return 0, 1, hi, lo
	}
	// Knuth's normalized estimate: shift the divisor left so its top bit is
	// set, take the leading 128 bits of the shifted dividend over the
	// divisor's leading word. The estimate is never below the true quotient
	// and never more than two above it.
	vHi := dHi<<sh | dLo>>(64-sh)
	uHi := nHi >> (64 - sh) // < 2^sh <= 2^63 <= vHi, so Div64 is in range
	uMid := nHi<<sh | nLo>>(64-sh)
	qhat, _ := bits.Div64(uHi, uMid, vHi)

	for {
		pHi, pLo, fits := mulU64Mag(qhat, dHi, dLo)
		if fits && cmpMag(pHi, pLo, nHi, nLo) <= 0 {
			hi, lo := subMag(nHi, nLo, pHi, pLo)
			// A guard, not a step this estimate needs: Knuth's normalized
			// qhat is never BELOW the true quotient, so the remainder here
			// is already under the divisor and this loop is provably dead
			// today. It is kept so the function stays correct under any
			// future estimate, and because "remainder < divisor" is the
			// cheapest possible assertion of that.
			for cmpMag(hi, lo, dHi, dLo) >= 0 {
				qhat++
				hi, lo = subMag(hi, lo, dHi, dLo)
			}
			return 0, qhat, hi, lo
		}
		// The load-bearing correction: the estimate can exceed the true
		// quotient by up to two, and near the top of the range that overshoot
		// also makes qhat*d overflow 128 bits — which is why mulU64Mag
		// reports the overflow instead of wrapping, and why `fits` is part of
		// the test above rather than an assumption.
		qhat-- // cannot underflow: the true quotient is at least 1 and satisfies the test
	}
}

// mulU64Mag multiplies a 128-bit magnitude by a 64-bit one, reporting whether
// the product still fits 128 bits.
func mulU64Mag(m uint64, dHi, dLo uint64) (hi, lo uint64, ok bool) {
	h0, l0 := bits.Mul64(m, dLo)
	h1, l1 := bits.Mul64(m, dHi)
	if h1 != 0 {
		return 0, 0, false
	}
	hi, carry := bits.Add64(h0, l1, 0)
	if carry != 0 {
		return 0, 0, false
	}
	return hi, l0, true
}

// cmpMag orders two unsigned 128-bit magnitudes.
func cmpMag(aHi, aLo, bHi, bLo uint64) int {
	switch {
	case aHi != bHi:
		if aHi < bHi {
			return -1
		}
		return 1
	case aLo != bLo:
		if aLo < bLo {
			return -1
		}
		return 1
	}
	return 0
}

// subMag returns a - b for two unsigned 128-bit magnitudes with a >= b.
func subMag(aHi, aLo, bHi, bLo uint64) (hi, lo uint64) {
	lo, borrow := bits.Sub64(aLo, bLo, 0)
	hi, _ = bits.Sub64(aHi, bHi, borrow)
	return hi, lo
}

// pow10Int128 holds 10^0 .. 10^38 — every power of ten a DECIMAL scale or
// precision can name, and the widest the carrier holds (10^39 > 2^127-1).
var pow10Int128 = func() [MaxDecimalPrecision + 1]Int128 {
	var t [MaxDecimalPrecision + 1]Int128
	v := Int128From(1)
	for i := range t {
		t[i] = v
		if i < MaxDecimalPrecision {
			next, ok := v.MulPow10(1)
			if !ok {
				panic("batch: 10^38 must fit an Int128")
			}
			v = next
		}
	}
	return t
}()

// DecimalPow10 returns 10^n as an Int128, ok=false past 10^38 where the
// carrier has no such value.
func DecimalPow10(n int) (Int128, bool) {
	if n < 0 || n > MaxDecimalPrecision {
		return Int128{}, false
	}
	return pow10Int128[n], true
}

// DecimalFitsPrecision reports whether the unscaled value v is inside the
// bound DECIMAL(p, s) declares: |v| < 10^p.
//
// This is the exported home of what exec.decimalFitsPrecision and
// physical.setOpDecimalFitsPrecision each rebuilt on their own side of a
// package boundary; both should call this so the single-process and stage-DAG
// overflow decisions cannot drift. Its two edge conventions are theirs,
// unchanged: p <= 0 is the codebase's "unconstrained" sentinel and p past 38
// is a bound the carrier cannot even express, so both mean "no bound to
// check" — true, not a rejection of every row.
func DecimalFitsPrecision(v Int128, p int) bool {
	limit, ok := DecimalPow10(p)
	if !ok || p <= 0 {
		return true
	}
	mag, ok := absInt128(v)
	if !ok {
		// -2^127 has no Int128 magnitude, so it is outside every precision
		// this carrier can declare.
		return false
	}
	return mag.Cmp(limit) < 0
}

// absInt128 returns |v|, ok=false for -2^127 whose magnitude has no Int128.
func absInt128(v Int128) (Int128, bool) {
	if !v.IsNegative() {
		return v, true
	}
	m := v.Neg()
	if m.IsNegative() {
		return Int128{}, false
	}
	return m, true
}

// Rescale moves an unscaled value from one scale to another: exactly when the
// scale rises, rounded HALF AWAY FROM ZERO when it falls, and ok=false when
// the exact result has no Int128.
//
// Rounding away from zero is PostgreSQL's numeric rounding and ADR-0024's, so
// 1.5 and -1.5 at scale 0 are 2 and -2, not the 2 and -2 of banker's rounding.
// The upward half is MulPow10, unchanged, so a rescale that only widens is the
// same exact shift every DECIMAL comparison already uses.
func Rescale(v Int128, fromScale, toScale int) (Int128, bool) {
	if fromScale < 0 || toScale < 0 {
		return Int128{}, false
	}
	if toScale == fromScale {
		return v, true
	}
	if toScale > fromScale {
		return v.MulPow10(toScale - fromScale)
	}
	drop := fromScale - toScale
	if drop > MaxDecimalPrecision {
		// The divisor is 10^39 or wider; half of it is above 5*10^38 and no
		// Int128 magnitude reaches 2^127-1 < 1.71*10^38, so every value
		// rounds to zero.
		return Int128{}, true
	}
	div := pow10Int128[drop]
	q, r, ok := v.QuoRem(div)
	if !ok {
		return Int128{}, false // unreachable: div is positive and not -1
	}
	if r.IsZero() {
		return q, true
	}
	mag, ok := absInt128(r)
	if !ok {
		return Int128{}, false
	}
	// |r|*2 >= div, written as |r| >= div - |r| because doubling a remainder
	// near 10^38 overflows the carrier while the subtraction cannot: both
	// sides are non-negative and below div.
	if mag.Cmp(div.Sub(mag)) >= 0 {
		step := Int128From(1)
		if v.IsNegative() {
			step = step.Neg()
		}
		return q.AddChecked(step)
	}
	return q, true
}

// DecimalAdd returns a (at aScale) + b (at bScale) as an unscaled value at
// outScale, exactly, rounded half away from zero if outScale is narrower than
// the sum's own scale. DecimalOverflow when the exact result has no Int128.
func DecimalAdd(a Int128, aScale int, b Int128, bScale int, outScale int) (Int128, DecimalStatus) {
	return decAddSub(a, aScale, b, bScale, outScale, false)
}

// DecimalSub returns a (at aScale) - b (at bScale) at outScale, under the same
// contract as DecimalAdd.
func DecimalSub(a Int128, aScale int, b Int128, bScale int, outScale int) (Int128, DecimalStatus) {
	return decAddSub(a, aScale, b, bScale, outScale, true)
}

func decAddSub(a Int128, aScale int, b Int128, bScale, outScale int, sub bool) (Int128, DecimalStatus) {
	if aScale < 0 || bScale < 0 || outScale < 0 {
		return Int128{}, DecimalInvalidScale
	}
	common := max(aScale, bScale)
	x, okA := Rescale(a, aScale, common)
	y, okB := Rescale(b, bScale, common)
	if okA && okB {
		var sum Int128
		var ok bool
		if sub {
			sum, ok = x.SubChecked(y)
		} else {
			sum, ok = x.AddChecked(y)
		}
		if ok {
			if out, ok := Rescale(sum, common, outScale); ok {
				return out, DecimalOK
			}
			// The sum itself fit and the rescale did not: the exact result
			// has no Int128 at outScale, and no wider intermediate changes
			// that. Rescale already rounded once, so this is final.
			return Int128{}, DecimalOverflow
		}
	}
	// An operand's rescale or the sum overflowed the carrier. The exact
	// result at outScale can still fit — outScale may be narrower than the
	// common scale — so recompute at big width and round once, the same way.
	n := new(big.Int).Mul(a.BigInt(), bigPow10(common-aScale))
	m := new(big.Int).Mul(b.BigInt(), bigPow10(common-bScale))
	if sub {
		n.Sub(n, m)
	} else {
		n.Add(n, m)
	}
	return decStatus(bigRescale(n, common, outScale))
}

// decStatus lifts an internal (value, fits-the-carrier) pair into the public
// status, so the only way a caller can see a value is with DecimalOK.
func decStatus(v Int128, ok bool) (Int128, DecimalStatus) {
	if !ok {
		return Int128{}, DecimalOverflow
	}
	return v, DecimalOK
}

// DecimalMul returns a (at aScale) * b (at bScale) at outScale. The product's
// natural scale is aScale+bScale; outScale below that rounds half away from
// zero, exactly once. DecimalOverflow when the exact result has no Int128.
func DecimalMul(a Int128, aScale int, b Int128, bScale int, outScale int) (Int128, DecimalStatus) {
	if aScale < 0 || bScale < 0 || outScale < 0 {
		return Int128{}, DecimalInvalidScale
	}
	if p, ok := a.Mul(b); ok {
		if out, ok := Rescale(p, aScale+bScale, outScale); ok {
			return out, DecimalOK
		}
		return Int128{}, DecimalOverflow
	}
	// The 128-bit product overflowed. At a narrower outScale the ANSWER may
	// still fit — DECIMAL(38,10) x DECIMAL(38,10) declared (38,6) is the
	// ordinary case, since ADR-0024 item 3's adjustment lands every wide
	// product on a scale well below s1+s2 — so the product is taken at 256
	// bits and divided back down.
	if drop := aScale + bScale - outScale; drop >= 1 && drop <= 19 {
		return decStatus(mulRescale256(a, b, drop))
	}
	n := new(big.Int).Mul(a.BigInt(), b.BigInt())
	return decStatus(bigRescale(n, aScale+bScale, outScale))
}

// mulRescale256 multiplies two Int128s and divides the exact 256-bit product
// by 10^drop, rounding half away from zero — the whole of a wide DECIMAL
// multiply without leaving the machine words, and so without the allocation
// per row the big.Int arm costs (measured 60x per element).
//
// drop must be in 1..19, where the divisor is a single uint64 and both the
// quotient's words and the remainder come straight out of a bits.Div64 chain.
// A wider drop is rare — it needs the two scales to sum past 19 more than the
// output keeps — and takes the big.Int path instead; a 256-by-128 division
// would buy that case the same win and is the obvious next step if a profile
// ever shows it.
func mulRescale256(a, b Int128, drop int) (Int128, bool) {
	aHi, aLo, aNeg := a.magnitude()
	bHi, bLo, bNeg := b.magnitude()
	neg := aNeg != bNeg

	r3, r2, r1, r0 := mulMag256(aHi, aLo, bHi, bLo)
	d := pow10u64[drop]
	q0, rem := bits.Div64(0, r3, d)
	q1, rem := bits.Div64(rem, r2, d)
	q2, rem := bits.Div64(rem, r1, d)
	q3, rem := bits.Div64(rem, r0, d)
	if q0 != 0 || q1 != 0 {
		return Int128{}, false // the quotient alone needs more than 128 bits
	}
	hi, lo := q2, q3
	// |r|*2 >= d, written so that a divisor above 2^63 cannot overflow the
	// doubling — the same form Rescale uses at 128 bits.
	if rem >= d-rem {
		var carry uint64
		lo, carry = bits.Add64(lo, 1, 0)
		hi, carry = bits.Add64(hi, 0, carry)
		if carry != 0 {
			return Int128{}, false
		}
	}
	if hi&(1<<63) != 0 {
		if !neg || hi != 1<<63 || lo != 0 {
			return Int128{}, false
		}
		return Int128Min, true
	}
	res := Int128{Hi: int64(hi), Lo: lo}
	if neg {
		res = res.Neg()
	}
	return res, true
}

// DecimalDiv returns a (at aScale) / b (at bScale) at outScale, rounded half
// away from zero, exactly once.
//
// The rounding decision is made from the REMAINDER of a single exact integer
// division, never from a quotient computed at some wider scale and rounded a
// second time: 0.1249 rounded to scale 3 is 0.125 and rounded again to scale 2
// is 0.13, where the one correct answer is 0.12. One division, one rounding.
//
// DecimalDivByZero and DecimalOverflow are separate answers here, and they
// are separate SQLSTATEs — 22012 and 22003 — so a caller must branch on the
// status rather than on "did it produce a value".
func DecimalDiv(a Int128, aScale int, b Int128, bScale int, outScale int) (Int128, DecimalStatus) {
	if aScale < 0 || bScale < 0 || outScale < 0 {
		return Int128{}, DecimalInvalidScale
	}
	if b.IsZero() {
		return Int128{}, DecimalDivByZero
	}
	// value = (a/10^aScale) / (b/10^bScale), so the unscaled result at
	// outScale is a * 10^(outScale+bScale-aScale) / b. A negative exponent
	// moves onto the divisor rather than truncating the dividend early.
	e := outScale + bScale - aScale
	num, den := a, b
	fast := true
	if e >= 0 {
		if n, ok := a.MulPow10(e); ok {
			num = n
		} else {
			fast = false
		}
	} else {
		if d, ok := b.MulPow10(-e); ok {
			den = d
		} else {
			fast = false
		}
	}
	// |Int128Min| has no Int128, and the rounding comparison below needs the
	// divisor's magnitude; that lone value takes the wide path.
	if fast && !den.Equal(Int128Min) {
		q, r, ok := num.QuoRem(den)
		if ok {
			if r.IsZero() {
				return q, DecimalOK
			}
			magR, okR := absInt128(r)
			magD, okD := absInt128(den)
			if okR && okD {
				if magR.Cmp(magD.Sub(magR)) >= 0 {
					step := Int128From(1)
					if num.IsNegative() != den.IsNegative() {
						step = step.Neg()
					}
					return decStatus(q.AddChecked(step))
				}
				return q, DecimalOK
			}
		}
	}
	bn, bd := a.BigInt(), b.BigInt()
	if e >= 0 {
		bn.Mul(bn, bigPow10(e))
	} else {
		bd.Mul(bd, bigPow10(-e))
	}
	return decStatus(bigDivRound(bn, bd))
}

// DecimalMod returns the remainder of a (at aScale) / b (at bScale) at
// outScale. The remainder takes the sign of the DIVIDEND, PostgreSQL's rule
// and Go's; its natural scale is max(aScale,bScale).
//
// A zero divisor is DecimalDivByZero: PostgreSQL raises 22012 for `%` exactly
// as it does for `/`, so the two ops report it the same way here.
func DecimalMod(a Int128, aScale int, b Int128, bScale int, outScale int) (Int128, DecimalStatus) {
	if aScale < 0 || bScale < 0 || outScale < 0 {
		return Int128{}, DecimalInvalidScale
	}
	if b.IsZero() {
		return Int128{}, DecimalDivByZero
	}
	common := max(aScale, bScale)
	x, okA := Rescale(a, aScale, common)
	y, okB := Rescale(b, bScale, common)
	if okA && okB && !y.IsZero() {
		if _, r, ok := x.QuoRem(y); ok {
			return decStatus(Rescale(r, common, outScale))
		}
	}
	n := new(big.Int).Mul(a.BigInt(), bigPow10(common-aScale))
	m := new(big.Int).Mul(b.BigInt(), bigPow10(common-bScale))
	if m.Sign() == 0 {
		return Int128{}, DecimalDivByZero
	}
	r := new(big.Int).Rem(n, m) // Rem truncates toward zero: the sign of n
	return decStatus(bigRescale(r, common, outScale))
}

// The ...At wrappers are the ops with ADR-0024 item 4 folded in: they compute
// at the declared scale s and then check the declared precision p, so a value
// that has an Int128 but no place in DECIMAL(p,s) comes back DecimalOverflow
// rather than as a number the column cannot hold. That second bound is the
// one the bare ops leave to the caller (see the package note), and the reason
// it is easy to forget is that the two disagree only near the top of the
// range: 9 + (10^38-1) at DECIMAL(38,0) is a perfectly good Int128.
//
// p <= 0 keeps the codebase's "unconstrained" meaning: no precision bound to
// apply, only the carrier's.

// DecimalAddAt returns a + b at the declared DECIMAL(p, s).
func DecimalAddAt(a Int128, aScale int, b Int128, bScale, p, s int) (Int128, DecimalStatus) {
	v, st := DecimalAdd(a, aScale, b, bScale, s)
	return decimalAtPrecision(v, st, p)
}

// DecimalSubAt returns a - b at the declared DECIMAL(p, s).
func DecimalSubAt(a Int128, aScale int, b Int128, bScale, p, s int) (Int128, DecimalStatus) {
	v, st := DecimalSub(a, aScale, b, bScale, s)
	return decimalAtPrecision(v, st, p)
}

// DecimalMulAt returns a * b at the declared DECIMAL(p, s).
func DecimalMulAt(a Int128, aScale int, b Int128, bScale, p, s int) (Int128, DecimalStatus) {
	v, st := DecimalMul(a, aScale, b, bScale, s)
	return decimalAtPrecision(v, st, p)
}

// DecimalDivAt returns a / b at the declared DECIMAL(p, s).
func DecimalDivAt(a Int128, aScale int, b Int128, bScale, p, s int) (Int128, DecimalStatus) {
	v, st := DecimalDiv(a, aScale, b, bScale, s)
	return decimalAtPrecision(v, st, p)
}

// DecimalModAt returns a % b at the declared DECIMAL(p, s).
func DecimalModAt(a Int128, aScale int, b Int128, bScale, p, s int) (Int128, DecimalStatus) {
	v, st := DecimalMod(a, aScale, b, bScale, s)
	return decimalAtPrecision(v, st, p)
}

// decimalAtPrecision applies the declared-precision bound to a value the
// carrier already accepted.
func decimalAtPrecision(v Int128, st DecimalStatus, p int) (Int128, DecimalStatus) {
	if st != DecimalOK {
		return Int128{}, st
	}
	if !DecimalFitsPrecision(v, p) {
		return Int128{}, DecimalOverflow
	}
	return v, DecimalOK
}

// bigPow10 returns 10^n as a big.Int, for n >= 0.
func bigPow10(n int) *big.Int {
	if n < 0 {
		n = 0
	}
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// bigRescale moves an exact big.Int unscaled value from one scale to another,
// rounding half away from zero downward, and narrows to Int128.
func bigRescale(n *big.Int, fromScale, toScale int) (Int128, bool) {
	if toScale >= fromScale {
		v := new(big.Int).Mul(n, bigPow10(toScale-fromScale))
		return narrowInt128(v)
	}
	return bigDivRound(n, bigPow10(fromScale-toScale))
}

// bigDivRound divides num by den exactly, rounds half away from zero and
// narrows to Int128. den must not be zero.
func bigDivRound(num, den *big.Int) (Int128, bool) {
	if den.Sign() == 0 {
		return Int128{}, false
	}
	q, r := new(big.Int).QuoRem(num, den, new(big.Int))
	if r.Sign() != 0 {
		r.Abs(r)
		r.Lsh(r, 1)
		if r.CmpAbs(den) >= 0 {
			if (num.Sign() < 0) != (den.Sign() < 0) {
				q.Sub(q, bigOne)
			} else {
				q.Add(q, bigOne)
			}
		}
	}
	return narrowInt128(q)
}

var bigOne = big.NewInt(1)

// narrowInt128 converts an exact big.Int to Int128, reporting false rather
// than saturating when it does not fit — the opposite of int128FromBig, whose
// saturation is right for a comparison bound and wrong for a value.
func narrowInt128(b *big.Int) (Int128, bool) {
	if !fitsInt128(b) {
		return Int128{}, false
	}
	return int128FromBig(b), true
}

// DecimalResultType returns the (precision, scale) of a DECIMAL arithmetic
// result, per ADR-0024 item 3 — the rule SQL Server, Spark and Hive converged
// on for a finite 38-digit carrier, adopted verbatim so the choice is not
// wadjet's own:
//
//	e1 + e2, e1 - e2 : p = max(s1,s2) + max(p1-s1, p2-s2) + 1 ; s = max(s1,s2)
//	e1 * e2          : p = p1 + p2 + 1                        ; s = s1 + s2
//	e1 / e2          : s = max(6, s1 + p2 + 1) ; p = p1 - s1 + s2 + s
//	e1 % e2          : p = min(p1-s1, p2-s2) + max(s1,s2)     ; s = max(s1,s2)
//
// op is the SQL operator text, matched case-insensitively: "+", "-", "*",
// "/", "%" (and "mod" for the function spelling). ok=false for an op with no
// DECIMAL rule, and then (p,s) is (0,0) — which the caller must NOT use as a
// type, because 0 is this codebase's "unconstrained" precision and a scale of
// 0 would silently truncate every fraction digit. It is a "there is no rule
// here" answer, not a type.
//
// The result always comes back through AdjustDecimalPrecisionScale, so it is
// a type the carrier can declare. An INTEGER operand is DECIMAL(10,0) or
// (19,0) — the caller decides which and passes it in.
func DecimalResultType(op string, p1, s1, p2, s2 int) (int, int, bool) {
	p1, s1 = normalizeDecimalPS(p1, s1)
	p2, s2 = normalizeDecimalPS(p2, s2)
	var p, s int
	switch strings.ToLower(strings.TrimSpace(op)) {
	case "+", "-":
		s = max(s1, s2)
		p = s + max(p1-s1, p2-s2) + 1
	case "*":
		s = s1 + s2
		p = p1 + p2 + 1
	case "/":
		s = max(6, s1+p2+1)
		p = p1 - s1 + s2 + s
	case "%", "mod":
		s = max(s1, s2)
		p = min(p1-s1, p2-s2) + s
	default:
		return 0, 0, false
	}
	p, s = AdjustDecimalPrecisionScale(p, s)
	return p, s, true
}

// AdjustDecimalPrecisionScale brings a computed (p,s) back inside the
// carrier, ADR-0024 item 3's clause:
//
//	when p > 38: intDigits = p - s; s = max(38 - intDigits, min(s, 6)); p = 38
//
// Fraction digits are what the rule spends: the integer part is kept whole
// for as long as the fraction floor min(s,6) allows, and only yields once
// keeping it would push the scale below that floor — which is Spark's
// adjustPrecisionScale exactly, `max(38-intDigits, min(s,6))` being the same
// function as its `if intDigits + minScale > 38 then minScale else
// 38 - intDigits`. That is a documented divergence from PostgreSQL in the
// NUMBER OF DIGITS KEPT, not in the digits themselves: both engines are exact
// to the digits they keep and agree to min(scale).
//
// Reducing the precision while leaving the scale alone would be the other
// thing entirely — a range reduction that shrinks the INTEGER part with no
// floor at all, which is what #552 is about on the set-operation path.
func AdjustDecimalPrecisionScale(p, s int) (int, int) {
	p, s = normalizeDecimalPS(p, s)
	if p <= MaxDecimalPrecision {
		return p, s
	}
	intDigits := p - s
	floor := min(s, 6)
	ns := MaxDecimalPrecision - intDigits
	if ns < floor {
		ns = floor
	}
	if ns > MaxDecimalScale {
		ns = MaxDecimalScale
	}
	return MaxDecimalPrecision, ns
}

// normalizeDecimalPS repairs a (p,s) pair that no column could declare, so
// the rules above are total: a negative scale is 0, a precision below its own
// scale is that scale, and a precision below 1 is 1.
func normalizeDecimalPS(p, s int) (int, int) {
	if s < 0 {
		s = 0
	}
	if p < s {
		p = s
	}
	if p < 1 {
		p = 1
	}
	return p, s
}
