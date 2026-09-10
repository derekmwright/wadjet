// This file holds expr math fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"math"
)

// --- Math function implementations ---

func fnAbs(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	// ABS answers in its argument's own domain (#768). An integer stays an
	// integer and a real stays a real, which is what PostgreSQL declares AND
	// what keeps `ABS(real 0.1)` from acquiring a double's digits. Everything
	// else falls through to the float path unchanged.
	if v, ok := absKeepsDomain(args[0]); ok {
		return v
	}
	return math.Abs(ToFloat64(args[0]))
}

func fnCeil(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Ceil(ToFloat64(args[0]))
}

func fnFloor(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Floor(ToFloat64(args[0]))
}

func fnRound(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	precision := 0
	if len(args) >= 2 && args[1] != nil {
		precision = int(ToFloat64(args[1]))
	}
	pow := math.Pow(10, float64(precision))
	return math.Round(v*pow) / pow
}

// fnRoundHalfEven is ROUND for a DOUBLE PRECISION (and REAL/FLOAT) operand.
// PostgreSQL rounds NUMERIC half AWAY from zero — fnRound's math.Round,
// above — but rounds DOUBLE PRECISION half TO EVEN ("banker's rounding"),
// which is what distinguishes ROUND(0.5) = 1 from ROUND(CAST(0.5 AS double
// precision)) = 0 (#381). compileFuncCallNode routes here when ROUND's
// argument is an explicit CAST to a binary float type; fnRound alone can't
// make this decision because Wadjet has no numeric tower — a NUMERIC
// literal and a DOUBLE PRECISION value are both a bare float64 by the time
// either kernel sees them, so the type distinction has to be resolved by
// the caller, before that boxing.
//
// This is unrelated to CAST(x AS integer)'s own rounding rule (#373, half
// away from zero): PostgreSQL specifies CAST and ROUND independently, and a
// tie-breaking rule chosen for one operation says nothing about the other —
// don't be tempted to unify them.
func fnRoundHalfEven(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	precision := 0
	if len(args) >= 2 && args[1] != nil {
		precision = int(ToFloat64(args[1]))
	}
	pow := math.Pow(10, float64(precision))
	return math.RoundToEven(v*pow) / pow
}

func fnPow(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	base, exp := ToFloat64(args[0]), ToFloat64(args[1])
	raisePowerDomain(base, exp)
	r := math.Pow(base, exp)
	// PostgreSQL's dpow overflow check, its own predicate: an infinite result
	// is an overflow only when NEITHER operand was already infinite.
	// `power(2, 'Infinity')` is Infinity there and `power('Infinity', 2)` is
	// Infinity, both measured — the first version of this check passed only
	// the BASE as the finite operand and so refused the first of that pair.
	//
	// There is NO underflow check, and that is a decision. PostgreSQL resolves
	// `POWER(0.5, 2000)` — the spelling a user writes — to power(numeric,
	// numeric), which has no range check and answers 0; only the float8
	// overload underflows, and wadjet has one float path. Refusing would be
	// ADR-0012 item 1's forbidden direction (refusing input PostgreSQL
	// accepts) for the common spelling, in exchange for matching the explicit
	// `::float8` one. Answering keeps the common spelling right and leaves a
	// SUPERSET on the explicit one, which is the direction ADR-0012 records as
	// acceptable. EXP is not the same case: its underflow raises under BOTH
	// overloads on the live server, so it keeps the check.
	if math.IsInf(r, 0) && !math.IsInf(base, 0) && !math.IsInf(exp, 0) {
		raiseFloatOverflow()
	}
	return r
}

func fnSqrt(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	raiseSquareRootDomain(v)
	return math.Sqrt(v)
}

func fnMod(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	// Integer remainder over two integers (#768): math.Mod answered `-0` for
	// MOD(-6, 3), a signed zero no integer remainder has, and declared it
	// double where PostgreSQL declares the argument's own integer width.
	if v, ok := modKeepsDomain(args[0], args[1]); ok {
		return v
	}
	// A zero divisor is 22012, PostgreSQL's division_by_zero, and not the NaN
	// math.Mod answers with. modKeepsDomain declines for a zero divisor as
	// well as for a non-integer operand, so this is the one place both reach.
	d := ToFloat64(args[1])
	if d == 0 {
		raiseDivisionByZero()
	}
	return math.Mod(ToFloat64(args[0]), d)
}

func fnLog(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if len(args) >= 2 && args[1] != nil {
		// LOG(base, value)
		base, val := v, ToFloat64(args[1])
		raiseLogarithmDomain(base)
		raiseLogarithmDomain(val)
		if base == 1 {
			// log(val)/log(1) divides by zero, and PostgreSQL reports it as
			// exactly that: `log(1::numeric, 8)` is 22012, not 2201E.
			raiseDivisionByZero()
		}
		return math.Log(val) / math.Log(base)
	}
	raiseLogarithmDomain(v)
	return math.Log10(v)
}

func fnLn(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	raiseLogarithmDomain(v)
	return math.Log(v)
}

func fnExp(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	// PostgreSQL's dexp handles NaN and the infinities EXPLICITLY, before any
	// range check, "to avoid needing to assume the platform's exp() conforms
	// to POSIX for these cases" — and per POSIX exp(-Inf) is ZERO. The first
	// version of this check tested `v != 0` for the underflow, which is true
	// of -Infinity, so `EXP(CAST('-Infinity' AS DOUBLE PRECISION))` raised
	// where the server answers 0. Only a FINITE argument is range-checked.
	switch {
	case math.IsNaN(v):
		return v
	case math.IsInf(v, 1):
		return v
	case math.IsInf(v, -1):
		return 0.0
	}
	r := math.Exp(v)
	if math.IsInf(r, 0) {
		raiseFloatOverflow()
	}
	if r == 0 {
		raiseFloatUnderflow()
	}
	return r
}

func fnSign(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	switch {
	case v > 0:
		return float64(1)
	case v < 0:
		return float64(-1)
	default:
		return float64(0)
	}
}

// fnGreatest and fnLeast order their arguments through compare, the same
// type-aware comparison =, < and > use — not ToFloat64, which is 0 for every
// string and therefore ranked every string argument equal, so GREATEST over
// two string columns always answered with argument 0. That was invisible while
// the projection was typed numeric and printed 0 for all of them (#333); with
// the type right, the ordering is what is left to be wrong.
func fnGreatest(args []any) any {
	var best any
	for _, a := range args {
		if a == nil {
			continue
		}
		if best == nil || compare(a, best, CmpGt) {
			best = a
		}
	}
	return best
}

func fnLeast(args []any) any {
	var best any
	for _, a := range args {
		if a == nil {
			continue
		}
		if best == nil || compare(a, best, CmpLt) {
			best = a
		}
	}
	return best
}
