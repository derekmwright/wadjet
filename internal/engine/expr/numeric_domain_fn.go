package expr

import (
	"math"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// ABS and MOD answer in their argument's OWN integer or real domain (#768).
//
// The seven "same-domain" math functions already answer exactly over a
// DECIMAL (decimal_scalar_fn.go, #668). Over an INTEGER or a REAL they all
// declared FLOAT64 and computed through ToFloat64, and for five of them that
// is RIGHT — measured on live PostgreSQL 17:
//
//	              int4      int8      float4    float8    numeric
//	ABS           integer   bigint    real      double    numeric
//	MOD           integer   bigint    (none)    (none)    numeric
//	CEIL/FLOOR/   double    double    double    double    numeric
//	ROUND/TRUNC/
//	SIGN
//	SQRT/POWER/   double    double    double    double    numeric
//	LN/EXP
//
// So `FLOOR(bigint)` IS double precision there and must stay double here; a
// blanket "type this family from its argument" pass would have introduced a
// divergence where none existed. Only ABS and MOD preserve the domain, and
// this file is only about them.
//
// Two defects, and the second is a wrong VALUE rather than a wrong OID:
//
//   - `ABS(real 0.1)` answered 0.10000000149011612 — the float32 widened to a
//     double, whose extra digits are the ones a real never had — where
//     PostgreSQL answers 0.1 under OID 700.
//   - `MOD(-6, 3)` answered `-0`, math.Mod's signed zero, where integer
//     remainder is 0.
//
// Both are the same cause as the OID: the declaration was FIXED, so the kernel
// had no domain to compute in. Declaring bigint over a ToFloat64 computation
// would have put a right OID on a rounded number, which is why the kernel
// moves with the declaration (protocol method 8).

// numericDomainScalarFns names the functions that answer in their INTEGER or
// REAL argument's own domain, and how many arguments each takes.
//
// MOD over a FLOAT is deliberately absent: PostgreSQL has no float `mod` at
// all (`MOD(1.5::float8, 2)` is `function mod(double precision, integer) does
// not exist`), so wadjet's float answer is a superset it keeps, computed the
// way it always was.
var numericDomainScalarFns = map[string]int{
	"abs": 1,
	"mod": 2,
}

// NumericDomainScalarFn reports whether name answers in its argument's own
// integer or real domain, and how many arguments it takes there.
func NumericDomainScalarFn(name string) (int, bool) {
	n, ok := numericDomainScalarFns[strings.ToLower(strings.TrimSpace(name))]
	return n, ok
}

// NumericDomainResult is the type ABS or MOD answers for arguments of the
// given types, or ok=false when this rule does not apply and the caller keeps
// the registry's FLOAT64 declaration.
//
// It is the DECLARATION half of the pair; absKeepsDomain and modKeepsDomain
// below are the value half, and the two are written next to each other so a
// change to one that is not made to the other is visible.
func NumericDomainResult(name string, args []batch.TypeID) (batch.TypeID, bool) {
	want, ok := NumericDomainScalarFn(name)
	if !ok || len(args) != want {
		return 0, false
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "abs":
		switch args[0] {
		case batch.TypeInt32, batch.TypeInt64, batch.TypeFloat32:
			return args[0], true
		}
	case "mod":
		// Both integers, and the wider of the two — PostgreSQL's
		// `MOD(int4, int8)` is bigint. A float or a decimal anywhere declines
		// and keeps the existing answer.
		l, r := args[0], args[1]
		if isDomainInt(l) && isDomainInt(r) {
			if l == batch.TypeInt64 || r == batch.TypeInt64 {
				return batch.TypeInt64, true
			}
			return batch.TypeInt32, true
		}
	}
	return 0, false
}

func isDomainInt(t batch.TypeID) bool {
	return t == batch.TypeInt32 || t == batch.TypeInt64
}

// absKeepsDomain is ABS over a boxed value whose Go type names an integer or a
// real. It answers in the SAME box, so the value and the declaration agree.
//
// ok=false leaves the caller on math.Abs(ToFloat64(...)), which is what every
// other argument type still gets.
func absKeepsDomain(v any) (any, bool) {
	switch x := v.(type) {
	case int32:
		if x < 0 {
			return -x, true
		}
		return x, true
	case int64:
		if x < 0 {
			return -x, true
		}
		return x, true
	case int:
		if x < 0 {
			return int64(-x), true
		}
		return int64(x), true
	case float32:
		// float32 arithmetic, not a widened double: the whole defect is that
		// `ABS(real 0.1)` came back with a double's digits.
		if x < 0 {
			return -x, true
		}
		return x, true
	}
	return nil, false
}

// modKeepsDomain is MOD over two boxed INTEGERS, computed as integer
// remainder rather than math.Mod, which is where `MOD(-6, 3)` acquired its
// signed zero.
//
// Division by zero declines rather than answering: the caller's float path
// produces NaN for it, which is the behaviour every other numeric type here
// has and is not this commit's to change.
func modKeepsDomain(a, b any) (any, bool) {
	l, lok := domainInt(a)
	r, rok := domainInt(b)
	if !lok || !rok || r == 0 {
		return nil, false
	}
	// int32 only when BOTH are int32, mirroring NumericDomainResult.
	if _, isL32 := a.(int32); isL32 {
		if _, isR32 := b.(int32); isR32 {
			return int32(l % r), true
		}
	}
	return l % r, true
}

func domainInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case int:
		return int64(x), true
	}
	return 0, false
}

// vecAbsDomain writes ABS into an output vector of the argument's own type.
// ok=false means the output is a float64 vector and the caller's existing loop
// runs unchanged.
func vecAbsDomain(src, out *batch.Vector, n int) bool {
	hasNulls := src.Nulls.HasNulls()
	switch out.Type {
	case batch.TypeInt32:
		if src.Type != batch.TypeInt32 || len(out.Int32Data) < n {
			return false
		}
		for i := 0; i < n; i++ {
			if hasNulls && src.Nulls.IsNullFast(i) {
				out.Nulls.SetNull(i)
				continue
			}
			if v := src.Int32Data[i]; v < 0 {
				out.Int32Data[i] = -v
			} else {
				out.Int32Data[i] = v
			}
		}
		return true
	case batch.TypeInt64:
		if src.Type != batch.TypeInt64 || len(out.Int64Data) < n {
			return false
		}
		for i := 0; i < n; i++ {
			if hasNulls && src.Nulls.IsNullFast(i) {
				out.Nulls.SetNull(i)
				continue
			}
			if v := src.Int64Data[i]; v < 0 {
				out.Int64Data[i] = -v
			} else {
				out.Int64Data[i] = v
			}
		}
		return true
	case batch.TypeFloat32:
		if src.Type != batch.TypeFloat32 || len(out.Float32Data) < n {
			return false
		}
		for i := 0; i < n; i++ {
			if hasNulls && src.Nulls.IsNullFast(i) {
				out.Nulls.SetNull(i)
				continue
			}
			if v := src.Float32Data[i]; v < 0 {
				out.Float32Data[i] = -v
			} else {
				out.Float32Data[i] = v
			}
		}
		return true
	}
	return false
}

// isConstNumericOperand reports whether an expression is a numeric CONSTANT —
// a literal, or a unary sign over one, possibly parenthesised.
//
// It selects the numeric-to-integer rounding rule for a CAST (#768): a bare
// `-0.5` is `numeric` in PostgreSQL and rounds half away from zero, while a
// float8 value rounds half to even. Both arrive here as a float64 box, so the
// EXPRESSION is what tells them apart.
func isConstNumericOperand(e Expr) bool {
	switch n := e.(type) {
	case *Lit:
		switch n.Val.(type) {
		case int, int32, int64, float32, float64:
			return true
		}
		return false
	case *UnaryOp:
		return isConstNumericOperand(n.Operand)
	}
	return false
}

var _ = math.Abs
