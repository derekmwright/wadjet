package expr

// The bitwise operators read their arguments EXACTLY and answer an integer.
//
// They used to do neither. `fnBitwiseAnd` was
//
//	float64(int64(ToFloat64(a)) & int64(ToFloat64(b)))
//
// declared `RetFloat64`, so a BIGINT argument made a round trip through a
// double before the AND ran and the result made a second one on the way out.
// Above 2^53 a double cannot hold the low bits of a 64-bit integer, so they
// were rounded away and then masked. Measured on PostgreSQL 17.11 and here,
// over `f8 = 4611686018427387922` (2^62 | 18):
//
//	PostgreSQL   (f8 & 18) = 18   ->  t
//	wadjet       BITWISE_AND(f8, 18)  ->  0
//
// A silently wrong answer, with nothing downstream able to tell it from a real
// one — the class ADR-0012 item 1 exists for.
//
// Two changes, and both are needed for the value to survive:
//
//   - the argument is read with `toInt64Safe`, which takes int64/int32/int as
//     themselves and never through a float. A float argument keeps the old
//     truncating conversion, because that is what it always did and a float is
//     not an integer's bit pattern in any case;
//   - the result is an int64 and the declaration is `RetInt64`. A double cannot
//     carry `BITWISE_OR(1<<62, 1)` either, so fixing only the input would move
//     the loss from the argument to the answer.
//
// The three SHIFT functions of the same family already declared `RetInt64` and
// already returned an int64 box, so this is the family's own precedent rather
// than a new direction. PostgreSQL declares `int4 & int4` as `int4` and
// `int8 & int8` as `int8`; answering int8 for both is a value-preserving
// widening, recorded in ADR-0012's divergence list.
//
// NULL propagates: any NULL argument makes the result NULL, as PostgreSQL's
// operators do.

// bitIntArg reads a bitwise operand as the 64-bit pattern it is.
//
// An integer box is taken as itself — that is the whole fix. Anything else
// (a float, a numeric string) keeps the conversion the family has always
// applied, so no shape that answered before starts refusing.
func bitIntArg(v any) int64 {
	if i, ok := toInt64Safe(v); ok {
		return i
	}
	return int64(ToFloat64(v))
}

func fnBitwiseAnd(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return bitIntArg(args[0]) & bitIntArg(args[1])
}

func fnBitwiseOr(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return bitIntArg(args[0]) | bitIntArg(args[1])
}

func fnBitwiseXor(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return bitIntArg(args[0]) ^ bitIntArg(args[1])
}

func fnBitwiseNot(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return ^bitIntArg(args[0])
}
