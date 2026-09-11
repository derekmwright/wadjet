package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// A BITWISE OPERATOR IS EXACT OVER A 64-BIT PATTERN.
//
// PostgreSQL 17.11, measured on the shared oracle server, is the authority for
// every cell below:
//
//	pg_typeof(2::int4 & 18::int4)                 integer
//	pg_typeof(2::int8 & 18::int8)                 bigint
//	(-1)::int4 & 18                               18
//	(-1)::int8 & 18                               18
//	((4611686018427387922::int8) & 18) = 18       t
//
// The last one is the defect: 4611686018427387922 is 2^62 | 18, and the whole
// family carried its arguments through a float64, which cannot hold the low
// bits of that number. The AND then ran on 2^62 and answered 0 — a value
// nothing downstream could tell from a real one.
//
// Reverting either half of bitwise_exact.go fails this test: reverting the
// argument read fails PastFloat64Precision, reverting the declaration fails
// TheResultIsAnInteger and OrKeepsAHighBit.
func TestABitwiseOperatorIsExactOverASixtyFourBitPattern(t *testing.T) {
	and := DefaultRegistry.Lookup("bitwise_and")
	or := DefaultRegistry.Lookup("bitwise_or")
	xor := DefaultRegistry.Lookup("bitwise_xor")
	not := DefaultRegistry.Lookup("bitwise_not")

	t.Run("PastFloat64Precision", func(t *testing.T) {
		// 2^62 | 18. A float64 rounds this to 2^62 exactly, losing SYN|ACK.
		const v = int64(1)<<62 | 18
		if got := and([]any{v, int64(18)}); got != int64(18) {
			t.Errorf("BITWISE_AND(%d, 18) = %#v; PostgreSQL 17.11 answers 18", v, got)
		}
		if got := and([]any{v, int64(2)}); got != int64(2) {
			t.Errorf("BITWISE_AND(%d, 2) = %#v; PostgreSQL 17.11 answers 2", v, got)
		}
		// The same value arriving as an int32-declared column's neighbour:
		// int32 boxes must not take the float route either.
		if got := and([]any{int32(511), int64(18)}); got != int64(18) {
			t.Errorf("BITWISE_AND(int32 511, 18) = %#v, want 18", got)
		}
	})

	t.Run("OrKeepsAHighBit", func(t *testing.T) {
		// The output half: a float64 result cannot carry this either.
		want := int64(1)<<62 | 1
		if got := or([]any{int64(1) << 62, int64(1)}); got != want {
			t.Errorf("BITWISE_OR(2^62, 1) = %#v, want %d", got, want)
		}
		if got := xor([]any{int64(1)<<62 | 3, int64(1)}); got != int64(1)<<62|2 {
			t.Errorf("BITWISE_XOR(2^62|3, 1) = %#v, want %d", got, int64(1)<<62|2)
		}
	})

	t.Run("TheResultIsAnInteger", func(t *testing.T) {
		for _, name := range []string{"bitwise_and", "bitwise_or", "bitwise_xor", "bitwise_not"} {
			decl, _ := DefaultRegistry.ReturnType(name).Resolve(0, nil)
			if decl.ID != batch.TypeInt64 {
				t.Errorf("%s declares %v; PostgreSQL's bitwise operators answer an integer "+
					"and the three shifts in this family already declare INT64", name, decl.ID)
			}
		}
	})

	t.Run("ANegativeValueIsABitPattern", func(t *testing.T) {
		// PG: (-1)::int4 & 18 = 18, (-1)::int8 & 18 = 18. Every bit set.
		if got := and([]any{int64(-1), int64(18)}); got != int64(18) {
			t.Errorf("BITWISE_AND(-1, 18) = %#v; PostgreSQL answers 18", got)
		}
		if got := not([]any{int64(0)}); got != int64(-1) {
			t.Errorf("BITWISE_NOT(0) = %#v; PostgreSQL answers -1", got)
		}
	})

	t.Run("NullPropagates", func(t *testing.T) {
		for _, fn := range []ScalarFunc{and, or, xor} {
			if got := fn([]any{nil, int64(1)}); got != nil {
				t.Errorf("a NULL argument answered %#v, want NULL", got)
			}
			if got := fn([]any{int64(1), nil}); got != nil {
				t.Errorf("a NULL argument answered %#v, want NULL", got)
			}
		}
		if got := not([]any{nil}); got != nil {
			t.Errorf("BITWISE_NOT(NULL) = %#v, want NULL", got)
		}
	})

	t.Run("AFloatArgumentIsUnchanged", func(t *testing.T) {
		// The float route is deliberately kept: a float is not an integer's
		// bit pattern, and no shape that answered before starts refusing.
		if got := and([]any{float64(0xFF), float64(0x0F)}); got != int64(0x0F) {
			t.Errorf("BITWISE_AND(255.0, 15.0) = %#v, want 15", got)
		}
	})
}
