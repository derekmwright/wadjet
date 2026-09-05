package batch

import (
	"math"
	"testing"
)

// int32BackedTypes is every vector type whose storage is Int32Data. All four
// narrow a widened box back at the STORE, so all four need the same guard;
// #841 converted TypeInt32 and left the other three wrapping silently
// (CodeQL go/incorrect-integer-conversion #32, #33, #34, plus the two `int`
// arms the scan did not flag because `int` is only 64 bits on the platforms
// this runs on).
var int32BackedTypes = []TypeID{TypeInt32, TypePort, TypeProtocol, TypeDate}

// A value with no int32 is 22003 at the STORE, on every int4-backed type and
// through every box the setter accepts. It is never wrapped.
//
// This is the SEAM gate for these types and it needs no plan and no data: the
// arms it covers are reached from a cast, a scan and a row reader, and only one
// of those shapes is spellable in SQL today (`<bigint>::DATE`). A gate that can
// only fire through the one reachable shape would go on passing while the other
// three wrapped.
func TestNoInt32BackedVectorStoresAValueItCannotHold(t *testing.T) {
	refused := []struct {
		name string
		box  any
	}{
		{"int64 MaxInt32+1", int64(math.MaxInt32) + 1},
		{"int64 MinInt32-1", int64(math.MinInt32) - 1},
		{"int64 3000000000", int64(3000000000)},
		{"int MaxInt32+1", int(math.MaxInt32) + 1},
		{"int MinInt32-1", int(math.MinInt32) - 1},
		{"float64 1e10", float64(1e10)},
		{"float64 NaN", math.NaN()},
		{"float64 +Inf", math.Inf(1)},
	}
	stored := []struct {
		name string
		box  any
		want int32
	}{
		{"int64 MaxInt32", int64(math.MaxInt32), math.MaxInt32},
		{"int64 MinInt32", int64(math.MinInt32), math.MinInt32},
		{"int32 MaxInt32", int32(math.MaxInt32), math.MaxInt32},
		{"int 0", int(0), 0},
		{"int64 443", int64(443), 443},
		{"float64 443", float64(443), 443},
	}

	for _, typ := range int32BackedTypes {
		for _, tc := range refused {
			t.Run(typ.String()+"/refused/"+tc.name, func(t *testing.T) {
				// DATE has no float64 arm at all — a float box there is a type
				// mismatch, not a range failure, and both are refusals.
				if typ == TypeDate {
					if _, isFloat := tc.box.(float64); isFloat {
						t.Skip("a DATE vector has no float box")
					}
				}
				v := NewVector(typ, 1)
				err := setValueErr(v, 0, tc.box)
				if err == nil {
					t.Fatalf("%s stored %v (%T) as %d; it has no int32",
						typ, tc.box, tc.box, v.Int32Data[0])
				}
				re, ok := err.(*IntegerRangeError)
				if !ok {
					t.Fatalf("%s refusing %v raised %T (%v), want *IntegerRangeError",
						typ, tc.box, err, err)
				}
				if re.SQLState() != "22003" {
					t.Errorf("%s refusing %v carried SQLSTATE %q, want 22003",
						typ, tc.box, re.SQLState())
				}
			})
		}
		for _, tc := range stored {
			t.Run(typ.String()+"/stored/"+tc.name, func(t *testing.T) {
				if typ == TypeDate {
					if _, isFloat := tc.box.(float64); isFloat {
						t.Skip("a DATE vector has no float box")
					}
				}
				v := NewVector(typ, 1)
				if err := setValueErr(v, 0, tc.box); err != nil {
					t.Fatalf("%s refused %v (%T), which an int32 holds: %v",
						typ, tc.box, tc.box, err)
				}
				if v.Int32Data[0] != tc.want {
					t.Errorf("%s stored %v as %d, want %d", typ, tc.box, v.Int32Data[0], tc.want)
				}
			})
		}
	}
}

// setValueErr runs SetValue and turns its panic-based refusal into an error.
// The guard panics because it sits in a per-row store the kernels call without
// an error return; exec's FatalEvalPanic boundary is what turns it into a
// query error.
func setValueErr(v *Vector, i int, val any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
				return
			}
			panic(r)
		}
	}()
	v.SetValue(i, val)
	return nil
}
