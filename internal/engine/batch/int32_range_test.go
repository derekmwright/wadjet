package batch

import (
	"math"
	"testing"
)

// The store into an INT32 vector either keeps the number or refuses it.
//
// This is the seam itself, asserted without a plan, a batch or a query — the
// reason CLAUDE.md gives for the group-key seam gate: a check that only fires
// under a particular plan shape is a check whose coverage depends on the
// planner. Every kernel that computes an int4 result in int64 crosses this
// one function.
func TestInt32StoreKeepsTheNumberOrRefuses(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  any
		want int32
		// raises is true when the value has no int32 at all.
		raises bool
	}{
		{name: "int32 box passes through", val: int32(-2147483648), want: math.MinInt32},
		{name: "int64 inside the range", val: int64(2147483647), want: math.MaxInt32},
		{name: "int64 at the floor", val: int64(-2147483648), want: math.MinInt32},
		// |MinInt32| — the value ABS over an int4 column computes in int64
		// and had nowhere to put. It wrapped to -2147483648 before the guard.
		{name: "int64 one past the ceiling raises", val: int64(2147483648), raises: true},
		{name: "int64 one past the floor raises", val: int64(-2147483649), raises: true},
		{name: "int box past the ceiling raises", val: int(1 << 40), raises: true},
		{name: "float inside the range truncates as before", val: float64(2.9), want: 2},
		{name: "float past the ceiling raises", val: float64(3e9), raises: true},
		// Go's float→int conversion is implementation-defined for a NaN, so
		// the check has to read the FLOAT. int32(math.NaN()) is a number on
		// every platform and a different one on some.
		{name: "NaN raises", val: math.NaN(), raises: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := NewVector(TypeInt32, 4)
			err := func() (err error) {
				defer func() {
					if r := recover(); r != nil {
						re, ok := r.(*IntegerRangeError)
						if !ok {
							t.Fatalf("panic value %T (%v), want *IntegerRangeError", r, r)
						}
						err = re
					}
				}()
				v.SetValue(0, tc.val)
				return nil
			}()
			if tc.raises {
				if err == nil {
					t.Fatalf("SetValue(%v) stored %d; a value with no int32 must refuse, "+
						"never wrap (ADR-0012 item 9)", tc.val, v.Int32Data[0])
				}
				if got := err.Error(); got != "integer out of range" {
					t.Errorf("message = %q, want PostgreSQL's %q", got, "integer out of range")
				}
				if got := err.(*IntegerRangeError).SQLState(); got != "22003" {
					t.Errorf("SQLSTATE = %s, want 22003 (numeric_value_out_of_range)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("SetValue(%v) refused with %v; the value fits an int32", tc.val, err)
			}
			if v.Int32Data[0] != tc.want {
				t.Errorf("stored %d, want %d", v.Int32Data[0], tc.want)
			}
		})
	}
}

// The refusal travels the exec.FatalEvalPanic route, the same one #361's
// type-mismatch guard takes: a query error, never a process exit. The
// interface lives in exec and cannot be imported here without a cycle, so the
// contract is asserted structurally.
func TestIntegerRangeErrorCarriesTheFatalEvalContract(t *testing.T) {
	var e any = &IntegerRangeError{Dst: TypeInt32, Val: int64(1 << 40)}
	fe, ok := e.(interface {
		Error() string
		FatalEvalError() error
	})
	if !ok {
		t.Fatal("IntegerRangeError must satisfy exec.FatalEvalPanic (Error + FatalEvalError)")
	}
	if fe.FatalEvalError() == nil {
		t.Error("FatalEvalError() must return the error itself, not nil")
	}
	if _, ok := e.(interface{ SQLState() string }); !ok {
		t.Error("IntegerRangeError must carry a SQLSTATE for sqlerr.StateOf")
	}
}
