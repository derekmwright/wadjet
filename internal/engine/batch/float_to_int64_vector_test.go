// SPDX-License-Identifier: MIT

package batch

import (
	"errors"
	"math"
	"testing"
)

// An INT64 vector's float arm converts on the shared bound (#1484): the
// double 2^63, a NaN and an infinity are refused with IntegerRangeError
// (22003, the FatalEvalPanic contract) instead of the implementation-defined
// int64 a bare conversion produces — MinInt64 on amd64 — and -2^63 and the
// largest double below 2^63 are stored exactly.
func TestInt64VectorFloatArmSharesTheBound(t *testing.T) {
	for _, f := range []float64{0x1p63, math.NaN(), math.Inf(1), math.Inf(-1), -0x1p63 - 2048} {
		func() {
			v := NewVector(TypeInt64, 1)
			defer func() {
				r := recover()
				e, ok := r.(*IntegerRangeError)
				if !ok {
					t.Errorf("SetValue(%v) into INT64: stored %d, want IntegerRangeError (panic %v)", f, v.Int64Data[0], r)
					return
				}
				var target *IntegerRangeError
				if !errors.As(e.FatalEvalError(), &target) {
					t.Errorf("SetValue(%v): the refusal does not carry the query-error contract", f)
				}
			}()
			v.SetValue(0, f)
		}()
	}
	v := NewVector(TypeInt64, 2)
	v.SetValue(0, -0x1p63)
	v.SetValue(1, math.Nextafter(0x1p63, 0))
	if v.Int64Data[0] != math.MinInt64 || v.Int64Data[1] != 9223372036854774784 {
		t.Fatalf("in-range doubles stored %v", v.Int64Data)
	}
}
