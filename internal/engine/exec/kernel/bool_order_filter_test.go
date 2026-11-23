// SPDX-License-Identifier: MIT

package kernel

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// TestBoolFilterKernelOrdersFalseBeforeTrue: a BOOLEAN column compared with a
// boolean constant under every operator selects the rows PostgreSQL 17.11
// selects (measured: `SELECT x, x > false, ... FROM (VALUES (false), (true),
// (NULL::bool)) t(x)`, 1681d evidence pg_bool_order.txt). The kernel read
// only = and <>, and every other operator selected the FALSE rows, so
// `c_b > false` answered the rows holding false (epic #1681, line 1681d).
func TestBoolFilterKernelOrdersFalseBeforeTrue(t *testing.T) {
	// want: does the operator admit the row holding false / true (NULL never).
	cases := []struct {
		op                  CompareOp
		name                string
		lit                 bool
		keepFalse, keepTrue bool
	}{
		{OpGt, ">", false, false, true}, {OpGt, ">", true, false, false},
		{OpGe, ">=", false, true, true}, {OpGe, ">=", true, false, true},
		{OpLt, "<", false, false, false}, {OpLt, "<", true, true, false},
		{OpLe, "<=", false, true, false}, {OpLe, "<=", true, true, true},
		{OpEq, "=", false, true, false}, {OpEq, "=", true, false, true},
		{OpNe, "<>", false, false, true}, {OpNe, "<>", true, true, false},
	}
	// rows: false, true, NULL, true, false
	vals := []any{false, true, nil, true, false}
	for _, withNulls := range []bool{true, false} {
		n := len(vals)
		if !withNulls {
			n = 2
		}
		vec := batch.NewVector(batch.TypeBool, n)
		src := vals
		if !withNulls {
			src = []any{false, true}
		}
		for i, v := range src {
			vec.SetValue(i, v)
		}
		for _, c := range cases {
			for _, lit := range []any{c.lit, fmt.Sprint(c.lit)} {
				kern := ResolveFilterKernel(batch.TypeBool, c.op, lit)
				if kern == nil {
					t.Fatalf("c_b %s %#v: no kernel", c.name, lit)
				}
				var want []uint32
				for i, v := range src {
					if v == nil {
						continue
					}
					if (v == false && c.keepFalse) || (v == true && c.keepTrue) {
						want = append(want, uint32(i))
					}
				}
				for _, sel := range [][]uint32{nil, boolTestAllRows(len(src))} {
					got := kern(vec, sel, len(src), make([]uint32, 0, len(src)))
					if fmt.Sprint(got) != fmt.Sprint(want) && !(len(got) == 0 && len(want) == 0) {
						t.Errorf("c_b %s %#v over %v (sel=%v): selected %v, PostgreSQL 17.11 selects %v",
							c.name, lit, src, sel != nil, got, want)
					}
				}
			}
		}
	}
}

func boolTestAllRows(n int) []uint32 {
	s := make([]uint32, n)
	for i := range s {
		s[i] = uint32(i)
	}
	return s
}
