// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestBoolFilterBothPathsOrderFalseBeforeTrue: the vectorized KernelFilter and
// the row-at-a-time ColumnCompareLit over a BOOLEAN column answer every
// operator against a boolean constant as PostgreSQL 17.11 does (false < true;
// NULL never qualifies; measured, 1681d evidence pg_bool_order.txt). The
// kernel selected the FALSE rows for >, >=, <, <=, and the row path selected
// no row for them (epic #1681, line 1681d).
func TestBoolFilterBothPathsOrderFalseBeforeTrue(t *testing.T) {
	mk := func() *batch.RecordBatch {
		b := batch.NewRecordBatch([]parquet.Column{{Name: "c_b", Type: parquet.TypeBool, Nullable: true}}, 3)
		b.Columns[0].SetValue(0, false)
		b.Columns[0].SetValue(1, true)
		b.Columns[0].SetValue(2, nil)
		return b
	}
	// want: the rows (0 = false, 1 = true) PostgreSQL keeps.
	cases := []struct {
		op   CompareOp
		name string
		lit  bool
		want []uint32
	}{
		{OpGt, ">", false, []uint32{1}}, {OpGt, ">", true, nil},
		{OpGe, ">=", false, []uint32{0, 1}}, {OpGe, ">=", true, []uint32{1}},
		{OpLt, "<", false, nil}, {OpLt, "<", true, []uint32{0}},
		{OpLe, "<=", false, []uint32{0}}, {OpLe, "<=", true, []uint32{0, 1}},
		{OpEq, "=", false, []uint32{0}}, {OpEq, "=", true, []uint32{1}},
		{OpNe, "<>", false, []uint32{1}}, {OpNe, "<>", true, []uint32{0}},
	}
	for _, c := range cases {
		label := fmt.Sprintf("c_b %s %v", c.name, c.lit)
		out, err := NewKernelFilterLit("c_b", c.op, c.lit, "").Execute(context.Background(), mk())
		if err != nil {
			t.Fatalf("%s: kernel: %v", label, err)
		}
		var got []uint32
		if out != nil {
			got = selOf(out)
		}
		if fmt.Sprint(got) != fmt.Sprint(c.want) && !(len(got) == 0 && len(c.want) == 0) {
			t.Errorf("%s: kernel selected %v, PostgreSQL 17.11 keeps %v", label, got, c.want)
		}
		pred := ColumnCompareLit("c_b", c.op, c.lit, "")
		b := mk()
		var rows []uint32
		for i := 0; i < b.Len; i++ {
			if pred(b, i) {
				rows = append(rows, uint32(i))
			}
		}
		if fmt.Sprint(rows) != fmt.Sprint(c.want) {
			t.Errorf("%s: row path kept %v, PostgreSQL 17.11 keeps %v", label, rows, c.want)
		}
	}
}
