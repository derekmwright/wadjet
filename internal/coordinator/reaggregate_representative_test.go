// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The coordinator's re-aggregation of probe-split partials is a place two
// members of one group meet: the group keeps the smaller representative
// (ADR-0024 §12, #1713) whichever partial arrives first, for a DECIMAL key
// (7 / 7.00) and a CIDR key (10.0.0.1 / 10.0.0.1/32).
func TestReAggregatePartialsKeepsTheSmallerRepresentative(t *testing.T) {
	for _, tc := range []struct {
		name string
		col  parquet.Column
		a, b string
		want string
	}{
		{"decimal", parquet.Column{Name: "k", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true}, "7.00", "7", "7"},
		{"cidr", parquet.Column{Name: "k", Type: parquet.TypeCIDR, Nullable: true}, "10.0.0.1/32", "10.0.0.1", "10.0.0.1"},
	} {
		schema := []parquet.Column{tc.col, {Name: "n", Type: parquet.TypeInt64}}
		mk := func(k string, n int64) *batch.RecordBatch {
			b := batch.NewRecordBatch(schema, 1)
			b.Columns[0].SetValue(0, k)
			b.Columns[0].Nulls.SetValid(0)
			b.Columns[1].Int64Data[0] = n
			b.Columns[1].Nulls.SetValid(0)
			b.Len = 1
			return b
		}
		mi := &logical.MergeInfo{HasAggregate: true, GroupBy: []string{"k"}, AggExprs: []logical.AggExpr{{Func: "sum", OutputCol: "n"}}}
		for _, order := range [][2]string{{tc.a, tc.b}, {tc.b, tc.a}} {
			c := &Coordinator{}
			out, err := c.reAggregatePartials([]*batch.RecordBatch{mk(order[0], 1), mk(order[1], 2)},
				[]string{"k", "n"}, map[string]int{"k": 0, "n": 1}, mi)
			if err != nil {
				t.Fatal(err)
			}
			if out[0].Len != 1 {
				t.Fatalf("%s %v: %d groups, want 1", tc.name, order, out[0].Len)
			}
			if got := out[0].Columns[0].GetValueOf(0, false); got != tc.want {
				t.Errorf("%s %v: key %v, want %s", tc.name, order, got, tc.want)
			}
		}
	}
}
