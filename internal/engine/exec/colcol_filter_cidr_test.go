package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// twoCidrBatch holds two CIDR columns exercising the three ADR-0012 item 10
// consequences a text-order comparator gets wrong:
//
//	row 0: c1="10.0.0.1", c2="10.0.0.1/32"     — one PostgreSQL inet value,
//	                                              two spellings (a bare
//	                                              address is a /32 host route)
//	row 1: c1="9.0.0.0/8", c2="10.0.0.0/8"     — common bits decide before
//	                                              the mask; text order puts
//	                                              "9..." ABOVE "10..."
//	row 2: c1="192.168.1.5/24", c2="192.168.1.0/32" — the MASK outranks the
//	                                              address; text order sees
//	                                              '5' > '0' and says the
//	                                              opposite
//	row 3: c1 is NULL — every operator must answer UNKNOWN, not compare
//	                    against c2's raw text
func twoCidrBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "c1", Type: parquet.TypeCIDR, Nullable: true},
		{Name: "c2", Type: parquet.TypeCIDR, Nullable: true},
	}, 4)
	for i, s := range []string{"10.0.0.1", "9.0.0.0/8", "192.168.1.5/24", "10.0.0.1"} {
		b.Columns[0].BytesData.Set(i, []byte(s))
	}
	for i, s := range []string{"10.0.0.1/32", "10.0.0.0/8", "192.168.1.0/32", "10.0.0.1/32"} {
		b.Columns[1].BytesData.Set(i, []byte(s))
	}
	b.Columns[0].Nulls.SetNull(3)
	return b
}

// TestColColFilterComparesTwoCidrColumnsByInetOrder is the regression for
// ResolveColColFilterKernel's TypeCIDR arm falling to colColFilterString's
// plain byte comparison. `WHERE c1 = c2` used to disagree with the
// column-vs-literal kernel (ResolveFilterKernel's TypeCIDR arm) and
// expr.CmpNetworkLit, both of which have compared PostgreSQL's inet order
// since #492 (ADR-0012 item 10) — row 0's pair answers `=` there but not
// through the unfixed col-col path.
func TestColColFilterComparesTwoCidrColumnsByInetOrder(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		op   CompareOp
		want []uint32
	}{
		{OpEq, []uint32{0}},
		{OpNe, []uint32{1, 2}},
		{OpLt, []uint32{1, 2}},
		{OpLe, []uint32{0, 1, 2}},
		{OpGt, nil},
		{OpGe, []uint32{0}},
	} {
		in := twoCidrBatch(t)
		out, err := NewColColFilter("c1", "c2", tc.op).Execute(ctx, in)
		if err != nil {
			t.Fatalf("op %d: %v", tc.op, err)
		}
		if len(tc.want) == 0 {
			if out != nil && len(out.Sel) != 0 {
				t.Errorf("op %d: selected %v, want none", tc.op, out.Sel)
			}
			continue
		}
		if out == nil {
			t.Fatalf("op %d: selected nothing, want %v", tc.op, tc.want)
		}
		if len(out.Sel) != len(tc.want) {
			t.Fatalf("op %d: selected %v, want %v", tc.op, out.Sel, tc.want)
		}
		for i, w := range tc.want {
			if out.Sel[i] != w {
				t.Fatalf("op %d: selected %v, want %v", tc.op, out.Sel, tc.want)
			}
		}
	}
}
