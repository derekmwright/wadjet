package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// decBytesBatch carries the two types ResolveFilterKernel had no arm for.
// c_dec is DECIMAL(18,4): 5.0005, 5.0006, 100.0000, -1.5000, NULL.
// c_bytes holds raw byte strings.
func decBytesBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "c_dec", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
		{Name: "c_bytes", Type: parquet.TypeBytes, Nullable: true},
	}, 5)
	units := []int64{50005, 50006, 1000000, -15000, 0}
	for i, u := range units {
		b.Columns[0].DecimalData.Data[i] = batch.Int128From(u)
	}
	b.Columns[0].Nulls.SetNull(4)
	for i, v := range [][]byte{
		[]byte("bytes-000015-xxxx"),
		[]byte("bytes-000016-xxxxx"),
		[]byte("aardvark"),
		[]byte("zebra"),
		nil,
	} {
		if v == nil {
			b.Columns[1].Nulls.SetNull(i)
			continue
		}
		b.Columns[1].BytesData.Set(i, v)
	}
	return b
}

// TestKernelFilterDecimalAndBytes is the #401 regression.
//
// ResolveFilterKernel had no DECIMAL and no BYTES arm, so it returned nil and
// KernelFilter reported that as `filter column %q does not exist in the input
// schema` — which is how #401 came to be filed as a name-resolution defect.
// Any `WHERE dec_col <op> literal` or `WHERE bytes_col <op> literal` that
// reached the operator-level filter instead of the scan failed the whole
// query.
//
// The DECIMAL cases include the boundary the truncation has to get right:
// with a scale-4 column, `> 5.00049` must ADMIT the row holding exactly
// 5.0005 and `> 5.0005` must exclude it, and `> 5.00051` must exclude it too
// even though both constants truncate to the same scale-4 value.
func TestKernelFilterDecimalAndBytes(t *testing.T) {
	cases := []struct {
		name string
		col  string
		op   CompareOp
		val  any
		want []uint32
	}{
		{"decimal eq", "c_dec", OpEq, 5.0005, []uint32{0}},
		{"decimal ne", "c_dec", OpNe, 5.0005, []uint32{1, 2, 3}},
		{"decimal gt", "c_dec", OpGt, 5.0005, []uint32{1, 2}},
		{"decimal ge", "c_dec", OpGe, 5.0005, []uint32{0, 1, 2}},
		{"decimal lt", "c_dec", OpLt, 5.0005, []uint32{3}},
		{"decimal negative", "c_dec", OpLt, 0.0, []uint32{3}},
		{"decimal residual below", "c_dec", OpGt, 5.00049, []uint32{0, 1, 2}},
		{"decimal residual above", "c_dec", OpGt, 5.00051, []uint32{1, 2}},
		{"decimal residual eq is never equal", "c_dec", OpEq, 5.00051, nil},
		{"decimal string literal", "c_dec", OpEq, "100.0000", []uint32{2}},
		{"bytes eq", "c_bytes", OpEq, "bytes-000015-xxxx", []uint32{0}},
		{"bytes eq from []byte", "c_bytes", OpEq, []byte("zebra"), []uint32{3}},
		{"bytes gt", "c_bytes", OpGt, "bytes-000015-xxxx", []uint32{1, 3}},
		{"bytes lt", "c_bytes", OpLt, "bytes", []uint32{2}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			f := NewKernelFilter(tc.col, tc.op, tc.val)
			out, err := f.Execute(context.Background(), decBytesBatch(t))
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			got := selOf(out)
			if len(got) != len(tc.want) {
				t.Fatalf("Sel = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("Sel = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestKernelFilterUnkernelledTypeNamesTheType: a column that resolves but
// whose TYPE has no comparison kernel must say so. Reporting it as "does not
// exist" sent every reader hunting a name-resolution bug (#401).
func TestKernelFilterUnkernelledTypeNamesTheType(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "c_arr", Type: parquet.TypeArray,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString}},
	}, 1)
	f := NewKernelFilter("c_arr", OpEq, "x")
	if _, err := f.Execute(context.Background(), b); err == nil {
		t.Fatal("want an error for a type with no comparison kernel")
	} else if !strings.Contains(err.Error(), "no comparison kernel") ||
		!strings.Contains(err.Error(), "ARRAY") {
		t.Fatalf("error %q names neither the missing kernel nor the type", err)
	}
}

// TestKernelFilterMissingColumnStillSaysMissing keeps the #147 message for the
// case it was written for.
func TestKernelFilterMissingColumnStillSaysMissing(t *testing.T) {
	f := NewKernelFilter("nope", OpEq, int64(1))
	if _, err := f.Execute(context.Background(), decBytesBatch(t)); err == nil {
		t.Fatal("want an error for an unresolvable column")
	} else if !strings.Contains(err.Error(), "does not exist in the input schema") {
		t.Fatalf("error %q lost the #147 wording", err)
	}
}
