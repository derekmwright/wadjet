package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The row-level flag kernel against the spelling it replaces.
//
// The base is the expression a user writes today — `BITWISE_AND(flags, 18) =
// 18` — which boxes the column value into an `any`, calls the registry
// function through an interface, boxes the result, and compares two boxes.
// `tcp_flags_has_all(flags,'SYN','ACK')` on the same column resolves the
// column ONCE, folds the mask at compile time and reads the typed slice.
//
// The generic arm is the same function WITHOUT the specialization
// (newFlagsTest declines a computed argument), so the three numbers separate
// "the family is cheaper than the bit spelling" from "the typed node is
// cheaper than the boxed call".
func benchFlagBatch(tb testing.TB) *batch.RecordBatch {
	tb.Helper()
	const n = batch.DefaultBatchSize
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "flags", Type: parquet.TypeInt64, Nullable: true},
	}, n)
	vals := []int64{0, 2, 18, 16, 4, 511, 24, 20, 256}
	for i := 0; i < n; i++ {
		b.Columns[0].Int64Data[i] = vals[i%len(vals)]
	}
	b.Len = n
	return b
}

func BenchmarkFlagPredicateTypedKernel(b *testing.B) {
	rb := benchFlagBatch(b)
	e := newFlagsTest(&FuncCall{Name: "tcp_flags_has_all", Args: []Expr{
		&ColRef{Name: "flags"}, &Lit{Val: "SYN"}, &Lit{Val: "ACK"}}})
	if e == nil {
		b.Fatal("no typed kernel was built")
	}
	pred := FilterPredicate(e)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		for row := 0; row < rb.Len; row++ {
			if pred(rb, row) {
				n++
			}
		}
		if n == 0 {
			b.Fatal("selected nothing")
		}
	}
}

func BenchmarkFlagPredicateGenericCall(b *testing.B) {
	rb := benchFlagBatch(b)
	fc := &FuncCall{Name: "tcp_flags_has_all", Args: []Expr{
		&ColRef{Name: "flags"}, &Lit{Val: "SYN"}, &Lit{Val: "ACK"}}}
	pred := FilterPredicate(fc)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		for row := 0; row < rb.Len; row++ {
			if pred(rb, row) {
				n++
			}
		}
		if n == 0 {
			b.Fatal("selected nothing")
		}
	}
}

// The base: `BITWISE_AND(flags, 18) = 18`, the spelling this family replaces.
func BenchmarkFlagPredicateBitwiseAndSpelling(b *testing.B) {
	rb := benchFlagBatch(b)
	and := &FuncCall{Name: "bitwise_and", Args: []Expr{
		&ColRef{Name: "flags"}, &Lit{Val: int64(18)}}}
	pred := FilterPredicate(NewCmp(and, &Lit{Val: int64(18)}, CmpEq))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		for row := 0; row < rb.Len; row++ {
			if pred(rb, row) {
				n++
			}
		}
		if n == 0 {
			b.Fatal("selected nothing")
		}
	}
}
