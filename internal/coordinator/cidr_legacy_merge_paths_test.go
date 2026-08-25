package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// This file gates three coordinator-side legacy paths that keyed or ordered
// a CIDR column by its raw stored TEXT instead of PostgreSQL's inet order
// (kernel.CidrOrderKey, #492/#520): the cross-worker GROUP BY re-aggregation
// key (reAggregatePartials' keyEncoders), the scalar MIN/MAX partial merge
// (mergeScalarAggregates via compareAnyValues), and the probe-split gather's
// ORDER BY (sortBatches via compareBatchRows). Each is a DIFFERENT code path
// from the engine's own kernels (kernel/sort.go, exec/aggregate.go) — a
// coordinator-only cross-worker boundary, the same class of "hardest to
// find" site #520's own commit named for the local morsel-parallel router.

// TestReAggregatePartialsCIDRGroupKeyUsesInetEquality: a bare address and its
// own /32 are one PostgreSQL inet value (#492), so two workers' partial
// GROUP BY results holding one spelling each must re-aggregate into ONE
// group, not two.
func TestReAggregatePartialsCIDRGroupKeyUsesInetEquality(t *testing.T) {
	schema := []parquet.Column{
		{Name: "c_cidr", Type: parquet.TypeCIDR},
		{Name: "n", Type: parquet.TypeFloat64},
	}
	b1 := batch.FromRows(schema, []map[string]any{{"c_cidr": "10.0.0.1", "n": float64(1)}})
	b2 := batch.FromRows(schema, []map[string]any{{"c_cidr": "10.0.0.1/32", "n": float64(1)}})

	mi := &logical.MergeInfo{
		GroupBy:  []string{"c_cidr"},
		AggExprs: []logical.AggExpr{{Func: "sum", OutputCol: "n"}},
	}
	colIdx := map[string]int{"c_cidr": 0, "n": 1}
	c := &Coordinator{}
	out, err := c.reAggregatePartials([]*batch.RecordBatch{b1, b2}, []string{"c_cidr", "n"}, colIdx, mi)
	if err != nil {
		t.Fatalf("reAggregatePartials: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output batch, got %d", len(out))
	}
	rows := out[0].ToRows()
	if len(rows) != 1 {
		t.Fatalf("re-aggregation produced %d groups, want 1 (the two spellings are one inet value): %#v",
			len(rows), rows)
	}
	if got := rows[0]["n"]; got != float64(2) {
		t.Errorf("SUM(n) for the collapsed group = %#v, want 2", got)
	}
}

// TestMergeScalarAggregatesCIDRUsesInetOrder: text order puts "9.0.0.0/8"
// ABOVE "10.0.0.0/8" ('9' > '1'); inet order — which the engine's own
// batch-level MIN/MAX kernel and Accumulator.Merge already use (#520) — puts
// it below. compareAnyValues used to fall to its plain `string` arm here.
func TestMergeScalarAggregatesCIDRUsesInetOrder(t *testing.T) {
	schema := []parquet.Column{
		{Name: "mn", Type: parquet.TypeCIDR},
		{Name: "mx", Type: parquet.TypeCIDR},
	}
	b1 := batch.FromRows(schema, []map[string]any{{"mn": "9.0.0.0/8", "mx": "9.0.0.0/8"}})
	b2 := batch.FromRows(schema, []map[string]any{{"mn": "10.0.0.0/8", "mx": "10.0.0.0/8"}})

	mi := &logical.MergeInfo{
		HasAggregate: true,
		AggExprs: []logical.AggExpr{
			{Func: "min", OutputCol: "mn"},
			{Func: "max", OutputCol: "mx"},
		},
	}
	rows := mergeScalarAggHelper(t, []*batch.RecordBatch{b1, b2}, []string{"mn", "mx"}, mi)[0].ToRows()
	if got := rows[0]["mn"]; got != "9.0.0.0/8" {
		t.Errorf("MIN(c_cidr) = %#v, want \"9.0.0.0/8\" (inet order, not text order)", got)
	}
	if got := rows[0]["mx"]; got != "10.0.0.0/8" {
		t.Errorf("MAX(c_cidr) = %#v, want \"10.0.0.0/8\"", got)
	}
}

// TestSortBatchesOrdersCidrByInetOrder: the probe-split gather's ORDER BY
// (Coordinator.sortBatches, merging one partial result per worker) had no
// TypeCIDR arm in compareBatchRows at all — extractFloat64 answers 0 for a
// text-backed column, so every pair tied and the "sort" was a no-op that
// happened to preserve arrival order, not even the old (wrong) text order.
func TestSortBatchesOrdersCidrByInetOrder(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "c_cidr", Type: parquet.TypeCIDR},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"id": int64(1), "c_cidr": "10.0.0.0/8"},
		{"id": int64(2), "c_cidr": "9.0.0.0/8"},
		{"id": int64(3), "c_cidr": "10.0.0.1"},
	})
	colIdx := map[string]int{"id": 0, "c_cidr": 1}
	orderBy := []logical.OrderExpr{{Column: "c_cidr"}}

	c := &Coordinator{}
	c.sortBatches([]*batch.RecordBatch{b}, []string{"id", "c_cidr"}, colIdx, orderBy)

	var order []int64
	rows := b.ActiveLen()
	for i := 0; i < rows; i++ {
		row := i
		if b.Sel != nil {
			row = int(b.Sel[i])
		}
		order = append(order, b.Columns[0].Int64Data[row])
	}
	// Inet order: 9.0.0.0/8 (id=2) sorts first (the differing leading octet
	// decides); then 10.0.0.0/8 (id=1); then 10.0.0.1 (id=3).
	want := []int64{2, 1, 3}
	if len(order) != len(want) {
		t.Fatalf("sortBatches produced %d rows, want %d", len(order), len(want))
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v (inet order, not text order or arrival order)", order, want)
		}
	}
}
