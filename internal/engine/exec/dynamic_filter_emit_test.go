package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestDynamicFilterEmitOpAccumulates verifies the emit op passes batches
// through unchanged and accumulates the expected partial stats: bloom
// contains every emitted key, min/max match the input range, RowCount is
// the active-row total.
func TestDynamicFilterEmitOpAccumulates(t *testing.T) {
	op := NewDynamicFilterEmitOp("f1", "id", "int64", 1024)
	if err := op.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	schema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
	keys := []int64{7, 13, 21, 42, 100, 100, 7}
	b := batch.NewRecordBatch(schema, len(keys))
	for i, k := range keys {
		b.Columns[0].Int64Data[i] = k
	}

	out, err := op.Execute(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if out != b {
		t.Fatalf("emit op should pass batch through unchanged; got different pointer")
	}
	if out.ActiveLen() != len(keys) {
		t.Fatalf("emit op changed active len: %d != %d", out.ActiveLen(), len(keys))
	}

	snap := op.Snapshot()
	if snap.RowCount != int64(len(keys)) {
		t.Errorf("RowCount = %d, want %d", snap.RowCount, len(keys))
	}
	if snap.Min != 7 || snap.Max != 100 {
		t.Errorf("range = [%d, %d], want [7, 100]", snap.Min, snap.Max)
	}
	if !snap.HasRange {
		t.Error("HasRange should be true after observing rows")
	}

	// Every emitted key must be present in the bloom (zero false negatives).
	for _, k := range keys {
		if !BloomContains(snap.Bloom, snap.BloomMask, BloomHashInt(k)) {
			t.Errorf("bloom missing key %d", k)
		}
	}
}

// TestDynamicFilterEmitOpRespectsSelectionVector verifies that the op only
// accumulates over selected rows when a selection vector is present
// (downstream of a filter). The post-filter set is what the bloom should
// reflect for dynamic-filter semantics.
func TestDynamicFilterEmitOpRespectsSelectionVector(t *testing.T) {
	op := NewDynamicFilterEmitOp("f1", "id", "int64", 256)
	op.Init(context.Background())

	schema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
	b := batch.NewRecordBatch(schema, 6)
	for i := 0; i < 6; i++ {
		b.Columns[0].Int64Data[i] = int64(i * 10) // 0, 10, 20, 30, 40, 50
	}
	b.Sel = []uint32{1, 3, 5} // keep only 10, 30, 50

	if _, err := op.Execute(context.Background(), b); err != nil {
		t.Fatal(err)
	}

	snap := op.Snapshot()
	if snap.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3 (selection vector ignored)", snap.RowCount)
	}
	if snap.Min != 10 || snap.Max != 50 {
		t.Errorf("range = [%d, %d], want [10, 50]", snap.Min, snap.Max)
	}
	for _, k := range []int64{10, 30, 50} {
		if !BloomContains(snap.Bloom, snap.BloomMask, BloomHashInt(k)) {
			t.Errorf("bloom missing selected key %d", k)
		}
	}
}

// TestDynamicFilterEmitOpUnionIsBitwiseOR verifies the core architectural
// property: two task-partials sized to the same BloomBits union to a
// bitset that contains every key from either partial. This is what makes
// the coordinator-side merge a trivial bitwise OR.
func TestDynamicFilterEmitOpUnionIsBitwiseOR(t *testing.T) {
	op1 := NewDynamicFilterEmitOp("f1", "id", "int64", 1024)
	op2 := NewDynamicFilterEmitOp("f1", "id", "int64", 1024)
	op1.Init(context.Background())
	op2.Init(context.Background())

	schema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
	b1 := batch.NewRecordBatch(schema, 3)
	for i, k := range []int64{1, 5, 9} {
		b1.Columns[0].Int64Data[i] = k
	}
	b2 := batch.NewRecordBatch(schema, 3)
	for i, k := range []int64{2, 6, 99} {
		b2.Columns[0].Int64Data[i] = k
	}
	op1.Execute(context.Background(), b1)
	op2.Execute(context.Background(), b2)

	s1, s2 := op1.Snapshot(), op2.Snapshot()
	if s1.BloomMask != s2.BloomMask {
		t.Fatalf("partials must share BloomMask for union; got %d vs %d", s1.BloomMask, s2.BloomMask)
	}

	// Manual OR-union.
	merged := make([]uint64, len(s1.Bloom))
	for i := range merged {
		merged[i] = s1.Bloom[i] | s2.Bloom[i]
	}
	mask := s1.BloomMask

	for _, k := range []int64{1, 5, 9, 2, 6, 99} {
		if !BloomContains(merged, mask, BloomHashInt(k)) {
			t.Errorf("union missing key %d", k)
		}
	}
}

// TestDynamicFilterEmitOpEmptyBatchNoOp confirms passing an empty / nil
// batch produces no row-count increment and no panic.
func TestDynamicFilterEmitOpEmptyBatchNoOp(t *testing.T) {
	op := NewDynamicFilterEmitOp("f1", "id", "int64", 64)
	op.Init(context.Background())
	if _, err := op.Execute(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if op.Snapshot().RowCount != 0 {
		t.Error("nil batch should not change RowCount")
	}
}

// TestDynamicFilterEmitOpUnresolvedPoison: an op that saw rows but could
// not resolve its key column must report Unresolved (the worker withholds
// the partial — an incomplete bloom falsely rejects rows downstream). An
// op that saw NO rows is not unresolved: its key set is legitimately empty.
func TestDynamicFilterEmitOpUnresolvedPoison(t *testing.T) {
	op := NewDynamicFilterEmitOp("f1", "missing_col", "int64", 1024)
	schema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
	b := batch.NewRecordBatch(schema, 4)
	if _, err := op.Execute(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if snap := op.Snapshot(); !snap.Unresolved {
		t.Fatal("op saw rows without resolving key column; Snapshot must be Unresolved")
	}

	idle := NewDynamicFilterEmitOp("f2", "missing_col", "int64", 1024)
	if snap := idle.Snapshot(); snap.Unresolved {
		t.Fatal("op that saw no rows must NOT be Unresolved (empty key set is valid)")
	}
}

// TestDynamicFilterEmitOpQualifiedSuffixResolution: join outputs may carry
// the key alias-qualified ("l1.l_orderkey"); a UNIQUE suffix match must
// resolve, while an ambiguous suffix must stay unresolved.
func TestDynamicFilterEmitOpQualifiedSuffixResolution(t *testing.T) {
	op := NewDynamicFilterEmitOp("f1", "l_orderkey", "int64", 1024)
	schema := []parquet.Column{
		{Name: "l1.l_orderkey", Type: parquet.TypeInt64},
		{Name: "other", Type: parquet.TypeInt64},
	}
	b := batch.NewRecordBatch(schema, 2)
	b.Columns[0].Int64Data[0] = 11
	b.Columns[0].Int64Data[1] = 22
	if _, err := op.Execute(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	snap := op.Snapshot()
	if snap.Unresolved || snap.RowCount != 2 {
		t.Fatalf("unique suffix must resolve: unresolved=%v rows=%d", snap.Unresolved, snap.RowCount)
	}
	for _, k := range []int64{11, 22} {
		if !BloomContains(snap.Bloom, snap.BloomMask, BloomHashInt(k)) {
			t.Errorf("bloom missing key %d", k)
		}
	}

	amb := NewDynamicFilterEmitOp("f2", "l_orderkey", "int64", 1024)
	schema2 := []parquet.Column{
		{Name: "l1.l_orderkey", Type: parquet.TypeInt64},
		{Name: "l2.l_orderkey", Type: parquet.TypeInt64},
	}
	b2 := batch.NewRecordBatch(schema2, 1)
	if _, err := amb.Execute(context.Background(), b2); err != nil {
		t.Fatal(err)
	}
	if snap := amb.Snapshot(); !snap.Unresolved {
		t.Fatal("ambiguous suffix must stay unresolved (wrong column would poison the bloom)")
	}
}
