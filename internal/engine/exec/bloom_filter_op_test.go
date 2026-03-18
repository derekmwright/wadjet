package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func buildHashJoin(t *testing.T, joinType JoinType, leftKeys, rightKeys []string, schema []parquet.Column, rows []map[string]any) *HashJoin {
	t.Helper()
	hj := NewHashJoin(joinType, leftKeys, rightKeys)
	src := NewSliceSource(schema, rows)
	if err := hj.Build(context.Background(), src); err != nil {
		t.Fatal(err)
	}
	return hj
}

func TestBloomFilterOp_IntKey(t *testing.T) {
	ctx := context.Background()

	buildSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	buildRows := []map[string]any{
		{"id": int64(1), "val": "a"},
		{"id": int64(3), "val": "b"},
		{"id": int64(5), "val": "c"},
	}

	hj := buildHashJoin(t, InnerJoin, []string{"id"}, []string{"id"}, buildSchema, buildRows)

	bf := hj.BloomPushdownOp()
	if bf == nil {
		t.Fatal("expected bloom filter op for inner join")
	}
	bf.Init(ctx)

	// Probe side: keys 1-6, only 1, 3, 5 should survive
	probeSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "data", Type: parquet.TypeString},
	}
	probeRows := []map[string]any{
		{"id": int64(1), "data": "x"},
		{"id": int64(2), "data": "y"},
		{"id": int64(3), "data": "z"},
		{"id": int64(4), "data": "w"},
		{"id": int64(5), "data": "v"},
		{"id": int64(6), "data": "u"},
	}
	probeBatch := batch.FromRows(probeSchema, probeRows)

	result, err := bf.Execute(ctx, probeBatch)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Bloom filter has false positives, so result may include extra rows,
	// but must include all true matches (1, 3, 5).
	activeLen := result.ActiveLen()
	if activeLen < 3 {
		t.Errorf("expected at least 3 rows (true matches), got %d", activeLen)
	}

	// Verify true matches are present
	matchIDs := make(map[int64]bool)
	idCol := result.ColumnByName("id")
	for _, idx := range result.Sel {
		matchIDs[idCol.Int64Data[idx]] = true
	}
	for _, expected := range []int64{1, 3, 5} {
		if !matchIDs[expected] {
			t.Errorf("expected key %d to pass bloom filter", expected)
		}
	}
}

func TestBloomFilterOp_StringKey(t *testing.T) {
	ctx := context.Background()

	buildSchema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeFloat64},
	}
	buildRows := []map[string]any{
		{"name": "alice", "val": 1.0},
		{"name": "bob", "val": 2.0},
	}

	hj := buildHashJoin(t, InnerJoin, []string{"name"}, []string{"name"}, buildSchema, buildRows)

	bf := hj.BloomPushdownOp()
	if bf == nil {
		t.Fatal("expected bloom filter op")
	}
	bf.Init(ctx)

	probeSchema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
	}
	probeRows := []map[string]any{
		{"name": "alice"},
		{"name": "charlie"},
		{"name": "bob"},
		{"name": "dave"},
	}
	probeBatch := batch.FromRows(probeSchema, probeRows)

	result, err := bf.Execute(ctx, probeBatch)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Must include alice and bob
	matchNames := make(map[string]bool)
	nameCol := result.ColumnByName("name")
	for _, idx := range result.Sel {
		matchNames[nameCol.GetValue(int(idx)).(string)] = true
	}
	if !matchNames["alice"] || !matchNames["bob"] {
		t.Errorf("expected alice and bob to pass bloom filter, got %v", matchNames)
	}
}

func TestBloomFilterOp_JoinTypeRestriction(t *testing.T) {
	ctx := context.Background()
	buildSchema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
	buildRows := []map[string]any{{"id": int64(1)}}

	for _, tc := range []struct {
		joinType JoinType
		wantNil  bool
	}{
		{InnerJoin, false},
		{SemiJoin, false},
		{RightJoin, false},
		{LeftJoin, true},
		{FullOuterJoin, true},
		{AntiJoin, true},
		{CrossJoin, true},
	} {
		hj := NewHashJoin(tc.joinType, []string{"id"}, []string{"id"})
		src := NewSliceSource(buildSchema, buildRows)
		if err := hj.Build(ctx, src); err != nil {
			t.Fatal(err)
		}
		bf := hj.BloomPushdownOp()
		if tc.wantNil && bf != nil {
			t.Errorf("join type %v: expected nil bloom filter op", tc.joinType)
		}
		if !tc.wantNil && bf == nil {
			t.Errorf("join type %v: expected non-nil bloom filter op", tc.joinType)
		}
	}
}

func TestBloomFilterOp_Clone(t *testing.T) {
	buildSchema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
	buildRows := []map[string]any{{"id": int64(1)}, {"id": int64(2)}}

	hj := buildHashJoin(t, InnerJoin, []string{"id"}, []string{"id"}, buildSchema, buildRows)

	bf := hj.BloomPushdownOp()
	clone := bf.Clone().(*BloomFilterOp)

	// Shared bloom data
	if &clone.bloom[0] != &bf.bloom[0] {
		t.Error("clone should share bloom data slice")
	}
	// Independent scratch
	if clone.resolved {
		t.Error("clone should have unresolved state")
	}
}

func TestBloomFilterOp_Pipeline(t *testing.T) {
	ctx := context.Background()

	buildSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}
	probeSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeFloat64},
	}

	buildRows := []map[string]any{
		{"id": int64(10), "name": "a"},
		{"id": int64(20), "name": "b"},
		{"id": int64(30), "name": "c"},
	}
	var probeRows []map[string]any
	for i := 0; i < 100; i++ {
		probeRows = append(probeRows, map[string]any{
			"id":     int64(i + 1),
			"amount": float64(i) * 10,
		})
	}

	hj := buildHashJoin(t, InnerJoin, []string{"id"}, []string{"id"}, buildSchema, buildRows)
	hj.FixKeyAssignment()

	bf := hj.BloomPushdownOp()
	probe := hj.Probe()
	sink := &CollectSink{}

	var ops []UnaryOperator
	if bf != nil {
		ops = append(ops, bf)
	}
	ops = append(ops, probe)

	pipe := &Pipeline{
		Source: NewSliceSource(probeSchema, probeRows),
		Ops:    ops,
		Sink:   sink,
	}
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	rows := sink.ToRows()
	if len(rows) != 3 {
		t.Fatalf("expected 3 matching rows, got %d", len(rows))
	}

	matchedIDs := make(map[int64]bool)
	for _, r := range rows {
		matchedIDs[r["id"].(int64)] = true
	}
	for _, expected := range []int64{10, 20, 30} {
		if !matchedIDs[expected] {
			t.Errorf("expected id=%d in results", expected)
		}
	}
}
