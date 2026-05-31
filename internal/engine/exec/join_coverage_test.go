package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestIsIntKeyColumn(t *testing.T) {
	intTypes := []batch.TypeID{
		batch.TypeInt32, batch.TypeInt64, batch.TypePort, batch.TypeProtocol,
		batch.TypeDate, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC,
		batch.TypeDuration,
	}
	for _, ty := range intTypes {
		if !isIntKeyColumn(ty) {
			t.Errorf("expected isIntKeyColumn(%d) to be true", ty)
		}
	}
	nonIntTypes := []batch.TypeID{
		batch.TypeString, batch.TypeFloat64, batch.TypeBool, batch.TypeBytes,
		batch.TypeDecimal, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID,
	}
	for _, ty := range nonIntTypes {
		if isIntKeyColumn(ty) {
			t.Errorf("expected isIntKeyColumn(%d) to be false", ty)
		}
	}
}

func TestIntKeyFromVector(t *testing.T) {
	// Int32 vector
	v32 := batch.NewVector(batch.TypeInt32, 2)
	v32.Int32Data[0] = 42
	v32.Nulls.SetNull(1)
	val, ok := intKeyFromVector(v32, 0)
	if !ok || val != 42 {
		t.Fatalf("expected (42, true), got (%d, %v)", val, ok)
	}
	_, ok = intKeyFromVector(v32, 1)
	if ok {
		t.Fatal("expected null row to return false")
	}

	// Int64 vector
	v64 := batch.NewVector(batch.TypeInt64, 1)
	v64.Int64Data[0] = 999
	val, ok = intKeyFromVector(v64, 0)
	if !ok || val != 999 {
		t.Fatalf("expected (999, true), got (%d, %v)", val, ok)
	}

	// Unsupported type
	vs := batch.NewVector(batch.TypeString, 1)
	vs.BytesData.Set(0, []byte("hello"))
	_, ok = intKeyFromVector(vs, 0)
	if ok {
		t.Fatal("expected string vector to return false")
	}
}

func TestHashBuildBytes(t *testing.T) {
	schema := []parquet.Column{
		{Name: "b", Type: parquet.TypeBool},
		{Name: "i32", Type: parquet.TypeInt32},
		{Name: "i64", Type: parquet.TypeInt64},
		{Name: "f32", Type: parquet.TypeFloat32},
		{Name: "f64", Type: parquet.TypeFloat64},
		{Name: "s", Type: parquet.TypeString},
		{Name: "d", Type: parquet.TypeDecimal},
	}
	b := batch.NewRecordBatch(schema, 10)
	// hashBuildBytes = honest MemBytes + the 40 B/row hash-index charge.
	if got, want := hashBuildBytes(b), b.MemBytes()+int64(b.Len)*40; got != want {
		t.Fatalf("hashBuildBytes = %d, want %d (MemBytes=%d + Len*40)", got, want, b.MemBytes())
	}
	if hashBuildBytes(b) <= 0 {
		t.Fatalf("expected positive size, got %d", hashBuildBytes(b))
	}
}

func TestHashJoinBuildRows(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	rows := []map[string]any{
		{"id": int64(1), "val": "a"},
		{"id": int64(2), "val": "b"},
	}
	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(schema, rows)
	if hj.BuildRows() != 2 {
		t.Fatalf("expected 2 build rows, got %d", hj.BuildRows())
	}
}

func TestHashJoinIntKeyPath(t *testing.T) {
	// Single int64 key should enable the int fast path
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}
	rightRows := []map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}
	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(schema, rightRows)

	if !hj.useIntKey {
		t.Fatal("expected int key fast path to be enabled for int64 column")
	}
	if hj.intIndex == nil {
		t.Fatal("expected intIndex to be initialized")
	}

	// Probe with matching int keys
	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	leftRows := []map[string]any{
		{"id": int64(1), "amount": 100.0},
		{"id": int64(3), "amount": 50.0}, // no match
	}

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(sink.Rows))
	}
}

func TestHashJoinBuildEmptyRows(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
	}
	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(schema, nil)
	if hj.BuildRows() != 0 {
		t.Fatalf("expected 0 build rows, got %d", hj.BuildRows())
	}
	if !hj.buildDone {
		t.Fatal("expected buildDone to be true after empty BuildFromRows")
	}
}

func TestHashJoinPruneBuildColumns(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
		{Name: "extra", Type: parquet.TypeString},
	}
	rows := []map[string]any{
		{"id": "1", "name": "Alice", "extra": "x"},
	}
	hj := NewHashJoin(SemiJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(schema, rows)

	// Prune to keep only "name"
	hj.PruneBuildColumns([]string{"name"})
	if len(hj.buildSchema) != 1 {
		t.Fatalf("expected 1 column after prune, got %d", len(hj.buildSchema))
	}
	if hj.buildSchema[0].Name != "name" {
		t.Fatalf("expected 'name' column, got %q", hj.buildSchema[0].Name)
	}
}

func TestHashJoinPruneBuildColumnsEmpty(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
	}
	rows := []map[string]any{
		{"id": "1"},
	}
	hj := NewHashJoin(AntiJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(schema, rows)

	// Prune with empty keep list clears build data
	hj.PruneBuildColumns(nil)
	if hj.buildBatches != nil {
		t.Fatal("expected nil buildBatches after empty prune")
	}
}

func TestHashJoinPruneBuildColumnsNoBatches(t *testing.T) {
	hj := NewHashJoin(SemiJoin, []string{"id"}, []string{"id"})
	// No build batches - should be a no-op
	hj.PruneBuildColumns([]string{"id"})
}

func TestHashJoinPruneBuildColumnsKeepAll(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
	}
	rows := []map[string]any{
		{"id": "1", "name": "Alice"},
	}
	hj := NewHashJoin(SemiJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(schema, rows)

	// Keep all columns - should be a no-op
	hj.PruneBuildColumns([]string{"id", "name"})
	if len(hj.buildSchema) != 2 {
		t.Fatalf("expected 2 columns (unchanged), got %d", len(hj.buildSchema))
	}
}

func TestHashJoinFixKeyAssignment(t *testing.T) {
	// Simulate misassigned keys: left key is in build schema, right key is not
	buildSchema := []parquet.Column{
		{Name: "nation_id", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
	}
	buildRows := []map[string]any{
		{"nation_id": "1", "name": "USA"},
		{"nation_id": "2", "name": "UK"},
	}

	// Left key "nation_id" is in build schema, right key "src_id" is not
	// FixKeyAssignment should swap them
	hj := NewHashJoin(InnerJoin, []string{"nation_id"}, []string{"src_id"})
	hj.BuildFromRows(buildSchema, buildRows)
	hj.FixKeyAssignment()

	// After fix, left should be "src_id" and right should be "nation_id"
	if hj.LeftKeys[0] != "src_id" {
		t.Fatalf("expected left key to be 'src_id' after fix, got %q", hj.LeftKeys[0])
	}
	if hj.RightKeys[0] != "nation_id" {
		t.Fatalf("expected right key to be 'nation_id' after fix, got %q", hj.RightKeys[0])
	}
}

func TestHashJoinFixKeyAssignmentNoSwap(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
	}
	buildRows := []map[string]any{
		{"id": "1", "name": "Alice"},
	}

	// Keys are already correct: left key "user_id" not in build, right key "id" is in build
	hj := NewHashJoin(InnerJoin, []string{"user_id"}, []string{"id"})
	hj.BuildFromRows(buildSchema, buildRows)
	hj.FixKeyAssignment()

	// Should remain unchanged
	if hj.LeftKeys[0] != "user_id" {
		t.Fatalf("expected left key unchanged, got %q", hj.LeftKeys[0])
	}
}

func TestHashJoinFixKeyAssignmentNoBuild(t *testing.T) {
	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	// No build data, should be a no-op
	hj.FixKeyAssignment()
}

func TestSemiJoin(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeFloat64},
	}
	leftRows := []map[string]any{
		{"id": "a", "val": 1.0},
		{"id": "b", "val": 2.0},
		{"id": "c", "val": 3.0}, // no match
	}

	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
		{Name: "extra", Type: parquet.TypeString},
	}
	rightRows := []map[string]any{
		{"id": "a", "extra": "x"},
		{"id": "b", "extra": "y"},
	}

	hj := NewHashJoin(SemiJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(rightSchema, rightRows)

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Semi join: only rows with matches, output has left schema only
	if len(sink.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(sink.Rows))
	}
	for _, row := range sink.Rows {
		if _, ok := row["extra"]; ok {
			t.Fatal("semi join should not include build-side columns")
		}
	}
}

func TestAntiJoin(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeFloat64},
	}
	leftRows := []map[string]any{
		{"id": "a", "val": 1.0},
		{"id": "b", "val": 2.0},
		{"id": "c", "val": 3.0}, // no match
	}

	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
	}
	rightRows := []map[string]any{
		{"id": "a"},
		{"id": "b"},
	}

	hj := NewHashJoin(AntiJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(rightSchema, rightRows)

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Anti join: only rows WITHOUT matches
	if len(sink.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(sink.Rows))
	}
	if sink.Rows[0]["id"] != "c" {
		t.Fatalf("expected id='c', got %v", sink.Rows[0]["id"])
	}
}

func TestSemiJoinWithFilter(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeFloat64},
	}
	leftRows := []map[string]any{
		{"id": "a", "val": 1.0},
		{"id": "a", "val": 2.0},
	}

	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
		{Name: "threshold", Type: parquet.TypeFloat64},
	}
	rightRows := []map[string]any{
		{"id": "a", "threshold": 1.5},
	}

	hj := NewHashJoin(SemiJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(rightSchema, rightRows)

	// Custom filter: val > threshold
	hj.SemiAntiFilter = func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
		probeVal := probe.Columns[1].Float64Data[probeRow]
		buildVal := build.Columns[1].Float64Data[buildRow]
		return probeVal > buildVal
	}

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Only val=2.0 > 1.5, so 1 row
	if len(sink.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(sink.Rows))
	}
}

func TestHashJoinProbeClose(t *testing.T) {
	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows([]parquet.Column{{Name: "id", Type: parquet.TypeString}}, nil)
	probe := hj.Probe()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHashJoinProbeInitNotBuilt(t *testing.T) {
	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	probe := hj.Probe()
	err := probe.Init(context.Background())
	if err == nil {
		t.Fatal("expected error when build phase not complete")
	}
}

func TestHashJoinBuildWithSource(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}
	buildRows := []map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}

	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	source := NewSliceSource(buildSchema, buildRows)
	if err := hj.Build(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if hj.BuildRows() != 2 {
		t.Fatalf("expected 2 build rows, got %d", hj.BuildRows())
	}
	if !hj.buildDone {
		t.Fatal("expected buildDone to be true")
	}
}

func TestHashJoinBuildCancelled(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
	}
	buildRows := []map[string]any{
		{"id": "1"},
	}

	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Build

	source := NewSliceSource(buildSchema, buildRows)
	err := hj.Build(ctx, source)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestHashJoinRightJoinIntKey(t *testing.T) {
	// Right join with integer keys exercises intMatched tracking
	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}
	rightRows := []map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
		{"id": int64(3), "name": "Carol"}, // no match
	}

	hj := NewHashJoin(RightJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(rightSchema, rightRows)

	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	leftRows := []map[string]any{
		{"id": int64(1), "amount": 100.0},
	}

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	unmatched := probe.FlushUnmatched(leftSchema)
	if unmatched == nil {
		t.Fatal("expected unmatched rows for right join")
	}
	unmatchedRows := unmatched.ToRows()
	// Bob and Carol are unmatched
	if len(unmatchedRows) != 2 {
		t.Fatalf("expected 2 unmatched rows, got %d", len(unmatchedRows))
	}
}

func TestFlushUnmatchedInnerJoin(t *testing.T) {
	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows([]parquet.Column{{Name: "id", Type: parquet.TypeString}},
		[]map[string]any{{"id": "1"}})
	probe := hj.Probe()
	// FlushUnmatched returns nil for non-right/full-outer joins
	result := probe.FlushUnmatched(nil)
	if result != nil {
		t.Fatal("expected nil for inner join FlushUnmatched")
	}
}

func TestHashJoinBuildTableAlias(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
	}
	leftRows := []map[string]any{
		{"id": "1", "name": "LeftName"},
	}

	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString}, // duplicate column name
	}
	rightRows := []map[string]any{
		{"id": "1", "name": "RightName"},
	}

	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.BuildTableAlias = "r"
	hj.BuildFromRows(rightSchema, rightRows)

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(sink.Rows))
	}
	// Should have "r.name" for the build-side duplicate column
	row := sink.Rows[0]
	if row["name"] != "LeftName" {
		t.Fatalf("expected left name='LeftName', got %v", row["name"])
	}
	if row["r.name"] != "RightName" {
		t.Fatalf("expected 'r.name'='RightName', got %v", row["r.name"])
	}
}

func TestHashJoinInnerNoMatch(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
	}
	leftRows := []map[string]any{
		{"id": "x"},
	}
	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
	}
	rightRows := []map[string]any{
		{"id": "y"},
	}

	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(rightSchema, rightRows)

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.Rows) != 0 {
		t.Fatalf("expected 0 rows for no-match inner join, got %d", len(sink.Rows))
	}
}

func TestHashJoinFixKeyAssignmentIntKey(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "nation_id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}
	buildRows := []map[string]any{
		{"nation_id": int64(1), "name": "USA"},
		{"nation_id": int64(2), "name": "UK"},
	}

	// Misassigned: left key is in build, right key is not
	hj := NewHashJoin(InnerJoin, []string{"nation_id"}, []string{"src_id"})
	hj.BuildFromRows(buildSchema, buildRows)
	hj.FixKeyAssignment()

	// After fix, the int key path should be rebuilt
	if hj.LeftKeys[0] != "src_id" {
		t.Fatalf("expected left key 'src_id', got %q", hj.LeftKeys[0])
	}
}

func TestSetVectorNull(t *testing.T) {
	// Test setVectorNull for bytes-based types
	v := batch.NewVector(batch.TypeString, 2)
	v.BytesData.Set(0, []byte("hello"))
	v.BytesData.Set(1, []byte("world"))

	setVectorNull(v, 0)
	if !v.Nulls.IsNullFast(0) {
		t.Fatal("expected null at index 0")
	}

	// Test for non-bytes type
	vi := batch.NewVector(batch.TypeInt64, 1)
	vi.Int64Data[0] = 42
	setVectorNull(vi, 0)
	if !vi.Nulls.IsNullFast(0) {
		t.Fatal("expected null at index 0")
	}
}

func TestHashJoinPortKey(t *testing.T) {
	// Test the int key fast path with Port type
	rightSchema := []parquet.Column{
		{Name: "port", Type: parquet.TypePort},
		{Name: "service", Type: parquet.TypeString},
	}
	rightRows := []map[string]any{
		{"port": int32(80), "service": "http"},
		{"port": int32(443), "service": "https"},
	}

	hj := NewHashJoin(InnerJoin, []string{"port"}, []string{"port"})
	hj.BuildFromRows(rightSchema, rightRows)

	if !hj.useIntKey {
		t.Fatal("expected int key path for Port type")
	}

	leftSchema := []parquet.Column{
		{Name: "port", Type: parquet.TypePort},
		{Name: "host", Type: parquet.TypeString},
	}
	leftRows := []map[string]any{
		{"port": int32(80), "host": "server1"},
		{"port": int32(22), "host": "server2"}, // no match
	}

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(sink.Rows))
	}
}

func TestIsRightJoinKey(t *testing.T) {
	hj := NewHashJoin(InnerJoin, []string{"lid"}, []string{"rid", "rname"})
	probe := hj.Probe()
	if !probe.isRightJoinKey("rid") {
		t.Fatal("expected rid to be a right join key")
	}
	if !probe.isRightJoinKey("rname") {
		t.Fatal("expected rname to be a right join key")
	}
	if probe.isRightJoinKey("lid") {
		t.Fatal("expected lid to NOT be a right join key")
	}
}

func TestLeftHasColumn(t *testing.T) {
	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	probe := hj.Probe()
	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
	}
	if !probe.leftHasColumn("id", leftSchema) {
		t.Fatal("expected 'id' to be in left schema")
	}
	if probe.leftHasColumn("missing", leftSchema) {
		t.Fatal("expected 'missing' to NOT be in left schema")
	}
}

func TestCrossJoinEmptyBuild(t *testing.T) {
	hj := NewHashJoin(CrossJoin, nil, nil)
	hj.BuildFromRows([]parquet.Column{{Name: "id", Type: parquet.TypeString}}, nil)

	leftSchema := []parquet.Column{
		{Name: "val", Type: parquet.TypeString},
	}
	leftRows := []map[string]any{
		{"val": "a"},
	}
	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.Rows) != 0 {
		t.Fatalf("expected 0 rows for cross join with empty build, got %d", len(sink.Rows))
	}
}

func TestAntiJoinNoMatches(t *testing.T) {
	// All rows should pass through anti join when build side is empty
	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
	}
	leftRows := []map[string]any{
		{"id": "a"},
		{"id": "b"},
	}
	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
	}

	hj := NewHashJoin(AntiJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(rightSchema, nil)

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.Rows) != 2 {
		t.Fatalf("expected 2 rows for anti join with empty build, got %d", len(sink.Rows))
	}
}

// TestHashJoinPooledBatchNullBitmapSize is a regression test for a panic where
// the join's pooled output batch had a null bitmap sized for DefaultBatchSize
// (2048) but the batch Len was set to the actual output size. When the output
// size matched a 64-bit word boundary (e.g. 256), accessing bit index == len
// caused an out-of-bounds panic in Bitmap.IsNullFast.
func TestHashJoinPooledBatchNullBitmapSize(t *testing.T) {
	// Build side: create enough rows that the join produces a pooled output
	// batch with Len < DefaultBatchSize, hitting the pooled path.
	buildSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString, Nullable: true},
	}
	// 200 build rows — join output will be ≤200 rows, well under DefaultBatchSize
	buildRows := make([]map[string]any, 200)
	for i := range buildRows {
		buildRows[i] = map[string]any{"id": int64(i), "val": "v"}
	}

	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(buildSchema, buildRows)

	probeSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "data", Type: parquet.TypeString},
	}
	probeRows := make([]map[string]any, 200)
	for i := range probeRows {
		probeRows[i] = map[string]any{"id": int64(i), "data": "d"}
	}

	source := NewSliceSource(probeSchema, probeRows)
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 200 {
		t.Fatalf("expected 200 rows, got %d", len(sink.Rows))
	}

	// The critical check: verify that the output batch null bitmaps are
	// correctly sized. Before the fix, the bitmap would be sized for
	// DefaultBatchSize but col.Len would be set to the actual output size,
	// causing IsNullFast to panic on boundary indices.
	for _, b := range sink.Batches() {
		for _, col := range b.Columns {
			for ri := 0; ri < b.ActiveLen(); ri++ {
				_ = col.Nulls.IsNull(ri)
			}
		}
	}
}

// testBatchSource is a simple Source for testing that yields pre-built batches.
type testBatchSource struct {
	batches []*batch.RecordBatch
	pos     int
}

func (s *testBatchSource) Init(_ context.Context) error { return nil }
func (s *testBatchSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	if s.pos >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.pos]
	s.pos++
	return b, nil
}
func (s *testBatchSource) Close() error { return nil }

// TestConsolidateBuildThreshold verifies that consolidateBuild skips
// consolidation for large build sides (>2M rows) while still producing
// correct join results. This is a regression test for the performance
// optimization that avoids O(n) copy cost for large build sides.
func TestConsolidateBuildThreshold(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}

	// Build a join with enough batches to test multi-batch probe path.
	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	ctx := context.Background()

	// Feed 10 batches of 100 rows each (1000 rows total, below threshold).
	// consolidateBuild should merge them into 1 batch.
	src := &testBatchSource{}
	for batchNum := 0; batchNum < 10; batchNum++ {
		b := batch.NewRecordBatch(schema, 100)
		for i := 0; i < 100; i++ {
			rowID := int64(batchNum*100 + i)
			b.Columns[0].Int64Data[i] = rowID
			b.Columns[1].BytesData.Set(i, []byte("v"))
		}
		b.Len = 100
		src.batches = append(src.batches, b)
	}

	if err := hj.Build(ctx, src); err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Below threshold: should consolidate to 1 batch
	if len(hj.buildBatches) != 1 {
		t.Errorf("expected 1 consolidated batch for 1000 rows, got %d", len(hj.buildBatches))
	}

	// Probe and verify correctness
	probe := hj.Probe()
	if err := probe.Init(ctx); err != nil {
		t.Fatalf("Probe.Init: %v", err)
	}

	probeBatch := batch.NewRecordBatch(schema[:1], 3)
	probeBatch.Columns[0].Int64Data[0] = 0
	probeBatch.Columns[0].Int64Data[1] = 500
	probeBatch.Columns[0].Int64Data[2] = 999
	probeBatch.Len = 3

	out, err := probe.Execute(ctx, probeBatch)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out == nil || out.ActiveLen() != 3 {
		t.Fatalf("expected 3 output rows, got %v", out)
	}
}

// --- Regression: hash table memory tracking (Q10 OOM fix) ---

func TestHashJoinReconcileHashMemory(t *testing.T) {
	// Q10 root cause: EstimateBatchBytes charged 40 bytes/row for hash overhead,
	// but actual hash table (entries + arena + chains) consumed 2-5x more.
	// Spill triggered too late → OOM. Fix: reconcileHashMemory adjusts tracker
	// to reflect actual hash table allocations after grow().

	ctx := context.Background()
	tracker := memory.NewTracker("test", 100*1024*1024) // 100 MB budget

	schema := []parquet.Column{
		{Name: "key", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeInt64},
	}

	hj := NewHashJoin(InnerJoin, []string{"key"}, []string{"key"})
	hj.MemTracker = tracker

	// Build with 50K rows — enough to trigger multiple grow() calls.
	nRows := 50000
	batchSize := 2048
	var buildBatches []*batch.RecordBatch
	for start := 0; start < nRows; start += batchSize {
		end := start + batchSize
		if end > nRows {
			end = nRows
		}
		n := end - start
		b := batch.NewRecordBatch(schema, n)
		for i := 0; i < n; i++ {
			b.Columns[0].Int64Data[i] = int64(start + i)
			b.Columns[1].Int64Data[i] = int64(start + i + 100)
		}
		buildBatches = append(buildBatches, b)
	}

	err := hj.Build(ctx, &covTestBatchSource{batches: buildBatches})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// The tracker should reflect more than just EstimateBatchBytes charges.
	// reconcileHashMemory adds the delta between actual hash table size and
	// the 40 bytes/row estimate charged per batch.
	trackerUsed := tracker.Used()
	if trackerUsed <= 0 {
		t.Errorf("tracker.Used() = %d, expected > 0", trackerUsed)
	}

	// Verify the hash table overhead method reports actual allocations.
	overhead := hj.hashTableOverhead()
	if overhead <= 0 {
		t.Errorf("hashTableOverhead() = %d, expected > 0", overhead)
	}
	// intHashTable at 50K entries at 70% load: cap ~73K, 73K × 16 = 1.17 MB
	// Plus arena (50K × 8 = 400 KB) + arenaNext (50K × 4 = 200 KB) + bloom
	expectedMin := int64(nRows) * 12 // very conservative: 12 bytes/row minimum
	if overhead < expectedMin {
		t.Errorf("hashTableOverhead() = %d, expected >= %d", overhead, expectedMin)
	}
}

// covTestBatchSource is a simple Source for testing that yields pre-built batches.
type covTestBatchSource struct {
	batches []*batch.RecordBatch
	idx     int
}

func (s *covTestBatchSource) Init(ctx context.Context) error { return nil }
func (s *covTestBatchSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if s.idx >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.idx]
	s.idx++
	return b, nil
}
func (s *covTestBatchSource) Close() error { return nil }
