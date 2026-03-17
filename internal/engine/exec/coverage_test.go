package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// --- Filter tests ---

func TestFilterEmptyResult(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"val": int64(1)},
		{"val": int64(2)},
	}
	source := NewSliceSource(schema, rows)
	filter := NewFilter(ColumnCompare("val", OpGt, int64(100)))
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{filter}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(sink.Rows))
	}
}

func TestFilterWithSelectionVector(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": int64(10)},
		{"val": int64(20)},
		{"val": int64(30)},
		{"val": int64(40)},
	})
	b.Sel = []uint16{1, 3} // only 20 and 40

	filter := NewFilter(ColumnCompare("val", OpGt, int64(25)))
	filter.Init(context.Background())
	out, err := filter.Execute(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("expected non-nil batch")
	}
	if len(out.Sel) != 1 { // only 40 > 25
		t.Fatalf("expected 1 row, got %d", len(out.Sel))
	}
}

func TestFilterClose(t *testing.T) {
	f := NewFilter(func(_ *batch.RecordBatch, _ int) bool { return true })
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestColumnCompareAllOps(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": int64(10)},
	})

	tests := []struct {
		op    CompareOp
		value int64
		want  bool
	}{
		{OpEq, 10, true},
		{OpEq, 11, false},
		{OpNe, 10, false},
		{OpNe, 11, true},
		{OpLt, 11, true},
		{OpLt, 10, false},
		{OpLe, 10, true},
		{OpLe, 9, false},
		{OpGt, 9, true},
		{OpGt, 10, false},
		{OpGe, 10, true},
		{OpGe, 11, false},
	}

	for _, tt := range tests {
		pred := ColumnCompare("val", tt.op, tt.value)
		got := pred(b, 0)
		if got != tt.want {
			t.Fatalf("op=%d val=%d: expected %v, got %v", tt.op, tt.value, tt.want, got)
		}
	}
}

func TestColumnCompareFloat64(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": 3.14},
	})
	pred := ColumnCompare("val", OpGt, 3.0)
	if !pred(b, 0) {
		t.Fatal("expected 3.14 > 3.0 to be true")
	}
}

func TestColumnCompareFloat32(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat32},
	}
	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].Float32Data[0] = 2.5

	pred := ColumnCompare("val", OpLt, float64(3.0))
	if !pred(b, 0) {
		t.Fatal("expected 2.5 < 3.0 to be true")
	}
}

func TestColumnCompareString(t *testing.T) {
	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"name": "bob"},
	})
	pred := ColumnCompare("name", OpEq, "bob")
	if !pred(b, 0) {
		t.Fatal("expected match")
	}
	pred2 := ColumnCompare("name", OpNe, "bob")
	if pred2(b, 0) {
		t.Fatal("expected no match")
	}
}

func TestColumnCompareBool(t *testing.T) {
	schema := []parquet.Column{
		{Name: "flag", Type: parquet.TypeBool},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"flag": true},
	})
	pred := ColumnCompare("flag", OpEq, true)
	if !pred(b, 0) {
		t.Fatal("expected true == true")
	}
	pred2 := ColumnCompare("flag", OpNe, true)
	if pred2(b, 0) {
		t.Fatal("expected true != true to be false")
	}
}

func TestColumnCompareIsNull(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": nil},
		{"val": int64(42)},
	})
	pred := ColumnCompare("val", OpIsNull, nil)
	if !pred(b, 0) {
		t.Fatal("expected null to match IS NULL")
	}
	if pred(b, 1) {
		t.Fatal("expected non-null to not match IS NULL")
	}

	predNot := ColumnCompare("val", OpIsNotNull, nil)
	if predNot(b, 0) {
		t.Fatal("expected null to not match IS NOT NULL")
	}
	if !predNot(b, 1) {
		t.Fatal("expected non-null to match IS NOT NULL")
	}
}

func TestColumnCompareNullValue(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": nil},
	})
	pred := ColumnCompare("val", OpEq, int64(42))
	if pred(b, 0) {
		t.Fatal("null value should not match equality")
	}
}

func TestColumnCompareMissingColumn(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": int64(1)},
	})
	pred := ColumnCompare("nonexistent", OpEq, int64(1))
	if pred(b, 0) {
		t.Fatal("missing column should not match")
	}
}

func TestColumnComparePort(t *testing.T) {
	schema := []parquet.Column{
		{Name: "port", Type: parquet.TypePort},
	}
	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].Int32Data[0] = 80

	pred := ColumnCompare("port", OpEq, int64(80))
	if !pred(b, 0) {
		t.Fatal("expected port 80 to match")
	}
}

func TestColumnCompareDuration(t *testing.T) {
	schema := []parquet.Column{
		{Name: "dur", Type: parquet.TypeDuration},
	}
	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].Int64Data[0] = 1000

	pred := ColumnCompare("dur", OpGt, int64(500))
	if !pred(b, 0) {
		t.Fatal("expected 1000 > 500")
	}
}

func TestColumnCompareDate(t *testing.T) {
	schema := []parquet.Column{
		{Name: "d", Type: parquet.TypeDate},
	}
	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].Int32Data[0] = 100

	pred := ColumnCompare("d", OpLe, int64(100))
	if !pred(b, 0) {
		t.Fatal("expected 100 <= 100")
	}
}

func TestColumnCompareIPv4(t *testing.T) {
	schema := []parquet.Column{
		{Name: "ip", Type: parquet.TypeIPv4},
	}
	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].SetValue(0, "192.168.1.1")

	pred := ColumnCompare("ip", OpEq, "192.168.1.1")
	if !pred(b, 0) {
		t.Fatal("expected match")
	}
}

func TestColumnCompareMAC(t *testing.T) {
	schema := []parquet.Column{
		{Name: "mac", Type: parquet.TypeMAC},
	}
	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].SetValue(0, "aa:bb:cc:dd:ee:ff")

	pred := ColumnCompare("mac", OpEq, "aa:bb:cc:dd:ee:ff")
	if !pred(b, 0) {
		t.Fatal("expected match")
	}
}

func TestColumnCompareIPv6(t *testing.T) {
	schema := []parquet.Column{
		{Name: "ip6", Type: parquet.TypeIPv6},
	}
	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].SetValue(0, "::1")

	pred := ColumnCompare("ip6", OpEq, "::1")
	if !pred(b, 0) {
		t.Fatal("expected match")
	}
}

func TestColumnCompareCIDR(t *testing.T) {
	schema := []parquet.Column{
		{Name: "cidr", Type: parquet.TypeCIDR},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"cidr": "10.0.0.0/8"},
	})
	pred := ColumnCompare("cidr", OpEq, "10.0.0.0/8")
	if !pred(b, 0) {
		t.Fatal("expected match")
	}
}

func TestColumnCompareUUID(t *testing.T) {
	schema := []parquet.Column{
		{Name: "uuid", Type: parquet.TypeUUID},
	}
	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].SetValue(0, "550e8400-e29b-41d4-a716-446655440000")

	pred := ColumnCompare("uuid", OpEq, "550e8400-e29b-41d4-a716-446655440000")
	// UUID comparison is on raw bytes, so this tests the string compare path
	_ = pred(b, 0) // just ensure no panic
}

// --- And/Or combinators ---

func TestAndPredicate(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": int64(15)},
	})
	pred := And(
		ColumnCompare("val", OpGt, int64(10)),
		ColumnCompare("val", OpLt, int64(20)),
	)
	if !pred(b, 0) {
		t.Fatal("expected 15 to be > 10 AND < 20")
	}

	pred2 := And(
		ColumnCompare("val", OpGt, int64(10)),
		ColumnCompare("val", OpLt, int64(14)),
	)
	if pred2(b, 0) {
		t.Fatal("expected 15 to NOT be < 14")
	}
}

func TestOrPredicate(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": int64(5)},
	})
	pred := Or(
		ColumnCompare("val", OpEq, int64(5)),
		ColumnCompare("val", OpEq, int64(10)),
	)
	if !pred(b, 0) {
		t.Fatal("expected 5 == 5 OR 5 == 10 to be true")
	}

	pred2 := Or(
		ColumnCompare("val", OpEq, int64(100)),
		ColumnCompare("val", OpEq, int64(200)),
	)
	if pred2(b, 0) {
		t.Fatal("expected no match")
	}
}

// --- KernelFilter tests ---

func TestKernelFilter(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"val": int64(10)},
		{"val": int64(20)},
		{"val": int64(30)},
	}
	source := NewSliceSource(schema, rows)
	kf := NewKernelFilter("val", OpGt, int64(15))
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{kf}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(sink.Rows))
	}
}

func TestKernelFilterMissingColumn(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": int64(1)},
	})
	kf := NewKernelFilter("nonexistent", OpEq, int64(1))
	kf.Init(context.Background())
	out, err := kf.Execute(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		t.Fatal("expected nil for missing column")
	}
}

func TestKernelFilterClose(t *testing.T) {
	kf := NewKernelFilter("val", OpEq, int64(1))
	if err := kf.Close(); err != nil {
		t.Fatal(err)
	}
}

// --- Compare functions ---

func TestCompareInt64AllOps(t *testing.T) {
	if !compareInt64(5, 10, OpLt) {
		t.Fatal("5 < 10")
	}
	if !compareInt64(10, 5, OpGt) {
		t.Fatal("10 > 5")
	}
	if compareInt64(5, 10, CompareOp(99)) {
		t.Fatal("unknown op should be false")
	}
}

func TestCompareFloat64AllOps(t *testing.T) {
	if !compareFloat64(1.0, 2.0, OpLt) {
		t.Fatal("1.0 < 2.0")
	}
	if !compareFloat64(2.0, 2.0, OpEq) {
		t.Fatal("2.0 == 2.0")
	}
	if !compareFloat64(2.0, 1.0, OpGt) {
		t.Fatal("2.0 > 1.0")
	}
	if !compareFloat64(2.0, 1.0, OpNe) {
		t.Fatal("2.0 != 1.0")
	}
	if !compareFloat64(2.0, 2.0, OpLe) {
		t.Fatal("2.0 <= 2.0")
	}
	if !compareFloat64(2.0, 2.0, OpGe) {
		t.Fatal("2.0 >= 2.0")
	}
	if compareFloat64(1.0, 2.0, CompareOp(99)) {
		t.Fatal("unknown op should be false")
	}
}

func TestCompareStringAllOps(t *testing.T) {
	if !compareString("a", "b", OpLt) {
		t.Fatal("a < b")
	}
	if !compareString("b", "a", OpGt) {
		t.Fatal("b > a")
	}
	if !compareString("a", "a", OpEq) {
		t.Fatal("a == a")
	}
	if !compareString("a", "b", OpNe) {
		t.Fatal("a != b")
	}
	if !compareString("a", "a", OpLe) {
		t.Fatal("a <= a")
	}
	if !compareString("a", "a", OpGe) {
		t.Fatal("a >= a")
	}
	if compareString("a", "b", CompareOp(99)) {
		t.Fatal("unknown op should be false")
	}
}

// --- toKernelOp ---

func TestToKernelOp(t *testing.T) {
	// Verify default case
	op := toKernelOp(CompareOp(99))
	if op != 0 { // kernel.OpEq is 0
		t.Fatalf("expected default to be 0, got %d", op)
	}
}

// --- Type conversion helpers in exec ---

func TestExecToInt64(t *testing.T) {
	if toInt64(int64(5)) != 5 {
		t.Fatal("int64")
	}
	if toInt64(int(5)) != 5 {
		t.Fatal("int")
	}
	if toInt64(int32(5)) != 5 {
		t.Fatal("int32")
	}
	if toInt64(float64(5.0)) != 5 {
		t.Fatal("float64")
	}
	if toInt64("string") != 0 {
		t.Fatal("default")
	}
}

func TestExecToFloat64(t *testing.T) {
	if toFloat64(float64(3.14)) != 3.14 {
		t.Fatal("float64")
	}
	if toFloat64(float32(2.5)) != float64(float32(2.5)) {
		t.Fatal("float32")
	}
	if toFloat64(int64(10)) != 10.0 {
		t.Fatal("int64")
	}
	if toFloat64(int(5)) != 5.0 {
		t.Fatal("int")
	}
	if toFloat64("string") != 0 {
		t.Fatal("default")
	}
}

// --- Project tests ---

func TestProjectClose(t *testing.T) {
	p := NewProject(nil)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProjectWithSelectionVector(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeFloat64},
		{Name: "b", Type: parquet.TypeFloat64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"a": 10.0, "b": 3.0},
		{"a": 20.0, "b": 7.0},
		{"a": 30.0, "b": 1.0},
	})
	b.Sel = []uint16{0, 2}

	proj := NewProject([]ProjectColumn{
		{Name: "a", Type: parquet.TypeFloat64, Expr: ColumnRef("a")},
	})
	proj.Init(context.Background())
	out, err := proj.Execute(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if out.Len != 2 {
		t.Fatalf("expected 2 rows, got %d", out.Len)
	}
}

func TestColumnRefMissing(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": int64(1)},
	})
	expr := ColumnRef("nonexistent")
	result := expr(b, 0)
	if result != nil {
		t.Fatal("expected nil for missing column")
	}
}

func TestLiteral(t *testing.T) {
	expr := Literal(int64(42))
	result := expr(nil, 0)
	if result.(int64) != 42 {
		t.Fatalf("expected 42, got %v", result)
	}
}

func TestArithExpr(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeFloat64},
		{Name: "b", Type: parquet.TypeFloat64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"a": 10.0, "b": 3.0},
	})

	tests := []struct {
		op   string
		want float64
	}{
		{"+", 13.0},
		{"-", 7.0},
		{"*", 30.0},
		{"/", 10.0 / 3.0},
	}

	for _, tt := range tests {
		expr := ArithExpr(ColumnRef("a"), ColumnRef("b"), tt.op)
		result := expr(b, 0)
		if result.(float64) != tt.want {
			t.Fatalf("op %s: expected %f, got %v", tt.op, tt.want, result)
		}
	}
}

func TestArithExprDivByZero(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeFloat64},
		{Name: "b", Type: parquet.TypeFloat64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"a": 10.0, "b": 0.0},
	})

	expr := ArithExpr(ColumnRef("a"), ColumnRef("b"), "/")
	result := expr(b, 0)
	if result != nil {
		t.Fatalf("expected nil for divide by zero, got %v", result)
	}
}

func TestArithExprNullInput(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeFloat64},
		{Name: "b", Type: parquet.TypeFloat64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"a": nil, "b": 3.0},
	})

	expr := ArithExpr(ColumnRef("a"), ColumnRef("b"), "+")
	result := expr(b, 0)
	if result != nil {
		t.Fatal("expected nil for null input")
	}
}

func TestArithExprUnknownOp(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeFloat64},
		{Name: "b", Type: parquet.TypeFloat64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"a": 10.0, "b": 3.0},
	})

	expr := ArithExpr(ColumnRef("a"), ColumnRef("b"), "%")
	result := expr(b, 0)
	if result != nil {
		t.Fatalf("expected nil for unknown op, got %v", result)
	}
}

// --- Distinct tests ---

func TestDistinctSingleCol(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"val": int64(1)},
		{"val": int64(2)},
		{"val": int64(1)}, // duplicate
		{"val": int64(3)},
		{"val": int64(2)}, // duplicate
	}

	source := NewSliceSource(schema, rows)
	d := NewDistinct()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{d}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.Rows) != 3 {
		t.Fatalf("expected 3 distinct rows, got %d", len(sink.Rows))
	}
}

func TestDistinctAllDuplicates(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": int64(1)},
		{"val": int64(1)},
		{"val": int64(1)},
	})
	d := NewDistinct()
	d.Init(context.Background())
	out, err := d.Execute(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Sel) != 1 {
		t.Fatalf("expected 1, got %d", len(out.Sel))
	}

	// Second batch with same value should be fully filtered
	b2 := batch.FromRows(schema, []map[string]any{
		{"val": int64(1)},
	})
	out2, err := d.Execute(context.Background(), b2)
	if err != nil {
		t.Fatal(err)
	}
	if out2 != nil {
		t.Fatal("expected nil (all duplicates)")
	}
}

func TestDistinctWithNull(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": nil},
		{"val": int64(1)},
		{"val": nil}, // duplicate null
	})
	d := NewDistinct()
	d.Init(context.Background())
	out, err := d.Execute(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Sel) != 2 { // nil and 1
		t.Fatalf("expected 2 distinct, got %d", len(out.Sel))
	}
}

func TestDistinctClose(t *testing.T) {
	d := NewDistinct()
	d.Init(context.Background())
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if d.seen != nil {
		t.Fatal("expected seen to be nil after close")
	}
}

func TestDistinctWithSelVector(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": int64(1)},
		{"val": int64(2)},
		{"val": int64(3)},
		{"val": int64(1)},
	})
	b.Sel = []uint16{0, 2, 3}

	d := NewDistinct()
	d.Init(context.Background())
	out, err := d.Execute(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Sel) != 2 { // 1, 3 (1 at index 3 is duplicate of 1 at index 0)
		t.Fatalf("expected 2 distinct, got %d", len(out.Sel))
	}
}

// --- Sort tests ---

func TestSortAscending(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"val": int64(30)},
		{"val": int64(10)},
		{"val": int64(20)},
	}

	s := NewSort([]SortKey{{Column: "val", Order: Ascending}})
	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: s}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, _ := s.Next(context.Background())
	result := b.ToRows()
	if result[0]["val"].(int64) != 10 {
		t.Fatalf("expected first=10, got %v", result[0]["val"])
	}
	if result[2]["val"].(int64) != 30 {
		t.Fatalf("expected last=30, got %v", result[2]["val"])
	}

	// Next call should return nil
	b2, _ := s.Next(context.Background())
	if b2 != nil {
		t.Fatal("expected nil after all batches consumed")
	}
}

func TestSortClose(t *testing.T) {
	s := NewSort(nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSortEmpty(t *testing.T) {
	s := NewSort([]SortKey{{Column: "val", Order: Ascending}})
	s.Init(context.Background())
	if err := s.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, _ := s.Next(context.Background())
	if b != nil {
		t.Fatal("expected nil for empty sort")
	}
}

func TestSortWithLimit(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	var rows []map[string]any
	for i := 0; i < 100; i++ {
		rows = append(rows, map[string]any{"val": float64(100 - i)})
	}

	s := NewSort([]SortKey{{Column: "val", Order: Ascending}})
	s.Limit = 5

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: s}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, _ := s.Next(context.Background())
	if b == nil {
		t.Fatal("expected non-nil batch")
	}
	result := b.ToRows()
	if len(result) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result))
	}
	if result[0]["val"].(float64) != 1.0 {
		t.Fatalf("expected first=1.0, got %v", result[0]["val"])
	}
}

func TestTruncate(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	var rows []map[string]any
	for i := 0; i < 10; i++ {
		rows = append(rows, map[string]any{"val": int64(i)})
	}

	s := NewSort([]SortKey{{Column: "val", Order: Ascending}})
	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: s}
	pipe.Run(context.Background())

	s.Truncate(3)
	var result []map[string]any
	for {
		b, _ := s.Next(context.Background())
		if b == nil {
			break
		}
		result = append(result, b.ToRows()...)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 rows after truncate, got %d", len(result))
	}
}

func TestTruncateZero(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{{"val": int64(1)}}
	s := NewSort([]SortKey{{Column: "val", Order: Ascending}})
	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: s}
	pipe.Run(context.Background())

	s.Truncate(0)
	b, _ := s.Next(context.Background())
	if b != nil {
		t.Fatal("expected nil after truncate to 0")
	}
}

// --- TopN tests ---

func TestTopN(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	var rows []map[string]any
	for i := 0; i < 100; i++ {
		rows = append(rows, map[string]any{"val": float64(100 - i)})
	}

	topN := NewTopN([]SortKey{{Column: "val", Order: Ascending}}, 5)
	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: topN}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	var result []map[string]any
	for {
		b, err := topN.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		result = append(result, b.ToRows()...)
	}
	if len(result) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result))
	}
}

func TestTopNInnerSort(t *testing.T) {
	topN := NewTopN([]SortKey{{Column: "val"}}, 10)
	inner := topN.InnerSort()
	if inner == nil {
		t.Fatal("expected non-nil inner sort")
	}
}

func TestTopNClose(t *testing.T) {
	topN := NewTopN(nil, 5)
	topN.Init(context.Background())
	if err := topN.Close(); err != nil {
		t.Fatal(err)
	}
}

// --- Pipeline tests ---

func TestPipelineClose(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	source := NewSliceSource(schema, nil)
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: nil, Sink: sink}
	if err := pipe.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPipelineContextCancellation(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	var rows []map[string]any
	for i := 0; i < 10000; i++ {
		rows = append(rows, map[string]any{"val": int64(i)})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	source := NewSliceSource(schema, rows)
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: nil, Sink: sink}
	err := pipe.Run(ctx)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestCollectSinkBatches(t *testing.T) {
	sink := &CollectSink{}
	sink.Init(context.Background())

	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": int64(1)},
	})
	sink.Consume(context.Background(), b)
	sink.Finalize(context.Background())

	batches := sink.Batches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
}

func TestCollectSinkClose(t *testing.T) {
	sink := &CollectSink{}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSliceSourceClose(t *testing.T) {
	source := NewSliceSource(nil, nil)
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
}

// --- compareAny tests ---

func TestCompareAny(t *testing.T) {
	tests := []struct {
		name string
		a, b any
		want int
	}{
		{"nil_nil", nil, nil, 0},
		{"nil_first", nil, int64(1), -1},
		{"nil_second", int64(1), nil, 1},
		{"int64_eq", int64(5), int64(5), 0},
		{"int64_lt", int64(3), int64(5), -1},
		{"int64_gt", int64(5), int64(3), 1},
		{"int32_converts", int32(5), int64(5), 0},
		{"float64_eq", 3.14, 3.14, 0},
		{"float64_lt", 1.0, 2.0, -1},
		{"float64_gt", 2.0, 1.0, 1},
		{"float32_converts", float32(5.0), float64(5.0), 0},
		{"string_eq", "abc", "abc", 0},
		{"string_lt", "abc", "xyz", -1},
		{"string_gt", "xyz", "abc", 1},
		{"string_nonstring", "abc", int64(5), 0},
		{"bool_eq", true, true, 0},
		{"bool_lt", false, true, -1},
		{"bool_gt", true, false, 1},
		{"bool_nonbool", true, int64(5), 0},
		{"unknown", struct{}{}, struct{}{}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareAny(tt.a, tt.b)
			if got != tt.want {
				t.Fatalf("compareAny(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// --- appendKeyValue tests ---

func TestAppendKeyValue(t *testing.T) {
	tests := []struct {
		input any
		want  string
	}{
		{nil, "<null>"},
		{int64(42), "42"},
		{int32(10), "10"},
		{float64(3.14), "3.14"},
		{float32(2.5), "2.5"},
		{"hello", "hello"},
		{true, "true"},
		{false, "false"},
		{struct{}{}, "<unknown>"},
	}
	for _, tt := range tests {
		buf := appendKeyValue(nil, tt.input)
		got := string(buf)
		if got != tt.want {
			t.Fatalf("appendKeyValue(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// --- Aggregate edge cases ---

func TestHashAggregateMinMax(t *testing.T) {
	schema := []parquet.Column{
		{Name: "group", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"group": "a", "val": 10.0},
		{"group": "a", "val": 30.0},
		{"group": "a", "val": 20.0},
	}

	agg := NewHashAggregate([]string{"group"}, []AggColumn{
		{Func: AggMin, InputCol: "val", OutputCol: "min_val", OutputType: parquet.TypeFloat64},
		{Func: AggMax, InputCol: "val", OutputCol: "max_val", OutputType: parquet.TypeFloat64},
		{Func: AggAvg, InputCol: "val", OutputCol: "avg_val", OutputType: parquet.TypeFloat64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, err := agg.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result := b.ToRows()
	if len(result) != 1 {
		t.Fatalf("expected 1 group, got %d", len(result))
	}
	if result[0]["min_val"].(float64) != 10.0 {
		t.Fatalf("expected min=10.0, got %v", result[0]["min_val"])
	}
	if result[0]["max_val"].(float64) != 30.0 {
		t.Fatalf("expected max=30.0, got %v", result[0]["max_val"])
	}
}

func TestHashAggregateClose(t *testing.T) {
	agg := NewHashAggregate(nil, nil)
	if err := agg.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHashAggregateNoGroups(t *testing.T) {
	agg := NewHashAggregate(nil, nil)
	agg.Init(context.Background())
	b, _ := agg.Next(context.Background())
	if b != nil {
		t.Fatal("expected nil for no groups")
	}
}

// --- copyVectorValue ---

func TestCopyVectorValueTypes(t *testing.T) {
	types := []struct {
		name string
		typ  parquet.TypeID
	}{
		{"bool", parquet.TypeBool},
		{"int32", parquet.TypeInt32},
		{"int64", parquet.TypeInt64},
		{"float32", parquet.TypeFloat32},
		{"float64", parquet.TypeFloat64},
		{"string", parquet.TypeString},
		{"decimal", parquet.TypeDecimal},
	}

	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			src := batch.NewVector(tt.typ, 1)
			dst := batch.NewVector(tt.typ, 1)

			// Set a non-null value
			switch tt.typ {
			case parquet.TypeBool:
				src.BoolData[0] = true
			case parquet.TypeInt32:
				src.Int32Data[0] = 42
			case parquet.TypeInt64:
				src.Int64Data[0] = 100
			case parquet.TypeFloat32:
				src.Float32Data[0] = 1.5
			case parquet.TypeFloat64:
				src.Float64Data[0] = 3.14
			case parquet.TypeString:
				src.BytesData.Set(0, []byte("test"))
				dst.BytesData = batch.NewBytesColumn(1)
			case parquet.TypeDecimal:
				src.DecimalData.Data[0] = batch.Int128From(42)
			}

			copyVectorValue(dst, 0, src, 0)
			// Just verify no panic

			// Test null copy
			src.Nulls.SetNull(0)
			dst2 := batch.NewVector(tt.typ, 1)
			if tt.typ == parquet.TypeString {
				dst2.BytesData = batch.NewBytesColumn(1)
			}
			copyVectorValue(dst2, 0, src, 0)
			if !dst2.Nulls.IsNull(0) {
				t.Fatal("expected null in destination")
			}
		})
	}
}
