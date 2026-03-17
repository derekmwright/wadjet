package exec

import (
	"context"
	"math"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestWindowClose(t *testing.T) {
	w := NewWindow(nil)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWindowNextEmpty(t *testing.T) {
	w := NewWindow(nil)
	_ = w.Init(context.Background())
	b, err := w.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b != nil {
		t.Fatal("expected nil batch for empty window")
	}
}

func TestWindowFinalizeEmpty(t *testing.T) {
	w := NewWindow([]WindowColumn{
		{Func: WinRowNumber, OutputCol: "rn", OutputType: parquet.TypeInt64},
	})
	_ = w.Init(context.Background())
	if err := w.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, _ := w.Next(context.Background())
	if b != nil {
		t.Fatal("expected nil batch after empty finalize")
	}
}

func TestCompareVectorValuesAllTypes(t *testing.T) {
	// Int64
	vi64 := batch.NewVector(batch.TypeInt64, 3)
	vi64.Int64Data[0] = 10
	vi64.Int64Data[1] = 20
	vi64.Int64Data[2] = 10
	if compareVectorValues(vi64, 0, 1) >= 0 {
		t.Error("expected 10 < 20")
	}
	if compareVectorValues(vi64, 1, 0) <= 0 {
		t.Error("expected 20 > 10")
	}
	if compareVectorValues(vi64, 0, 2) != 0 {
		t.Error("expected 10 == 10")
	}

	// Float64
	vf64 := batch.NewVector(batch.TypeFloat64, 2)
	vf64.Float64Data[0] = 1.5
	vf64.Float64Data[1] = 2.5
	if compareVectorValues(vf64, 0, 1) >= 0 {
		t.Error("expected 1.5 < 2.5")
	}
	if compareVectorValues(vf64, 1, 0) <= 0 {
		t.Error("expected 2.5 > 1.5")
	}

	// Int32
	vi32 := batch.NewVector(batch.TypeInt32, 2)
	vi32.Int32Data[0] = 5
	vi32.Int32Data[1] = 10
	if compareVectorValues(vi32, 0, 1) >= 0 {
		t.Error("expected 5 < 10")
	}
	if compareVectorValues(vi32, 1, 0) <= 0 {
		t.Error("expected 10 > 5")
	}

	// Float32
	vf32 := batch.NewVector(batch.TypeFloat32, 2)
	vf32.Float32Data[0] = 1.0
	vf32.Float32Data[1] = 2.0
	if compareVectorValues(vf32, 0, 1) >= 0 {
		t.Error("expected 1.0 < 2.0")
	}

	// Bool
	vb := batch.NewVector(batch.TypeBool, 2)
	vb.BoolData[0] = false
	vb.BoolData[1] = true
	if compareVectorValues(vb, 0, 1) >= 0 {
		t.Error("expected false < true")
	}
	if compareVectorValues(vb, 1, 0) <= 0 {
		t.Error("expected true > false")
	}
	vb.BoolData[1] = false
	if compareVectorValues(vb, 0, 1) != 0 {
		t.Error("expected false == false")
	}

	// String
	vs := batch.NewVector(batch.TypeString, 2)
	vs.BytesData.Set(0, []byte("abc"))
	vs.BytesData.Set(1, []byte("xyz"))
	if compareVectorValues(vs, 0, 1) >= 0 {
		t.Error("expected 'abc' < 'xyz'")
	}
	if compareVectorValues(vs, 1, 0) <= 0 {
		t.Error("expected 'xyz' > 'abc'")
	}
}

func TestVecFloat64AllTypes(t *testing.T) {
	// nil vector
	if vecFloat64(nil, 0) != 0 {
		t.Error("expected 0 for nil vector")
	}

	// Float64
	vf64 := batch.NewVector(batch.TypeFloat64, 1)
	vf64.Float64Data[0] = 3.14
	if vecFloat64(vf64, 0) != 3.14 {
		t.Error("expected 3.14 for Float64")
	}

	// Float32
	vf32 := batch.NewVector(batch.TypeFloat32, 1)
	vf32.Float32Data[0] = 2.5
	if math.Abs(vecFloat64(vf32, 0)-2.5) > 0.001 {
		t.Errorf("expected ~2.5 for Float32, got %f", vecFloat64(vf32, 0))
	}

	// Int64
	vi64 := batch.NewVector(batch.TypeInt64, 1)
	vi64.Int64Data[0] = 42
	if vecFloat64(vi64, 0) != 42.0 {
		t.Error("expected 42.0 for Int64")
	}

	// Int32
	vi32 := batch.NewVector(batch.TypeInt32, 1)
	vi32.Int32Data[0] = 10
	if vecFloat64(vi32, 0) != 10.0 {
		t.Error("expected 10.0 for Int32")
	}

	// Null value
	vi64n := batch.NewVector(batch.TypeInt64, 1)
	vi64n.Nulls.SetNull(0)
	if vecFloat64(vi64n, 0) != 0 {
		t.Error("expected 0 for null value")
	}

	// Unsupported type
	vs := batch.NewVector(batch.TypeString, 1)
	vs.BytesData.Set(0, []byte("abc"))
	if vecFloat64(vs, 0) != 0 {
		t.Error("expected 0 for string type")
	}
}

func TestSameColumnar(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeString},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].Int64Data[0] = 1
	b.Columns[0].Int64Data[1] = 1
	b.Columns[0].Int64Data[2] = 2
	b.Columns[1].BytesData.Set(0, []byte("x"))
	b.Columns[1].BytesData.Set(1, []byte("x"))
	b.Columns[1].BytesData.Set(2, []byte("x"))

	if !sameColumnar(b, 0, 1, []int{0, 1}) {
		t.Error("expected rows 0 and 1 to be same")
	}
	if sameColumnar(b, 0, 2, []int{0, 1}) {
		t.Error("expected rows 0 and 2 to differ")
	}

	// Test with null values
	b.Columns[0].Nulls.SetNull(1)
	if sameColumnar(b, 0, 1, []int{0}) {
		t.Error("expected non-null vs null to differ")
	}

	b.Columns[0].Nulls.SetNull(0)
	if !sameColumnar(b, 0, 1, []int{0}) {
		t.Error("expected both-null to be same")
	}

	// Test with negative index (skip)
	if !sameColumnar(b, 0, 2, []int{-1}) {
		t.Error("expected negative index to be skipped")
	}
}

func TestWindowCopyVectorRangeAllTypes(t *testing.T) {
	types := []struct {
		name string
		typ  batch.TypeID
	}{
		{"Bool", batch.TypeBool},
		{"Int32", batch.TypeInt32},
		{"Int64", batch.TypeInt64},
		{"Float32", batch.TypeFloat32},
		{"Float64", batch.TypeFloat64},
		{"String", batch.TypeString},
		{"Decimal", batch.TypeDecimal},
	}

	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			src := batch.NewVector(tt.typ, 3)
			dst := batch.NewVector(tt.typ, 3)

			// Set some values
			switch tt.typ {
			case batch.TypeBool:
				src.BoolData[0] = true
				src.BoolData[1] = false
			case batch.TypeInt32:
				src.Int32Data[0] = 42
				src.Int32Data[1] = 99
			case batch.TypeInt64:
				src.Int64Data[0] = 100
				src.Int64Data[1] = 200
			case batch.TypeFloat32:
				src.Float32Data[0] = 1.5
				src.Float32Data[1] = 2.5
			case batch.TypeFloat64:
				src.Float64Data[0] = 3.14
				src.Float64Data[1] = 2.72
			case batch.TypeString:
				src.BytesData.Set(0, []byte("hello"))
				src.BytesData.Set(1, []byte("world"))
			case batch.TypeDecimal:
				src.DecimalData.Data[0] = batch.Int128{Lo: 100}
				src.DecimalData.Data[1] = batch.Int128{Lo: 200}
			}
			src.Nulls.SetNull(2) // third element is null

			windowCopyVectorRange(dst, src, 0, 0, 3)

			if !dst.Nulls.IsNullFast(2) {
				t.Error("expected null to be copied")
			}
		})
	}
}

func TestWindowGatherVectorAllTypes(t *testing.T) {
	types := []struct {
		name string
		typ  batch.TypeID
	}{
		{"Bool", batch.TypeBool},
		{"Int32", batch.TypeInt32},
		{"Int64", batch.TypeInt64},
		{"Float32", batch.TypeFloat32},
		{"Float64", batch.TypeFloat64},
		{"String", batch.TypeString},
		{"Decimal", batch.TypeDecimal},
	}

	perm := []int{2, 0, 1}
	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			src := batch.NewVector(tt.typ, 3)
			dst := batch.NewVector(tt.typ, 3)

			// Set some values and make index 1 null
			src.Nulls.SetNull(1)
			switch tt.typ {
			case batch.TypeBool:
				src.BoolData[0] = true
				src.BoolData[2] = false
			case batch.TypeInt32:
				src.Int32Data[0] = 10
				src.Int32Data[2] = 30
			case batch.TypeInt64:
				src.Int64Data[0] = 100
				src.Int64Data[2] = 300
			case batch.TypeFloat32:
				src.Float32Data[0] = 1.0
				src.Float32Data[2] = 3.0
			case batch.TypeFloat64:
				src.Float64Data[0] = 1.0
				src.Float64Data[2] = 3.0
			case batch.TypeString:
				src.BytesData.Set(0, []byte("a"))
				src.BytesData.Set(2, []byte("c"))
			case batch.TypeDecimal:
				src.DecimalData.Data[0] = batch.Int128{Lo: 10}
				src.DecimalData.Data[2] = batch.Int128{Lo: 30}
			}

			windowGatherVector(dst, src, perm)

			// After gather with perm [2,0,1]:
			// dst[0] = src[2], dst[1] = src[0], dst[2] = src[1] (null)
			if !dst.Nulls.IsNullFast(2) {
				t.Error("expected index 2 to be null after gather (from src[1])")
			}
		})
	}
}

func TestWindowSumNoOrder(t *testing.T) {
	// SUM without ORDER BY: entire partition gets same total
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 10.0},
		{"val": 20.0},
		{"val": 30.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinSum,
			InputCol:   "val",
			OutputCol:  "total",
			OutputType: parquet.TypeFloat64,
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, err := win.Next(ctx)
	if err != nil || b == nil {
		t.Fatal("expected result batch")
	}

	result := b.ToRows()
	for _, row := range result {
		total := row["total"].(float64)
		if total != 60.0 {
			t.Errorf("expected total=60.0 for all rows, got %f", total)
		}
	}
}

func TestWindowAvgRunning(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 10.0},
		{"val": 20.0},
		{"val": 30.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinAvg,
			InputCol:   "val",
			OutputCol:  "avg",
			OutputType: parquet.TypeFloat64,
			OrderBy:    []SortKey{{Column: "val", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	// Running avg: 10/1=10, 30/2=15, 60/3=20
	expected := []float64{10.0, 15.0, 20.0}
	for i, row := range result {
		avg := row["avg"].(float64)
		if math.Abs(avg-expected[i]) > 0.001 {
			t.Errorf("row %d: expected avg=%f, got %f", i, expected[i], avg)
		}
	}
}

func TestWindowAvgNoOrder(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 10.0},
		{"val": 20.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinAvg,
			InputCol:   "val",
			OutputCol:  "avg",
			OutputType: parquet.TypeFloat64,
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	for _, row := range result {
		avg := row["avg"].(float64)
		if avg != 15.0 {
			t.Errorf("expected avg=15.0, got %f", avg)
		}
	}
}

func TestWindowMinMax(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 30.0},
		{"val": 10.0},
		{"val": 20.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinMin,
			InputCol:   "val",
			OutputCol:  "min_val",
			OutputType: parquet.TypeFloat64,
		},
		{
			Func:       WinMax,
			InputCol:   "val",
			OutputCol:  "max_val",
			OutputType: parquet.TypeFloat64,
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	for _, row := range result {
		minV := row["min_val"].(float64)
		maxV := row["max_val"].(float64)
		if minV != 10.0 {
			t.Errorf("expected min=10.0, got %f", minV)
		}
		if maxV != 30.0 {
			t.Errorf("expected max=30.0, got %f", maxV)
		}
	}
}

func TestWindowMinMaxRunning(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 30.0},
		{"val": 10.0},
		{"val": 20.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinMin,
			InputCol:   "val",
			OutputCol:  "run_min",
			OutputType: parquet.TypeFloat64,
			OrderBy:    []SortKey{{Column: "val", Order: Ascending}},
		},
		{
			Func:       WinMax,
			InputCol:   "val",
			OutputCol:  "run_max",
			OutputType: parquet.TypeFloat64,
			OrderBy:    []SortKey{{Column: "val", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	// Sorted ASC: 10, 20, 30
	// Running min: 10, 10, 10
	// Running max: 10, 20, 30
	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}
}

func TestWindowLagLeadDefault(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 1.0},
		{"val": 2.0},
		{"val": 3.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:           WinLag,
			InputCol:       "val",
			OutputCol:      "lag_val",
			OutputType:     parquet.TypeFloat64,
			OrderBy:        []SortKey{{Column: "val", Order: Ascending}},
			LagLeadOffset:  1,
			LagLeadDefault: -1.0,
		},
		{
			Func:           WinLead,
			InputCol:       "val",
			OutputCol:      "lead_val",
			OutputType:     parquet.TypeFloat64,
			OrderBy:        []SortKey{{Column: "val", Order: Ascending}},
			LagLeadOffset:  1,
			LagLeadDefault: -1.0,
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	// LAG(val,1,-1) ordered ASC: [-1, 1, 2]
	// LEAD(val,1,-1) ordered ASC: [2, 3, -1]
	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}
	// First row lag should be -1.0 (default)
	if result[0]["lag_val"].(float64) != -1.0 {
		t.Errorf("expected first lag=-1.0, got %v", result[0]["lag_val"])
	}
	// Last row lead should be -1.0 (default)
	if result[2]["lead_val"].(float64) != -1.0 {
		t.Errorf("expected last lead=-1.0, got %v", result[2]["lead_val"])
	}
}

func TestWindowLagZeroOffset(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 1.0},
		{"val": 2.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:          WinLag,
			InputCol:      "val",
			OutputCol:     "lag_val",
			OutputType:    parquet.TypeFloat64,
			OrderBy:       []SortKey{{Column: "val", Order: Ascending}},
			LagLeadOffset: 0, // Should default to 1
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}
	b, _ := win.Next(ctx)
	if b == nil {
		t.Fatal("expected result batch")
	}
}

func TestWindowFirstValueNull(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	b := batch.NewRecordBatch(schema, 2)
	b.Columns[0].Nulls.SetNull(0)
	b.Columns[0].Float64Data[1] = 5.0

	win := NewWindow([]WindowColumn{
		{
			Func:       WinFirstValue,
			InputCol:   "val",
			OutputCol:  "first",
			OutputType: parquet.TypeFloat64,
		},
	})

	_ = win.Init(context.Background())
	_ = win.Consume(context.Background(), b)
	_ = win.Finalize(context.Background())

	out, _ := win.Next(context.Background())
	if out == nil {
		t.Fatal("expected output")
	}
	result := out.ToRows()
	// First value is null, so all rows should have null first_value
	for _, row := range result {
		if row["first"] != nil {
			t.Errorf("expected null first value, got %v", row["first"])
		}
	}
}

func TestWindowLastValueNoOrder(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 1.0},
		{"val": 2.0},
		{"val": 3.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinLastValue,
			InputCol:   "val",
			OutputCol:  "last_val",
			OutputType: parquet.TypeFloat64,
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	// Without ORDER BY, last value is the last value in partition
	for _, row := range result {
		v := row["last_val"].(float64)
		if v != 3.0 {
			t.Errorf("expected last_val=3.0, got %f", v)
		}
	}
}

func TestWindowLastValueWithOrder(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 1.0},
		{"val": 2.0},
		{"val": 3.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinLastValue,
			InputCol:   "val",
			OutputCol:  "last_val",
			OutputType: parquet.TypeFloat64,
			OrderBy:    []SortKey{{Column: "val", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	// With ORDER BY ASC, running last value = current row's value
	expected := []float64{1.0, 2.0, 3.0}
	for i, row := range result {
		v := row["last_val"].(float64)
		if v != expected[i] {
			t.Errorf("row %d: expected %f, got %f", i, expected[i], v)
		}
	}
}

func TestWindowNtileBucketDistribution(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 1.0},
		{"val": 2.0},
		{"val": 3.0},
		{"val": 4.0},
		{"val": 5.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:         WinNtile,
			OutputCol:    "ntile",
			OutputType:   parquet.TypeInt64,
			NtileBuckets: 3,
			OrderBy:      []SortKey{{Column: "val", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	// 5 rows, 3 buckets: sizes 2,2,1
	// Bucket 1: rows 0,1  Bucket 2: rows 2,3  Bucket 3: row 4
	expected := []int64{1, 1, 2, 2, 3}
	for i, row := range result {
		ntile := row["ntile"].(int64)
		if ntile != expected[i] {
			t.Errorf("row %d: expected ntile=%d, got %d", i, expected[i], ntile)
		}
	}
}

func TestWindowNtileZeroBuckets(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 1.0},
		{"val": 2.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:         WinNtile,
			OutputCol:    "ntile",
			OutputType:   parquet.TypeInt64,
			NtileBuckets: 0, // defaults to 1
			OrderBy:      []SortKey{{Column: "val", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	for _, row := range result {
		ntile := row["ntile"].(int64)
		if ntile != 1 {
			t.Errorf("expected ntile=1 with default 1 bucket, got %d", ntile)
		}
	}
}

func TestWindowPercentRankCoverage(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 10.0},
		{"val": 20.0},
		{"val": 30.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinPercentRank,
			OutputCol:  "pr",
			OutputType: parquet.TypeFloat64,
			OrderBy:    []SortKey{{Column: "val", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	// PERCENT_RANK = (rank-1)/(n-1)
	// Row 0: (1-1)/(3-1) = 0.0
	// Row 1: (2-1)/(3-1) = 0.5
	// Row 2: (3-1)/(3-1) = 1.0
	expected := []float64{0.0, 0.5, 1.0}
	for i, row := range result {
		pr := row["pr"].(float64)
		if math.Abs(pr-expected[i]) > 0.001 {
			t.Errorf("row %d: expected pr=%f, got %f", i, expected[i], pr)
		}
	}
}

func TestWindowPercentRankSingleRow(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 10.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinPercentRank,
			OutputCol:  "pr",
			OutputType: parquet.TypeFloat64,
			OrderBy:    []SortKey{{Column: "val", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	if result[0]["pr"].(float64) != 0.0 {
		t.Errorf("expected percent_rank=0 for single row, got %v", result[0]["pr"])
	}
}

func TestWindowCumeDistCoverage(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 10.0},
		{"val": 10.0}, // tie
		{"val": 20.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinCumeDist,
			OutputCol:  "cd",
			OutputType: parquet.TypeFloat64,
			OrderBy:    []SortKey{{Column: "val", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	// CUME_DIST: 2/3 for first two (tie), 3/3=1.0 for last
	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}
}

func TestWindowNthValueCoverage(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 10.0},
		{"val": 20.0},
		{"val": 30.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinNthValue,
			InputCol:   "val",
			OutputCol:  "nth",
			OutputType: parquet.TypeFloat64,
			NthValueN:  2,
			OrderBy:    []SortKey{{Column: "val", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	// NTH_VALUE(val, 2) = 20.0 for all rows
	for _, row := range result {
		nth := row["nth"].(float64)
		if nth != 20.0 {
			t.Errorf("expected nth=20.0, got %f", nth)
		}
	}
}

func TestWindowNthValueOutOfRange(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 10.0},
		{"val": 20.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinNthValue,
			InputCol:   "val",
			OutputCol:  "nth",
			OutputType: parquet.TypeFloat64,
			NthValueN:  5, // out of range
			OrderBy:    []SortKey{{Column: "val", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	for _, row := range result {
		if row["nth"] != nil {
			t.Errorf("expected nil for out-of-range NTH_VALUE, got %v", row["nth"])
		}
	}
}

func TestWindowNthValueZero(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 10.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinNthValue,
			InputCol:   "val",
			OutputCol:  "nth",
			OutputType: parquet.TypeFloat64,
			NthValueN:  0, // defaults to 1
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	if result[0]["nth"].(float64) != 10.0 {
		t.Errorf("expected 10.0, got %v", result[0]["nth"])
	}
}

func TestWindowRunningCount(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 1.0},
		{"val": 2.0},
		{"val": 3.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinCount,
			OutputCol:  "cnt",
			OutputType: parquet.TypeInt64,
			OrderBy:    []SortKey{{Column: "val", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	// Running count: 1, 2, 3
	expected := []int64{1, 2, 3}
	for i, row := range result {
		cnt := row["cnt"].(int64)
		if cnt != expected[i] {
			t.Errorf("row %d: expected cnt=%d, got %d", i, expected[i], cnt)
		}
	}
}

func TestWindowNoSortKeys(t *testing.T) {
	// Window function with no partition by and no order by
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"val": 1.0},
		{"val": 2.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinRowNumber,
			OutputCol:  "rn",
			OutputType: parquet.TypeInt64,
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
}

func TestWindowConcatBatchesEmpty(t *testing.T) {
	result := windowConcatBatches(nil, nil)
	if result != nil {
		t.Fatal("expected nil for empty batches")
	}
}
