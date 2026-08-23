package exec

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestSerializeKey(t *testing.T) {
	buf := make([]byte, 0, 64)

	tests := []struct {
		vals []any
		want string
	}{
		{nil, ""},
		{[]any{int64(42)}, "42"},
		{[]any{"hello"}, "hello"},
		{[]any{nil}, "<null>"},
		{[]any{int64(1), "two"}, "1\x00two"},
	}

	for _, tt := range tests {
		got := serializeKey(buf, tt.vals)
		if got != tt.want {
			t.Fatalf("serializeKey(%v) = %q, want %q", tt.vals, got, tt.want)
		}
	}
}

func TestComputePercentileCont(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		if computePercentileCont(nil, 0.5) != nil {
			t.Fatal("expected nil for empty")
		}
	})

	t.Run("p=0", func(t *testing.T) {
		result := computePercentileCont([]float64{10, 20, 30}, 0)
		if result.(float64) != 10 {
			t.Fatalf("expected 10, got %v", result)
		}
	})

	t.Run("p=1", func(t *testing.T) {
		result := computePercentileCont([]float64{10, 20, 30}, 1)
		if result.(float64) != 30 {
			t.Fatalf("expected 30, got %v", result)
		}
	})

	t.Run("p=0.5 odd", func(t *testing.T) {
		result := computePercentileCont([]float64{10, 20, 30}, 0.5)
		if result.(float64) != 20 {
			t.Fatalf("expected 20, got %v", result)
		}
	})

	t.Run("p=0.5 even", func(t *testing.T) {
		result := computePercentileCont([]float64{10, 20, 30, 40}, 0.5)
		if result.(float64) != 25 {
			t.Fatalf("expected 25, got %v", result)
		}
	})

	t.Run("p=0.25", func(t *testing.T) {
		result := computePercentileCont([]float64{10, 20, 30, 40}, 0.25)
		if result.(float64) != 17.5 {
			t.Fatalf("expected 17.5, got %v", result)
		}
	})
}

func TestComputePercentileDisc(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		if computePercentileDisc(nil, 0.5) != nil {
			t.Fatal("expected nil for empty")
		}
	})

	t.Run("p=0", func(t *testing.T) {
		result := computePercentileDisc([]float64{10, 20, 30}, 0)
		if result.(float64) != 10 {
			t.Fatalf("expected 10, got %v", result)
		}
	})

	t.Run("p=1", func(t *testing.T) {
		result := computePercentileDisc([]float64{10, 20, 30}, 1)
		if result.(float64) != 30 {
			t.Fatalf("expected 30, got %v", result)
		}
	})

	t.Run("p=0.5", func(t *testing.T) {
		result := computePercentileDisc([]float64{10, 20, 30}, 0.5)
		// ceil(0.5*3) - 1 = 1; values[1] = 20
		if result.(float64) != 20 {
			t.Fatalf("expected 20, got %v", result)
		}
	})
}

func TestComputeMode(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		if computeMode(nil) != nil {
			t.Fatal("expected nil for empty")
		}
	})

	t.Run("single", func(t *testing.T) {
		result := computeMode([]float64{42})
		if result.(float64) != 42 {
			t.Fatalf("expected 42, got %v", result)
		}
	})

	t.Run("mode", func(t *testing.T) {
		result := computeMode([]float64{1, 2, 2, 3, 3, 3})
		if result.(float64) != 3 {
			t.Fatalf("expected 3 (mode), got %v", result)
		}
	})
}

func TestVecToFloat64(t *testing.T) {
	t.Run("Int64", func(t *testing.T) {
		v := batch.NewVector(batch.TypeInt64, 1)
		v.Int64Data[0] = 42
		if vecToFloat64(v, 0) != 42.0 {
			t.Fatal("expected 42.0")
		}
	})

	t.Run("Int32", func(t *testing.T) {
		v := batch.NewVector(batch.TypeInt32, 1)
		v.Int32Data[0] = 10
		if vecToFloat64(v, 0) != 10.0 {
			t.Fatal("expected 10.0")
		}
	})

	t.Run("Float64", func(t *testing.T) {
		v := batch.NewVector(batch.TypeFloat64, 1)
		v.Float64Data[0] = 3.14
		if vecToFloat64(v, 0) != 3.14 {
			t.Fatal("expected 3.14")
		}
	})

	t.Run("Float32", func(t *testing.T) {
		v := batch.NewVector(batch.TypeFloat32, 1)
		v.Float32Data[0] = 2.5
		if vecToFloat64(v, 0) != 2.5 {
			t.Fatal("expected 2.5")
		}
	})

	t.Run("Decimal", func(t *testing.T) {
		v := batch.NewVectorWithScale(batch.TypeDecimal, 1, 2)
		v.DecimalData.Data[0] = batch.Int128From(1000)
		if vecToFloat64(v, 0) != 10.0 {
			t.Fatal("expected 10.0")
		}
	})

	t.Run("Unsupported", func(t *testing.T) {
		v := batch.NewVector(batch.TypeString, 1)
		v.BytesData.Set(0, []byte("x"))
		if vecToFloat64(v, 0) != 0 {
			t.Fatal("expected 0 for unsupported type")
		}
	})
}

func TestAppendColumnValueTypes(t *testing.T) {
	t.Run("Int32", func(t *testing.T) {
		v := batch.NewVector(batch.TypeInt32, 1)
		v.Int32Data[0] = 42
		buf := appendColumnValue(nil, v, 0, batch.TypeInt32)
		// Binary encoding: 4 bytes little-endian
		expected := []byte{42, 0, 0, 0}
		if !bytesEqual(buf, expected) {
			t.Fatalf("expected %v, got %v", expected, buf)
		}
	})

	t.Run("Float32", func(t *testing.T) {
		v := batch.NewVector(batch.TypeFloat32, 1)
		v.Float32Data[0] = 2.5
		buf := appendColumnValue(nil, v, 0, batch.TypeFloat32)
		if len(buf) != 4 {
			t.Fatalf("expected 4 bytes, got %d", len(buf))
		}
	})

	t.Run("Bool_true", func(t *testing.T) {
		v := batch.NewVector(batch.TypeBool, 1)
		v.BoolData[0] = true
		buf := appendColumnValue(nil, v, 0, batch.TypeBool)
		if len(buf) != 1 || buf[0] != 1 {
			t.Fatalf("expected [1], got %v", buf)
		}
	})

	t.Run("Bool_false", func(t *testing.T) {
		v := batch.NewVector(batch.TypeBool, 1)
		v.BoolData[0] = false
		buf := appendColumnValue(nil, v, 0, batch.TypeBool)
		if len(buf) != 1 || buf[0] != 0 {
			t.Fatalf("expected [0], got %v", buf)
		}
	})

	t.Run("Decimal", func(t *testing.T) {
		v := batch.NewVectorWithScale(batch.TypeDecimal, 1, 2)
		v.DecimalData.Data[0] = batch.Int128From(1000)
		buf := appendColumnValue(nil, v, 0, batch.TypeDecimal)
		if len(buf) != 8 {
			t.Fatalf("expected 8 bytes, got %d", len(buf))
		}
	})

	t.Run("Unknown", func(t *testing.T) {
		// Not reachable for any of the 22 column types since #408 — every one
		// has an arm. The marker is deliberately long and self-describing:
		// the '?' it replaced was a plausible-looking single byte that
		// silently made every ARRAY/ROW/MAP/VECTOR value equal.
		v := batch.NewVector(batch.TypeBool, 1) // doesn't matter
		buf := appendColumnValue(nil, v, 0, batch.TypeID(99))
		if string(buf) != "\x00unsupported-key-type" {
			t.Fatalf("expected the unsupported-type marker, got %q", string(buf))
		}
	})

	t.Run("Roundtrip_distinct_keys", func(t *testing.T) {
		// Verify different values produce different keys
		v := batch.NewVector(batch.TypeInt64, 2)
		v.Int64Data[0] = 100
		v.Int64Data[1] = 200
		buf1 := appendColumnValue(nil, v, 0, batch.TypeInt64)
		buf2 := appendColumnValue(nil, v, 1, batch.TypeInt64)
		if bytesEqual(buf1, buf2) {
			t.Fatal("different values should produce different keys")
		}
	})
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestVarianceState(t *testing.T) {
	t.Run("Pop_empty", func(t *testing.T) {
		v := &varianceState{}
		if v.variancePop() != 0 {
			t.Fatal("expected 0 for empty")
		}
	})

	t.Run("Samp_one", func(t *testing.T) {
		v := &varianceState{}
		v.update(10.0)
		if v.varianceSamp() != 0 {
			t.Fatal("expected 0 for n<2")
		}
	})

	t.Run("Values", func(t *testing.T) {
		v := &varianceState{}
		values := []float64{2, 4, 4, 4, 5, 5, 7, 9}
		for _, x := range values {
			v.update(x)
		}
		// Population variance of these values = 4.0
		pop := v.variancePop()
		if math.Abs(pop-4.0) > 0.001 {
			t.Fatalf("expected pop variance ~4.0, got %f", pop)
		}
	})
}

func TestCovarianceState(t *testing.T) {
	t.Run("Pop_empty", func(t *testing.T) {
		s := &covarianceState{}
		if s.covarPop() != 0 {
			t.Fatal("expected 0 for empty")
		}
	})

	t.Run("Samp_one", func(t *testing.T) {
		s := &covarianceState{}
		s.update(1, 2)
		if s.covarSamp() != 0 {
			t.Fatal("expected 0 for n<2")
		}
	})

	t.Run("Corr_zero_variance", func(t *testing.T) {
		s := &covarianceState{}
		s.update(5, 1)
		s.update(5, 2) // x constant => m2x = 0
		if s.correlation() != 0 {
			t.Fatal("expected 0 for zero variance")
		}
	})

	t.Run("Corr_perfect", func(t *testing.T) {
		s := &covarianceState{}
		// Perfect positive correlation
		for i := 1.0; i <= 10; i++ {
			s.update(i, i*2)
		}
		corr := s.correlation()
		if math.Abs(corr-1.0) > 0.001 {
			t.Fatalf("expected correlation ~1.0, got %f", corr)
		}
	})
}

func TestLimitClose(t *testing.T) {
	l := NewLimit(10, 0)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

// Test Hash Aggregate with aggregates that use different types

func TestHashAggregateStringAgg(t *testing.T) {
	schema := []parquet.Column{
		{Name: "group", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
	}
	rows := []map[string]any{
		{"group": "a", "name": "alice"},
		{"group": "a", "name": "bob"},
		{"group": "b", "name": "carol"},
	}

	agg := NewHashAggregate([]string{"group"}, []AggColumn{
		{Func: AggStringAgg, InputCol: "name", OutputCol: "names", OutputType: parquet.TypeString, Separator: ","},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(context.Background())
	result := b.ToRows()
	for _, row := range result {
		if row["group"] == "a" {
			names := row["names"].(string)
			if names != "alice,bob" {
				t.Fatalf("expected alice,bob, got %q", names)
			}
		}
	}
}

func TestHashAggregateBoolAgg(t *testing.T) {
	schema := []parquet.Column{
		{Name: "group", Type: parquet.TypeString},
		{Name: "flag", Type: parquet.TypeBool},
	}
	rows := []map[string]any{
		{"group": "a", "flag": true},
		{"group": "a", "flag": true},
		{"group": "b", "flag": true},
		{"group": "b", "flag": false},
	}

	agg := NewHashAggregate([]string{"group"}, []AggColumn{
		{Func: AggBoolAnd, InputCol: "flag", OutputCol: "all_true", OutputType: parquet.TypeBool},
		{Func: AggBoolOr, InputCol: "flag", OutputCol: "any_true", OutputType: parquet.TypeBool},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(context.Background())
	result := b.ToRows()
	for _, row := range result {
		if row["group"] == "a" {
			if row["all_true"] != true {
				t.Fatalf("group a: expected all_true=true, got %v", row["all_true"])
			}
		}
		if row["group"] == "b" {
			if row["all_true"] != false {
				t.Fatalf("group b: expected all_true=false, got %v", row["all_true"])
			}
			if row["any_true"] != true {
				t.Fatalf("group b: expected any_true=true, got %v", row["any_true"])
			}
		}
	}
}

func TestHashAggregateVariance(t *testing.T) {
	schema := []parquet.Column{
		{Name: "group", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"group": "a", "val": 2.0},
		{"group": "a", "val": 4.0},
		{"group": "a", "val": 4.0},
		{"group": "a", "val": 4.0},
		{"group": "a", "val": 5.0},
		{"group": "a", "val": 5.0},
		{"group": "a", "val": 7.0},
		{"group": "a", "val": 9.0},
	}

	agg := NewHashAggregate([]string{"group"}, []AggColumn{
		{Func: AggVarPop, InputCol: "val", OutputCol: "var_pop", OutputType: parquet.TypeFloat64},
		{Func: AggStddevPop, InputCol: "val", OutputCol: "stddev_pop", OutputType: parquet.TypeFloat64},
		{Func: AggVariance, InputCol: "val", OutputCol: "var_samp", OutputType: parquet.TypeFloat64},
		{Func: AggStddev, InputCol: "val", OutputCol: "stddev_samp", OutputType: parquet.TypeFloat64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(context.Background())
	result := b.ToRows()
	if len(result) != 1 {
		t.Fatalf("expected 1 group, got %d", len(result))
	}

	varPop := result[0]["var_pop"].(float64)
	if math.Abs(varPop-4.0) > 0.001 {
		t.Fatalf("expected var_pop ~4.0, got %f", varPop)
	}

	stdPop := result[0]["stddev_pop"].(float64)
	if math.Abs(stdPop-2.0) > 0.001 {
		t.Fatalf("expected stddev_pop ~2.0, got %f", stdPop)
	}
}

func TestHashAggregatePercentile(t *testing.T) {
	schema := []parquet.Column{
		{Name: "group", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"group": "a", "val": 10.0},
		{"group": "a", "val": 20.0},
		{"group": "a", "val": 30.0},
		{"group": "a", "val": 40.0},
	}

	agg := NewHashAggregate([]string{"group"}, []AggColumn{
		{Func: AggPercentileCont, InputCol: "val", OutputCol: "p50", OutputType: parquet.TypeFloat64, Percentile: 0.5},
		{Func: AggPercentileDisc, InputCol: "val", OutputCol: "p50d", OutputType: parquet.TypeFloat64, Percentile: 0.5},
		{Func: AggMedian, InputCol: "val", OutputCol: "median", OutputType: parquet.TypeFloat64},
		{Func: AggMode, InputCol: "val", OutputCol: "mode", OutputType: parquet.TypeFloat64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(context.Background())
	result := b.ToRows()
	if len(result) != 1 {
		t.Fatalf("expected 1 group, got %d", len(result))
	}
	p50 := result[0]["p50"].(float64)
	if p50 != 25.0 {
		t.Fatalf("expected p50=25.0, got %v", p50)
	}
}

func TestHashAggregateCorrelation(t *testing.T) {
	schema := []parquet.Column{
		{Name: "group", Type: parquet.TypeString},
		{Name: "x", Type: parquet.TypeFloat64},
		{Name: "y", Type: parquet.TypeFloat64},
	}
	var rows []map[string]any
	for i := 1; i <= 10; i++ {
		rows = append(rows, map[string]any{
			"group": "a",
			"x":     float64(i),
			"y":     float64(i * 2),
		})
	}

	agg := NewHashAggregate([]string{"group"}, []AggColumn{
		{Func: AggCorr, InputCol: "x", InputCol2: "y", OutputCol: "corr", OutputType: parquet.TypeFloat64},
		{Func: AggCovarSamp, InputCol: "x", InputCol2: "y", OutputCol: "cov_s", OutputType: parquet.TypeFloat64},
		{Func: AggCovarPop, InputCol: "x", InputCol2: "y", OutputCol: "cov_p", OutputType: parquet.TypeFloat64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(context.Background())
	result := b.ToRows()
	if len(result) != 1 {
		t.Fatalf("expected 1 group, got %d", len(result))
	}

	corr := result[0]["corr"].(float64)
	if math.Abs(corr-1.0) > 0.001 {
		t.Fatalf("expected corr ~1.0, got %f", corr)
	}
}

// TestHashAggregateMergeSinkInheritsGroupSchema is a regression test for the
// Q20 bug where the parent HashAggregate (created via CloneSink and never
// fed a batch — happens when runParallel's warmup batch is fully filtered
// out) had empty groupColTypes. outputSchema then defaults every GROUP BY
// column to TypeString, the runtime stores int64 group keys as their decimal
// string form, and any downstream filter (HAVING, projection equality) fails
// to match the expected typed value.
func TestHashAggregateMergeSinkInheritsGroupSchema(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"k": int32(1), "v": int64(10)},
		{"k": int32(1), "v": int64(20)},
	}

	parent := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	parent.Init(context.Background())

	worker := parent.CloneSink().(*HashAggregate)
	worker.Init(context.Background())
	if err := worker.Consume(context.Background(), batch.FromRows(schema, rows)); err != nil {
		t.Fatal(err)
	}

	parent.MergeSink(worker)

	// Parent must have inherited the worker's typed group schema; otherwise
	// the output column would be TypeString and the int64 key would render
	// as "1".
	out, _ := parent.Next(context.Background())
	if out == nil {
		t.Fatal("expected non-nil batch")
	}
	got := out.ToRows()
	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	if k, ok := got[0]["k"].(int32); !ok || k != 1 {
		t.Errorf("k: got %v (%T), want int32(1)", got[0]["k"], got[0]["k"])
	}
	if s, ok := got[0]["s"].(int64); !ok || s != 30 {
		t.Errorf("s: got %v (%T), want int64(30)", got[0]["s"], got[0]["s"])
	}
}

// TestHashAggregateMergeSinkDistinctSets is a regression test for the Q16
// bug where COUNT(DISTINCT col) under-counted when a group was split across
// parallel workers. MergeSink merged accs but ignored gs.distinctSets, so
// each parallel worker's partial set of distinct values was discarded.
func TestHashAggregateMergeSinkDistinctSets(t *testing.T) {
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeInt64},
	}

	parent := NewHashAggregate([]string{"g"}, []AggColumn{
		{Func: AggCountDistinct, InputCol: "v", OutputCol: "n", OutputType: parquet.TypeInt64},
	})
	parent.Init(context.Background())

	w1 := parent.CloneSink().(*HashAggregate)
	w1.Init(context.Background())
	if err := w1.Consume(context.Background(), batch.FromRows(schema, []map[string]any{
		{"g": "a", "v": int64(1)},
		{"g": "a", "v": int64(2)},
	})); err != nil {
		t.Fatal(err)
	}

	w2 := parent.CloneSink().(*HashAggregate)
	w2.Init(context.Background())
	if err := w2.Consume(context.Background(), batch.FromRows(schema, []map[string]any{
		{"g": "a", "v": int64(2)}, // overlap with w1
		{"g": "a", "v": int64(3)},
		{"g": "a", "v": int64(4)},
	})); err != nil {
		t.Fatal(err)
	}

	parent.MergeSink(w1)
	parent.MergeSink(w2)

	out, _ := parent.Next(context.Background())
	if out == nil {
		t.Fatal("expected non-nil batch")
	}
	got := out.ToRows()
	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	// Distinct values across workers: {1, 2, 3, 4} → 4
	if n, ok := got[0]["n"].(int64); !ok || n != 4 {
		t.Errorf("count(distinct): got %v (%T), want int64(4)", got[0]["n"], got[0]["n"])
	}
}

// TestHashAggregateScalarMergeSink is a regression test for the bug where
// MergeSink ignored partial-worker scalar accumulators when the parent
// HashAggregate (created by CloneSink and never fed a batch directly) had
// not yet resolved as scalar. Symptom: scalar aggregates over filtered input
// silently returned the identity value (0 for COUNT/SUM) when the planner
// ran the aggregate in cloned-worker form. Originally surfaced by Q06/Q14/
// Q19 returning revenue=0 against DuckDB-shaped SF0.01 data.
func TestHashAggregateScalarMergeSink(t *testing.T) {
	schema := []parquet.Column{
		{Name: "v", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"v": int64(1)}, {"v": int64(2)}, {"v": int64(3)},
	}

	// Parent aggregate — what the coordinator holds. Receives merges only.
	parent := NewHashAggregate(nil, []AggColumn{
		{Func: AggCount, InputCol: "", OutputCol: "cnt", OutputType: parquet.TypeInt64},
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	parent.Init(context.Background())

	// Two cloned workers, each consumes one batch.
	workers := []*HashAggregate{
		parent.CloneSink().(*HashAggregate),
		parent.CloneSink().(*HashAggregate),
	}
	for _, w := range workers {
		w.Init(context.Background())
		b := batch.FromRows(schema, rows)
		if err := w.Consume(context.Background(), b); err != nil {
			t.Fatalf("worker consume: %v", err)
		}
	}

	for _, w := range workers {
		parent.MergeSink(w)
	}

	out, _ := parent.Next(context.Background())
	if out == nil {
		t.Fatal("expected non-nil output batch")
	}
	got := out.ToRows()
	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	wantCount := int64(2 * len(rows))    // 2 workers × 3 rows = 6
	wantSum := int64(2 * (1 + 2 + 3))    // 2 workers × sum(1+2+3) = 12
	if got[0]["cnt"].(int64) != wantCount {
		t.Errorf("cnt: got %v, want %d", got[0]["cnt"], wantCount)
	}
	if got[0]["s"].(int64) != wantSum {
		t.Errorf("s: got %v, want %d", got[0]["s"], wantSum)
	}
}

func TestHashAggregateCountStar(t *testing.T) {
	schema := []parquet.Column{
		{Name: "group", Type: parquet.TypeString},
	}
	rows := []map[string]any{
		{"group": "a"},
		{"group": "a"},
		{"group": "b"},
	}

	agg := NewHashAggregate([]string{"group"}, []AggColumn{
		{Func: AggCount, InputCol: "", OutputCol: "cnt", OutputType: parquet.TypeInt64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(context.Background())
	result := b.ToRows()
	for _, row := range result {
		if row["group"] == "a" && row["cnt"].(int64) != 2 {
			t.Fatalf("expected cnt=2 for a, got %v", row["cnt"])
		}
	}
}

func TestHashAggregateMinBy(t *testing.T) {
	schema := []parquet.Column{
		{Name: "group", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
		{Name: "score", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"group": "a", "name": "alice", "score": 30.0},
		{"group": "a", "name": "bob", "score": 10.0},
		{"group": "a", "name": "carol", "score": 20.0},
	}

	agg := NewHashAggregate([]string{"group"}, []AggColumn{
		{Func: AggMinBy, InputCol: "name", InputCol2: "score", OutputCol: "min_name", OutputType: parquet.TypeString},
		{Func: AggMaxBy, InputCol: "name", InputCol2: "score", OutputCol: "max_name", OutputType: parquet.TypeString},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(context.Background())
	result := b.ToRows()
	if result[0]["min_name"] != "bob" {
		t.Fatalf("expected min_name=bob, got %v", result[0]["min_name"])
	}
	if result[0]["max_name"] != "alice" {
		t.Fatalf("expected max_name=alice, got %v", result[0]["max_name"])
	}
}

func TestHashAggregateApproxDistinct(t *testing.T) {
	schema := []parquet.Column{
		{Name: "group", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeString},
	}
	rows := []map[string]any{
		{"group": "a", "val": "x"},
		{"group": "a", "val": "y"},
		{"group": "a", "val": "x"},
	}

	agg := NewHashAggregate([]string{"group"}, []AggColumn{
		{Func: AggApproxDistinct, InputCol: "val", OutputCol: "approx", OutputType: parquet.TypeInt64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(context.Background())
	result := b.ToRows()
	if result[0]["approx"].(int64) != 2 {
		t.Fatalf("expected 2, got %v", result[0]["approx"])
	}
}

func TestHashAggregateWithTableQualifiedColumn(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"val": int64(1)},
		{"val": int64(2)},
		{"val": int64(3)},
	}

	// Use table.column notation for group-by
	agg := NewHashAggregate([]string{"t.val"}, []AggColumn{
		{Func: AggCount, InputCol: "", OutputCol: "cnt", OutputType: parquet.TypeInt64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(context.Background())
	if b == nil {
		t.Fatal("expected non-nil batch")
	}
}

func TestSortWithSelectionVector(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"val": int64(30)},
		{"val": int64(10)},
		{"val": int64(20)},
		{"val": int64(40)},
	})
	b.Sel = []uint32{0, 2, 3} // 30, 20, 40

	s := NewSort([]SortKey{{Column: "val", Order: Ascending}})
	s.Init(context.Background())
	s.Consume(context.Background(), b)
	s.Finalize(context.Background())

	result, _ := s.Next(context.Background())
	if result == nil {
		t.Fatal("expected non-nil")
	}
	rows := result.ToRows()
	if len(rows) != 3 {
		t.Fatalf("expected 3, got %d", len(rows))
	}
	if rows[0]["val"].(int64) != 20 {
		t.Fatalf("expected first=20, got %v", rows[0]["val"])
	}
}

func TestSortNullsLast(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"val": nil},
		{"val": int64(20)},
		{"val": int64(10)},
	}

	s := NewSort([]SortKey{{Column: "val", Order: Ascending, NullsLast: true}})
	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: s}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, _ := s.Next(context.Background())
	result := b.ToRows()
	// Nulls should be last
	if result[0]["val"].(int64) != 10 {
		t.Fatalf("expected first=10, got %v", result[0]["val"])
	}
	if result[2]["val"] != nil {
		t.Fatalf("expected last=nil, got %v", result[2]["val"])
	}
}
