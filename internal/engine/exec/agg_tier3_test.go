package exec

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func TestTier3ApproxDistinct(t *testing.T) {
	schema := []parquet.Column{
		{Name: "grp", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeString},
	}
	rows := []map[string]any{
		{"grp": "a", "val": "x"},
		{"grp": "a", "val": "y"},
		{"grp": "a", "val": "x"},
		{"grp": "a", "val": "z"},
		{"grp": "b", "val": "p"},
		{"grp": "b", "val": "p"},
	}

	agg := NewHashAggregate([]string{"grp"}, []AggColumn{
		{Func: AggApproxDistinct, InputCol: "val", OutputCol: "approx_cnt", OutputType: parquet.TypeInt64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := agg.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	b, err := agg.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	result := b.ToRows()
	if len(result) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(result))
	}

	for _, row := range result {
		grp := row["grp"].(string)
		cnt := row["approx_cnt"].(int64)
		switch grp {
		case "a":
			if cnt != 3 {
				t.Errorf("approx_distinct for grp=a: got %d, want 3", cnt)
			}
		case "b":
			if cnt != 1 {
				t.Errorf("approx_distinct for grp=b: got %d, want 1", cnt)
			}
		}
	}
}

func TestTier3Corr(t *testing.T) {
	schema := []parquet.Column{
		{Name: "y", Type: parquet.TypeFloat64},
		{Name: "x", Type: parquet.TypeFloat64},
	}
	// Perfect positive correlation: y = 2*x
	rows := []map[string]any{
		{"y": 2.0, "x": 1.0},
		{"y": 4.0, "x": 2.0},
		{"y": 6.0, "x": 3.0},
		{"y": 8.0, "x": 4.0},
		{"y": 10.0, "x": 5.0},
	}

	agg := NewHashAggregate(nil, []AggColumn{
		{Func: AggCorr, InputCol: "y", InputCol2: "x", OutputCol: "correlation", OutputType: parquet.TypeFloat64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := agg.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(ctx)
	result := b.ToRows()
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}

	corr := result[0]["correlation"].(float64)
	if math.Abs(corr-1.0) > 0.001 {
		t.Errorf("corr = %v, want 1.0 (perfect positive correlation)", corr)
	}
}

func TestTier3CovarSampPop(t *testing.T) {
	schema := []parquet.Column{
		{Name: "y", Type: parquet.TypeFloat64},
		{Name: "x", Type: parquet.TypeFloat64},
	}
	// x = [1, 2, 3, 4, 5], y = [2, 4, 5, 4, 5]
	// mean_x = 3, mean_y = 4
	// cov_pop = sum((xi-mx)(yi-my))/n = ((1-3)(2-4)+(2-3)(4-4)+(3-3)(5-4)+(4-3)(4-4)+(5-3)(5-4))/5
	//         = (4 + 0 + 0 + 0 + 2)/5 = 6/5 = 1.2
	// cov_samp = sum((xi-mx)(yi-my))/(n-1) = 6/4 = 1.5
	rows := []map[string]any{
		{"y": 2.0, "x": 1.0},
		{"y": 4.0, "x": 2.0},
		{"y": 5.0, "x": 3.0},
		{"y": 4.0, "x": 4.0},
		{"y": 5.0, "x": 5.0},
	}

	agg := NewHashAggregate(nil, []AggColumn{
		{Func: AggCovarSamp, InputCol: "y", InputCol2: "x", OutputCol: "cov_samp", OutputType: parquet.TypeFloat64},
		{Func: AggCovarPop, InputCol: "y", InputCol2: "x", OutputCol: "cov_pop", OutputType: parquet.TypeFloat64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := agg.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(ctx)
	result := b.ToRows()
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}

	row := result[0]

	covSamp := row["cov_samp"].(float64)
	if math.Abs(covSamp-1.5) > 0.01 {
		t.Errorf("covar_samp = %v, want 1.5", covSamp)
	}

	covPop := row["cov_pop"].(float64)
	if math.Abs(covPop-1.2) > 0.01 {
		t.Errorf("covar_pop = %v, want 1.2", covPop)
	}
}

func TestTier3PercentileCont(t *testing.T) {
	schema := []parquet.Column{
		{Name: "value", Type: parquet.TypeFloat64},
	}
	// Values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	rows := make([]map[string]any, 10)
	for i := 0; i < 10; i++ {
		rows[i] = map[string]any{"value": float64(i + 1)}
	}

	agg := NewHashAggregate(nil, []AggColumn{
		{Func: AggPercentileCont, InputCol: "value", OutputCol: "p25", OutputType: parquet.TypeFloat64, Percentile: 0.25},
		{Func: AggPercentileCont, InputCol: "value", OutputCol: "p50", OutputType: parquet.TypeFloat64, Percentile: 0.5},
		{Func: AggPercentileCont, InputCol: "value", OutputCol: "p75", OutputType: parquet.TypeFloat64, Percentile: 0.75},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := agg.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(ctx)
	result := b.ToRows()
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}

	row := result[0]

	// percentile_cont with interpolation on 1..10:
	// p25: index = 0.25 * 9 = 2.25 -> 3*(1-0.25) + 4*0.25 = 3.25
	p25 := row["p25"].(float64)
	if math.Abs(p25-3.25) > 0.01 {
		t.Errorf("percentile_cont(0.25) = %v, want 3.25", p25)
	}

	// p50: index = 0.5 * 9 = 4.5 -> 5*(1-0.5) + 6*0.5 = 5.5
	p50 := row["p50"].(float64)
	if math.Abs(p50-5.5) > 0.01 {
		t.Errorf("percentile_cont(0.5) = %v, want 5.5", p50)
	}

	// p75: index = 0.75 * 9 = 6.75 -> 7*(1-0.75) + 8*0.75 = 7.75
	p75 := row["p75"].(float64)
	if math.Abs(p75-7.75) > 0.01 {
		t.Errorf("percentile_cont(0.75) = %v, want 7.75", p75)
	}
}

func TestTier3PercentileDisc(t *testing.T) {
	schema := []parquet.Column{
		{Name: "value", Type: parquet.TypeFloat64},
	}
	// Values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	rows := make([]map[string]any, 10)
	for i := 0; i < 10; i++ {
		rows[i] = map[string]any{"value": float64(i + 1)}
	}

	agg := NewHashAggregate(nil, []AggColumn{
		{Func: AggPercentileDisc, InputCol: "value", OutputCol: "p25", OutputType: parquet.TypeFloat64, Percentile: 0.25},
		{Func: AggPercentileDisc, InputCol: "value", OutputCol: "p50", OutputType: parquet.TypeFloat64, Percentile: 0.5},
		{Func: AggPercentileDisc, InputCol: "value", OutputCol: "p90", OutputType: parquet.TypeFloat64, Percentile: 0.9},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := agg.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(ctx)
	result := b.ToRows()
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}

	row := result[0]

	// percentile_disc uses nearest rank: ceil(p * n) - 1
	// p25: ceil(0.25 * 10) - 1 = 3 - 1 = 2 -> value 3
	p25 := row["p25"].(float64)
	if math.Abs(p25-3.0) > 0.01 {
		t.Errorf("percentile_disc(0.25) = %v, want 3.0", p25)
	}

	// p50: ceil(0.5 * 10) - 1 = 5 - 1 = 4 -> value 5
	p50 := row["p50"].(float64)
	if math.Abs(p50-5.0) > 0.01 {
		t.Errorf("percentile_disc(0.5) = %v, want 5.0", p50)
	}

	// p90: ceil(0.9 * 10) - 1 = 9 - 1 = 8 -> value 9
	p90 := row["p90"].(float64)
	if math.Abs(p90-9.0) > 0.01 {
		t.Errorf("percentile_disc(0.9) = %v, want 9.0", p90)
	}
}

func TestTier3Mode(t *testing.T) {
	schema := []parquet.Column{
		{Name: "grp", Type: parquet.TypeString},
		{Name: "value", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"grp": "a", "value": 1.0},
		{"grp": "a", "value": 2.0},
		{"grp": "a", "value": 2.0},
		{"grp": "a", "value": 3.0},
		{"grp": "a", "value": 3.0},
		{"grp": "a", "value": 3.0},
		{"grp": "b", "value": 10.0},
		{"grp": "b", "value": 20.0},
		{"grp": "b", "value": 20.0},
	}

	agg := NewHashAggregate([]string{"grp"}, []AggColumn{
		{Func: AggMode, InputCol: "value", OutputCol: "mode_val", OutputType: parquet.TypeFloat64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := agg.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(ctx)
	result := b.ToRows()
	if len(result) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(result))
	}

	for _, row := range result {
		grp := row["grp"].(string)
		mode := row["mode_val"].(float64)
		switch grp {
		case "a":
			if math.Abs(mode-3.0) > 0.01 {
				t.Errorf("mode for grp=a: got %v, want 3.0", mode)
			}
		case "b":
			if math.Abs(mode-20.0) > 0.01 {
				t.Errorf("mode for grp=b: got %v, want 20.0", mode)
			}
		}
	}
}

func TestTier3MinByMaxBy(t *testing.T) {
	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
		{Name: "score", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"name": "alice", "score": 85.0},
		{"name": "bob", "score": 92.0},
		{"name": "carol", "score": 78.0},
		{"name": "dave", "score": 95.0},
		{"name": "eve", "score": 88.0},
	}

	agg := NewHashAggregate(nil, []AggColumn{
		{Func: AggMinBy, InputCol: "name", InputCol2: "score", OutputCol: "worst_name", OutputType: parquet.TypeString},
		{Func: AggMaxBy, InputCol: "name", InputCol2: "score", OutputCol: "best_name", OutputType: parquet.TypeString},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := agg.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(ctx)
	result := b.ToRows()
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}

	row := result[0]

	worstName := row["worst_name"].(string)
	if worstName != "carol" {
		t.Errorf("min_by(name, score) = %q, want \"carol\" (score 78)", worstName)
	}

	bestName := row["best_name"].(string)
	if bestName != "dave" {
		t.Errorf("max_by(name, score) = %q, want \"dave\" (score 95)", bestName)
	}
}

func TestTier3Median(t *testing.T) {
	schema := []parquet.Column{
		{Name: "value", Type: parquet.TypeFloat64},
	}

	t.Run("odd_count", func(t *testing.T) {
		// 5 values: 1, 3, 5, 7, 9 -> median = 5.0
		rows := []map[string]any{
			{"value": 9.0},
			{"value": 1.0},
			{"value": 5.0},
			{"value": 7.0},
			{"value": 3.0},
		}

		agg := NewHashAggregate(nil, []AggColumn{
			{Func: AggMedian, InputCol: "value", OutputCol: "med", OutputType: parquet.TypeFloat64},
		})

		source := NewSliceSource(schema, rows)
		pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
		ctx := context.Background()
		if err := pipe.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if err := agg.Finalize(ctx); err != nil {
			t.Fatal(err)
		}

		b, _ := agg.Next(ctx)
		result := b.ToRows()
		if len(result) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result))
		}

		med := result[0]["med"].(float64)
		if math.Abs(med-5.0) > 0.01 {
			t.Errorf("median = %v, want 5.0", med)
		}
	})

	t.Run("even_count", func(t *testing.T) {
		// 4 values: 1, 3, 5, 7 -> median = (3+5)/2 = 4.0
		rows := []map[string]any{
			{"value": 7.0},
			{"value": 1.0},
			{"value": 5.0},
			{"value": 3.0},
		}

		agg := NewHashAggregate(nil, []AggColumn{
			{Func: AggMedian, InputCol: "value", OutputCol: "med", OutputType: parquet.TypeFloat64},
		})

		source := NewSliceSource(schema, rows)
		pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
		ctx := context.Background()
		if err := pipe.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if err := agg.Finalize(ctx); err != nil {
			t.Fatal(err)
		}

		b, _ := agg.Next(ctx)
		result := b.ToRows()
		if len(result) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result))
		}

		med := result[0]["med"].(float64)
		if math.Abs(med-4.0) > 0.01 {
			t.Errorf("median = %v, want 4.0", med)
		}
	})
}

func TestTier3CorrNegative(t *testing.T) {
	schema := []parquet.Column{
		{Name: "y", Type: parquet.TypeFloat64},
		{Name: "x", Type: parquet.TypeFloat64},
	}
	// Perfect negative correlation: y = -x
	rows := []map[string]any{
		{"y": -1.0, "x": 1.0},
		{"y": -2.0, "x": 2.0},
		{"y": -3.0, "x": 3.0},
		{"y": -4.0, "x": 4.0},
	}

	agg := NewHashAggregate(nil, []AggColumn{
		{Func: AggCorr, InputCol: "y", InputCol2: "x", OutputCol: "correlation", OutputType: parquet.TypeFloat64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := agg.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(ctx)
	result := b.ToRows()
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}

	corr := result[0]["correlation"].(float64)
	if math.Abs(corr-(-1.0)) > 0.001 {
		t.Errorf("corr = %v, want -1.0 (perfect negative correlation)", corr)
	}
}

func TestTier3MinByMaxByGrouped(t *testing.T) {
	schema := []parquet.Column{
		{Name: "dept", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
		{Name: "salary", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"dept": "eng", "name": "alice", "salary": 120000.0},
		{"dept": "eng", "name": "bob", "salary": 95000.0},
		{"dept": "eng", "name": "carol", "salary": 140000.0},
		{"dept": "sales", "name": "dave", "salary": 80000.0},
		{"dept": "sales", "name": "eve", "salary": 110000.0},
	}

	agg := NewHashAggregate([]string{"dept"}, []AggColumn{
		{Func: AggMinBy, InputCol: "name", InputCol2: "salary", OutputCol: "lowest_paid", OutputType: parquet.TypeString},
		{Func: AggMaxBy, InputCol: "name", InputCol2: "salary", OutputCol: "highest_paid", OutputType: parquet.TypeString},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := agg.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := agg.Next(ctx)
	result := b.ToRows()
	if len(result) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(result))
	}

	for _, row := range result {
		dept := row["dept"].(string)
		lowest := row["lowest_paid"].(string)
		highest := row["highest_paid"].(string)
		switch dept {
		case "eng":
			if lowest != "bob" {
				t.Errorf("eng lowest_paid = %q, want \"bob\"", lowest)
			}
			if highest != "carol" {
				t.Errorf("eng highest_paid = %q, want \"carol\"", highest)
			}
		case "sales":
			if lowest != "dave" {
				t.Errorf("sales lowest_paid = %q, want \"dave\"", lowest)
			}
			if highest != "eve" {
				t.Errorf("sales highest_paid = %q, want \"eve\"", highest)
			}
		}
	}
}
