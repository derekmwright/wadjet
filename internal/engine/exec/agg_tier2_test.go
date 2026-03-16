package exec

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func TestAggStringAgg(t *testing.T) {
	schema := []parquet.Column{
		{Name: "dept", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
	}
	rows := []map[string]any{
		{"dept": "eng", "name": "alice"},
		{"dept": "eng", "name": "bob"},
		{"dept": "eng", "name": "carol"},
		{"dept": "sales", "name": "dave"},
		{"dept": "sales", "name": "eve"},
	}

	agg := NewHashAggregate([]string{"dept"}, []AggColumn{
		{Func: AggStringAgg, InputCol: "name", OutputCol: "names", OutputType: parquet.TypeString, Separator: ","},
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
		names := row["names"].(string)
		dept := row["dept"].(string)
		if dept == "eng" && len(names) == 0 {
			t.Error("eng names should not be empty")
		}
		if dept == "sales" && len(names) == 0 {
			t.Error("sales names should not be empty")
		}
	}
}

func TestAggBoolAndOr(t *testing.T) {
	schema := []parquet.Column{
		{Name: "grp", Type: parquet.TypeString},
		{Name: "flag", Type: parquet.TypeBool},
	}
	rows := []map[string]any{
		{"grp": "all_true", "flag": true},
		{"grp": "all_true", "flag": true},
		{"grp": "mixed", "flag": true},
		{"grp": "mixed", "flag": false},
		{"grp": "all_false", "flag": false},
		{"grp": "all_false", "flag": false},
	}

	agg := NewHashAggregate([]string{"grp"}, []AggColumn{
		{Func: AggBoolAnd, InputCol: "flag", OutputCol: "all_true", OutputType: parquet.TypeBool},
		{Func: AggBoolOr, InputCol: "flag", OutputCol: "any_true", OutputType: parquet.TypeBool},
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

	for _, row := range result {
		grp := row["grp"].(string)
		boolAnd := row["all_true"].(bool)
		boolOr := row["any_true"].(bool)

		switch grp {
		case "all_true":
			if !boolAnd {
				t.Error("bool_and for all_true should be true")
			}
			if !boolOr {
				t.Error("bool_or for all_true should be true")
			}
		case "mixed":
			if boolAnd {
				t.Error("bool_and for mixed should be false")
			}
			if !boolOr {
				t.Error("bool_or for mixed should be true")
			}
		case "all_false":
			if boolAnd {
				t.Error("bool_and for all_false should be false")
			}
			if boolOr {
				t.Error("bool_or for all_false should be false")
			}
		}
	}
}

func TestAggStddevVariance(t *testing.T) {
	schema := []parquet.Column{
		{Name: "value", Type: parquet.TypeFloat64},
	}
	// Values: 2, 4, 4, 4, 5, 5, 7, 9
	// Mean = 5, Population variance = 4, Sample variance = 4.571
	rows := []map[string]any{
		{"value": 2.0},
		{"value": 4.0},
		{"value": 4.0},
		{"value": 4.0},
		{"value": 5.0},
		{"value": 5.0},
		{"value": 7.0},
		{"value": 9.0},
	}

	agg := NewHashAggregate(nil, []AggColumn{
		{Func: AggVariance, InputCol: "value", OutputCol: "var_samp", OutputType: parquet.TypeFloat64},
		{Func: AggVarPop, InputCol: "value", OutputCol: "var_pop", OutputType: parquet.TypeFloat64},
		{Func: AggStddev, InputCol: "value", OutputCol: "stddev_samp", OutputType: parquet.TypeFloat64},
		{Func: AggStddevPop, InputCol: "value", OutputCol: "stddev_pop", OutputType: parquet.TypeFloat64},
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

	// Sample variance = 32/7 ≈ 4.571
	varSamp := row["var_samp"].(float64)
	if math.Abs(varSamp-32.0/7.0) > 0.01 {
		t.Errorf("var_samp = %v, want ~4.571", varSamp)
	}

	// Population variance = 4.0
	varPop := row["var_pop"].(float64)
	if math.Abs(varPop-4.0) > 0.01 {
		t.Errorf("var_pop = %v, want 4.0", varPop)
	}

	// stddev_samp = sqrt(var_samp)
	stddevSamp := row["stddev_samp"].(float64)
	if math.Abs(stddevSamp-math.Sqrt(32.0/7.0)) > 0.01 {
		t.Errorf("stddev_samp = %v, want ~2.138", stddevSamp)
	}

	// stddev_pop = sqrt(var_pop) = 2.0
	stddevPop := row["stddev_pop"].(float64)
	if math.Abs(stddevPop-2.0) > 0.01 {
		t.Errorf("stddev_pop = %v, want 2.0", stddevPop)
	}
}
