package worker

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestApplyCovarFold is the final stage's half of #353: the merged
// (count, meanX, meanY, C, M2x, M2y) state becomes the number the query
// asked for, under the name it asked for, and the synthetic column does not
// reach the client.
func TestApplyCovarFold(t *testing.T) {
	stateOf := func(xs, ys []float64) string {
		agg := exec.NewHashAggregate(nil, []exec.AggColumn{
			{Func: exec.AggCovarState, InputCol: "x", InputCol2: "y",
				OutputCol: "st", OutputType: parquet.TypeString},
		})
		if err := agg.Init(t.Context()); err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, len(xs))
		for i := range xs {
			rows[i] = map[string]any{"x": xs[i], "y": ys[i]}
		}
		schema := []parquet.Column{
			{Name: "x", Type: parquet.TypeFloat64},
			{Name: "y", Type: parquet.TypeFloat64},
		}
		if err := agg.Consume(t.Context(), batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
		out, err := agg.Next(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return out.ToRows()[0]["st"].(string)
	}

	// y = 2x exactly, so the correlation is 1, the population covariance is
	// 2·var_pop(x) = 2·1.25 = 2.5 and the sample one 2·(5/3). Values chosen
	// so the reference needs no floating-point argument.
	xs := []float64{1, 2, 3, 4}
	ys := []float64{2, 4, 6, 8}
	single := stateOf([]float64{7}, []float64{9})

	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: covarStatePrefix + exec.CovarKindCorr + "#c", Type: parquet.TypeString},
		{Name: covarStatePrefix + exec.CovarKindCovarPop + "#cp", Type: parquet.TypeString},
	}
	in := batch.FromRows(schema, []map[string]any{
		{"g": "a", schema[1].Name: stateOf(xs, ys), schema[2].Name: stateOf(xs, ys)},
		{"g": "b", schema[1].Name: single, schema[2].Name: single},
	})

	out, err := applyCovarFold([]*batch.RecordBatch{in})
	if err != nil {
		t.Fatalf("applyCovarFold: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("%d batches out, want 1", len(out))
	}
	for _, c := range out[0].Schema {
		if len(c.Name) > 2 && c.Name[:2] == "__" {
			t.Errorf("synthetic column %q survived the fold", c.Name)
		}
	}
	rows := out[0].ToRows()
	if len(rows) != 2 {
		t.Fatalf("%d rows, want 2", len(rows))
	}
	if got := rows[0]["c"].(float64); math.Abs(got-1) > 1e-12 {
		t.Errorf("group a: CORR of y=2x = %v, want 1", got)
	}
	if got := rows[0]["cp"].(float64); math.Abs(got-2.5) > 1e-12 {
		t.Errorf("group a: COVAR_POP = %v, want 2.5", got)
	}
	// One row has no CORR (it needs two) but does have a population
	// covariance of 0.
	if v := rows[1]["c"]; v != nil {
		t.Errorf("group b: CORR over one row = %v, want NULL", v)
	}
	if got, ok := rows[1]["cp"].(float64); !ok || got != 0 {
		t.Errorf("group b: COVAR_POP over one row = %v, want 0", rows[1]["cp"])
	}
}

// TestApplyCovarFoldPassThrough: a batch with no synthetic columns is
// returned untouched, and a malformed synthetic name is left alone rather
// than guessed at.
func TestApplyCovarFoldPassThrough(t *testing.T) {
	schema := []parquet.Column{{Name: "c", Type: parquet.TypeFloat64}}
	in := batch.FromRows(schema, []map[string]any{{"c": 0.5}})
	out, err := applyCovarFold([]*batch.RecordBatch{in})
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != in {
		t.Error("a batch with no state column should be returned as-is")
	}
	if cols := findStateFoldCols(
		[]parquet.Column{{Name: covarStatePrefix + "nokind", Type: parquet.TypeString}},
		covarStatePrefix); len(cols) != 0 {
		t.Errorf("a synthetic name with no kind separator was folded anyway: %+v", cols)
	}
	// The two prefixes must not see each other's columns.
	if cols := findStateFoldCols(
		[]parquet.Column{{Name: varStatePrefix + "stddev_samp#s", Type: parquet.TypeString}},
		covarStatePrefix); len(cols) != 0 {
		t.Errorf("the covariance fold claimed a variance state column: %+v", cols)
	}
}

// TestParseAggFuncStringKnowsEveryPlannedName: the worker's `default:
// AggSum` is what made MEDIAN, MODE, PERCENTILE_*, CORR and COVAR_* answer
// with the sum of their first argument on the stage DAG. Every name the
// coordinator can dispatch has to be recognized, and anything else has to
// report so rather than silently become a SUM.
func TestParseAggFuncStringKnowsEveryPlannedName(t *testing.T) {
	for _, name := range []string{
		"sum", "count", "min", "max", "avg", "count_distinct", "approx_distinct",
		"string_agg", "bool_and", "every", "bool_or",
		"stddev", "stddev_samp", "variance", "var_samp", "stddev_pop", "var_pop",
		"var_state", "var_state_merge", "covar_state", "covar_state_merge",
		"corr", "covar_samp", "covar_pop",
		"median", "percentile_cont", "percentile_disc", "quantile_cont", "quantile_disc",
		"mode", "min_by", "max_by",
	} {
		if _, ok := parseAggFuncString(name); !ok {
			t.Errorf("%q is not recognized, so it would have become a SUM", name)
		}
	}
	for _, name := range []string{"", "no_such_agg", "listagg", "sum(x)"} {
		if _, ok := parseAggFuncString(name); ok {
			t.Errorf("%q was accepted", name)
		}
	}
}

// TestBuildFragmentHashAggregateRefusesUnknownFunc: the refusal has to
// reach the task, not just the parse helper.
func TestBuildFragmentHashAggregateRefusesUnknownFunc(t *testing.T) {
	e := &Executor{}
	_, err := e.buildFragmentHashAggregate(distributed.OpSpec{
		GroupByCols: []string{"g"},
		Aggregates:  []distributed.AggSpec{{Func: "no_such_agg", InputCol: "v", OutputCol: "x"}},
	})
	if err == nil {
		t.Fatal("an unknown aggregate function built a HashAggregate; it used to build a SUM")
	}
}

// TestBuildFragmentHashAggregateCarriesExtraArgs: the second column, the
// separator and the fraction have to survive the wire→operator conversion,
// or the worker aggregates something other than what was planned.
func TestBuildFragmentHashAggregateCarriesExtraArgs(t *testing.T) {
	e := &Executor{}
	agg, err := e.buildFragmentHashAggregate(distributed.OpSpec{
		Aggregates: []distributed.AggSpec{
			{Func: "min_by", InputCol: "label", InputCol2: "k", OutputCol: "mn"},
			{Func: "string_agg", InputCol: "p", Separator: "::", OutputCol: "s"},
			{Func: "percentile_cont", InputCol: "v", Percentile: 0.9, OutputCol: "p90"},
		},
		GroupByCols: []string{"g"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if agg.Aggs[0].InputCol2 != "k" {
		t.Errorf("min_by ordering column %q, want \"k\"", agg.Aggs[0].InputCol2)
	}
	if agg.Aggs[1].Separator != "::" {
		t.Errorf("string_agg separator %q, want \"::\"", agg.Aggs[1].Separator)
	}
	if agg.Aggs[2].Percentile != 0.9 {
		t.Errorf("percentile fraction %v, want 0.9", agg.Aggs[2].Percentile)
	}
}

// TestBuildFragmentHashAggregateMergesCovarState: in merge mode the partial
// form becomes the merge form, exactly as var_state does. Without it the
// merge stage would run COVAR_STATE over a column of encoded strings, which
// accumulates nothing.
func TestBuildFragmentHashAggregateMergesCovarState(t *testing.T) {
	e := &Executor{}
	agg, err := e.buildFragmentHashAggregate(distributed.OpSpec{
		MergeMode:   true,
		GroupByCols: []string{"g"},
		Aggregates: []distributed.AggSpec{
			{Func: "covar_state", InputCol: "x", InputCol2: "y", OutputCol: "st"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if agg.Aggs[0].Func != exec.AggCovarStateMerge {
		t.Errorf("merge-mode covar_state resolved to %v, want AggCovarStateMerge", agg.Aggs[0].Func)
	}
	if agg.Aggs[0].InputCol != "st" {
		t.Errorf("merge-mode input column %q, want the partial's output column \"st\"", agg.Aggs[0].InputCol)
	}
}
