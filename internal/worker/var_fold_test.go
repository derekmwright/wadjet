package worker

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestApplyVarFold is the final stage's half of #339: the merged
// (count, mean, M2) state becomes the number the query asked for, under the
// name it asked for, and the synthetic column does not reach the client.
func TestApplyVarFold(t *testing.T) {
	// Two groups' worth of state, built by the same accumulator the engine
	// uses, over values whose variance is known here.
	stateOf := func(vals []float64) string {
		agg := exec.NewHashAggregate(nil, []exec.AggColumn{
			{Func: exec.AggVarState, InputCol: "v", OutputCol: "st", OutputType: parquet.TypeString},
		})
		if err := agg.Init(t.Context()); err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, len(vals))
		for i, x := range vals {
			rows[i] = map[string]any{"v": x}
		}
		if err := agg.Consume(t.Context(), batch.FromRows(
			[]parquet.Column{{Name: "v", Type: parquet.TypeFloat64}}, rows)); err != nil {
			t.Fatal(err)
		}
		out, err := agg.Next(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return out.ToRows()[0]["st"].(string)
	}

	groupA := []float64{1, 2, 3, 4} // sample variance 5/3
	groupB := []float64{7}          // sample variance undefined → NULL

	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: varStatePrefix + exec.VarKindStddevSamp + "#s", Type: parquet.TypeString},
		{Name: varStatePrefix + exec.VarKindVarPop + "#vp", Type: parquet.TypeString},
	}
	in := batch.FromRows(schema, []map[string]any{
		{"g": "a", schema[1].Name: stateOf(groupA), schema[2].Name: stateOf(groupA)},
		{"g": "b", schema[1].Name: stateOf(groupB), schema[2].Name: stateOf(groupB)},
	})

	out, err := applyVarFold([]*batch.RecordBatch{in})
	if err != nil {
		t.Fatalf("applyVarFold: %v", err)
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

	if got := rows[0]["s"].(float64); math.Abs(got-math.Sqrt(5.0/3.0)) > 1e-12 {
		t.Errorf("group a: STDDEV = %v, want %v", got, math.Sqrt(5.0/3.0))
	}
	if got := rows[0]["vp"].(float64); math.Abs(got-1.25) > 1e-12 {
		t.Errorf("group a: VAR_POP = %v, want 1.25", got)
	}
	// One row has no SAMPLE answer but does have a population one (0).
	if v := rows[1]["s"]; v != nil {
		t.Errorf("group b: STDDEV over one row = %v, want NULL", v)
	}
	if got, ok := rows[1]["vp"].(float64); !ok || got != 0 {
		t.Errorf("group b: VAR_POP over one row = %v, want 0", rows[1]["vp"])
	}
}

// TestApplyVarFoldPassThrough: a batch with no synthetic columns is returned
// untouched, and the malformed name (no kind separator) is left alone rather
// than guessed at.
func TestApplyVarFoldPassThrough(t *testing.T) {
	schema := []parquet.Column{{Name: "s", Type: parquet.TypeFloat64}}
	in := batch.FromRows(schema, []map[string]any{{"s": 1.5}})
	out, err := applyVarFold([]*batch.RecordBatch{in})
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != in {
		t.Error("a batch with no state column should be returned as-is")
	}

	if cols := findStateFoldCols([]parquet.Column{{Name: varStatePrefix + "nokind", Type: parquet.TypeString}}, varStatePrefix); len(cols) != 0 {
		t.Errorf("a synthetic name with no kind separator was folded anyway: %+v", cols)
	}
}
