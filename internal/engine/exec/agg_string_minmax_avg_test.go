package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// ClickBench-surfaced aggregate gaps:
//   - MIN/MAX over string columns resolved to a nil updater and returned
//     NULL (Q22, MIN(URL)).
//   - AVG over int64 accumulated in the wrapping int64 SUM kernel; large
//     hash-like values (Q04, AVG(UserID)) produced a garbage mean.
//   - MIN/MAX over a DATE column surfaced raw epoch days because the
//     planner-declared float64 output type erased the input type (Q07).
func runAgg(t *testing.T, schema []parquet.Column, rows []map[string]any, groupBy []string, aggs []AggColumn) []map[string]any {
	t.Helper()
	agg := NewHashAggregate(groupBy, aggs)
	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for {
		b, err := agg.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		out = append(out, b.ToRows()...)
	}
	return out
}

func TestMinMaxOverStrings(t *testing.T) {
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}
	rows := []map[string]any{
		{"g": "a", "s": "mango"},
		{"g": "a", "s": "apple"},
		{"g": "a", "s": "zebra"},
		{"g": "b", "s": "kiwi"},
		{"g": "b"}, // NULL s
	}

	t.Run("grouped", func(t *testing.T) {
		out := runAgg(t, schema, rows, []string{"g"}, []AggColumn{
			{Func: AggMin, InputCol: "s", OutputCol: "mn", OutputType: parquet.TypeFloat64},
			{Func: AggMax, InputCol: "s", OutputCol: "mx", OutputType: parquet.TypeFloat64},
		})
		want := map[string][2]string{"a": {"apple", "zebra"}, "b": {"kiwi", "kiwi"}}
		if len(out) != 2 {
			t.Fatalf("got %d groups: %v", len(out), out)
		}
		for _, row := range out {
			g := row["g"].(string)
			if row["mn"] != want[g][0] || row["mx"] != want[g][1] {
				t.Errorf("group %s: min=%v max=%v, want %v", g, row["mn"], row["mx"], want[g])
			}
		}
	})

	t.Run("scalar", func(t *testing.T) {
		out := runAgg(t, schema, rows, nil, []AggColumn{
			{Func: AggMin, InputCol: "s", OutputCol: "mn", OutputType: parquet.TypeFloat64},
			{Func: AggMax, InputCol: "s", OutputCol: "mx", OutputType: parquet.TypeFloat64},
		})
		if len(out) != 1 || out[0]["mn"] != "apple" || out[0]["mx"] != "zebra" {
			t.Fatalf("scalar min/max: %v", out)
		}
	})
}

func TestAvgInt64NoOverflow(t *testing.T) {
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	// Values near int64 max: the naive int64 sum wraps immediately.
	const big = int64(1) << 62
	rows := []map[string]any{
		{"g": int64(1), "v": big},
		{"g": int64(1), "v": big},
		{"g": int64(1), "v": big},
		{"g": int64(1), "v": big},
	}
	wantAvg := float64(big)

	t.Run("scalar", func(t *testing.T) {
		out := runAgg(t, schema, rows, nil, []AggColumn{
			{Func: AggAvg, InputCol: "v", OutputCol: "a", OutputType: parquet.TypeFloat64},
		})
		if len(out) != 1 {
			t.Fatalf("rows: %v", out)
		}
		if got := out[0]["a"].(float64); got != wantAvg {
			t.Errorf("scalar AVG: got %v, want %v", got, wantAvg)
		}
	})

	t.Run("grouped_int_key", func(t *testing.T) {
		out := runAgg(t, schema, rows, []string{"g"}, []AggColumn{
			{Func: AggAvg, InputCol: "v", OutputCol: "a", OutputType: parquet.TypeFloat64},
		})
		if len(out) != 1 {
			t.Fatalf("rows: %v", out)
		}
		if got := out[0]["a"].(float64); got != wantAvg {
			t.Errorf("grouped AVG: got %v, want %v", got, wantAvg)
		}
	})
}

func TestMinMaxDateKeepsDateType(t *testing.T) {
	schema := []parquet.Column{
		{Name: "d", Type: parquet.TypeDate},
	}
	// 2013-07-01 = 15887, 2013-07-31 = 15917.
	rows := []map[string]any{
		{"d": int32(15900)},
		{"d": int32(15887)},
		{"d": int32(15917)},
	}
	out := runAgg(t, schema, rows, nil, []AggColumn{
		{Func: AggMin, InputCol: "d", OutputCol: "mn", OutputType: parquet.TypeFloat64},
		{Func: AggMax, InputCol: "d", OutputCol: "mx", OutputType: parquet.TypeFloat64},
	})
	if len(out) != 1 {
		t.Fatalf("rows: %v", out)
	}
	mn, mx := fmt.Sprint(out[0]["mn"]), fmt.Sprint(out[0]["mx"])
	if mn != "2013-07-01" || mx != "2013-07-31" {
		t.Errorf("date min/max: got %q / %q, want 2013-07-01 / 2013-07-31", mn, mx)
	}
}
