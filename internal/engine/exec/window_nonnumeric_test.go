package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestWindowSumOverNonNumericMatchesTheGroupedNull is the regression for #412.
//
// The promotion table (numeric_promote.go) says IPV4 and MAC have no numeric
// reading — PostgreSQL has no sum or avg over inet or macaddr — and the
// grouped path honours it: kernel.ResolveRowSum has no arm for either, the
// updater is nil, and SUM(ipv4_col) answers NULL. vecFloat64 asked the same
// table and DISCARDED the answer (`f, _ := numericFloat64(...)`), so
// SUM(ipv4_col) OVER () computed 0 and marked the row VALID. Two forms of one
// question, two answers, and the windowed one is a wrong number rather than a
// missing one — invisible to any oracle that only checks the grouped form.
//
// Every window shape reads through vecFloat64, so all three are checked: the
// whole-partition frame, the running frame an ORDER BY produces, and a
// PARTITION BY.
func TestWindowSumOverNonNumericMatchesTheGroupedNull(t *testing.T) {
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: "ip", Type: parquet.TypeIPv4},
		{Name: "mac", Type: parquet.TypeMAC},
	}
	rows := []map[string]any{
		{"g": "a", "ip": "10.0.0.1", "mac": "aa:bb:cc:dd:ee:01"},
		{"g": "a", "ip": "10.0.0.2", "mac": "aa:bb:cc:dd:ee:02"},
		{"g": "b", "ip": "10.0.0.3", "mac": "aa:bb:cc:dd:ee:03"},
		{"g": "b", "ip": "10.0.0.4", "mac": "aa:bb:cc:dd:ee:04"},
	}

	// The grouped answer is the reference. Assert it rather than assume it:
	// if the grouped path ever starts producing a number the window must
	// follow, and this test has to fail rather than pin a stale NULL.
	for _, col := range []string{"ip", "mac"} {
		agg := NewHashAggregate([]string{"g"}, []AggColumn{
			{Func: AggSum, InputCol: col, OutputCol: "s", OutputType: parquet.TypeFloat64},
		})
		pipe := &Pipeline{Source: NewSliceSource(schema, rows), Sink: agg}
		if err := pipe.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		b, err := agg.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			t.Fatalf("grouped SUM(%s) produced no rows", col)
		}
		for _, row := range b.ToRows() {
			if row["s"] != nil {
				t.Fatalf("grouped SUM(%s) = %v, want NULL — the reference the "+
					"window has to match changed", col, row["s"])
			}
		}
	}

	shapes := []struct {
		name string
		wc   WindowColumn
	}{
		{"sum whole partition", WindowColumn{
			Func: WinSum, InputCol: "ip", OutputCol: "w", OutputType: parquet.TypeFloat64}},
		{"avg whole partition", WindowColumn{
			Func: WinAvg, InputCol: "ip", OutputCol: "w", OutputType: parquet.TypeFloat64}},
		{"sum running", WindowColumn{
			Func: WinSum, InputCol: "ip", OutputCol: "w", OutputType: parquet.TypeFloat64,
			OrderBy: []SortKey{{Column: "ip", Order: Ascending}}}},
		{"avg running", WindowColumn{
			Func: WinAvg, InputCol: "ip", OutputCol: "w", OutputType: parquet.TypeFloat64,
			OrderBy: []SortKey{{Column: "ip", Order: Ascending}}}},
		{"sum partitioned", WindowColumn{
			Func: WinSum, InputCol: "ip", OutputCol: "w", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"g"}}},
		{"sum mac", WindowColumn{
			Func: WinSum, InputCol: "mac", OutputCol: "w", OutputType: parquet.TypeFloat64}},
		{"avg mac partitioned", WindowColumn{
			Func: WinAvg, InputCol: "mac", OutputCol: "w", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"g"}}},
	}
	for _, sh := range shapes {
		sh := sh
		t.Run(sh.name, func(t *testing.T) {
			win := NewWindow([]WindowColumn{sh.wc})
			pipe := &Pipeline{Source: NewSliceSource(schema, rows), Sink: win}
			ctx := context.Background()
			if err := pipe.Run(ctx); err != nil {
				t.Fatal(err)
			}
			seen := 0
			for {
				b, err := win.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if b == nil {
					break
				}
				for _, row := range b.ToRows() {
					seen++
					if row["w"] != nil {
						t.Fatalf("window %s = %v, want NULL — the grouped form of "+
							"the same aggregate answers NULL (#412)", sh.name, row["w"])
					}
				}
			}
			if seen != len(rows) {
				t.Fatalf("window emitted %d rows, want %d", seen, len(rows))
			}
		})
	}
}

// TestWindowSumOverPromotableTypesStillAnswers guards the other direction:
// the bool vecFloat64 now returns must not turn a type the promotion table
// ADMITS into a NULL. DATE and DURATION are the two that only reached the
// table recently.
func TestWindowSumOverPromotableTypesStillAnswers(t *testing.T) {
	schema := []parquet.Column{
		{Name: "d", Type: parquet.TypeDate},
		{Name: "dur", Type: parquet.TypeDuration},
		{Name: "p", Type: parquet.TypePort},
	}
	rows := []map[string]any{
		{"d": "1970-01-11", "dur": int64(1000), "p": int32(80)},
		{"d": "1970-01-21", "dur": int64(2000), "p": int32(443)},
	}
	want := map[string]float64{"d": 30, "dur": 3000, "p": 523}
	for col, sum := range want {
		win := NewWindow([]WindowColumn{{
			Func: WinSum, InputCol: col, OutputCol: "w", OutputType: parquet.TypeFloat64,
		}})
		pipe := &Pipeline{Source: NewSliceSource(schema, rows), Sink: win}
		ctx := context.Background()
		if err := pipe.Run(ctx); err != nil {
			t.Fatal(err)
		}
		b, err := win.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range b.ToRows() {
			got, ok := row["w"].(float64)
			if !ok || got != sum {
				t.Fatalf("window SUM(%s) = %v, want %v", col, row["w"], sum)
			}
		}
	}
}
