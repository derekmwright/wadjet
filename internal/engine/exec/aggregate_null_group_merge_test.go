package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The merge half of the NULL-group-key family (#338). Parallel workers each
// accumulate their own partial state and MergeSink combines them by key;
// GROUP BY treats every NULL as the SAME key, so the partials' NULL groups
// must land on one another.
//
// The single-string fast path is the one that broke: its NULL group lives at
// strNullGroupIdx and is deliberately absent from strGroupIndex (a real
// 1-byte "\x01" key would otherwise collide with the sentinel), so the merge
// loop's table probe never matched it and every merge appended one more NULL
// group. No row was ever lost — the count split evenly across the partials,
// which is why the totals still added up and every SF0.01 suite stayed green.
func TestNullGroupKey_MergeSinkKeepsOneGroup(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		schema  []parquet.Column
		groupBy []string
		// partials[i] is one worker sink's rows.
		partials [][]map[string]any
		wantRows int
		// wantNullSum is SUM(v) over the single all-NULL-key group.
		wantNullSum string
	}{
		{
			name: "single string key",
			schema: []parquet.Column{
				{Name: "k", Type: parquet.TypeString, Nullable: true},
				{Name: "v", Type: parquet.TypeInt64},
			},
			groupBy: []string{"k"},
			partials: [][]map[string]any{
				{{"k": nil, "v": int64(1)}, {"k": "a", "v": int64(1)}},
				{{"k": nil, "v": int64(2)}, {"k": "a", "v": int64(2)}},
				{{"k": nil, "v": int64(4)}, {"k": "a", "v": int64(4)}},
			},
			wantRows:    2,
			wantNullSum: "7",
		},
		{
			name: "single int key",
			schema: []parquet.Column{
				{Name: "k", Type: parquet.TypeInt64, Nullable: true},
				{Name: "v", Type: parquet.TypeInt64},
			},
			groupBy: []string{"k"},
			partials: [][]map[string]any{
				{{"k": nil, "v": int64(1)}, {"k": int64(9), "v": int64(1)}},
				{{"k": nil, "v": int64(2)}, {"k": int64(9), "v": int64(2)}},
				{{"k": nil, "v": int64(4)}, {"k": int64(9), "v": int64(4)}},
			},
			wantRows:    2,
			wantNullSum: "7",
		},
		{
			// Multi-column keys where only SOME columns are NULL: ("x", NULL)
			// and (NULL, NULL) are two distinct groups, and each must survive
			// the merge exactly once — the case a NULL-key fix narrowed to
			// "the key is entirely NULL" would miss.
			name: "two columns, one null",
			schema: []parquet.Column{
				{Name: "k", Type: parquet.TypeString, Nullable: true},
				{Name: "j", Type: parquet.TypeString, Nullable: true},
				{Name: "v", Type: parquet.TypeInt64},
			},
			groupBy: []string{"k", "j"},
			partials: [][]map[string]any{
				{{"k": "x", "j": nil, "v": int64(1)}, {"k": nil, "j": nil, "v": int64(1)}},
				{{"k": "x", "j": nil, "v": int64(2)}, {"k": nil, "j": nil, "v": int64(2)}},
				{{"k": "x", "j": nil, "v": int64(4)}, {"k": nil, "j": nil, "v": int64(4)}},
			},
			wantRows:    2,
			wantNullSum: "7",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			primary := NewHashAggregate(tc.groupBy, []AggColumn{
				{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
			})
			if err := primary.Init(ctx); err != nil {
				t.Fatal(err)
			}
			sinks := []*HashAggregate{primary}
			for i := 1; i < len(tc.partials); i++ {
				c := primary.CloneSink().(*HashAggregate)
				if err := c.Init(ctx); err != nil {
					t.Fatal(err)
				}
				sinks = append(sinks, c)
			}
			for i, rows := range tc.partials {
				if err := sinks[i].Consume(ctx, batch.FromRows(tc.schema, rows)); err != nil {
					t.Fatal(err)
				}
			}
			for i := 1; i < len(sinks); i++ {
				primary.MergeSink(sinks[i])
			}

			rows := aggRows(t, primary)
			if len(rows) != tc.wantRows {
				t.Fatalf("group count = %d, want %d (NULL group split across partials): %v",
					len(rows), tc.wantRows, rows)
			}
			nulls := 0
			for _, r := range rows {
				allNull := true
				for _, col := range tc.groupBy {
					if r[col] != nil {
						allNull = false
					}
				}
				if !allNull {
					continue
				}
				nulls++
				if got := fmt.Sprintf("%v", r["s"]); got != tc.wantNullSum {
					t.Errorf("all-NULL group SUM = %v, want %v (partial states not merged)", got, tc.wantNullSum)
				}
			}
			if nulls != 1 {
				t.Fatalf("all-NULL group appeared %d times, want exactly 1: %v", nulls, rows)
			}
		})
	}
}

// TestNullGroupKey_SurvivesSpill asks the same question of the OTHER place
// group states are matched by key: the partial-state runs an aggregate
// writes under memory pressure and the k-way merge that reads them back. A
// NULL key that reloaded as a zero or an empty string would corrupt results
// only under pressure, which is the hardest possible failure to see.
//
// The run format encodes the NULL group's key as the generic binary form
// (one 0x01 null-flag byte), distinct from every real string, so it survives
// — this test is what says it keeps doing so.
func TestNullGroupKey_SurvivesSpill(t *testing.T) {
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64},
	}
	keys := []any{"alpha", "beta", nil, "gamma", nil}
	var batches []*batch.RecordBatch
	wantNull := int64(0)
	for bi := 0; bi < 12; bi++ {
		rows := make([]map[string]any, 0, 100)
		for ri := 0; ri < 100; ri++ {
			k := keys[(bi+ri)%len(keys)]
			rows = append(rows, map[string]any{"g": k, "v": int64(1)})
			if k == nil {
				wantNull++
			}
		}
		batches = append(batches, batch.FromRows(schema, rows))
	}

	// A budget this tight forces the drain-to-runs path on nearly every
	// batch, so the answer comes back through the external merge.
	tracker := memory.NewTracker("test", 1_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	tracker.ForceReserve(900)

	h := &HashAggregate{
		GroupByCols: []string{"g"},
		Aggs: []AggColumn{
			{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
		},
		Spill: sm,
	}
	rows := runHashAggToMap(t, h, batches)
	if len(rows) != 4 {
		t.Fatalf("group count = %d, want 4 (alpha, beta, gamma, NULL): %v", len(rows), rows)
	}
	nulls, nullSum := 0, int64(0)
	for _, r := range rows {
		if r["g"] == nil {
			nulls++
			nullSum += r["s"].(int64)
		}
	}
	if nulls != 1 {
		t.Errorf("NULL group came back %d times, want 1: %v", nulls, rows)
	}
	if nullSum != wantNull {
		t.Errorf("NULL group SUM = %d, want %d — a spilled NULL key reloaded as something else", nullSum, wantNull)
	}
}

// TestNullGroupKey_ParallelPipelineOneGroup is the same invariant one level
// up: a real parallel pipeline over a nullable string key. Partitioned
// aggregation is switched off so what has to get this right is the MERGE,
// not the router's all-NULLs-to-one-owner routing.
func TestNullGroupKey_ParallelPipelineOneGroup(t *testing.T) {
	prev := partitionedAggToggle.Set(false)
	defer partitionedAggToggle.Set(prev)

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeString, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64},
	}
	const n = 20000
	rows := make([]map[string]any, 0, n)
	wantNull := int64(0)
	for i := 0; i < n; i++ {
		if i%3 == 0 {
			rows = append(rows, map[string]any{"k": nil, "v": int64(1)})
			wantNull++
			continue
		}
		rows = append(rows, map[string]any{"k": fmt.Sprintf("k%d", i%4), "v": int64(1)})
	}
	agg := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	pipe := &Pipeline{Source: NewSliceSource(schema, rows), Sink: agg, Workers: 4}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	nulls, nullSum, total := 0, int64(0), int64(0)
	for {
		b, err := agg.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		for _, r := range b.ToRows() {
			total += r["s"].(int64)
			if r["k"] == nil {
				nulls++
				nullSum += r["s"].(int64)
			}
		}
	}
	if nulls != 1 {
		t.Errorf("NULL group emitted %d times, want 1", nulls)
	}
	if nullSum != wantNull {
		t.Errorf("NULL group SUM = %d, want %d", nullSum, wantNull)
	}
	if total != int64(n) {
		t.Errorf("total SUM = %d, want %d", total, n)
	}
}
