package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Regression tests for the flat-accumulator count[] layout: MIN/MAX carry no
// count array at all, and aggregates whose count updates are provably
// identical share one. Both are invisible in results by design — these tests
// pin the layout AND the values it has to keep producing.

// TestFlatAccumCountPlan pins which aggregates own, share, or lack a count[].
func TestFlatAccumCountPlan(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
		{Name: "w", Type: parquet.TypeInt64, Nullable: true},
	}
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggCount, InputCol: "", OutputCol: "cstar", OutputType: parquet.TypeInt64},
		{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeInt64},
		{Func: AggAvg, InputCol: "v", OutputCol: "av", OutputType: parquet.TypeFloat64},
		{Func: AggCount, InputCol: "v", OutputCol: "cv", OutputType: parquet.TypeInt64},
		{Func: AggSum, InputCol: "w", OutputCol: "sw", OutputType: parquet.TypeInt64},
		{Func: AggMin, InputCol: "w", OutputCol: "mw", OutputType: parquet.TypeInt64},
		{Func: AggMax, InputCol: "w", OutputCol: "xw", OutputType: parquet.TypeInt64},
		{Func: AggCount, InputCol: "", OutputCol: "cstar2", OutputType: parquet.TypeInt64},
	})
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"k": int64(1), "v": int64(2), "w": int64(3)},
	})); err != nil {
		t.Fatal(err)
	}
	if len(h.intFlatAccs) != 8 {
		t.Fatalf("intFlatAccs = %d, want 8", len(h.intFlatAccs))
	}
	// owner index per aggregate; -1 means "no count array at all".
	want := []int32{0, 1, 1, 1, 4, -1, -1, 0}
	for i, w := range want {
		fa := &h.intFlatAccs[i]
		if w < 0 {
			if fa.count != nil || countArrayOf(h.intFlatAccs, i) != nil {
				t.Errorf("agg %d (%s): expected no count array", i, h.Aggs[i].OutputCol)
			}
			continue
		}
		if fa.countFrom != w {
			t.Errorf("agg %d (%s): countFrom = %d, want %d", i, h.Aggs[i].OutputCol, fa.countFrom, w)
		}
		if got, exp := countArrayOf(h.intFlatAccs, i), h.intFlatAccs[w].count; exp == nil || &got[0] != &exp[0] {
			t.Errorf("agg %d (%s): count array not resolved to owner %d", i, h.Aggs[i].OutputCol, w)
		}
		if w != int32(i) && fa.count != nil {
			t.Errorf("agg %d (%s): sharer must not own a count array", i, h.Aggs[i].OutputCol)
		}
	}
}

// TestCountSharingValues drives the shared-count shape through every SoA
// group-key path with NULLs in the aggregate columns, where a double- or
// under-counted shared array would show up immediately in AVG and in SUM's
// empty-group NULL rule.
func TestCountSharingValues(t *testing.T) {
	aggs := []AggColumn{
		{Func: AggCount, InputCol: "", OutputCol: "cstar", OutputType: parquet.TypeInt64},
		{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeFloat64},
		{Func: AggAvg, InputCol: "v", OutputCol: "av", OutputType: parquet.TypeFloat64},
		{Func: AggCount, InputCol: "v", OutputCol: "cv", OutputType: parquet.TypeInt64},
		{Func: AggMin, InputCol: "v", OutputCol: "mv", OutputType: parquet.TypeFloat64},
	}

	cases := []struct {
		name    string
		groupBy []string
		schema  []parquet.Column
		rows    []map[string]any
		// key of the group whose values we check, plus the expected values.
		want map[string]map[string]any
	}{
		{
			name:    "single-int",
			groupBy: []string{"k"},
			schema: []parquet.Column{
				{Name: "k", Type: parquet.TypeInt64},
				{Name: "v", Type: parquet.TypeFloat64, Nullable: true},
			},
			rows: []map[string]any{
				{"k": int64(1), "v": 2.0},
				{"k": int64(1), "v": nil},
				{"k": int64(1), "v": 4.0},
				{"k": int64(2), "v": nil},
			},
			want: map[string]map[string]any{
				"1": {"cstar": int64(3), "sv": 6.0, "av": 3.0, "cv": int64(2), "mv": 2.0},
				"2": {"cstar": int64(1), "sv": nil, "av": nil, "cv": int64(0), "mv": nil},
			},
		},
		{
			name:    "dual-int",
			groupBy: []string{"k", "k2"},
			schema: []parquet.Column{
				{Name: "k", Type: parquet.TypeInt64},
				{Name: "k2", Type: parquet.TypeInt64},
				{Name: "v", Type: parquet.TypeFloat64, Nullable: true},
			},
			rows: []map[string]any{
				{"k": int64(1), "k2": int64(9), "v": 2.0},
				{"k": int64(1), "k2": int64(9), "v": nil},
				{"k": int64(1), "k2": int64(9), "v": 4.0},
				{"k": int64(2), "k2": int64(9), "v": nil},
			},
			want: map[string]map[string]any{
				"1": {"cstar": int64(3), "sv": 6.0, "av": 3.0, "cv": int64(2), "mv": 2.0},
				"2": {"cstar": int64(1), "sv": nil, "av": nil, "cv": int64(0), "mv": nil},
			},
		},
		{
			name:    "generic-soa",
			groupBy: []string{"k", "k2"},
			schema: []parquet.Column{
				{Name: "k", Type: parquet.TypeInt64},
				{Name: "k2", Type: parquet.TypeString},
				{Name: "v", Type: parquet.TypeFloat64, Nullable: true},
			},
			rows: []map[string]any{
				{"k": int64(1), "k2": "x", "v": 2.0},
				{"k": int64(1), "k2": "x", "v": nil},
				{"k": int64(1), "k2": "x", "v": 4.0},
				{"k": int64(2), "k2": "x", "v": nil},
			},
			want: map[string]map[string]any{
				"1": {"cstar": int64(3), "sv": 6.0, "av": 3.0, "cv": int64(2), "mv": 2.0},
				"2": {"cstar": int64(1), "sv": nil, "av": nil, "cv": int64(0), "mv": nil},
			},
		},
		{
			name:    "single-string",
			groupBy: []string{"k"},
			schema: []parquet.Column{
				{Name: "k", Type: parquet.TypeString},
				{Name: "v", Type: parquet.TypeFloat64, Nullable: true},
			},
			rows: []map[string]any{
				{"k": "a", "v": 2.0},
				{"k": "a", "v": nil},
				{"k": "a", "v": 4.0},
				{"k": "b", "v": nil},
			},
			want: map[string]map[string]any{
				"a": {"cstar": int64(3), "sv": 6.0, "av": 3.0, "cv": int64(2), "mv": 2.0},
				"b": {"cstar": int64(1), "sv": nil, "av": nil, "cv": int64(0), "mv": nil},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHashAggregate(tc.groupBy, aggs)
			ctx := context.Background()
			if err := h.Init(ctx); err != nil {
				t.Fatal(err)
			}
			// Two batches so the shared array is grown and re-scattered.
			for _, r := range tc.rows {
				if err := h.Consume(ctx, batch.FromRows(tc.schema, []map[string]any{r})); err != nil {
					t.Fatal(err)
				}
			}
			rows := aggRows(t, h)
			if len(rows) != len(tc.want) {
				t.Fatalf("groups = %d, want %d: %v", len(rows), len(tc.want), rows)
			}
			for _, got := range rows {
				key := keyString(got[tc.groupBy[0]])
				exp, ok := tc.want[key]
				if !ok {
					t.Fatalf("unexpected group %q: %v", key, got)
				}
				for col, wantVal := range exp {
					if !valuesEqual(got[col], wantVal) {
						t.Errorf("group %q %s = %v, want %v", key, col, got[col], wantVal)
					}
				}
			}
		})
	}
}

// TestCountSharingAcrossMerge exercises the SoA merge path (which copies flat
// rows slot-by-slot) with a shared count array on both sides.
func TestCountSharingAcrossMerge(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64, Nullable: true},
	}
	parent := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeFloat64},
		{Func: AggAvg, InputCol: "v", OutputCol: "av", OutputType: parquet.TypeFloat64},
		{Func: AggMax, InputCol: "v", OutputCol: "xv", OutputType: parquet.TypeFloat64},
	})
	ctx := context.Background()
	if err := parent.Init(ctx); err != nil {
		t.Fatal(err)
	}

	feed := func(rows []map[string]any) *HashAggregate {
		c := parent.CloneSink().(*HashAggregate)
		if err := c.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if err := c.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
		return c
	}
	parent.MergeSink(feed([]map[string]any{
		{"k": int64(1), "v": 2.0},
		{"k": int64(1), "v": nil},
		{"k": int64(2), "v": 8.0},
	}))
	parent.MergeSink(feed([]map[string]any{
		{"k": int64(1), "v": 4.0},
		{"k": int64(3), "v": nil},
	}))

	rows := aggRows(t, parent)
	if len(rows) != 3 {
		t.Fatalf("groups = %d, want 3: %v", len(rows), rows)
	}
	want := map[string]map[string]any{
		"1": {"sv": 6.0, "av": 3.0, "xv": 4.0},
		"2": {"sv": 8.0, "av": 8.0, "xv": 8.0},
		"3": {"sv": nil, "av": nil, "xv": nil},
	}
	for _, got := range rows {
		exp := want[keyString(got["k"])]
		if exp == nil {
			t.Fatalf("unexpected group: %v", got)
		}
		for col, wantVal := range exp {
			if !valuesEqual(got[col], wantVal) {
				t.Errorf("group %v %s = %v, want %v", got["k"], col, got[col], wantVal)
			}
		}
	}
}

func keyString(v any) string { return fmt.Sprintf("%v", v) }

func valuesEqual(got, want any) bool {
	if want == nil {
		return got == nil
	}
	switch w := want.(type) {
	case float64:
		g, ok := got.(float64)
		return ok && g == w
	case int64:
		g, ok := got.(int64)
		return ok && g == w
	}
	return got == want
}
