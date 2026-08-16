package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestReAggregatePartialsCorrectness — verifies the hot-loop refactor
// (function-pointer dispatch + first-row capture) preserves SUM/MIN/MAX
// semantics across all supported group-by and aggregate types.
//
// Regression for the 2026-05-03 perf fix that hoisted per-row type-switch
// out of the hot loop in reAggregatePartials.
func TestReAggregatePartialsCorrectness(t *testing.T) {
	tests := []struct {
		name      string
		schema    []parquet.Column
		mi        *logical.MergeInfo
		batches   [][]rowVal
		wantRows  int
		wantSums  map[string]float64
		wantMins  map[string]float64
		wantMaxes map[string]float64
	}{
		{
			name: "int64 group + sum/min/max",
			schema: []parquet.Column{
				{Name: "k", Type: parquet.TypeInt64},
				{Name: "s", Type: parquet.TypeFloat64},
				{Name: "lo", Type: parquet.TypeFloat64},
				{Name: "hi", Type: parquet.TypeFloat64},
			},
			mi: &logical.MergeInfo{
				GroupBy: []string{"k"},
				AggExprs: []logical.AggExpr{
					{Func: "SUM", OutputCol: "s"},
					{Func: "MIN", OutputCol: "lo"},
					{Func: "MAX", OutputCol: "hi"},
				},
			},
			batches: [][]rowVal{
				{
					{kInt64: 1, sFloat64: 10, loFloat64: 5, hiFloat64: 50},
					{kInt64: 1, sFloat64: 20, loFloat64: 8, hiFloat64: 30},
					{kInt64: 2, sFloat64: 100, loFloat64: 90, hiFloat64: 110},
				},
				{
					{kInt64: 1, sFloat64: 30, loFloat64: 2, hiFloat64: 25},
					{kInt64: 2, sFloat64: 200, loFloat64: 95, hiFloat64: 105},
				},
			},
			wantRows:  2,
			wantSums:  map[string]float64{"1": 60, "2": 300},
			wantMins:  map[string]float64{"1": 2, "2": 90},
			wantMaxes: map[string]float64{"1": 50, "2": 110},
		},
		{
			name: "string group + sum",
			schema: []parquet.Column{
				{Name: "k", Type: parquet.TypeString},
				{Name: "s", Type: parquet.TypeFloat64},
				{Name: "lo", Type: parquet.TypeFloat64},
				{Name: "hi", Type: parquet.TypeFloat64},
			},
			mi: &logical.MergeInfo{
				GroupBy: []string{"k"},
				AggExprs: []logical.AggExpr{
					{Func: "SUM", OutputCol: "s"},
					{Func: "MIN", OutputCol: "lo"},
					{Func: "MAX", OutputCol: "hi"},
				},
			},
			batches: [][]rowVal{
				{
					{kString: "alpha", sFloat64: 10, loFloat64: 5, hiFloat64: 50},
					{kString: "alpha", sFloat64: 20, loFloat64: 8, hiFloat64: 30},
					{kString: "beta", sFloat64: 100, loFloat64: 90, hiFloat64: 110},
				},
			},
			wantRows:  2,
			wantSums:  map[string]float64{"alpha": 30, "beta": 100},
			wantMins:  map[string]float64{"alpha": 5, "beta": 90},
			wantMaxes: map[string]float64{"alpha": 50, "beta": 110},
		},
	}

	c := &Coordinator{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batches := make([]*batch.RecordBatch, 0, len(tc.batches))
			for _, rows := range tc.batches {
				b := newRecordBatchFromRows(t, tc.schema, rows)
				batches = append(batches, b)
			}
			columns := make([]string, len(tc.schema))
			colIdx := make(map[string]int, len(tc.schema))
			for i, c := range tc.schema {
				columns[i] = c.Name
				colIdx[c.Name] = i
			}
			out := c.reAggregatePartials(batches, columns, colIdx, tc.mi)
			if len(out) != 1 {
				t.Fatalf("expected 1 output batch, got %d", len(out))
			}
			res := out[0]
			if res.Len != tc.wantRows {
				t.Fatalf("expected %d rows, got %d", tc.wantRows, res.Len)
			}

			kIdx := colIdx["k"]
			sIdx := colIdx["s"]
			loIdx := colIdx["lo"]
			hiIdx := colIdx["hi"]
			for ri := 0; ri < res.Len; ri++ {
				var key string
				switch tc.schema[kIdx].Type {
				case parquet.TypeInt64:
					key = string(rune('0' + int(res.Columns[kIdx].Int64Data[ri])))
				case parquet.TypeString:
					key = string(res.Columns[kIdx].BytesData.Value(ri))
				}
				if got, want := res.Columns[sIdx].Float64Data[ri], tc.wantSums[key]; got != want {
					t.Errorf("group %q: SUM got %v want %v", key, got, want)
				}
				if got, want := res.Columns[loIdx].Float64Data[ri], tc.wantMins[key]; got != want {
					t.Errorf("group %q: MIN got %v want %v", key, got, want)
				}
				if got, want := res.Columns[hiIdx].Float64Data[ri], tc.wantMaxes[key]; got != want {
					t.Errorf("group %q: MAX got %v want %v", key, got, want)
				}
			}
		})
	}
}

// rowVal is a small typed struct for building test batches.
type rowVal struct {
	kInt64    int64
	kString   string
	sFloat64  float64
	loFloat64 float64
	hiFloat64 float64
}

func newRecordBatchFromRows(t *testing.T, schema []parquet.Column, rows []rowVal) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch(schema, len(rows))
	for ri, r := range rows {
		for ci, col := range schema {
			switch col.Name {
			case "k":
				switch col.Type {
				case parquet.TypeInt64:
					b.Columns[ci].Int64Data[ri] = r.kInt64
				case parquet.TypeString:
					b.Columns[ci].BytesData.Set(ri, []byte(r.kString))
				}
			case "s":
				b.Columns[ci].Float64Data[ri] = r.sFloat64
			case "lo":
				b.Columns[ci].Float64Data[ri] = r.loFloat64
			case "hi":
				b.Columns[ci].Float64Data[ri] = r.hiFloat64
			}
			b.Columns[ci].Nulls.SetValid(ri)
		}
	}
	b.Len = len(rows)
	return b
}

// BenchmarkReAggregatePartialsManyGroups measures the hot loop on a workload
// with many distinct groups (typical SF100 final-merge shape: ~100K groups,
// 5 group-by cols, 2 aggregates).
func BenchmarkReAggregatePartialsManyGroups(b *testing.B) {
	const numGroups = 10000
	const numBatches = 4
	schema := []parquet.Column{
		{Name: "k1", Type: parquet.TypeInt64},
		{Name: "k2", Type: parquet.TypeString},
		{Name: "s", Type: parquet.TypeFloat64},
		{Name: "m", Type: parquet.TypeFloat64},
	}
	mi := &logical.MergeInfo{
		GroupBy: []string{"k1", "k2"},
		AggExprs: []logical.AggExpr{
			{Func: "SUM", OutputCol: "s"},
			{Func: "MAX", OutputCol: "m"},
		},
	}
	columns := []string{"k1", "k2", "s", "m"}
	colIdx := map[string]int{"k1": 0, "k2": 1, "s": 2, "m": 3}

	batches := make([]*batch.RecordBatch, numBatches)
	for bi := 0; bi < numBatches; bi++ {
		rb := batch.NewRecordBatch(schema, numGroups)
		for ri := 0; ri < numGroups; ri++ {
			rb.Columns[0].Int64Data[ri] = int64(ri)
			rb.Columns[1].BytesData.Set(ri, []byte("group"))
			rb.Columns[2].Float64Data[ri] = float64(ri)
			rb.Columns[3].Float64Data[ri] = float64(ri * 2)
			for ci := range schema {
				rb.Columns[ci].Nulls.SetValid(ri)
			}
		}
		rb.Len = numGroups
		batches[bi] = rb
	}

	c := &Coordinator{}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = c.reAggregatePartials(batches, columns, colIdx, mi)
	}
}
