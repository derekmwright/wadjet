package physical

import (
	"reflect"
	"testing"
)

func TestPruneFusedAggOutputCols(t *testing.T) {
	cases := []struct {
		name    string
		cols    []string
		groupBy []string
		specs   []AggSpec
		filters []string
		want    []string
	}{
		{
			name:    "strips pure synthetic output (Q18 shape)",
			cols:    []string{"__having_0", "l_orderkey", "l_quantity"},
			groupBy: []string{"l_orderkey"},
			specs:   []AggSpec{{Func: "sum", InputCol: "l_quantity", OutputCol: "__having_0"}},
			want:    []string{"l_orderkey", "l_quantity"},
		},
		{
			name:    "output aliasing its own input is kept (SUM(x) AS x)",
			cols:    []string{"g", "x"},
			groupBy: []string{"g"},
			specs:   []AggSpec{{Func: "sum", InputCol: "x", OutputCol: "x"}},
			want:    []string{"g", "x"},
		},
		{
			name:    "output aliasing a group key is kept",
			cols:    []string{"g", "x"},
			groupBy: []string{"g"},
			specs:   []AggSpec{{Func: "count", InputCol: "x", OutputCol: "g"}},
			want:    []string{"g", "x"},
		},
		{
			name:    "derived input expression identifiers are read",
			cols:    []string{"__agg_0", "l_extendedprice", "l_discount", "l_partkey"},
			groupBy: []string{"l_partkey"},
			specs: []AggSpec{{
				Func: "sum", InputCol: "__agg_0", OutputCol: "__agg_0",
				InputExpr: "l_extendedprice * (1 - l_discount)",
			}},
			want: []string{"l_extendedprice", "l_discount", "l_partkey"},
		},
		{
			name:    "output referenced by scan filter is kept",
			cols:    []string{"o", "x", "g"},
			groupBy: []string{"g"},
			specs:   []AggSpec{{Func: "sum", InputCol: "x", OutputCol: "o"}},
			filters: []string{"o > 5"},
			want:    []string{"o", "x", "g"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := append([]string(nil), tc.cols...)
			got := pruneFusedAggOutputCols(in, tc.groupBy, tc.specs, tc.filters)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestFusedScanAggReadSet_Q18 pins the plan-level effect: the fused
// scan-aggregate's read set must not carry the HAVING synthetic — one
// unknown name reverts the worker's parquet projection to full width
// (143 B/row vs ~25 B/row on Q18's 600M-row fused lineitem leg at SF100).
func TestFusedScanAggReadSet_Q18(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	sql := `SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
	SUM(l_quantity) as total_qty
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON o_orderkey = l_orderkey
WHERE o_orderkey IN (
	SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
)
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100`
	stages := sqlToStages(t, cat, ctx, sql, 3)
	fused := 0
	for _, s := range stages {
		if s.Type != StageScan || len(s.FusedAggSpecs) == 0 {
			continue
		}
		fused++
		for _, spec := range s.FusedAggSpecs {
			for _, c := range s.Columns {
				if c == spec.OutputCol && c != spec.InputCol {
					t.Errorf("fused scan %s read set %v contains agg output %q", s.ID, s.Columns, spec.OutputCol)
				}
			}
		}
	}
	if fused == 0 {
		t.Fatal("no fused scan-aggregate stage in Q18 plan (shape changed?)")
	}
}
