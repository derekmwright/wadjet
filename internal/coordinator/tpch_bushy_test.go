package coordinator

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/planner/logical"
)

// TestTPCHNativeDAG_BushyForced is the distributed Layer B parity gate from
// docs/design/bushy-join-cbo.md §4: the full native-DAG suite with
// BushyJoinReorder forced on. The May 2026 bushy attempt returned wrong rows
// on Q02/Q07/Q08/Q09/Q21 precisely on this path — row-count regressions
// here block the flag, cheaply.
func TestTPCHNativeDAG_BushyForced(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TPCH SF0.01 bushy native-DAG suite in short mode")
	}
	logical.BushyJoinReorder.Store(true)
	defer logical.BushyJoinReorder.Store(false)

	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cat := coord.catalog

	data := tpch.Generate(tpch.SF001)
	tableOrder := []string{"region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"}
	for _, table := range tableOrder {
		rows := data[table]
		if rows == nil {
			t.Fatalf("datagen missing table %s", table)
		}
		ingestTPCHTable(t, ctx, store, cat, table, tpch.AllTables[table], rows)
	}

	expected := map[int]int{
		1: 6, 2: 5, 3: 10, 4: 5, 5: 5, 6: 1, 7: 4, 8: 2,
		9: 150, 10: 20, 11: 235, 12: 2, 13: 100, 14: 1, 15: 1,
		16: 293, 17: 1, 18: 0, 19: 1, 20: 3, 21: 1, 22: 7,
	}
	tol := map[int]int{2: 4, 22: 4}

	qNums := make([]int, 0, len(tpch.TPCHQueries))
	for n := range tpch.TPCHQueries {
		qNums = append(qNums, n)
	}
	sort.Ints(qNums)

	plannedBefore := logical.BushyJoinsPlanned.Load()
	var failures []string
	for _, qNum := range qNums {
		q := tpch.TPCHQueries[qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", qNum, q.Name), func(t *testing.T) {
			res, err := coord.ExecuteSQL(ctx, q.SQL)
			if err != nil {
				failures = append(failures, fmt.Sprintf("Q%02d: %v", qNum, err))
				t.Fatalf("bushy native-DAG Q%02d: %v", qNum, err)
			}
			got := int(res.TotalRows)
			want, ok := expected[qNum]
			if !ok {
				t.Logf("Q%02d returned %d rows (no expected)", qNum, got)
				return
			}
			tolerance := tol[qNum]
			diff := got - want
			if diff < -tolerance || diff > tolerance {
				failures = append(failures, fmt.Sprintf("Q%02d: got %d rows, want %d (±%d)", qNum, got, want, tolerance))
				t.Errorf("Q%02d row count: got %d, want %d (±%d)", qNum, got, want, tolerance)
			}
		})
	}
	if len(failures) > 0 {
		t.Logf("failing queries summary:\n  %v", failures)
	}
	if planned := logical.BushyJoinsPlanned.Load() - plannedBefore; planned == 0 {
		t.Fatal("bushy flag planned zero bushy joins across the distributed suite — gate proved nothing")
	} else {
		t.Logf("bushy join orders chosen across the distributed suite: %d", planned)
	}
}
