package tpch

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// TestTPCHOptimizationInvariance is the kill-switch differential oracle
// (#287) over the TPC-H corpus at SF0.01: all 22 queries run with every
// registered optimization enabled, then once per kill switch with just
// that optimization disabled, then with all disabled — results must be
// identical. A failing subtest names the diverging optimization.
func TestTPCHOptimizationInvariance(t *testing.T) {
	db := setupTPCH(t, SF001)
	ctx := context.Background()

	queryNums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	queries := make([]oracle.Query, 0, len(queryNums))
	for _, qNum := range queryNums {
		q := oracle.Query{
			Name: fmt.Sprintf("Q%02d", qNum),
			SQL:  TPCHQueries[qNum].SQL,
		}
		// Q02/Q22: float-threshold row membership shifts with accumulation
		// order between two correct runs (same tolerance as TestTPCHQueries).
		if qNum == 2 || qNum == 22 {
			q.CountOnly = true
			q.Tolerance = 4
		}
		queries = append(queries, q)
	}

	// Offsets-shape corpus: TPC-H's 22 queries contain no LENGTH(), no
	// IS NULL, and no empty-string comparison, so nothing in the canonical
	// set would exercise the lengths-only decode arm — an oracle arm that
	// never fires proves nothing. These are ad-hoc (kept out of
	// TPCHQueries, which feeds the correctness gate and the banked
	// baselines) and cover each member of the class over the .._comment
	// columns, plus mixed shape/value use that must stay on the full
	// decode.
	queries = append(queries,
		oracle.Query{Name: "ShapeLen01", SQL: "SELECT l_returnflag, AVG(LENGTH(l_comment)) AS avglen, COUNT(*) AS c FROM lineitem WHERE l_comment <> '' GROUP BY l_returnflag ORDER BY l_returnflag"},
		oracle.Query{Name: "ShapeLen02", SQL: "SELECT o_orderpriority, SUM(octet_length(o_comment)) AS s, MAX(LENGTH(o_comment)) AS m FROM orders GROUP BY o_orderpriority ORDER BY o_orderpriority"},
		oracle.Query{Name: "ShapeNull03", SQL: "SELECT COUNT(*) AS c FROM part WHERE p_comment IS NOT NULL"},
		oracle.Query{Name: "ShapeCount04", SQL: "SELECT c_mktsegment, COUNT(c_comment) AS c FROM customer GROUP BY c_mktsegment ORDER BY c_mktsegment"},
		oracle.Query{Name: "ShapeEmpty05", SQL: "SELECT COUNT(*) AS c FROM supplier WHERE s_comment = ''"},
		oracle.Query{Name: "ShapeMixed06", SQL: "SELECT MIN(l_comment) AS lo, MIN(LENGTH(l_comment)) AS lolen FROM lineitem"},
		oracle.Query{Name: "ShapeRune07", SQL: "SELECT SUM(char_length(l_comment)) AS runes, SUM(LENGTH(l_comment)) AS bytes FROM lineitem"},
	)

	pushdownsBefore := physical.ScanFilterPushdowns.Load()
	shapeOnlyBefore := physical.ShapeOnlyColumnsPlanned.Load()
	oracle.RunDifferential(ctx, t, oracle.ExpandLimits(queries), func(ctx context.Context, sql string) (*oracle.Result, error) {
		res, err := db.Query(ctx, sql)
		if err != nil {
			return nil, err
		}
		return &oracle.Result{Columns: res.Columns, Rows: res.Rows}, nil
	})
	// Engagement check: the corpus must actually exercise scan-filter
	// pushdown, or the scan-filter arm proves nothing.
	if physical.ScanFilterPushdowns.Load() == pushdownsBefore {
		t.Error("scan-filter pushdown never engaged across the corpus — oracle arm is dormant")
	}
	if physical.ShapeOnlyColumnsPlanned.Load() == shapeOnlyBefore {
		t.Error("shape-only column analysis never engaged across the corpus — lengths-only-decode arm is dormant")
	}
}
