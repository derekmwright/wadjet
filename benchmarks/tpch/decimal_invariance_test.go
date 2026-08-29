package tpch

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// The invariance suites under the DECIMAL(15,2) fixture (ADR-0024).
//
// Both of these already run over the FLOAT64 fixture. Running them again over
// the decimal one is not duplication: an optimization that is invariant on
// float64 plans can still change a decimal plan's row set, because the
// DECIMAL type resolution decides join key widths, scan-filter literal
// comparisons and aggregate state layout differently — and none of those code
// paths had a benchmark before this fixture existed.

// decimalCorpusQueries is the 22 query numbers in order. Nothing is filtered
// here on the strength of a pin: a query can be pinned for a DECLARED-TYPE
// defect and still answer, and both suites below want it. What each suite
// drops is what it observes cannot RUN, which is data-dependent (#695 errors
// on Q08 over this generator and answers over the dbgen fixture) and so
// cannot be a static list.
func decimalCorpusQueries(t *testing.T) []int {
	t.Helper()
	nums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	return nums
}

// TestTPCHOptimizationInvarianceDecimal is the kill-switch differential
// oracle (#287) over the DECIMAL(15,2) fixture: every query with each
// registered optimization individually disabled must answer what it answers
// with them all on. A failing subtest names the diverging toggle.
func TestTPCHOptimizationInvarianceDecimal(t *testing.T) {
	if testing.Short() {
		t.Skip("sweeps every kill switch over the corpus")
	}
	db := setupTPCHFixture(t, SF001, DecimalFixture)
	ctx := context.Background()

	// The corpus is probed rather than filtered by pin: #695 is
	// data-dependent (Q08's CASE errors on this generator, where its decimal
	// branch does fire, and answers on the dbgen fixture, where it does
	// not), so a static exclusion list would be wrong on one of the two.
	// A query that errors is not an invariance result, and every one of them
	// is already named by a pin in decimal_variant_test.go.
	nums := make([]int, 0, len(TPCHQueries))
	var excluded []string
	for _, n := range decimalCorpusQueries(t) {
		if _, err := db.Query(ctx, TPCHQueries[n].SQL); err != nil {
			excluded = append(excluded, fmt.Sprintf("Q%02d (%v)", n, err))
			continue
		}
		nums = append(nums, n)
	}
	if len(excluded) > 0 {
		t.Logf("not swept, the DECIMAL carrier cannot run these yet (pinned in decimal_variant_test.go): %v", excluded)
	}

	queries := make([]oracle.Query, 0, len(nums))
	for _, n := range nums {
		q := oracle.Query{Name: fmt.Sprintf("Q%02d", n), SQL: TPCHQueries[n].SQL}
		// Q02/Q22 select rows by comparing against an aggregate threshold,
		// so membership at the boundary shifts with accumulation order —
		// the same relaxation the FLOAT64 arm applies.
		if n == 2 || n == 22 {
			q.CountOnly = true
			q.Tolerance = 4
		}
		queries = append(queries, q)
	}

	// Shapes the 22 leave dark on this carrier specifically: a DECIMAL
	// GROUP BY key, a DECIMAL join key, a DECIMAL scan-filter literal, a
	// DECIMAL DISTINCT and a DECIMAL window. Each is a place an
	// optimization decides something about a decimal that it decides
	// differently about a float64.
	queries = append(queries,
		oracle.Query{Name: "DecGroupKey", SQL: "SELECT l_discount, COUNT(*) AS c, SUM(l_extendedprice) AS s FROM lineitem GROUP BY l_discount ORDER BY l_discount"},
		oracle.Query{Name: "DecScanFilter", SQL: "SELECT COUNT(*) AS c FROM lineitem WHERE l_extendedprice > 50000.00 AND l_discount <= 0.05"},
		oracle.Query{Name: "DecDistinct", SQL: "SELECT DISTINCT l_tax FROM lineitem ORDER BY l_tax"},
		oracle.Query{Name: "DecJoinKey", SQL: "SELECT COUNT(*) AS c FROM lineitem JOIN orders ON l_orderkey = o_orderkey AND l_extendedprice = o_totalprice"},
		oracle.Query{Name: "DecWindow", SQL: "SELECT l_returnflag, SUM(SUM(l_extendedprice)) OVER (PARTITION BY l_returnflag) AS w FROM lineitem GROUP BY l_returnflag ORDER BY l_returnflag"},
		oracle.Query{Name: "DecOrderBy", SQL: "SELECT o_totalprice FROM orders ORDER BY o_totalprice DESC, o_orderkey LIMIT 25"},
	)

	oracle.RunDifferential(ctx, t, oracle.ExpandLimits(queries), func(ctx context.Context, sql string) (*oracle.Result, error) {
		res, err := db.Query(ctx, sql)
		if err != nil {
			return nil, err
		}
		return &oracle.Result{Columns: res.Columns, Rows: res.Rows}, nil
	})
}

// TestTwoPathInvarianceDecimal runs the corpus on the single-process path and
// on the stage DAG over the DECIMAL fixture and requires the two to agree.
// TestTPCHQueriesDecimal already holds both arms against DuckDB; this holds
// them against EACH OTHER, which is what catches a divergence the two arms
// would otherwise have to be wrong about in the same direction to hide.
func TestTwoPathInvarianceDecimal(t *testing.T) {
	if testing.Short() {
		t.Skip("stands up a three-worker cluster")
	}
	ctx := context.Background()
	data := decimalFixtureRows(t)
	fast, dag := setupClusterFixture(t, ctx, DecimalFixture, data)
	embedded := openFixtureDB(t, ctx, DecimalFixture, data)

	for _, n := range decimalCorpusQueries(t) {
		name := fmt.Sprintf("Q%02d", n)
		sql := TPCHQueries[n].SQL
		t.Run(name, func(t *testing.T) {
			q := twoPathQuery{name: name, sql: sql, cmp: cmpUnordered}
			if hasTopLevelOrderBy(sql) {
				q.cmp = cmpOrdered
			}
			if n == 2 || n == 22 {
				q.cmp, q.tolerance = cmpCount, 4
			}
			if trailingLimit(sql) > 0 {
				// Rows tied at the cut are interchangeable between two
				// correct paths; the count is not.
				q.cmp, q.limit = cmpCount, trailingLimit(sql)
			}
			if why, arm, pinned := decimalPin(n); pinned && arm == armDAG {
				t.Skipf("PINNED, stage DAG: %s", why)
			}

			aRows, aCols, err := runArm(t, ctx, fast, sql)
			if err != nil {
				// The fast path declines to route some subquery plans; the
				// embedded engine is the same single-process pipeline.
				res, eerr := embedded.Query(ctx, sql)
				if eerr != nil {
					if why, _, pinned := decimalPin(n); pinned {
						t.Skipf("PINNED, the DECIMAL carrier cannot run this: %v\n  %s", eerr, why)
					}
					t.Fatalf("arm A: coordinator %v; embedded %v", err, eerr)
				}
				aRows, aCols = res.Rows, res.Columns
			}
			bRows, bCols, err := runArm(t, ctx, dag, sql)
			if err != nil {
				t.Fatalf("arm B (stage DAG): %v", err)
			}
			compareArms(t, q, aRows, aCols, bRows, bCols)
		})
	}
}
