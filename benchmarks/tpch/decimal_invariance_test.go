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
		// No count-only relaxation for Q02/Q22, unlike the FLOAT64 arm: its
		// reason there is that membership at a float threshold shifts with
		// accumulation order, and on exact fixed-point the threshold is one
		// exact value. See decimalCorpus.
		queries = append(queries, oracle.Query{Name: fmt.Sprintf("Q%02d", n), SQL: TPCHQueries[n].SQL})
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
//
// Arm A is the EMBEDDED engine, not the coordinator with its fast path
// enabled. That is not a shortcut, it is the point: for Q15 and Q22 the
// fast-path coordinator DECLINES the plan and answers from the stage DAG, so
// using it as "the single-process arm" compares the DAG against itself. This
// suite reported both queries as agreeing that way while the single-process
// engine disagreed with both of them (#696) — a blind spot the arms' names
// hid. The embedded DB is the same pipeline the fast path would run, and it
// is unambiguously not the DAG.
func TestTwoPathInvarianceDecimal(t *testing.T) {
	if testing.Short() {
		t.Skip("stands up a three-worker cluster")
	}
	ctx := context.Background()
	data := decimalFixtureRows(t)
	_, dag := setupClusterFixture(t, ctx, DecimalFixture, data)
	embedded := openFixtureDB(t, ctx, DecimalFixture, data)

	for _, n := range decimalCorpusQueries(t) {
		name := fmt.Sprintf("Q%02d", n)
		sql := TPCHQueries[n].SQL
		t.Run(name, func(t *testing.T) {
			q := twoPathQuery{name: name, sql: sql, cmp: cmpUnordered}
			if hasTopLevelOrderBy(sql) {
				q.cmp = cmpOrdered
			}
			// No count-only relaxation for Q02/Q22 — see decimalCorpus. The
			// only count-only entries are the ones a trailing LIMIT creates,
			// where rows tied at the cut are genuinely interchangeable
			// between two correct paths.
			if trailingLimit(sql) > 0 {
				q.cmp, q.limit = cmpCount, trailingLimit(sql)
			}

			var aRows []map[string]any
			var aCols []string
			aRes, aErr := embedded.Query(ctx, sql)
			if aErr == nil {
				aRows, aCols = aRes.Rows, aRes.Columns
			}
			bRows, bCols, bErr := runArm(t, ctx, dag, sql)

			why, pinned := twoPathDecimalPin(n)
			if !pinned {
				if aErr != nil {
					t.Fatalf("arm A (single-process): %v", aErr)
				}
				if bErr != nil {
					t.Fatalf("arm B (stage DAG): %v", bErr)
				}
				compareArms(t, q, aRows, aCols, bRows, bCols)
				return
			}
			// A pin here is a RATCHET, not a skip (ADR-0013): the query
			// still runs, and the subtest FAILS the day the two paths start
			// agreeing, because that is when the pin has outlived the bug.
			if aErr != nil || bErr != nil {
				t.Logf("PINNED (an arm still cannot run, as recorded): A=%v B=%v\n  %s", aErr, bErr, why)
				return
			}
			diff := twoPathDisagreement(q, aRows, aCols, bRows, bCols)
			if diff == "" {
				t.Fatalf("the two paths now AGREE on %s, but it is still pinned as %q — delete its "+
					"entry from twoPathDecimalPin so the query is gated again", name, why)
			}
			t.Logf("PINNED (the paths still disagree, as recorded): %s\n  %s", diff, why)
		})
	}
}

// twoPathDecimalPin names the queries whose two execution paths do NOT agree
// with each other today under the DECIMAL fixture, with the issue each is
// filed as. It is deliberately NOT decimalPin: that one records divergence
// from DUCKDB, which is a different question. Q08 diverges from DuckDB on
// both arms (#695's wrong carrier) and the two arms agree with EACH OTHER, so
// it belongs there and not here.
func twoPathDecimalPin(n int) (why string, ok bool) {
	switch n {
	case 14:
		return "#695 — a CASE over a DECIMAL column and a numeric literal declares the literal's type, " +
			"so neither path can run this query at all.", true
	case 15:
		return scalarSubqueryBug + " Arm A is right here and arm B answers 0 rows.", true
	case 22:
		return scalarSubqueryBug + " Both arms are wrong and differently: arm B answers 0 rows, arm A " +
			"the right number of groups with inflated membership.", true
	}
	return "", false
}

// twoPathDisagreement returns a description of the first difference between
// the two arms, or "" when they agree. It is compareArms's comparison without
// the reporting, so a pin can assert that a divergence is STILL there.
func twoPathDisagreement(q twoPathQuery, aRows []map[string]any, aCols []string, bRows []map[string]any, bCols []string) string {
	if q.cmp == cmpCount {
		if diff := len(bRows) - len(aRows); diff < -q.tolerance || diff > q.tolerance {
			return fmt.Sprintf("row count A=%d B=%d (±%d allowed)", len(aRows), len(bRows), q.tolerance)
		}
		return ""
	}
	aligned, missing := realign(bRows, aCols)
	if missing != "" {
		return fmt.Sprintf("arm B has no column %q (A: %v, B: %v)", missing, aCols, bCols)
	}
	a := &oracle.Result{Columns: aCols, Rows: aRows}
	b := &oracle.Result{Columns: aCols, Rows: aligned}
	var canonA, canonB *oracle.Canon
	if q.cmp == cmpOrdered {
		canonA, canonB = oracle.CanonicalizeOrdered(a), oracle.CanonicalizeOrdered(b)
	} else {
		canonA, canonB = oracle.Canonicalize(a), oracle.Canonicalize(b)
	}
	return canonA.Diff(canonB, oracle.Query{Name: q.name})
}
