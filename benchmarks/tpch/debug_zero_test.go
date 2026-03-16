package tpch

import (
	"context"
	"testing"
	"time"
)

func TestDebugZeroRows(t *testing.T) {
	db := setupTPCH(t, SF001)
	ctx := context.Background()

	run := func(label, sql string) {
		t.Helper()
		ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		result, err := db.Query(ctx2, sql)
		if err != nil {
			t.Logf("  %s: ERROR: %v", label, err)
			return
		}
		t.Logf("  %s: %d rows", label, len(result.Rows))
		for i, r := range result.Rows {
			if i >= 5 {
				t.Logf("  ... (%d more)", len(result.Rows)-5)
				break
			}
			t.Logf("    row %d: %v", i, r)
		}
	}

	t.Log("=== Q12 Debug ===")
	run("two col compare",
		"SELECT COUNT(*) as cnt FROM lineitem WHERE l_commitdate < l_receiptdate AND l_shipdate < l_commitdate")
	run("IN + receiptdate range",
		"SELECT COUNT(*) as cnt FROM lineitem WHERE l_shipmode IN ('MAIL', 'SHIP') AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'")
	run("three conditions",
		"SELECT COUNT(*) as cnt FROM lineitem WHERE l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'")
	run("all four conditions",
		"SELECT COUNT(*) as cnt FROM lineitem WHERE l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'")
	run("sample dates",
		"SELECT l_shipdate, l_commitdate, l_receiptdate FROM lineitem LIMIT 5")

	t.Log("=== Q11 Aggregate Debug ===")
	run("simple SUM no multiply",
		"SELECT SUM(ps_supplycost * ps_availqty) as total FROM partsupp JOIN supplier ON ps_suppkey = s_suppkey JOIN nation ON s_nationkey = n_nationkey WHERE n_name = 'GERMANY'")
	run("SUM with multiply by 0.0001",
		"SELECT SUM(ps_supplycost * ps_availqty) * 0.0001 as threshold FROM partsupp JOIN supplier ON ps_suppkey = s_suppkey JOIN nation ON s_nationkey = n_nationkey WHERE n_name = 'GERMANY'")
	run("count germany partsupp",
		"SELECT COUNT(*) as cnt FROM partsupp JOIN supplier ON ps_suppkey = s_suppkey JOIN nation ON s_nationkey = n_nationkey WHERE n_name = 'GERMANY'")

	t.Log("=== Q11 HAVING Debug ===")
	run("Q11 without HAVING",
		`SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) as value
		FROM partsupp JOIN supplier ON ps_suppkey = s_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name = 'GERMANY'
		GROUP BY ps_partkey
		ORDER BY value DESC LIMIT 5`)
	run("Q11 HAVING literal",
		`SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) as value
		FROM partsupp JOIN supplier ON ps_suppkey = s_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name = 'GERMANY'
		GROUP BY ps_partkey
		HAVING SUM(ps_supplycost * ps_availqty) > 57614
		ORDER BY value DESC LIMIT 5`)
	run("Q11 full with HAVING subquery",
		`SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) as value
		FROM partsupp JOIN supplier ON ps_suppkey = s_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name = 'GERMANY'
		GROUP BY ps_partkey
		HAVING SUM(ps_supplycost * ps_availqty) > (
			SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
			FROM partsupp JOIN supplier ON ps_suppkey = s_suppkey
			JOIN nation ON s_nationkey = n_nationkey
			WHERE n_name = 'GERMANY'
		)
		ORDER BY value DESC LIMIT 5`)
	run("Q11 HAVING simple subquery",
		`SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) as value
		FROM partsupp JOIN supplier ON ps_suppkey = s_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name = 'GERMANY'
		GROUP BY ps_partkey
		HAVING SUM(ps_supplycost * ps_availqty) > (
			SELECT MAX(ps_supplycost) FROM partsupp
		)
		ORDER BY value DESC LIMIT 5`)
	run("Q11 HAVING nested agg subquery",
		`SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) as value
		FROM partsupp JOIN supplier ON ps_suppkey = s_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name = 'GERMANY'
		GROUP BY ps_partkey
		HAVING SUM(ps_supplycost * ps_availqty) > (
			SELECT SUM(ps_supplycost) * 0.0001 FROM partsupp
		)
		ORDER BY value DESC LIMIT 5`)
	run("Q11 HAVING join subquery",
		`SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) as value
		FROM partsupp JOIN supplier ON ps_suppkey = s_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name = 'GERMANY'
		GROUP BY ps_partkey
		HAVING SUM(ps_supplycost * ps_availqty) > (
			SELECT SUM(ps_supplycost) FROM partsupp
			JOIN supplier ON ps_suppkey = s_suppkey
			JOIN nation ON s_nationkey = n_nationkey
			WHERE n_name = 'GERMANY'
		)
		ORDER BY value DESC LIMIT 5`)

	t.Log("=== Expression Aggregate Debug ===")
	run("SUM complex expr no GB",
		"SELECT SUM(l_extendedprice * (1 - l_discount)) as rev FROM lineitem")
	run("SUM complex expr with GB",
		"SELECT l_suppkey, SUM(l_extendedprice * (1 - l_discount)) as rev FROM lineitem GROUP BY l_suppkey LIMIT 3")
	run("SUM complex expr with GB alias",
		"SELECT l_suppkey as sk, SUM(l_extendedprice * (1 - l_discount)) as rev FROM lineitem GROUP BY l_suppkey LIMIT 3")

	t.Log("=== Aggregate Expression Debug ===")
	run("SUM of expr no GB",
		"SELECT SUM(ps_supplycost * ps_availqty) as val FROM partsupp LIMIT 5")
	run("SUM of expr with GB",
		"SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) as val FROM partsupp GROUP BY ps_partkey LIMIT 5")
	run("SUM of simple col no GB",
		"SELECT SUM(ps_supplycost) as val FROM partsupp")
	run("SUM of simple col with GB",
		"SELECT ps_partkey, SUM(ps_supplycost) as val FROM partsupp GROUP BY ps_partkey LIMIT 5")

	t.Log("=== Q22 SUBSTR Debug ===")
	run("SUBSTR basic", "SELECT SUBSTR('hello', 1, 2) as s")
	run("SUBSTR on column", "SELECT c_phone, SUBSTR(c_phone, 1, 2) as cc FROM customer LIMIT 3")
	run("SUBSTRING alias", "SELECT SUBSTRING(c_phone, 1, 2) as cc FROM customer LIMIT 3")
	run("sample phones", "SELECT c_phone FROM customer LIMIT 5")

	t.Log("=== Q15 Debug ===")
	run("Q15 CTE standalone",
		`WITH revenue AS (
			SELECT l_suppkey as supplier_no, SUM(l_extendedprice * (1 - l_discount)) as total_revenue
			FROM lineitem WHERE l_shipdate >= '1996-01-01' AND l_shipdate < '1996-04-01'
			GROUP BY l_suppkey
		)
		SELECT supplier_no, total_revenue FROM revenue ORDER BY total_revenue DESC LIMIT 3`)
	run("Q15 CTE with join",
		`WITH revenue AS (
			SELECT l_suppkey as supplier_no, SUM(l_extendedprice * (1 - l_discount)) as total_revenue
			FROM lineitem WHERE l_shipdate >= '1996-01-01' AND l_shipdate < '1996-04-01'
			GROUP BY l_suppkey
		)
		SELECT s_suppkey, s_name, total_revenue FROM supplier JOIN revenue ON s_suppkey = supplier_no
		ORDER BY total_revenue DESC LIMIT 3`)
	run("Q15 CTE MAX subquery",
		`WITH revenue AS (
			SELECT l_suppkey as supplier_no, SUM(l_extendedprice * (1 - l_discount)) as total_revenue
			FROM lineitem WHERE l_shipdate >= '1996-01-01' AND l_shipdate < '1996-04-01'
			GROUP BY l_suppkey
		)
		SELECT MAX(total_revenue) as max_rev FROM revenue`)
}

func TestDebugQ02(t *testing.T) {
	db := setupTPCH(t, SF001)
	ctx := context.Background()

	run := func(label, sql string) {
		t.Helper()
		ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		result, err := db.Query(ctx2, sql)
		if err != nil {
			t.Logf("  %s: ERROR: %v", label, err)
			return
		}
		t.Logf("  %s: %d rows", label, len(result.Rows))
		for i, r := range result.Rows {
			if i >= 10 {
				t.Logf("  ... (%d more)", len(result.Rows)-10)
				break
			}
			t.Logf("    row %d: %v", i, r)
		}
	}

	// ---- Step 1: Check base table row counts ----
	t.Log("=== Step 1: Base table counts ===")
	run("part count", "SELECT COUNT(*) as cnt FROM part")
	run("partsupp count", "SELECT COUNT(*) as cnt FROM partsupp")
	run("supplier count", "SELECT COUNT(*) as cnt FROM supplier")
	run("nation count", "SELECT COUNT(*) as cnt FROM nation")
	run("region count", "SELECT COUNT(*) as cnt FROM region")

	// ---- Step 2: Test individual filter conditions ----
	t.Log("=== Step 2: Filter conditions ===")
	run("p_size=15", "SELECT COUNT(*) as cnt FROM part WHERE p_size = 15")
	run("p_type LIKE '%BRASS'",
		"SELECT COUNT(*) as cnt FROM part WHERE p_type LIKE '%BRASS'")
	run("p_size=15 AND p_type LIKE '%BRASS'",
		"SELECT COUNT(*) as cnt FROM part WHERE p_size = 15 AND p_type LIKE '%BRASS'")
	run("sample p_type values",
		"SELECT DISTINCT p_type FROM part LIMIT 20")
	run("sample p_type ending in BRASS",
		"SELECT p_partkey, p_type, p_size FROM part WHERE p_type LIKE '%BRASS' LIMIT 10")
	run("r_name='EUROPE'", "SELECT COUNT(*) as cnt FROM region WHERE r_name = 'EUROPE'")
	run("EUROPE region row", "SELECT r_regionkey, r_name FROM region WHERE r_name = 'EUROPE'")
	run("nations in EUROPE",
		"SELECT n_nationkey, n_name, n_regionkey FROM nation JOIN region ON n_regionkey = r_regionkey WHERE r_name = 'EUROPE'")

	// ---- Step 3: Test the LIKE operator specifically ----
	t.Log("=== Step 3: LIKE operator tests ===")
	run("LIKE suffix match",
		"SELECT p_partkey, p_type FROM part WHERE p_type LIKE '%BRASS' LIMIT 5")
	run("LIKE exact match test",
		"SELECT COUNT(*) as cnt FROM part WHERE p_type = 'STANDARD ANODIZED BRASS'")
	run("all BRASS types",
		"SELECT DISTINCT p_type FROM part WHERE p_type LIKE '%BRASS'")

	// ---- Step 4: Test join chain without subquery ----
	t.Log("=== Step 4: Join chain (no subquery, no part filters) ===")
	run("partsupp-supplier join",
		"SELECT COUNT(*) as cnt FROM partsupp JOIN supplier ON s_suppkey = ps_suppkey")
	run("partsupp-supplier-nation join",
		"SELECT COUNT(*) as cnt FROM partsupp JOIN supplier ON s_suppkey = ps_suppkey JOIN nation ON s_nationkey = n_nationkey")
	run("4-way join (no region filter)",
		"SELECT COUNT(*) as cnt FROM partsupp JOIN supplier ON s_suppkey = ps_suppkey JOIN nation ON s_nationkey = n_nationkey JOIN region ON n_regionkey = r_regionkey")
	run("4-way join with EUROPE filter",
		"SELECT COUNT(*) as cnt FROM partsupp JOIN supplier ON s_suppkey = ps_suppkey JOIN nation ON s_nationkey = n_nationkey JOIN region ON n_regionkey = r_regionkey WHERE r_name = 'EUROPE'")

	// ---- Step 5: Full 5-way join WITHOUT the correlated subquery ----
	t.Log("=== Step 5: 5-way join without subquery ===")
	run("5-way join no filters",
		`SELECT COUNT(*) as cnt
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		JOIN supplier ON s_suppkey = ps_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey`)
	run("5-way join EUROPE only",
		`SELECT COUNT(*) as cnt
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		JOIN supplier ON s_suppkey = ps_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey
		WHERE r_name = 'EUROPE'`)
	run("5-way join EUROPE + p_size=15",
		`SELECT COUNT(*) as cnt
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		JOIN supplier ON s_suppkey = ps_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey
		WHERE r_name = 'EUROPE' AND p_size = 15`)
	run("5-way join EUROPE + p_size=15 + LIKE BRASS",
		`SELECT COUNT(*) as cnt
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		JOIN supplier ON s_suppkey = ps_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey
		WHERE r_name = 'EUROPE' AND p_size = 15 AND p_type LIKE '%BRASS'`)
	run("5-way join sample rows (no subquery)",
		`SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, ps_supplycost
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		JOIN supplier ON s_suppkey = ps_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey
		WHERE r_name = 'EUROPE' AND p_size = 15 AND p_type LIKE '%BRASS'
		ORDER BY s_acctbal DESC
		LIMIT 10`)

	// ---- Step 6: Test the subquery standalone ----
	t.Log("=== Step 6: Subquery standalone ===")
	run("MIN(ps_supplycost) for partkey=1 EUROPE",
		`SELECT MIN(ps_supplycost) as min_cost
		FROM partsupp
		JOIN supplier ON s_suppkey = ps_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey
		WHERE ps_partkey = 1 AND r_name = 'EUROPE'`)
	run("MIN(ps_supplycost) for partkey=2 EUROPE",
		`SELECT MIN(ps_supplycost) as min_cost
		FROM partsupp
		JOIN supplier ON s_suppkey = ps_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey
		WHERE ps_partkey = 2 AND r_name = 'EUROPE'`)
	run("all ps_supplycost for partkey=1",
		"SELECT ps_suppkey, ps_supplycost FROM partsupp WHERE ps_partkey = 1")
	run("MIN(ps_supplycost) all regions partkey=1",
		`SELECT MIN(ps_supplycost) as min_cost FROM partsupp WHERE ps_partkey = 1`)

	// ---- Step 7: Test correlated subquery with hardcoded supplycost ----
	t.Log("=== Step 7: Hardcoded supplycost comparison ===")
	// First, get a supplycost value we know exists from the 5-way join
	run("get a known supplycost",
		`SELECT ps_partkey, ps_supplycost
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		JOIN supplier ON s_suppkey = ps_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey
		WHERE r_name = 'EUROPE' AND p_size = 15 AND p_type LIKE '%BRASS'
		LIMIT 5`)
	// Use a literal 0 to test the = comparison mechanics (should return 0 rows since costs > 0)
	run("supplycost = 0 (expect 0 rows)",
		`SELECT COUNT(*) as cnt
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		JOIN supplier ON s_suppkey = ps_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey
		WHERE r_name = 'EUROPE' AND p_size = 15 AND p_type LIKE '%BRASS'
			AND ps_supplycost = 0`)
	// Use a very high threshold to see if any rows survive
	run("supplycost < 9999 (should match all)",
		`SELECT COUNT(*) as cnt
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		JOIN supplier ON s_suppkey = ps_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey
		WHERE r_name = 'EUROPE' AND p_size = 15 AND p_type LIKE '%BRASS'
			AND ps_supplycost < 9999`)

	// ---- Step 8: Test the correlated subquery in WHERE ----
	t.Log("=== Step 8: Correlated subquery tests ===")
	// Simple uncorrelated scalar subquery in WHERE
	run("uncorrelated scalar subquery",
		`SELECT COUNT(*) as cnt
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		WHERE ps_supplycost = (SELECT MIN(ps_supplycost) FROM partsupp)`)
	// Correlated subquery (the actual Q02 pattern) - simpler version
	run("correlated subquery simple (no region filter)",
		`SELECT COUNT(*) as cnt
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		WHERE ps_supplycost = (
			SELECT MIN(ps_supplycost) FROM partsupp ps2
			WHERE ps2.ps_partkey = p_partkey
		)`)
	// Correlated subquery with joins in the subquery
	run("correlated subquery with joins",
		`SELECT COUNT(*) as cnt
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		JOIN supplier ON s_suppkey = ps_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey
		WHERE r_name = 'EUROPE'
			AND ps_supplycost = (
				SELECT MIN(ps_supplycost)
				FROM partsupp
				JOIN supplier ON s_suppkey = ps_suppkey
				JOIN nation ON s_nationkey = n_nationkey
				JOIN region ON n_regionkey = r_regionkey
				WHERE ps_partkey = p_partkey
					AND r_name = 'EUROPE'
			)`)

	// ---- Step 9: Deeper correlated subquery diagnostics ----
	t.Log("=== Step 9: Correlated subquery diagnostics ===")

	// Test if the subquery is even being recognized as correlated
	// Use a simpler 2-table case with known part key
	run("manual correlated check for partkey=36",
		`SELECT ps_supplycost, (SELECT MIN(ps_supplycost) FROM partsupp ps2 WHERE ps2.ps_partkey = 36) as min_cost
		FROM partsupp
		WHERE ps_partkey = 36`)

	// Now test if the correlated version returns rows at all (without the = comparison)
	run("correlated subquery result as column",
		`SELECT p_partkey, ps_supplycost,
			(SELECT MIN(ps_supplycost) FROM partsupp ps2 WHERE ps2.ps_partkey = p_partkey) as min_cost
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		WHERE p_partkey <= 3`)

	// Test: is the problem with the = comparison, or with the subquery returning NULL?
	run("correlated subquery IS NOT NULL test",
		`SELECT COUNT(*) as cnt
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		WHERE p_partkey <= 5
			AND (SELECT MIN(ps_supplycost) FROM partsupp ps2 WHERE ps2.ps_partkey = p_partkey) IS NOT NULL`)

	// Test with a very simple correlated subquery (single table, no joins)
	run("simplest correlated: partsupp self-ref",
		`SELECT ps_partkey, ps_supplycost
		FROM partsupp
		WHERE ps_supplycost = (SELECT MIN(ps_supplycost) FROM partsupp ps2 WHERE ps2.ps_partkey = ps_partkey)
		LIMIT 5`)

	// Test: does table aliasing affect detection?
	run("correlated with explicit table ref in outer",
		`SELECT COUNT(*) as cnt
		FROM part p1
		JOIN partsupp ON p1.p_partkey = ps_partkey
		WHERE ps_supplycost = (
			SELECT MIN(ps_supplycost) FROM partsupp ps2
			WHERE ps2.ps_partkey = p1.p_partkey
		)`)

	// ---- Step 10: Type and comparison diagnostics ----
	t.Log("=== Step 10: Type and equality diagnostics ===")

	// Check if the issue is type mismatch between ps_supplycost and correlated subquery result
	// Compare ps_supplycost to a literal that equals what the subquery should return
	// For partkey=1, min_cost=337.73 (from step 9)
	run("literal supplycost=337.73 for partkey=1",
		`SELECT ps_partkey, ps_supplycost
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		WHERE p_partkey = 1 AND ps_supplycost = 337.73`)

	// Test: compare correlated subquery result with CAST
	run("correlated eq check with explicit partkeys",
		`SELECT p_partkey, ps_supplycost,
			(SELECT MIN(ps_supplycost) FROM partsupp ps2 WHERE ps2.ps_partkey = p_partkey) as min_cost,
			CASE WHEN ps_supplycost = (SELECT MIN(ps_supplycost) FROM partsupp ps2 WHERE ps2.ps_partkey = p_partkey)
				THEN 'MATCH' ELSE 'NO_MATCH' END as matches
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		WHERE p_partkey = 1`)

	// Test WHERE predicate with correlated subquery as inequality (not equals)
	run("correlated subquery with >= instead of =",
		`SELECT COUNT(*) as cnt
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		WHERE p_partkey <= 3
			AND ps_supplycost >= (
				SELECT MIN(ps_supplycost) FROM partsupp ps2
				WHERE ps2.ps_partkey = p_partkey
			)`)

	// Test WHERE predicate with correlated subquery as < (should filter some)
	run("correlated subquery with < comparison",
		`SELECT COUNT(*) as cnt
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		WHERE p_partkey <= 3
			AND ps_supplycost < (
				SELECT MAX(ps_supplycost) FROM partsupp ps2
				WHERE ps2.ps_partkey = p_partkey
			)`)

	// Test = with correlated subquery returning MAX instead of MIN
	run("correlated subquery = MAX",
		`SELECT COUNT(*) as cnt
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		WHERE p_partkey <= 3
			AND ps_supplycost = (
				SELECT MAX(ps_supplycost) FROM partsupp ps2
				WHERE ps2.ps_partkey = p_partkey
			)`)

	// The critical comparison: use the correlated subquery in WHERE with =
	run("correlated subquery = MIN for partkey 1-3",
		`SELECT p_partkey, ps_supplycost
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		WHERE p_partkey <= 3
			AND ps_supplycost = (
				SELECT MIN(ps_supplycost) FROM partsupp ps2
				WHERE ps2.ps_partkey = p_partkey
			)`)

	// ---- Step 11: Predicate pushdown confirmation ----
	t.Log("=== Step 11: Predicate pushdown confirmation ===")

	// THEORY: predicate pushdown pushes "ps_supplycost = (correlated subquery)"
	// down to the partsupp scan side because collectColTableRefs doesn't walk
	// SubqueryNode, so it thinks only partsupp is referenced. Once pushed down,
	// p_partkey is not available in the batch, causing the correlated subquery
	// to get NULL for the outer ref.

	// Test A: correlated subquery where both sides reference the SAME table (partsupp)
	// This should work even with incorrect pushdown because the column IS available
	run("same-table correlated (no cross-table ref)",
		`SELECT ps_partkey, ps_supplycost
		FROM partsupp
		WHERE ps_supplycost = (
			SELECT MIN(ps_supplycost) FROM partsupp ps2
			WHERE ps2.ps_partkey = ps_partkey
		)
		LIMIT 5`)

	// Test B: force no pushdown by adding a ref to both tables in the predicate
	// If the outer column ref from the subquery is not detected, adding an explicit
	// cross-table ref in the outer predicate should keep it above the join
	run("cross-table ref prevents pushdown",
		`SELECT p_partkey, ps_supplycost
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		WHERE p_partkey <= 3
			AND p_partkey = ps_partkey
			AND ps_supplycost = (
				SELECT MIN(ps_supplycost) FROM partsupp ps2
				WHERE ps2.ps_partkey = p_partkey
			)`)

	// Test C: single-table query (no join, so no pushdown possible)
	run("single-table correlated (no join to push through)",
		`SELECT ps_partkey, ps_supplycost
		FROM partsupp
		WHERE ps_partkey <= 3
			AND ps_supplycost = (
				SELECT MIN(ps_supplycost) FROM partsupp ps2
				WHERE ps2.ps_partkey = ps_partkey
			)`)

	// ---- Step 12: Conclusive predicate pushdown test ----
	t.Log("=== Step 12: Conclusive pushdown test ===")

	// ROOT CAUSE TEST: If the predicate has a subquery, collectColTableRefs
	// doesn't handle SubqueryNode and only sees outer column refs (e.g., ps_supplycost).
	// This causes the predicate to be pushed down to the wrong scan node.
	//
	// Test: rewrite the query so the comparison column is from the SAME table
	// as the correlated ref (both from part). If pushdown is the issue, this
	// should push the predicate to part's scan where p_partkey IS available.
	// But correlation detection would still fail if partsupp is in inner tables.
	//
	// Instead, test: use a column from 'part' in the outer comparison
	// p_retailprice = (subquery using p_partkey). The predicate refs would be
	// {part} only, pushed to the part scan. If correlated ref also from part,
	// both are available on the same scan.
	run("same-side pushdown (both refs from part)",
		`SELECT p_partkey, p_retailprice
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		WHERE p_partkey <= 3
			AND p_retailprice = (
				SELECT MAX(p_retailprice) FROM part p2
				WHERE p2.p_partkey = p_partkey
			)`)

	// ---- Step 13: Prevent pushdown by combining with cross-table ref in OR ----
	t.Log("=== Step 13: Prevent pushdown via non-splittable predicate ===")

	// If predicates are split on AND, each is evaluated independently for pushdown.
	// But an OR or a combined expression referencing both tables can't be pushed to one side.
	// We use: (ps_supplycost = (subquery) AND p_partkey IS NOT NULL) -- wrapping in parentheses
	// with a cross-table ref so it stays above the join.
	run("non-splittable predicate (CASE prevents pushdown)",
		`SELECT p_partkey, ps_supplycost
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		WHERE p_partkey <= 3
			AND CASE WHEN p_partkey > 0 THEN ps_supplycost ELSE -1 END = (
				SELECT MIN(ps_supplycost) FROM partsupp ps2
				WHERE ps2.ps_partkey = p_partkey
			)`)

	// ---- Step 14: The full Q02 query ----
	t.Log("=== Step 14: Full Q02 ===")
	run("Q02 full",
		`SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		JOIN supplier ON s_suppkey = ps_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey
		WHERE r_name = 'EUROPE'
			AND p_size = 15
			AND p_type LIKE '%BRASS'
			AND ps_supplycost = (
				SELECT MIN(ps_supplycost)
				FROM partsupp
				JOIN supplier ON s_suppkey = ps_suppkey
				JOIN nation ON s_nationkey = n_nationkey
				JOIN region ON n_regionkey = r_regionkey
				WHERE ps_partkey = p_partkey
					AND r_name = 'EUROPE'
			)
		ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
		LIMIT 100`)
}

// =============================================================================
// Q17 Debug: correlated subquery in WHERE with l_quantity < (subquery)
// =============================================================================

func TestDebugQ17(t *testing.T) {
	db := setupTPCH(t, SF001)
	ctx := context.Background()

	run := func(label, sql string) {
		t.Helper()
		ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		result, err := db.Query(ctx2, sql)
		if err != nil {
			t.Logf("  %s: ERROR: %v", label, err)
			return
		}
		t.Logf("  %s: %d rows", label, len(result.Rows))
		for i, r := range result.Rows {
			if i >= 10 {
				t.Logf("  ... (%d more)", len(result.Rows)-10)
				break
			}
			t.Logf("    row %d: %v", i, r)
		}
	}

	// ---- Step 1: Base data checks ----
	t.Log("=== Q17 Step 1: Base data ===")
	run("lineitem count", "SELECT COUNT(*) as cnt FROM lineitem")
	run("part count", "SELECT COUNT(*) as cnt FROM part")

	// ---- Step 2: Filter conditions ----
	t.Log("=== Q17 Step 2: Filter conditions ===")
	run("Brand#23 parts", "SELECT COUNT(*) as cnt FROM part WHERE p_brand = 'Brand#23'")
	run("MED BOX parts", "SELECT COUNT(*) as cnt FROM part WHERE p_container = 'MED BOX'")
	run("Brand#23 AND MED BOX", "SELECT COUNT(*) as cnt FROM part WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'")
	run("sample Brand#23 MED BOX",
		"SELECT p_partkey, p_brand, p_container FROM part WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX' LIMIT 5")

	// ---- Step 3: Base join without subquery ----
	t.Log("=== Q17 Step 3: Base join ===")
	run("lineitem-part join (all)",
		"SELECT COUNT(*) as cnt FROM lineitem JOIN part ON p_partkey = l_partkey")
	run("lineitem-part join with brand/container filter",
		"SELECT COUNT(*) as cnt FROM lineitem JOIN part ON p_partkey = l_partkey WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'")
	run("sample joined rows",
		`SELECT l_partkey, l_quantity, l_extendedprice, p_brand, p_container
		FROM lineitem JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
		LIMIT 5`)

	// ---- Step 4: Subquery standalone ----
	t.Log("=== Q17 Step 4: Subquery standalone ===")
	// Get a part key that matches Brand#23 + MED BOX
	run("get partkey",
		"SELECT p_partkey FROM part WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX' LIMIT 1")
	// Test AVG(l_quantity) for a specific partkey
	run("AVG l_quantity for partkey=1",
		"SELECT AVG(l_quantity) as avg_qty FROM lineitem WHERE l_partkey = 1")
	run("0.2 * AVG l_quantity for partkey=1",
		"SELECT 0.2 * AVG(l_quantity) as threshold FROM lineitem WHERE l_partkey = 1")

	// ---- Step 5: Correlated subquery as column ----
	t.Log("=== Q17 Step 5: Correlated subquery as column ===")
	run("subquery as column",
		`SELECT l_partkey, l_quantity,
			(SELECT 0.2 * AVG(l_quantity) FROM lineitem li2 WHERE li2.l_partkey = l_partkey) as threshold
		FROM lineitem
		WHERE l_partkey <= 3
		LIMIT 5`)

	// ---- Step 6: Correlated subquery in WHERE with literal threshold ----
	t.Log("=== Q17 Step 6: Literal threshold comparison ===")
	run("l_quantity < 10 (literal)",
		`SELECT SUM(l_extendedprice) / 7.0 as avg_yearly
		FROM lineitem JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
			AND l_quantity < 10`)

	// ---- Step 7: Correlated subquery in WHERE ----
	t.Log("=== Q17 Step 7: Correlated subquery in WHERE ===")
	// Test without join first (single table)
	run("single-table correlated",
		`SELECT COUNT(*) as cnt FROM lineitem
		WHERE l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem li2 WHERE li2.l_partkey = l_partkey)`)
	// Test with join
	run("cross-table correlated (the Q17 pattern)",
		`SELECT COUNT(*) as cnt
		FROM lineitem JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
			AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)`)

	// ---- Step 8: Check if the problem is correlation detection ----
	t.Log("=== Q17 Step 8: Correlation detection ===")
	// The subquery is: SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey
	// p_partkey is from the outer 'part' table. But the subquery's inner table
	// is also 'lineitem', and l_partkey is an inner column. The correlation
	// ref is p_partkey (unqualified), which maps to the 'part' table.
	// If FindCorrelatedRefs doesn't detect p_partkey as an outer ref because
	// both the outer query and subquery have a 'lineitem' table, p_partkey
	// might be misidentified.

	// Test: explicitly qualify the outer ref
	run("qualified outer ref",
		`SELECT COUNT(*) as cnt
		FROM lineitem JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
			AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem li2 WHERE li2.l_partkey = part.p_partkey)`)

	// Test: use column alias to avoid name clash
	run("no-clash subquery ref",
		`SELECT COUNT(*) as cnt
		FROM lineitem JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
			AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem li2 WHERE li2.l_partkey = l_partkey)`)

	// ---- Step 9: The full Q17 ----
	t.Log("=== Q17 Step 9: Full Q17 ===")
	run("Q17 full",
		`SELECT SUM(l_extendedprice) / 7.0 as avg_yearly
		FROM lineitem JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
			AND l_quantity < (
				SELECT 0.2 * AVG(l_quantity)
				FROM lineitem
				WHERE l_partkey = p_partkey
			)`)
}

// =============================================================================
// Q20 Debug: nested IN subqueries + correlated scalar subquery
// =============================================================================

func TestDebugQ20(t *testing.T) {
	db := setupTPCH(t, SF001)
	ctx := context.Background()

	run := func(label, sql string) {
		t.Helper()
		ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		result, err := db.Query(ctx2, sql)
		if err != nil {
			t.Logf("  %s: ERROR: %v", label, err)
			return
		}
		t.Logf("  %s: %d rows", label, len(result.Rows))
		for i, r := range result.Rows {
			if i >= 10 {
				t.Logf("  ... (%d more)", len(result.Rows)-10)
				break
			}
			t.Logf("    row %d: %v", i, r)
		}
	}

	// ---- Step 1: Base data ----
	t.Log("=== Q20 Step 1: Base data ===")
	run("supplier-nation for CANADA",
		"SELECT COUNT(*) as cnt FROM supplier JOIN nation ON s_nationkey = n_nationkey WHERE n_name = 'CANADA'")
	run("sample CANADA suppliers",
		"SELECT s_suppkey, s_name FROM supplier JOIN nation ON s_nationkey = n_nationkey WHERE n_name = 'CANADA' LIMIT 5")

	// ---- Step 2: Innermost subquery: part names matching forest% ----
	t.Log("=== Q20 Step 2: Part name filter ===")
	run("parts with forest%",
		"SELECT COUNT(*) as cnt FROM part WHERE p_name LIKE 'forest%'")
	run("sample forest parts",
		"SELECT p_partkey, p_name FROM part WHERE p_name LIKE 'forest%' LIMIT 5")

	// ---- Step 3: IN (SELECT p_partkey) standalone ----
	t.Log("=== Q20 Step 3: Inner IN subquery ===")
	run("partsupp with forest parts (IN subquery)",
		`SELECT COUNT(*) as cnt FROM partsupp
		WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')`)
	run("sample partsupp forest",
		`SELECT ps_partkey, ps_suppkey, ps_availqty FROM partsupp
		WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
		LIMIT 5`)

	// ---- Step 4: Correlated subquery: SUM(l_quantity) ----
	t.Log("=== Q20 Step 4: Correlated SUM subquery ===")
	run("SUM l_quantity for specific partkey/suppkey",
		`SELECT 0.5 * SUM(l_quantity) as threshold
		FROM lineitem
		WHERE l_partkey = 1 AND l_suppkey = 1
			AND l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'`)
	// Test correlated version standalone
	run("correlated SUM as column",
		`SELECT ps_partkey, ps_suppkey, ps_availqty,
			(SELECT 0.5 * SUM(l_quantity) FROM lineitem
			 WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
			 AND l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01') as threshold
		FROM partsupp
		WHERE ps_partkey <= 3
		LIMIT 5`)

	// ---- Step 5: ps_availqty > correlated subquery ----
	t.Log("=== Q20 Step 5: availqty > correlated SUM ===")
	run("availqty > literal threshold",
		`SELECT COUNT(*) as cnt FROM partsupp
		WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
			AND ps_availqty > 100`)
	run("availqty > correlated SUM",
		`SELECT COUNT(*) as cnt FROM partsupp
		WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
			AND ps_availqty > (
				SELECT 0.5 * SUM(l_quantity) FROM lineitem
				WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
				AND l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
			)`)

	// ---- Step 6: Middle subquery (SELECT ps_suppkey) ----
	t.Log("=== Q20 Step 6: Middle IN subquery ===")
	run("ps_suppkey from middle subquery (literal threshold)",
		`SELECT ps_suppkey FROM partsupp
		WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
			AND ps_availqty > 100
		LIMIT 10`)
	run("ps_suppkey from middle subquery (correlated)",
		`SELECT ps_suppkey FROM partsupp
		WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
			AND ps_availqty > (
				SELECT 0.5 * SUM(l_quantity) FROM lineitem
				WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
				AND l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
			)
		LIMIT 10`)

	// ---- Step 7: Outer s_suppkey IN (middle subquery) ----
	t.Log("=== Q20 Step 7: Outer IN with nested subqueries ===")
	// Use literal suppkey list to test outer query
	run("outer query with literal suppkeys",
		`SELECT s_name, s_address FROM supplier JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name = 'CANADA' AND s_suppkey IN (1, 2, 3)
		ORDER BY s_name LIMIT 5`)
	// Test the outer IN with a simple subquery first
	run("outer IN with simple subquery",
		`SELECT s_name FROM supplier JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name = 'CANADA' AND s_suppkey IN (
			SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (
				SELECT p_partkey FROM part WHERE p_name LIKE 'forest%'
			)
		)
		ORDER BY s_name LIMIT 5`)

	// ---- Step 8: Data analysis for correlated subquery ----
	t.Log("=== Q20 Step 8: Data analysis ===")
	// Check if any forest part + partsupp combos actually have matching lineitems
	run("forest partsupp with matching lineitems",
		`SELECT ps_partkey, ps_suppkey, ps_availqty,
			(SELECT SUM(l_quantity) FROM lineitem
			 WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey) as total_qty,
			(SELECT SUM(l_quantity) FROM lineitem
			 WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
			 AND l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01') as filtered_qty
		FROM partsupp
		WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
		LIMIT 10`)

	// Check lineitem distribution for a specific forest part
	run("lineitem for partkey=135",
		`SELECT l_suppkey, COUNT(*) as cnt, SUM(l_quantity) as total_qty
		FROM lineitem WHERE l_partkey = 135
		GROUP BY l_suppkey`)

	// Check if aggregate returns 0 rows vs 1 row with NULL
	run("SUM with no matches",
		"SELECT SUM(l_quantity) as s FROM lineitem WHERE l_partkey = 999999")
	run("0.5 * SUM with no matches",
		"SELECT 0.5 * SUM(l_quantity) as s FROM lineitem WHERE l_partkey = 999999")

	// Check what happens with availqty > NULL
	run("availqty > NULL comparison",
		"SELECT ps_partkey, ps_availqty FROM partsupp WHERE ps_availqty > NULL LIMIT 3")

	// ---- Step 9: Full Q20 ----
	t.Log("=== Q20 Step 9: Full Q20 ===")
	run("Q20 full",
		`SELECT s_name, s_address
		FROM supplier JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name = 'CANADA'
			AND s_suppkey IN (
				SELECT ps_suppkey FROM partsupp
				WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
				AND ps_availqty > (
					SELECT 0.5 * SUM(l_quantity) FROM lineitem
					WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
					AND l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
				)
			)
		ORDER BY s_name`)
}

// =============================================================================
// Q21 Debug: EXISTS + NOT EXISTS with aliased lineitem (l1, l2, l3)
// =============================================================================

func TestDebugQ21(t *testing.T) {
	db := setupTPCH(t, SF001)
	ctx := context.Background()

	run := func(label, sql string) {
		t.Helper()
		ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		result, err := db.Query(ctx2, sql)
		if err != nil {
			t.Logf("  %s: ERROR: %v", label, err)
			return
		}
		t.Logf("  %s: %d rows", label, len(result.Rows))
		for i, r := range result.Rows {
			if i >= 10 {
				t.Logf("  ... (%d more)", len(result.Rows)-10)
				break
			}
			t.Logf("    row %d: %v", i, r)
		}
	}

	// ---- Step 1: Base data ----
	t.Log("=== Q21 Step 1: Base data ===")
	run("Saudi Arabia suppliers",
		"SELECT COUNT(*) as cnt FROM supplier JOIN nation ON s_nationkey = n_nationkey WHERE n_name = 'SAUDI ARABIA'")
	run("orders with status F",
		"SELECT COUNT(*) as cnt FROM orders WHERE o_orderstatus = 'F'")
	run("lineitem with receiptdate > commitdate",
		"SELECT COUNT(*) as cnt FROM lineitem WHERE l_receiptdate > l_commitdate")

	// ---- Step 2: Base join (no subqueries) ----
	t.Log("=== Q21 Step 2: Base join ===")
	run("4-way join no exists",
		`SELECT COUNT(*) as cnt
		FROM supplier
		JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
		JOIN orders ON o_orderkey = l1.l_orderkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE o_orderstatus = 'F'
			AND l1.l_receiptdate > l1.l_commitdate
			AND n_name = 'SAUDI ARABIA'`)
	run("sample base rows",
		`SELECT s_name, l1.l_orderkey, l1.l_suppkey
		FROM supplier
		JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
		JOIN orders ON o_orderkey = l1.l_orderkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE o_orderstatus = 'F'
			AND l1.l_receiptdate > l1.l_commitdate
			AND n_name = 'SAUDI ARABIA'
		LIMIT 5`)

	// ---- Step 3: EXISTS subquery standalone ----
	t.Log("=== Q21 Step 3: EXISTS standalone ===")
	// For a known order, check if there are other suppliers
	run("multi-supplier orders",
		`SELECT l_orderkey, COUNT(DISTINCT l_suppkey) as supp_cnt
		FROM lineitem GROUP BY l_orderkey HAVING COUNT(DISTINCT l_suppkey) > 1 LIMIT 5`)
	// EXISTS with literal orderkey and suppkey
	run("EXISTS literal test",
		`SELECT 1 as x FROM lineitem l2 WHERE l2.l_orderkey = 1 AND l2.l_suppkey != 1 LIMIT 1`)

	// ---- Step 4: EXISTS decorrelation check ----
	t.Log("=== Q21 Step 4: EXISTS decorrelation ===")
	// decorrelateExists only handles single-table subqueries.
	// Q21's EXISTS subquery: SELECT 1 FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey != l1.l_suppkey
	// This IS a single-table subquery (just lineitem), so it should be decorrelatable.
	// But: the outer refs are l1.l_orderkey and l1.l_suppkey (qualified with alias l1).
	// The outer tables include: supplier, lineitem (aliased as l1), orders, nation
	// Key question: is 'l1' in outerTables? collectTableNames checks n.TableAlias.

	// Test EXISTS in isolation
	run("simple EXISTS (orders table)",
		`SELECT COUNT(*) as cnt FROM orders
		WHERE EXISTS (SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey)`)

	// Test EXISTS with aliased outer table
	run("EXISTS with aliased outer (like Q21)",
		`SELECT COUNT(*) as cnt
		FROM lineitem l1
		WHERE EXISTS (SELECT 1 FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey != l1.l_suppkey)
		LIMIT 5`)

	// Test NOT EXISTS with aliased outer
	run("NOT EXISTS with aliased outer (like Q21)",
		`SELECT COUNT(*) as cnt
		FROM lineitem l1
		WHERE l1.l_receiptdate > l1.l_commitdate
			AND NOT EXISTS (
				SELECT 1 FROM lineitem l3
				WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey
				AND l3.l_receiptdate > l3.l_commitdate
			)`)

	// ---- Step 5: Combined EXISTS + NOT EXISTS ----
	t.Log("=== Q21 Step 5: Combined EXISTS + NOT EXISTS ===")
	run("EXISTS + NOT EXISTS",
		`SELECT COUNT(*) as cnt
		FROM lineitem l1
		WHERE l1.l_receiptdate > l1.l_commitdate
			AND EXISTS (SELECT 1 FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey != l1.l_suppkey)
			AND NOT EXISTS (SELECT 1 FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)`)

	// ---- Step 6: Full join + EXISTS without NOT EXISTS ----
	t.Log("=== Q21 Step 6: Full join + EXISTS only ===")
	run("full join + EXISTS only",
		`SELECT s_name, COUNT(*) as numwait
		FROM supplier
		JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
		JOIN orders ON o_orderkey = l1.l_orderkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE o_orderstatus = 'F'
			AND l1.l_receiptdate > l1.l_commitdate
			AND n_name = 'SAUDI ARABIA'
			AND EXISTS (SELECT 1 FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey != l1.l_suppkey)
		GROUP BY s_name
		ORDER BY numwait DESC, s_name
		LIMIT 10`)

	// ---- Step 7: Full join + NOT EXISTS only ----
	t.Log("=== Q21 Step 7: Full join + NOT EXISTS only ===")
	run("full join + NOT EXISTS only",
		`SELECT s_name, COUNT(*) as numwait
		FROM supplier
		JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
		JOIN orders ON o_orderkey = l1.l_orderkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE o_orderstatus = 'F'
			AND l1.l_receiptdate > l1.l_commitdate
			AND n_name = 'SAUDI ARABIA'
			AND NOT EXISTS (SELECT 1 FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
		GROUP BY s_name
		ORDER BY numwait DESC, s_name
		LIMIT 10`)

	// ---- Step 8: Investigate NOT EXISTS failure with joins ----
	t.Log("=== Q21 Step 8: NOT EXISTS + join investigation ===")

	// The NOT EXISTS works correctly in isolation (Step 5: 27 rows)
	// but returns 0 rows when combined with the 4-way join (Step 7: 0 rows).
	// Hypothesis: decorrelateExists creates the anti-join, then pushdownPredicates
	// incorrectly pushes predicates through it, or the column names get mangled.

	// Test with a simpler join (just lineitem l1 + orders)
	run("2-way join + NOT EXISTS",
		`SELECT COUNT(*) as cnt
		FROM lineitem l1
		JOIN orders ON o_orderkey = l1.l_orderkey
		WHERE o_orderstatus = 'F'
			AND l1.l_receiptdate > l1.l_commitdate
			AND NOT EXISTS (
				SELECT 1 FROM lineitem l3
				WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey
				AND l3.l_receiptdate > l3.l_commitdate
			)`)

	// Test with supplier + lineitem l1 (no orders/nation)
	run("2-way join (supplier+l1) + NOT EXISTS",
		`SELECT COUNT(*) as cnt
		FROM supplier
		JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
		WHERE l1.l_receiptdate > l1.l_commitdate
			AND NOT EXISTS (
				SELECT 1 FROM lineitem l3
				WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey
				AND l3.l_receiptdate > l3.l_commitdate
			)`)

	// Test NOT EXISTS with a different inner filter (no l_receiptdate > l_commitdate)
	run("4-way join + simpler NOT EXISTS (no inner filter)",
		`SELECT COUNT(*) as cnt
		FROM supplier
		JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
		JOIN orders ON o_orderkey = l1.l_orderkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE o_orderstatus = 'F'
			AND l1.l_receiptdate > l1.l_commitdate
			AND n_name = 'SAUDI ARABIA'
			AND NOT EXISTS (
				SELECT 1 FROM lineitem l3
				WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey
			)`)

	// ---- Step 9: Narrow down 4-way vs 2-way NOT EXISTS failure ----
	t.Log("=== Q21 Step 9: 3-way join + NOT EXISTS ===")

	// 2-way join (lineitem+orders) with NOT EXISTS works (11 rows)
	// 4-way join (supplier+lineitem+orders+nation) with NOT EXISTS fails (0 rows)
	// Let's test 3-way joins to see when it breaks

	run("3-way join (supplier+l1+orders) + NOT EXISTS",
		`SELECT COUNT(*) as cnt
		FROM supplier
		JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
		JOIN orders ON o_orderkey = l1.l_orderkey
		WHERE o_orderstatus = 'F'
			AND l1.l_receiptdate > l1.l_commitdate
			AND NOT EXISTS (
				SELECT 1 FROM lineitem l3
				WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey
				AND l3.l_receiptdate > l3.l_commitdate
			)`)

	run("3-way join (l1+orders+nation) + NOT EXISTS",
		`SELECT COUNT(*) as cnt
		FROM lineitem l1
		JOIN orders ON o_orderkey = l1.l_orderkey
		JOIN nation ON 1 = 1
		WHERE o_orderstatus = 'F'
			AND l1.l_receiptdate > l1.l_commitdate
			AND NOT EXISTS (
				SELECT 1 FROM lineitem l3
				WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey
				AND l3.l_receiptdate > l3.l_commitdate
			)
		LIMIT 5`)

	// ---- Step 10: Check if pushdown is the culprit ----
	t.Log("=== Q21 Step 10: Pushdown investigation ===")

	// Test the NOT EXISTS predicate with 4 tables but WITHOUT extra WHERE conditions
	// If pushdown is the problem, having fewer pushable predicates might help
	run("4-way join + NOT EXISTS (no extra WHERE)",
		`SELECT COUNT(*) as cnt
		FROM supplier
		JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
		JOIN orders ON o_orderkey = l1.l_orderkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE NOT EXISTS (
			SELECT 1 FROM lineitem l3
			WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey
			AND l3.l_receiptdate > l3.l_commitdate
		)`)

	// Check if the anti-join inner filter (l_receiptdate > l_commitdate) is getting
	// confused with the outer predicate (l1.l_receiptdate > l1.l_commitdate)
	run("4-way join + NOT EXISTS (no outer receipt filter)",
		`SELECT COUNT(*) as cnt
		FROM supplier
		JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
		JOIN orders ON o_orderkey = l1.l_orderkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE o_orderstatus = 'F'
			AND n_name = 'SAUDI ARABIA'
			AND NOT EXISTS (
				SELECT 1 FROM lineitem l3
				WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey
				AND l3.l_receiptdate > l3.l_commitdate
			)`)

	// ---- Step 11: Check if Q21 zero rows is actually correct ----
	t.Log("=== Q21 Step 11: Verify correctness ===")

	// The 2-way join (l1+orders) with NOT EXISTS gave 11 rows.
	// The 4-way join filters to only Saudi Arabia suppliers (3 suppkeys).
	// Check if ANY of the 11 rows from 2-way join are from Saudi Arabia suppliers.
	run("Saudi Arabia suppkeys",
		"SELECT s_suppkey FROM supplier JOIN nation ON s_nationkey = n_nationkey WHERE n_name = 'SAUDI ARABIA'")

	// Check the 2-way join NOT EXISTS rows' suppkeys
	run("2-way NOT EXISTS suppkeys",
		`SELECT l1.l_suppkey, l1.l_orderkey
		FROM lineitem l1
		JOIN orders ON o_orderkey = l1.l_orderkey
		WHERE o_orderstatus = 'F'
			AND l1.l_receiptdate > l1.l_commitdate
			AND NOT EXISTS (
				SELECT 1 FROM lineitem l3
				WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey
				AND l3.l_receiptdate > l3.l_commitdate
			)`)

	// Check: how many lineitem rows per order for the Saudi Arabia suppliers
	run("lineitems per order for SA suppliers",
		`SELECT l1.l_orderkey, COUNT(*) as cnt
		FROM lineitem l1
		JOIN orders ON o_orderkey = l1.l_orderkey
		JOIN supplier ON s_suppkey = l1.l_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE o_orderstatus = 'F'
			AND l1.l_receiptdate > l1.l_commitdate
			AND n_name = 'SAUDI ARABIA'
		GROUP BY l1.l_orderkey
		LIMIT 10`)

	// ---- Step 12: Full Q21 ----
	t.Log("=== Q21 Step 12: Full Q21 ===")
	run("Q21 full",
		`SELECT s_name, COUNT(*) as numwait
		FROM supplier
		JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
		JOIN orders ON o_orderkey = l1.l_orderkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE o_orderstatus = 'F'
			AND l1.l_receiptdate > l1.l_commitdate
			AND n_name = 'SAUDI ARABIA'
			AND EXISTS (SELECT 1 FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey != l1.l_suppkey)
			AND NOT EXISTS (SELECT 1 FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
		GROUP BY s_name
		ORDER BY numwait DESC, s_name
		LIMIT 100`)
}

// =============================================================================
// Q22 Debug: SUBSTR in GROUP BY + NOT EXISTS + scalar subquery
// =============================================================================

func TestDebugQ22(t *testing.T) {
	db := setupTPCH(t, SF001)
	ctx := context.Background()

	run := func(label, sql string) {
		t.Helper()
		ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		result, err := db.Query(ctx2, sql)
		if err != nil {
			t.Logf("  %s: ERROR: %v", label, err)
			return
		}
		t.Logf("  %s: %d rows", label, len(result.Rows))
		for i, r := range result.Rows {
			if i >= 10 {
				t.Logf("  ... (%d more)", len(result.Rows)-10)
				break
			}
			t.Logf("    row %d: %v", i, r)
		}
	}

	// ---- Step 1: SUBSTR basics ----
	t.Log("=== Q22 Step 1: SUBSTR basics ===")
	run("SUBSTR literal", "SELECT SUBSTR('hello', 1, 2) as s")
	run("SUBSTR on c_phone", "SELECT c_phone, SUBSTR(c_phone, 1, 2) as cc FROM customer LIMIT 5")
	run("sample phones", "SELECT c_phone FROM customer LIMIT 10")

	// ---- Step 2: Phone prefix distribution ----
	t.Log("=== Q22 Step 2: Phone prefix distribution ===")
	run("all phone prefixes",
		`SELECT SUBSTR(c_phone, 1, 2) as prefix, COUNT(*) as cnt
		FROM customer GROUP BY SUBSTR(c_phone, 1, 2) ORDER BY prefix`)
	run("target prefixes",
		`SELECT COUNT(*) as cnt FROM customer
		WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')`)

	// ---- Step 3: Scalar subquery: AVG(c_acctbal) ----
	t.Log("=== Q22 Step 3: AVG subquery ===")
	run("AVG acctbal all",
		"SELECT AVG(c_acctbal) as avg_bal FROM customer")
	run("AVG acctbal > 0",
		"SELECT AVG(c_acctbal) as avg_bal FROM customer WHERE c_acctbal > 0.00")
	run("AVG acctbal > 0 with prefix filter",
		`SELECT AVG(c_acctbal) as avg_bal FROM customer
		WHERE c_acctbal > 0.00
			AND SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')`)

	// ---- Step 4: c_acctbal > (scalar subquery) ----
	t.Log("=== Q22 Step 4: acctbal > scalar subquery ===")
	run("acctbal > literal 0",
		`SELECT COUNT(*) as cnt FROM customer
		WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
			AND c_acctbal > 0`)
	run("acctbal > scalar subquery (uncorrelated)",
		`SELECT COUNT(*) as cnt FROM customer
		WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
			AND c_acctbal > (
				SELECT AVG(c_acctbal) FROM customer
				WHERE c_acctbal > 0.00
					AND SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
			)`)

	// ---- Step 5: NOT EXISTS (orders) ----
	t.Log("=== Q22 Step 5: NOT EXISTS ===")
	run("customers with orders",
		"SELECT COUNT(*) as cnt FROM customer WHERE EXISTS (SELECT 1 FROM orders WHERE o_custkey = c_custkey)")
	run("customers without orders",
		"SELECT COUNT(*) as cnt FROM customer WHERE NOT EXISTS (SELECT 1 FROM orders WHERE o_custkey = c_custkey)")

	// ---- Step 6: Combined: prefix filter + acctbal > AVG ----
	t.Log("=== Q22 Step 6: Combined prefix + acctbal ===")
	run("prefix + acctbal > AVG (no NOT EXISTS)",
		`SELECT SUBSTR(c_phone, 1, 2) as cntrycode, COUNT(*) as numcust, SUM(c_acctbal) as totacctbal
		FROM customer
		WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
			AND c_acctbal > (
				SELECT AVG(c_acctbal) FROM customer
				WHERE c_acctbal > 0.00
					AND SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
			)
		GROUP BY SUBSTR(c_phone, 1, 2)
		ORDER BY cntrycode`)

	// ---- Step 7: Combined: prefix filter + NOT EXISTS ----
	t.Log("=== Q22 Step 7: Combined prefix + NOT EXISTS ===")
	run("prefix + NOT EXISTS (no acctbal filter)",
		`SELECT SUBSTR(c_phone, 1, 2) as cntrycode, COUNT(*) as numcust, SUM(c_acctbal) as totacctbal
		FROM customer
		WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
			AND NOT EXISTS (SELECT 1 FROM orders WHERE o_custkey = c_custkey)
		GROUP BY SUBSTR(c_phone, 1, 2)
		ORDER BY cntrycode`)

	// ---- Step 8: Investigate GROUP BY SUBSTR ----
	t.Log("=== Q22 Step 8: GROUP BY SUBSTR investigation ===")

	// Step 6 showed 176 rows all with cntrycode=<nil>. This means GROUP BY SUBSTR
	// is creating one group per row (or treating all SUBSTR results as NULL).

	// Test simpler GROUP BY SUBSTR
	run("GROUP BY SUBSTR simple",
		`SELECT SUBSTR(c_phone, 1, 2) as prefix, COUNT(*) as cnt
		FROM customer
		LIMIT 10`)
	run("GROUP BY SUBSTR with explicit group",
		`SELECT SUBSTR(c_phone, 1, 2) as prefix, COUNT(*) as cnt
		FROM customer
		GROUP BY SUBSTR(c_phone, 1, 2)
		LIMIT 10`)
	// Test if GROUP BY expression matching works
	run("GROUP BY column alias",
		`SELECT SUBSTR(c_phone, 1, 2) as prefix, COUNT(*) as cnt
		FROM customer
		GROUP BY prefix
		LIMIT 10`)
	// Test GROUP BY with a simpler function expression
	run("GROUP BY LENGTH",
		`SELECT LENGTH(c_phone) as phonelen, COUNT(*) as cnt
		FROM customer
		GROUP BY LENGTH(c_phone)`)

	// ---- Step 9: Investigate NOT EXISTS timeout ----
	t.Log("=== Q22 Step 9: NOT EXISTS investigation ===")

	// Step 5 showed EXISTS/NOT EXISTS timeout. The subquery is:
	// SELECT 1 FROM orders WHERE o_custkey = c_custkey
	// This is a correlated subquery on 'orders' table, referencing 'c_custkey' from customer.
	// Since it's EXISTS, decorrelateExists should convert it to a semi/anti join.
	// But it timed out. Let's check if decorrelation works.

	// Small-scale test
	run("NOT EXISTS with LIMIT on outer",
		`SELECT c_custkey FROM customer
		WHERE c_custkey <= 10
			AND NOT EXISTS (SELECT 1 FROM orders WHERE o_custkey = c_custkey)`)

	// Check if the issue is correlated EXISTS failing to decorrelate
	run("EXISTS as column (should be fast if decorrelated)",
		`SELECT c_custkey,
			EXISTS (SELECT 1 FROM orders WHERE o_custkey = c_custkey) as has_order
		FROM customer
		WHERE c_custkey <= 5`)

	// ---- Step 10: Full Q22 ----
	t.Log("=== Q22 Step 10: Full Q22 ===")
	run("Q22 full",
		`SELECT SUBSTR(c_phone, 1, 2) as cntrycode, COUNT(*) as numcust, SUM(c_acctbal) as totacctbal
		FROM customer
		WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
			AND c_acctbal > (
				SELECT AVG(c_acctbal) FROM customer
				WHERE c_acctbal > 0.00
					AND SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
			)
			AND NOT EXISTS (SELECT 1 FROM orders WHERE o_custkey = c_custkey)
		GROUP BY SUBSTR(c_phone, 1, 2)
		ORDER BY cntrycode`)
}
