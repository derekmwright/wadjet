package physical

import (
	"strings"
	"testing"
)

// TestDynamicFilterEligibilityQ18 audits v1 coverage of Q18.
//
// Q18 plan (SF10-shaped) has THREE hash joins:
//
//	join-4:  orders ⨝ customer        on (o_custkey = c_custkey)
//	join-8:  (orders⨝cust) ⨝ lineitem on (o_orderkey = l_orderkey)
//	join-51: (above) ⨝ final_agg-48   on (o_orderkey = l_orderkey) [SEMI]
//
// v1 algorithm requires each join's build side to walk back to a LEAF
// SCAN through exchange-repartition stages only. Joins 4 and 8 satisfy
// this; join-51's build is final_aggregate-48 (a non-leaf), so v1 will
// not annotate it. This test documents which joins v1 covers and which
// require v2.
func TestDynamicFilterEligibilityQ18(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	const sql = `SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
		SUM(l_quantity) AS total_qty
	FROM customer
	JOIN orders ON c_custkey = o_custkey
	JOIN lineitem ON o_orderkey = l_orderkey
	WHERE o_orderkey IN (
		SELECT l_orderkey
		FROM lineitem
		GROUP BY l_orderkey
		HAVING SUM(l_quantity) > 300
	)
	GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
	ORDER BY o_totalprice DESC, o_orderdate
	LIMIT 100`
	stages := sqlToStagesWithDynamicFilters(t, cat, ctx, sql, 3)

	emits := 0
	consumes := 0
	for _, s := range stages {
		emits += len(s.EmitDynamicFilters)
		consumes += len(s.ConsumeDynamicFilters)
		if len(s.EmitDynamicFilters) > 0 {
			cols := make([]string, len(s.EmitDynamicFilters))
			for i, e := range s.EmitDynamicFilters {
				cols[i] = e.KeyColumn
			}
			t.Logf("EMIT  on %s (table=%s) cols=%s", s.ID, s.TableName, strings.Join(cols, ","))
		}
		if len(s.ConsumeDynamicFilters) > 0 {
			cols := make([]string, len(s.ConsumeDynamicFilters))
			for i, c := range s.ConsumeDynamicFilters {
				cols[i] = c.TargetColumn + "←" + c.SourceStageID
			}
			t.Logf("CONS  on %s (table=%s) cols=%s", s.ID, s.TableName, strings.Join(cols, ","))
		}
	}

	t.Logf("Q18: total emits=%d consumes=%d", emits, consumes)

	// We EXPECT at least one annotation pair (join-4: customer-build is
	// small enough to be eligible). If we get zero, v1 misses Q18 entirely
	// — surface that as a failure so we know to revisit.
	if emits == 0 {
		t.Errorf("Q18: expected at least 1 emit/consume pair from join-4 (orders.o_custkey ⨝ customer.c_custkey); got 0")
	}
}

// TestDynamicFilterEligibilityQ20 audits v1 coverage of Q20. Q20 has
// many small-build joins (region/nation/supplier dimension chain) plus
// a multi-column join (partsupp ⨝ lineitem-agg on partkey+suppkey).
// v1 single-column-key constraint rules out the composite-key join;
// dimension joins should still be eligible.
func TestDynamicFilterEligibilityQ20(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	const sql = `SELECT s_name, s_address
		FROM supplier
		JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name = 'CANADA'
			AND s_suppkey IN (
				SELECT ps_suppkey
				FROM partsupp
				WHERE ps_partkey IN (
					SELECT p_partkey FROM part WHERE p_name LIKE 'forest%'
				)
				AND ps_availqty > (
					SELECT 0.5 * SUM(l_quantity)
					FROM lineitem
					WHERE l_partkey = ps_partkey
						AND l_suppkey = ps_suppkey
						AND l_shipdate >= '1994-01-01'
						AND l_shipdate < '1995-01-01'
				)
			)
		ORDER BY s_name`
	stages := sqlToStagesWithDynamicFilters(t, cat, ctx, sql, 3)

	emits := 0
	consumes := 0
	for _, s := range stages {
		emits += len(s.EmitDynamicFilters)
		consumes += len(s.ConsumeDynamicFilters)
		if len(s.EmitDynamicFilters) > 0 {
			cols := make([]string, len(s.EmitDynamicFilters))
			for i, e := range s.EmitDynamicFilters {
				cols[i] = e.KeyColumn
			}
			t.Logf("EMIT  on %s (table=%s) cols=%s", s.ID, s.TableName, strings.Join(cols, ","))
		}
		if len(s.ConsumeDynamicFilters) > 0 {
			cols := make([]string, len(s.ConsumeDynamicFilters))
			for i, c := range s.ConsumeDynamicFilters {
				cols[i] = c.TargetColumn + "←" + c.SourceStageID
			}
			t.Logf("CONS  on %s (table=%s) cols=%s", s.ID, s.TableName, strings.Join(cols, ","))
		}
	}
	t.Logf("Q20: total emits=%d consumes=%d", emits, consumes)

	if emits == 0 {
		t.Errorf("Q20: expected at least 1 emit/consume pair (filtered dimension join); got 0")
	}
}
