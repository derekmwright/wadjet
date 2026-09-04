package tpch

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// q2CommaSpelling is TPC-H Q2 as the SPECIFICATION writes it: comma joins with
// the equi-predicates in WHERE, and a correlated scalar subquery whose own FROM
// is a comma join too.
//
// benchmarks/tpch/queries.go spells all 22 with explicit `JOIN … ON`, so the
// shape every TPC-H-derived client actually emits had no coverage; the comma
// spellings of Q3, Q5, Q7–Q11 and Q17–Q21 were added to the DuckDB corpus for
// that reason and Q2 was DELIBERATELY LEFT OUT, because it could not be
// answered. Its inner comma FROM lost every relation past the first —
// the decorrelations built their build side out of `NewScan(info.Tables[0])`
// — so the plan either errored with an unresolved join-key kernel or
// deadlocked on the shared scan cache's ready channel, depending on the base
// (#616). A hang would have wedged CI, which is why it was absent rather than
// pinned.
//
// The inner FROM is lifted into a join tree before the correlation terms are
// classified now (logical.decorrelatedInnerPlan, ADR-0021 §1j).
const q2CommaSpelling = `SELECT
	s_acctbal, s_name, n_name, p_partkey, p_mfgr,
	s_address, s_phone, s_comment
FROM part, partsupp, supplier, nation, region
WHERE p_partkey = ps_partkey
	AND s_suppkey = ps_suppkey
	AND s_nationkey = n_nationkey
	AND n_regionkey = r_regionkey
	AND r_name = 'EUROPE'
	AND p_size = 15
	AND p_type LIKE '%BRASS'
	AND ps_supplycost = (
		SELECT MIN(ps_supplycost)
		FROM partsupp, supplier, nation, region
		WHERE s_suppkey = ps_suppkey
			AND s_nationkey = n_nationkey
			AND n_regionkey = r_regionkey
			AND ps_partkey = p_partkey
			AND r_name = 'EUROPE'
	)
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100`

// TestQ2CommaSpellingAnswersTheSameAsTheExplicitJoin is #616's gate.
//
// The two texts ask the identical question, so the comparison is row for row
// and cell for cell rather than a count — a spelling that lost the inner comma
// join's other three relations would not merely return fewer rows, it would
// return a MIN over the wrong set, and only the values say so. That IS the
// defect: at 2d4220c9 the rewrite fired on a comma inner and built only its
// FIRST relation, and this query then failed LOUDLY in 22 ms with
// `ColColFilter: could not resolve kernel for s_suppkey 0 ps_suppkey`
// (measured at that revision by round 1's review). The DEADLOCK #616's title
// reports belongs to an earlier base than this arc's, on the same shape and
// from the same missing relations; which of the two a given base shows is what
// the issue's own "depending on the base" records.
//
// WHAT MAKES THIS PASS is the BUILD SIDE — logical.decorrelatedInnerPlan
// planning the subquery's whole FROM clause instead of `Tables[0]` — and NOT
// the in-place comma lift beside it. Measured, because the difference matters
// to a reader deciding what to revert: with `commaChain` forced false, so the
// equalities are left to Optimize's own later liftWhereEquiPredsIntoJoins,
// this gate still PASSES, in 7.1 s rather than 91 ms. The in-place lift is a
// ~75x performance lever on top of a correctness fix, not the correctness fix.
//
// No single revert available in THIS tree reproduces the base's failure, and
// that is worth stating rather than papering over: the tree no longer contains
// the path that produced it ("fire the rewrite and drop the relations").
// Turning the #852 kill switch off (WADJET_DECORRELATED_INNER_PLAN=0) declines
// the comma inner instead, so the per-row re-run answers it correctly in 91 ms
// and this gate passes. The gate's claim is therefore about the ANSWER at the
// tip and about the base it fails at, which the review verified directly.
//
// The DEADLINE is the other half of the gate and it is deliberate. One of the
// two failure modes this shape has shown is a DEADLOCK, and a hang pinned with
// a generous timeout is a slow test while a hang pinned with a short one is a
// fact: at this tip the query answers in under a tenth of a second at SF0.01,
// so thirty seconds separates "answered" from "wedged" with no room for
// argument.
func TestQ2CommaSpellingAnswersTheSameAsTheExplicitJoin(t *testing.T) {
	db := setupTPCH(t, SF001)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	got, err := db.Query(ctx, q2CommaSpelling)
	elapsed := time.Since(start).Round(time.Millisecond)
	if err != nil {
		if ctx.Err() != nil {
			t.Fatalf("the comma spelling did not finish within the deadline — it is "+
				"deadlocking again (#616): %v", err)
		}
		t.Fatalf("comma spelling failed after %v: %v", elapsed, err)
	}
	t.Logf("comma spelling: %d rows in %v", len(got.Rows), elapsed)

	want, err := db.Query(context.Background(), TPCHQueries[2].SQL)
	if err != nil {
		t.Fatalf("explicit-join Q2: %v", err)
	}
	if len(got.Rows) != len(want.Rows) {
		t.Fatalf("the two spellings of Q2 return different row counts: comma %d, "+
			"explicit join %d", len(got.Rows), len(want.Rows))
	}
	if len(got.Rows) == 0 {
		t.Fatal("both spellings returned no rows; the fixture stopped reaching the shape")
	}
	for i := range got.Rows {
		g, w := fmt.Sprint(got.Rows[i]), fmt.Sprint(want.Rows[i])
		if g != w {
			t.Fatalf("row %d differs between the two spellings of one query:\n"+
				"  comma:          %s\n  explicit join:  %s", i, g, w)
		}
	}
}
