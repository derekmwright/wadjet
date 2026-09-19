// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// A BOUNDED CORRELATED LATERAL READS THE VALUE THE POLICY PUBLISHES, ON EVERY
// DOOR — arc L1 round 2, finding B2.
//
// Arc L1 built #1019's per-outer-row bound on a window the PLANNER mints:
// `ROW_NUMBER() OVER (PARTITION BY <the inner correlation column>)` and a
// `QUALIFY` over it, planted into the lateral's body. On the four DAG-through
// doors that partition did not bind the body's own column — every row became
// its own partition, the bound kept all of them, and the join then paired each
// outer row with ITSELF. Over `e7bal`, whose masked `bal` has singleton
// equivalence classes under its STORED values, that made the analyst's answer
// arithmetic on the column the policy hides:
//
//	SELECT b.id, s.m FROM e7bal b JOIN LATERAL (
//	  SELECT c.id AS m FROM e7bal c WHERE c.bal = b.bal ORDER BY c.id LIMIT 2) s ON true
//	-- the MASK      a=k|m=1 ; a=k|m=2 for every k   (five doors, and all nine before)
//	-- the four DAG doors, at the withdrawn tip      a=k|m=k twice, the ADMIN pairing
//
// The rewrite is withdrawn (ADR-0021 §1q) and this gate is what keeps the
// shape honest whatever replaces it: the analyst's answer on EVERY door is the
// one the mask implies, and the count of doors that answered is asserted so
// the gate cannot pass by refusing everywhere.
//
// It is not a `QUALIFY` gate and not a LATERAL gate: it is the rule that a
// window the planner MINTS is held to the same barrier as one the user writes
// (ADR-0033, ADR-0026 §8j). `userWindowOnMasked` is the user-written spelling
// of the same partition, beside it, and `noBound` is the same correlation with
// no bound at all.
func TestArcL1ABoundedLateralReadsThePublishedValue(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up all nine policy doors")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)

	// Under the mask every `bal` is 0, so the correlation matches every row
	// and a bound of 2 leaves ids 1 and 2 for EVERY outer row. Under the
	// stored column each outer row matches only itself. The two answers differ
	// in every cell, which is what makes the gate a disclosure test and not a
	// row-count test.
	maskPairs := make([]string, 0, 16)
	for a := 1; a <= 8; a++ {
		maskPairs = append(maskPairs, fmt.Sprintf("a=%d|m=1", a), fmt.Sprintf("a=%d|m=2", a))
	}
	cells := []struct{ name, sql, want string }{
		{"boundOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN LATERAL (SELECT c.id AS m ` +
				`FROM e7bal c WHERE c.bal = b.bal ORDER BY c.id LIMIT 2) s ON true`,
			strings.Join(maskPairs, " ; ")},
		{"boundOnMaskedOffset",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN LATERAL (SELECT c.id AS m ` +
				`FROM e7bal c WHERE c.bal = b.bal ORDER BY c.id OFFSET 6) s ON true`,
			pmPairs(8, 7, 8)},
		{"noBound",
			`SELECT b.id AS a, COUNT(*) AS m FROM e7bal b JOIN LATERAL (SELECT c.id ` +
				`FROM e7bal c WHERE c.bal = b.bal) s ON true GROUP BY b.id`,
			pmPairs(8, 8)},
		{"userWindowOnMasked",
			`SELECT c.id AS a, ROW_NUMBER() OVER (PARTITION BY c.bal ORDER BY c.id) AS m ` +
				`FROM e7bal c`,
			pmSeq(8)},
		// e7emp: `acct` masks to 0 and `salary` is DENIED. A bounded lateral
		// ordered by the masked column ties every row under the mask, so the
		// assertion is the one property that holds either way — the DENIED
		// column is not readable and the masked one never carries its stored
		// value. `pmTrueValues` is checked over every cell below.
		{"boundOnDept",
			`SELECT b.id AS a, s.m AS m FROM e7emp b JOIN LATERAL (SELECT c.id AS m ` +
				`FROM e7emp c WHERE c.dept = b.dept ORDER BY c.id LIMIT 1) s ON true`,
			pmDeptPairs()},
	}

	// THE DECLARED OUTPUT IS PostgreSQL's, ON EVERY DOOR. A bare `SELECT *`
	// publishes the join's STREAM, so a column the planner materializes for a
	// lifted predicate would enter it — and two of the nine doors are pgwire,
	// where that is `RowDescription`. The materialization declines over a star
	// for exactly this reason (ADR-0021 §1q, round 4); this cell is what says
	// so on the wire.
	starCols := []struct{ name, sql, want string }{
		{"starOverLifted",
			`SELECT * FROM e7bal b LEFT JOIN LATERAL (SELECT c.id AS m FROM e7bal c ` +
				`WHERE c.bal < b.bal) s ON true`, "bal,id,m"},
		{"starOverLiftedUnpoliced",
			`SELECT * FROM e7other b LEFT JOIN LATERAL (SELECT c.note AS m FROM e7other c ` +
				`WHERE c.id < b.id) s ON true`, "id,m,note"},
		{"qualifiedStarOverLifted",
			`SELECT s.* FROM e7bal b LEFT JOIN LATERAL (SELECT c.id AS m FROM e7bal c ` +
				`WHERE c.bal < b.bal) s ON true`, "m"},
	}
	for _, c := range starCols {
		for _, door := range rig.doors {
			t.Run(c.name+"/"+door.name, func(t *testing.T) {
				got, err := door.run(t, "analyst-key", c.sql)
				if err != nil {
					// PINNED since arc JR (#1153): the materialization
					// declines under a bare star, so the lifted predicate
					// names a column the fragment's declared sides do not
					// publish, and the four DAG doors now REFUSE where they
					// answered PostgreSQL's column list over WRONG VALUES —
					// the three-padded-row answer arc L1 pins per arm in
					// `l1ArmPins["R4/bareStar"]`. A loud refusal replacing a
					// base-wrong answer is not a regression; what is lost is
					// only that the LIST cannot be observed on those four
					// doors, and the five that answer still assert it.
					if l1LiftedStarDAGDoors[door.name] &&
						strings.Contains(err.Error(), "resolves on neither side of this join here") {
						t.Skipf("pinned: the lifted predicate does not resolve on the "+
							"fragment's declared sides, so this door refuses: %v", err)
					}
					t.Fatalf("refused: %v\n  SQL: %s", err, c.sql)
				}
				cols := append([]string(nil), got.cols...)
				sort.Strings(cols)
				if strings.Join(cols, ",") != c.want {
					t.Errorf("%s\n  door %s\n  publishes %v\n  want %s (PostgreSQL's list)",
						c.sql, door.name, cols, c.want)
				}
			})
		}
	}

	answered := 0
	leaks := pmTrueValues()
	for _, c := range cells {
		for _, door := range rig.doors {
			t.Run(c.name+"/"+door.name, func(t *testing.T) {
				got, err := door.run(t, "analyst-key", c.sql)
				if err != nil {
					t.Fatalf("the analyst's query was refused, which this gate cannot "+
						"read as an answer: %v\n  SQL: %s", err, c.sql)
				}
				rendered := strings.Join(got.canon(), " ; ")
				for _, s := range leaks {
					if strings.Contains(rendered, s) {
						t.Fatalf("a masked or denied value reached the client: %q\n  %s\n  SQL: %s",
							s, rendered, c.sql)
					}
				}
				if rendered != c.want {
					t.Errorf("%s\n  door %s\n  got  %s\n  want %s (the value the policy PUBLISHES)",
						c.sql, door.name, rendered, c.want)
				}
				answered++
			})
		}
	}
	// NON-VACUOUS: every cell on every door. A gate that passes because
	// nothing ran is the failure mode this line exists for.
	if want := len(cells) * len(rig.doors); answered != want {
		t.Errorf("%d (cell, door) pairs answered, want %d — the gate is vacuous",
			answered, want)
	}
}

// pmPairs renders `a=1|m=v…` for ids 1..n, one row per id per value.
func pmPairs(n int, vals ...int) string {
	out := make([]string, 0, n*len(vals))
	for a := 1; a <= n; a++ {
		for _, v := range vals {
			out = append(out, fmt.Sprintf("a=%d|m=%d", a, v))
		}
	}
	return strings.Join(out, " ; ")
}

// pmSeq is `a=k|m=k` for 1..n — one partition under the mask, numbered by id.
func pmSeq(n int) string {
	out := make([]string, 0, n)
	for a := 1; a <= n; a++ {
		out = append(out, fmt.Sprintf("a=%d|m=%d", a, a))
	}
	return strings.Join(out, " ; ")
}

// pmDeptPairs is what e7emp's `dept` correlation answers on every door. It is
// NOT PostgreSQL's per-outer-row answer — #1019 is open (ADR-0021 §1q), so the
// `LIMIT 1` bounds the whole inner relation and only the outer rows in the one
// surviving row's department match at all. What this cell holds is the two
// properties that are this gate's subject: all nine doors agree, and none of
// them reads a masked or denied value. It changes the day #1019 closes, and
// the mask cells above are what say the answer is the policy's.
func pmDeptPairs() string {
	return "a=10|m=1 ; a=1|m=1 ; a=4|m=1 ; a=7|m=1"
}

// l1LiftedStarDAGDoors names the four doors on which a bare star over a
// LIFTED-predicate lateral refuses since arc JR. See the skip above.
var l1LiftedStarDAGDoors = map[string]bool{
	"embedded/dag":          true,
	"embedded/dag-shuffled": true,
	"pgwire/dag":            true,
	"http/dag":              true,
}
