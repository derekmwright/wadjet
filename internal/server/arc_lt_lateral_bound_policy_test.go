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

// A BODY EVALUATED PER OUTER ROW READS THE VALUE THE POLICY PUBLISHES, ON
// EVERY DOOR — arc LT, the masking gate COMMON names for a rewrite that
// re-runs, rewrites or partitions over a relation.
//
// Arc LT applies a correlated LATERAL's bound per outer row through a window
// the PLANNER mints — `ROW_NUMBER() OVER (PARTITION BY <the inner correlation
// column> ORDER BY <the body's own ORDER BY>)` — declines the EXISTS rewrite
// to the per-row rerun for a body it does not reproduce, and REFUSES a body
// with no equality key. Each of those touches a policed relation in a way the
// row-level census does not: the minted window partitions on the correlation
// column, and over `e7bal` — whose masked `bal` has singleton equivalence
// classes under its STORED values — a partition bound to the stored column is
// arithmetic on the value the policy hides (the disclosure arc L1 round 2
// measured on four doors, ADR-0021 §1q). So every cell here is asserted
// against the MASK's answer on all nine doors, every rendered cell is checked
// against every stored policed value, and the count of (cell, door) pairs
// that answered is asserted so the gate cannot pass by refusing everywhere.
//
// `TestArcL1ABoundedLateralReadsThePublishedValue` beside this one holds the
// INNER `ORDER BY c.id LIMIT 2` / `OFFSET 6` spellings and the user-written
// window; this table is the rest of the seam: LEFT, comma, grouped, two
// bounds, the enclosing WHERE, EXISTS with the bound the rewrite now declines,
// and the refusals — which must refuse without a stored value in their text.
func TestArcLTAPerOuterRowBodyReadsThePublishedValueOnEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up all nine policy doors")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	// Under the mask every `bal` is 0, so a correlation on it matches EVERY
	// row of e7bal (8 rows) for every outer row; under the stored column each
	// outer row matches only itself. The two readings differ in every cell.
	cells := []struct{ name, sql, want string }{
		// LEFT and comma spellings of L1's inner cell.
		{"leftBoundOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b LEFT JOIN LATERAL (SELECT c.id AS m ` +
				`FROM e7bal c WHERE c.bal = b.bal ORDER BY c.id LIMIT 2) s ON true`,
			pmPairs(8, 1, 2)},
		{"commaBoundOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b, LATERAL (SELECT c.id AS m ` +
				`FROM e7bal c WHERE c.bal = b.bal ORDER BY c.id DESC LIMIT 1) s`,
			pmPairs(8, 8)},
		// LIMIT 1 OFFSET 1: the second-smallest id of the mask's one class.
		{"limitOffsetOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN LATERAL (SELECT c.id AS m ` +
				`FROM e7bal c WHERE c.bal = b.bal ORDER BY c.id LIMIT 1 OFFSET 1) s ON true`,
			pmPairs(8, 2)},
		// A GROUPED body bounded by its aggregate: under the mask every outer
		// row sees all eight rows, one group per id, and the bound keeps the
		// smallest id's group — its id, 1, for every outer row. (A GROUP BY on
		// an EXPRESSION inside a correlated lateral refuses before any bound,
		// `GROUP BY key "c.id % 2" is not a column of its input`, identically
		// at 51addfb6 — recorded in arc LT's notes, not this gate's subject.)
		{"groupedBoundOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN LATERAL (SELECT MIN(c.id) AS m ` +
				`FROM e7bal c WHERE c.bal = b.bal GROUP BY c.id ORDER BY MIN(c.id) LIMIT 1) s ON true`,
			pmPairs(8, 1)},
		// TWO bounded laterals in one statement (the slot-collision seam).
		{"twoBoundsOnMasked",
			`SELECT b.id AS a, s.m + t.m AS m FROM e7bal b ` +
				`JOIN LATERAL (SELECT c.id AS m FROM e7bal c WHERE c.bal = b.bal ORDER BY c.id LIMIT 1) s ON true ` +
				`JOIN LATERAL (SELECT d.id AS m FROM e7bal d WHERE d.bal = b.bal ORDER BY d.id DESC LIMIT 1) t ON true`,
			pmPairs(8, 9)},
		// The bound's survivor read in the enclosing WHERE.
		{"boundThenEnclosingWhere",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN LATERAL (SELECT c.id AS m ` +
				`FROM e7bal c WHERE c.bal = b.bal ORDER BY c.id LIMIT 3) s ON true WHERE s.m > 2`,
			pmPairs(8, 3)},
		// THE USER-WRITTEN SPELLINGS OF THE SAME SEAM. Before arc LT's guard
		// (dagplan.CheckPolicedWindowUnderJoin) the four DAG doors answered
		// the ADMIN pairing `a=k|m=k` for every one of these — a window over
		// the policed scan feeding a join — with no planner-minted window in
		// the statement at all; the stage plan carried the security
		// projection on both scans. They are the base's own disclosure, and
		// the guard routes the plan to the single-process pipeline, which
		// answers the mask on all nine doors.
		{"userWindowInBody",
			`SELECT b.id AS a, s.m AS m, s.rn AS rn FROM e7bal b JOIN LATERAL (SELECT c.id AS m, ` +
				`ROW_NUMBER() OVER (PARTITION BY c.bal ORDER BY c.id) AS rn FROM e7bal c WHERE c.bal = b.bal) s ON true`,
			pmWindowPairs()},
		{"userQualifyInBody",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN LATERAL (SELECT c.id AS m FROM e7bal c ` +
				`WHERE c.bal = b.bal QUALIFY ROW_NUMBER() OVER (PARTITION BY c.bal ORDER BY c.id) <= 2) s ON true`,
			pmPairs(8, 1, 2)},
		{"userDerivedQualifyJoinedOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN (SELECT c.bal AS k, c.id AS m FROM e7bal c ` +
				`QUALIFY ROW_NUMBER() OVER (PARTITION BY c.bal ORDER BY c.id) <= 2) s ON s.k = b.bal`,
			pmPairs(8, 1, 2)},
		{"userWindowUncorrelatedBody",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN LATERAL (SELECT c.id AS m, ` +
				`ROW_NUMBER() OVER (PARTITION BY c.bal ORDER BY c.id) AS rn FROM e7bal c) s ON s.rn <= 1`,
			pmPairs(8, 1)},
		// e7emp: the correlation on the UNPOLICED dept, the body publishing
		// the masked ssn — the mask, per outer row, never the stored value.
		{"boundPublishesMaskedColumn",
			`SELECT b.id AS a, s.m AS m FROM e7emp b JOIN LATERAL (SELECT c.ssn AS m ` +
				`FROM e7emp c WHERE c.dept = b.dept ORDER BY c.id LIMIT 1) s ON true`,
			pmSSNPairs()},
		// EXISTS with a bound the rewrite now DECLINES to the per-row rerun:
		// `LIMIT 0` is empty for every row; `LIMIT 1` keeps the semi join and
		// the mask's one class matches every row.
		{"existsLimit0OnMasked",
			`SELECT b.id AS a, COUNT(*) AS m FROM e7bal b WHERE EXISTS (SELECT 1 FROM e7bal c ` +
				`WHERE c.bal = b.bal LIMIT 0) GROUP BY b.id`,
			""},
		{"existsLimit1OnMasked",
			`SELECT b.id AS a, COUNT(*) AS m FROM e7bal b WHERE EXISTS (SELECT 1 FROM e7bal c ` +
				`WHERE c.bal = b.bal LIMIT 1) GROUP BY b.id`,
			pmPairs(8, 1)},
		// EXISTS over a GROUPED body with a HAVING (#1274), per row: under the
		// mask the one group has 8 rows, so every outer row survives `> 4` and
		// none survives `> 8`.
		{"existsHavingOnMaskedTrue",
			`SELECT b.id AS a, COUNT(*) AS m FROM e7bal b WHERE EXISTS (SELECT 1 FROM e7bal c ` +
				`WHERE c.bal = b.bal GROUP BY c.bal HAVING COUNT(*) > 4) GROUP BY b.id`,
			pmPairs(8, 1)},
		{"existsHavingOnMaskedFalse",
			`SELECT b.id AS a, COUNT(*) AS m FROM e7bal b WHERE EXISTS (SELECT 1 FROM e7bal c ` +
				`WHERE c.bal = b.bal GROUP BY c.bal HAVING COUNT(*) > 8) GROUP BY b.id`,
			""},
	}

	refusals := []struct{ name, sql, class string }{
		// A bound with no equality key over a POLICED comparison is refused,
		// and the refusal carries no stored value.
		{"boundNoKeyOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN LATERAL (SELECT c.id AS m ` +
				`FROM e7bal c WHERE c.bal < b.bal ORDER BY c.id LIMIT 1) s ON true`,
			"cannot apply that bound per outer row"},
		{"boundMixedOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN LATERAL (SELECT c.id AS m ` +
				`FROM e7bal c WHERE c.id = b.id AND c.bal < b.bal ORDER BY c.id LIMIT 1) s ON true`,
			"cannot apply that bound per outer row"},
		{"distinctLiftedOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN LATERAL (SELECT DISTINCT c.id % 2 AS m ` +
				`FROM e7bal c WHERE c.bal < b.bal) s ON true`,
			"which would have to publish the column it names"},
		{"contestedLiftedOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN LATERAL (SELECT c.bal AS m ` +
				`FROM e7bal c WHERE c.id < b.id) s ON true`,
			"which the enclosing relation also publishes"},
	}

	answered := 0
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
	refused := 0
	for _, c := range refusals {
		for _, door := range rig.doors {
			t.Run(c.name+"/"+door.name, func(t *testing.T) {
				got, err := door.run(t, "analyst-key", c.sql)
				if err == nil {
					t.Fatalf("answered where arc LT refuses (%q): %v\n  SQL: %s", c.class, got.canon(), c.sql)
				}
				if !strings.Contains(err.Error(), c.class) {
					t.Fatalf("refused with a different sentence: %v\n  SQL: %s", err, c.sql)
				}
				for _, s := range leaks {
					if strings.Contains(err.Error(), s) {
						t.Fatalf("a policed value reached the client inside a refusal: %q\n  %v", s, err)
					}
				}
				refused++
			})
		}
	}
	// NON-VACUOUS: every cell on every door, both tables.
	if want := len(cells) * len(rig.doors); answered != want {
		t.Errorf("%d (cell, door) pairs answered, want %d — the gate is vacuous", answered, want)
	}
	if want := len(refusals) * len(rig.doors); refused != want {
		t.Errorf("%d (cell, door) pairs refused, want %d — the refusal half is vacuous", refused, want)
	}
}

// pmSSNPairs is `a=k|m=***` for every e7emp row: the masked ssn of the
// smallest id in each department, which under the mask is the mask.
func pmSSNPairs() string {
	out := make([]string, 0, pmRows)
	for a := 1; a <= pmRows; a++ {
		out = append(out, fmt.Sprintf("a=%d|m=%s", a, pmMaskSSN))
	}
	sort.Strings(out)
	return strings.Join(out, " ; ")
}

// pmWindowPairs is `a=k|m=j|rn=j` for every k and j in 1..8: under the mask
// the one partition numbers all eight rows in id order, for every outer row.
func pmWindowPairs() string {
	out := make([]string, 0, 64)
	for a := 1; a <= 8; a++ {
		for j := 1; j <= 8; j++ {
			out = append(out, fmt.Sprintf("a=%d|m=%d|rn=%d", a, j, j))
		}
	}
	sort.Strings(out)
	return strings.Join(out, " ; ")
}
