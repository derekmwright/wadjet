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

	// THE DECLARED OUTPUT IS PostgreSQL's, ON EVERY DOOR. A column the
	// planner materializes for a lifted predicate must never enter a star's
	// list — two of the nine doors are pgwire, where that is `RowDescription`.
	// Since arc JP round 4 a BARE star over a LATERAL is expanded into the
	// FROM arms' own lists, which hide it as the qualified star always did, so
	// the two bare-star cells ANSWER — with the mask's rows (`c.bal < b.bal` is
	// false for every pair under the mask, so every outer row pads NULL) and
	// exactly PostgreSQL's columns, which the canonical rows spell (`want`).
	starCols := []struct{ name, sql, want string }{
		{"starOverLifted",
			`SELECT * FROM e7bal b LEFT JOIN LATERAL (SELECT c.id AS m FROM e7bal c ` +
				`WHERE c.bal < b.bal) s ON true`,
			"bal=0|id=1|m=NULL ; bal=0|id=2|m=NULL ; bal=0|id=3|m=NULL ; bal=0|id=4|m=NULL ; " +
				"bal=0|id=5|m=NULL ; bal=0|id=6|m=NULL ; bal=0|id=7|m=NULL ; bal=0|id=8|m=NULL"},
		{"starOverLiftedUnpoliced",
			`SELECT * FROM e7other b LEFT JOIN LATERAL (SELECT c.note AS m FROM e7other c ` +
				`WHERE c.id < b.id) s ON true`,
			"id=1|m=NULL|note=n1 ; id=2|m=n1|note=n2 ; id=3|m=n1|note=n3 ; id=3|m=n2|note=n3"},
		{"qualifiedStarOverLifted",
			`SELECT s.* FROM e7bal b LEFT JOIN LATERAL (SELECT c.id AS m FROM e7bal c ` +
				`WHERE c.bal < b.bal) s ON true`, "m"},
	}
	for _, c := range starCols {
		for _, door := range rig.doors {
			t.Run(c.name+"/"+door.name, func(t *testing.T) {
				got, err := door.run(t, "analyst-key", c.sql)
				if c.name != "qualifiedStarOverLifted" {
					if err != nil {
						t.Fatalf("a bare star over a lifted-predicate lateral was refused: %v\n  SQL: %s", err, c.sql)
					}
					rendered := strings.Join(got.canon(), " ; ")
					for _, v := range pmTrueValues() {
						if strings.Contains(rendered, v) {
							t.Fatalf("a masked or denied value reached the client: %q\n  %s", v, rendered)
						}
					}
					if rendered != c.want {
						t.Errorf("%s\n  door %s\n  got  %s\n  want %s (the value the policy PUBLISHES)",
							c.sql, door.name, rendered, c.want)
					}
					return
				}
				// SINCE ARC LT the materialization no longer DECLINES under an
				// enclosing BARE star — it REFUSES, on every door, because the
				// declined shape answered a NULL-padded row per outer row for
				// PostgreSQL's rows (ADR-0021 §1s; `ltLiftedRefCannotPublish`).
				// The QUALIFIED star reads the body's own list and would
				// answer, but THIS body's lifted column `bal` is one the
				// enclosing e7bal publishes too, so it refuses with the
				// contested-name sentence instead. Either way the declared
				// list is not observable here any more, and what each cell
				// holds now is the refusal itself and the absence of every
				// policed value in its text.
				want := "which would have to publish the column it names"
				if c.name == "qualifiedStarOverLifted" {
					want = "which the enclosing relation also publishes"
				}
				if err == nil {
					// The QUALIFIED star over the contested body ANSWERS on
					// the four DAG doors (round 2: the contested refusal is
					// the single path's alone) — and the answer must be the
					// MASK's: `c.bal < b.bal` is false for every pair under
					// the mask, so every outer row pads NULL.
					if c.name == "qualifiedStarOverLifted" {
						if r := strings.Join(got.canon(), " ; "); r == strings.Repeat("m=NULL ; ", 7)+"m=NULL" {
							return
						}
					}
					t.Fatalf("a star over a lifted-predicate lateral answered where "+
						"arc LT refuses it: %v\n  SQL: %s", got.canon(), c.sql)
				}
				// The four DAG doors refuse the bare-star shapes with arc JR's
				// residual sentence (the decline is the single path's alone
				// since round 2), and the five single-process doors with the
				// star sentence; either is the refusal this cell holds.
				if !strings.Contains(err.Error(), want) &&
					!strings.Contains(err.Error(), "and a bare star would publish that column too") &&
					!strings.Contains(err.Error(), "resolves on neither side") {
					t.Fatalf("refused with a different sentence: %v\n  SQL: %s", err, c.sql)
				}
				// The `(SQLSTATE 42000)` suffix carries "200" as a substring;
				// the scan reads the sentence without it.
				text := pmStripSQLState(err.Error())
				for _, s := range pmTrueValues() {
					if strings.Contains(text, s) {
						t.Fatalf("a policed value reached the client in a refusal: %q\n  %v", s, err)
					}
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

// pmDeptPairs is PostgreSQL's per-outer-row answer for e7emp's `dept`
// correlation: each outer row pairs with the SMALLEST id in its own
// department (dept is `d<i%3>`, so ids 1,4,7,10 / 2,5,8 / 3,6,9). It held the
// whole-relation bound's four pairs while #1019 was open; arc LT applies the
// bound per outer row (ADR-0021 §1s) and the cell asserts PostgreSQL on every
// door — and still none of them reads a masked or denied value.
func pmDeptPairs() string {
	out := make([]string, 0, pmRows)
	for a := 1; a <= pmRows; a++ {
		m := 1 + ((a - 1) % 3)
		if a%3 == 0 {
			m = 3
		}
		out = append(out, fmt.Sprintf("a=%d|m=%d", a, m))
	}
	sort.Strings(out)
	return strings.Join(out, " ; ")
}

// l1LiftedStarDAGDoors names the four doors on which a bare star over a
// LIFTED-predicate lateral refuses since arc JR. See the skip above.
var l1LiftedStarDAGDoors = map[string]bool{
	"embedded/dag":          true,
	"embedded/dag-shuffled": true,
	"pgwire/dag":            true,
	"http/dag":              true,
}

// pmStripSQLState removes a trailing `(SQLSTATE xxxxx)` so a leak scan over a
// refusal's text does not read the code's digits as a stored value.
func pmStripSQLState(msg string) string {
	if i := strings.LastIndex(msg, "(SQLSTATE "); i >= 0 {
		return msg[:i]
	}
	return msg
}
