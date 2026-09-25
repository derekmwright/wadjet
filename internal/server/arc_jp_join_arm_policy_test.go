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

// A REORDERED OR RE-KEYED JOIN READS THE VALUE THE POLICY PUBLISHES, ON EVERY
// DOOR — arc JP, the masking gate COMMON names for a fix that relocates or
// materializes over a relation.
//
// Arc JP changes two things a policed relation passes through. The join
// reorderer now hangs an ON conjunct on the join that holds every relation it
// NAMES by qualifier (#1299), which relocates the copies of a self-joined
// relation in the join tree. And a correlated LATERAL equality whose outer
// side is an expression now EMITS the body's key column above the join, where
// the equality is evaluated (#1302). Over e7bal — `bal` masked to 0 from
// stored values that are distinct per row — a join keyed on the MASK pairs
// every row with every row, and one keyed on the stored column pairs a row
// only with itself; over e7emp a correlated body publishes the masked ssn.
// Every cell is asserted against the mask's answer on all nine doors, every
// rendered cell is checked against every stored policed value, and the count
// of (cell, door) pairs is asserted so the gate cannot pass by refusing.
func TestArcJPAReorderedAndReKeyedJoinsReadThePublishedValueOnEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up all nine policy doors")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	cells := []struct{ name, sql, want string }{
		// #1299: three copies of e7bal keyed on the masked column, each with
		// its own filter. Under the mask each outer row pairs with c ∈ {1,2}
		// and d ∈ {3,4}; under the stored values nothing pairs at all.
		{"selfJoinStarOnMasked",
			`SELECT b.id AS a, c.id * 10 + d.id AS m FROM e7bal b ` +
				`JOIN e7bal c ON c.bal = b.bal AND c.id IN (1,2) ` +
				`JOIN e7bal d ON d.bal = b.bal AND d.id IN (3,4)`,
			pmPairs(8, 13, 14, 23, 24)},
		{"selfJoinChainOnMasked",
			`SELECT b.id AS a, c.id * 10 + d.id AS m FROM e7bal b ` +
				`JOIN e7bal c ON c.bal = b.bal AND c.id IN (1,2) ` +
				`JOIN e7bal d ON d.bal = c.bal AND d.id IN (3,4)`,
			pmPairs(8, 13, 14, 23, 24)},
		// The issue's shape: d's key names a column (`id`) every copy
		// carries, and a key hung on the c–d join binds `b.id` to c's id.
		// Under the mask c pairs with every b; d is b itself.
		{"selfJoinCrossKeyOnMasked",
			`SELECT b.id AS a, c.id * 10 + d.id AS m FROM e7bal b ` +
				`JOIN e7bal c ON c.bal = b.bal AND c.id IN (1,2) ` +
				`JOIN e7bal d ON d.id = b.id AND d.id IN (3,4,5)`,
			pmSorted("a=3|m=13 ; a=3|m=23 ; a=4|m=14 ; a=4|m=24 ; a=5|m=15 ; a=5|m=25")},
		{"selfJoinCrossKeyFourOnMasked",
			`SELECT b.id AS a, c.id * 100 + d.id * 10 + e.id AS m FROM e7bal b ` +
				`JOIN e7bal c ON c.bal = b.bal AND c.id IN (1,2) ` +
				`JOIN e7bal d ON d.id = b.id AND d.id IN (3,4) ` +
				`JOIN e7bal e ON e.id = b.id AND e.bal = c.bal`,
			pmSorted("a=3|m=133 ; a=3|m=233 ; a=4|m=144 ; a=4|m=244")},
		// Four copies of e7emp, keyed on the unpoliced dept with per-arm id
		// filters, publishing the masked ssn: one row per outer row, the mask.
		{"selfJoinFourPublishesMasked",
			`SELECT b.id AS a, e.ssn AS m FROM e7emp b ` +
				`JOIN e7emp c ON c.dept = b.dept AND c.id IN (1,2,3) ` +
				`JOIN e7emp d ON d.dept = b.dept AND d.id IN (4,5,6) ` +
				`JOIN e7emp e ON e.id = c.id`,
			pmSSNAll()},
		{"selfJoinLeftOnMasked",
			`SELECT b.id AS a, c.id AS m FROM e7bal b ` +
				`LEFT JOIN e7bal c ON c.bal = b.bal AND c.id IN (1,2) ` +
				`LEFT JOIN e7bal d ON d.bal = c.bal AND d.id IN (3,4)`,
			pmSorted(pmPairs(8, 1, 1, 2, 2))},
		// #1302: the outer side of the correlated equality an expression over
		// the masked column. Under the mask it is 0 for every outer row and
		// matches all eight inner rows; bound to the stored value it would
		// match the row itself.
		{"exprKeyOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN LATERAL (SELECT c.id AS m ` +
				`FROM e7bal c WHERE c.bal = b.bal - 0) s ON true`,
			pmPairs(8, 1, 2, 3, 4, 5, 6, 7, 8)},
		{"exprKeyBoundOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN LATERAL (SELECT c.id AS m ` +
				`FROM e7bal c WHERE c.bal = b.bal + 0 ORDER BY c.id LIMIT 2) s ON true`,
			pmPairs(8, 1, 2)},
		{"exprKeyLeftOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b LEFT JOIN LATERAL (SELECT c.id AS m ` +
				`FROM e7bal c WHERE c.bal = abs(b.bal) ORDER BY c.id DESC LIMIT 1) s ON true`,
			pmPairs(8, 8)},
		{"exprKeyCommaOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b, LATERAL (SELECT c.id AS m ` +
				`FROM e7bal c WHERE coalesce(b.bal, 1) = c.bal ORDER BY c.id LIMIT 1) s`,
			pmPairs(8, 1)},
		{"exprKeyCountOnMasked",
			`SELECT b.id AS a, s.m AS m FROM e7bal b JOIN LATERAL (SELECT COUNT(*) AS m ` +
				`FROM e7bal c WHERE c.bal = b.bal * 1) s ON true`,
			pmPairs(8, 8)},
		{"exprKeyQualifiedStarOnMasked",
			`SELECT b.id AS a, s.* FROM e7bal b JOIN LATERAL (SELECT c.id AS m ` +
				`FROM e7bal c WHERE c.bal = b.bal - 0 ORDER BY c.id LIMIT 1) s ON true`,
			pmPairs(8, 1)},
		// e7emp: the key an expression over the masked ssn (all '***' under
		// the mask, so every row counts twelve), and one over the unpoliced
		// dept publishing the masked ssn.
		{"exprKeyOnMaskedString",
			`SELECT b.id AS a, s.m AS m FROM e7emp b JOIN LATERAL (SELECT COUNT(*) AS m ` +
				`FROM e7emp c WHERE c.ssn = b.ssn || '') s ON true`,
			pmAllWith(pmRows, "12")},
		{"exprKeyPublishesMasked",
			`SELECT b.id AS a, s.m AS m FROM e7emp b JOIN LATERAL (SELECT c.ssn AS m ` +
				`FROM e7emp c WHERE c.dept = b.dept || '' ORDER BY c.id LIMIT 1) s ON true`,
			pmSSNPairs()},
		// Round 2: bodies that publish their columns UNALIASED, so every
		// name they publish is a name the outer copy of the same table
		// publishes too. Read by name, `s.id` would be the OUTER `b.id` (one
		// pair per row); the lateral's own rows are all eight under the mask.
		{"r2DistinctUnaliasedOnMasked",
			`SELECT b.id AS a, s.id AS m FROM e7bal b JOIN LATERAL (SELECT DISTINCT c.id ` +
				`FROM e7bal c WHERE c.bal = b.bal - 0) s ON true`,
			pmPairs(8, 1, 2, 3, 4, 5, 6, 7, 8)},
		{"r2BoundUnaliasedOnMasked",
			`SELECT b.id AS a, s.id AS m FROM e7bal b JOIN LATERAL (SELECT c.id ` +
				`FROM e7bal c WHERE c.bal = b.bal + 0 ORDER BY c.id LIMIT 2) s ON true`,
			pmPairs(8, 1, 2)},
		{"r2GroupedUnaliasedOnMasked",
			`SELECT b.id AS a, s.n AS m FROM e7bal b JOIN LATERAL (SELECT c.bal, count(*) AS n ` +
				`FROM e7bal c WHERE c.bal = b.bal * 1 GROUP BY c.bal) s ON true`,
			pmPairs(8, 8)},
		{"r2UnaliasedPublishesMasked",
			`SELECT b.id AS a, s.ssn AS m FROM e7emp b JOIN LATERAL (SELECT c.ssn ` +
				`FROM e7emp c WHERE c.dept = b.dept || '' ORDER BY c.id LIMIT 1) s ON true`,
			pmSSNPairs()},
		// P1: an alias that is the OTHER table's name names that FROM item
		// only — `e7emp.` is the e7bal copy here, whose masked bal is 0.
		// Round 4: a bare star over an expression-keyed LATERAL is expanded
		// to the FROM arms' own lists — the POLICED list of the outer e7bal
		// (its masked bal publishes 0) and the lateral's `m` — never the key
		// slot the join evaluates the equality against. Under the mask every
		// bal is 0, so every visible row pairs with all eight.
		{"exprKeyBareStarOnMasked",
			`SELECT * FROM e7bal b JOIN LATERAL (SELECT c.id AS m ` +
				`FROM e7bal c WHERE c.bal = b.bal - 0) s ON true`,
			pmStarPairs(8)},
		{"r2AliasIsOtherTablesNameOnMasked",
			`SELECT e7bal.id AS a, e7emp.id AS m FROM e7emp e7bal JOIN e7bal e7emp ` +
				`ON e7emp.id = e7bal.id + 1 AND e7emp.bal = 0`,
			"a=1|m=2 ; a=2|m=3 ; a=3|m=4 ; a=4|m=5 ; a=5|m=6 ; a=6|m=7 ; a=7|m=8"},
	}
	// No refusal remains: the bare star that was refused through round 3 is
	// an answered cell above (arc JP round 4).
	refusals := []struct{ name, sql, class string }{}

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
					t.Fatalf("answered where arc JP refuses (%q): %v\n  SQL: %s", c.class, got.canon(), c.sql)
				}
				if !strings.Contains(err.Error(), c.class) {
					t.Fatalf("refused with a different sentence: %v\n  SQL: %s", err, c.sql)
				}
				text := pmStripSQLState(err.Error())
				for _, s := range leaks {
					if strings.Contains(text, s) {
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

// pmSSNAll is `a=k|m=***` for every e7emp row, sorted as canon sorts.
func pmSSNAll() string { return pmAllWith(pmRows, pmMaskSSN) }

// pmAllWith is `a=k|m=<v>` for k in 1..n, sorted as canon sorts.
func pmAllWith(n int, v string) string {
	out := make([]string, 0, n)
	for a := 1; a <= n; a++ {
		out = append(out, fmt.Sprintf("a=%d|m=%s", a, v))
	}
	sort.Strings(out)
	return strings.Join(out, " ; ")
}

// pmSorted re-sorts a rendered row list the way canon does.
func pmSorted(s string) string {
	rows := strings.Split(s, " ; ")
	sort.Strings(rows)
	return strings.Join(rows, " ; ")
}

// pmStarPairs is `bal=0|id=k|m=j` for every k, j in 1..n: `SELECT *` over
// e7bal (masked bal 0) beside the lateral's `m`.
func pmStarPairs(n int) string {
	out := make([]string, 0, n*n)
	for a := 1; a <= n; a++ {
		for m := 1; m <= n; m++ {
			out = append(out, fmt.Sprintf("bal=0|id=%d|m=%d", a, m))
		}
	}
	return strings.Join(out, " ; ")
}
