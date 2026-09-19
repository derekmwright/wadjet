// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"strings"
	"testing"
	"time"
)

// ARC PT's MASKING GATE: a POLICED column keeps its mask when the statement
// RENAMES it, and when the new predicate and function spellings read it.
//
// Two of this arc's changes touch a relation's columns above the scan. A
// column-alias list on a FROM item renames them positionally — `FROM e7emp
// AS e(i, d, s, a, sal, m)` publishes the masked `ssn` under the name `s`
// (#1184 made the list reach a table function; arc PS made it reach a base
// table) — and the predicate band now admits `SIMILAR TO`, `LIKE … ESCAPE`,
// `#` and a comparison after BETWEEN over any column, which is where a
// per-row disclosure of a masked value would show (#859's own mechanism: a
// predicate that reads the STORED column answers a different row set from one
// that reads the mask).
//
// The claim is the census's: no true value of a masked column reaches a
// client, and the DENIED column is in no published list, whatever shape
// carried it — on all nine doors. The count of (cell, door) pairs that
// ANSWERED is asserted, because a gate whose cells all refuse proves nothing.
func TestArcPTARenamedOrRepredicatedPolicedColumnKeepsItsMask(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	cells := []struct{ name, sql string }{
		// --- the column-alias list RENAMES the policed columns ------------
		// The DENIED column is not part of the relation this identity sees, so
		// the list is FIVE names wide here — `id, dept, ssn, acct, amt` — and
		// the masked `ssn` is renamed to `s`.
		{"alias-list-renames-the-masked-column",
			`SELECT s FROM e7emp AS e(i, d, s, a, m)`},
		{"alias-list-star", `SELECT * FROM e7emp AS e(i, d, s, a, m)`},
		{"alias-list-qualified-star", `SELECT e.* FROM e7emp AS e(i, d, s, a, m)`},
		{"alias-list-without-as", `SELECT s, a FROM e7emp e(i, d, s, a, m)`},
		{"alias-list-prefix-only", `SELECT ssn FROM e7emp AS e(i, d)`},
		{"alias-list-renames-only-the-masked-column",
			`SELECT s FROM e7emp AS e(i, d, s)`},
		{"alias-list-under-an-aggregate",
			`SELECT COUNT(*) AS n, MAX(s) AS mx FROM e7emp AS e(i, d, s, a, m)`},
		{"alias-list-in-a-join",
			`SELECT e.s FROM e7emp AS e(i, d, s, a, m) JOIN e7other o ON o.id = e.i`},
		{"alias-list-over-the-row-filtered-table",
			`SELECT b FROM e7bal AS z(i, b) ORDER BY i`},
		{"alias-list-in-a-derived-table",
			`SELECT s FROM (SELECT * FROM e7emp) AS e(i, d, s, a, m)`},
		{"alias-list-in-a-predicate-over-the-mask",
			`SELECT i FROM e7emp AS e(i, d, s, a, m) WHERE s SIMILAR TO 'true-%'`},
		{"alias-list-with-a-name-for-the-denied-column",
			`SELECT s FROM e7emp AS e(i, d, s, a, sal, m)`},

		// --- the new predicate and function spellings read the column -----
		{"similar-to-over-a-masked-column", `SELECT id, ssn FROM e7emp WHERE ssn SIMILAR TO '%'`},
		{"similar-to-selects-on-the-mask",
			`SELECT id FROM e7emp WHERE ssn SIMILAR TO 'true-%'`},
		{"not-similar-to-over-a-masked-column",
			`SELECT id FROM e7emp WHERE ssn NOT SIMILAR TO 'true-%'`},
		{"like-escape-over-a-masked-column",
			`SELECT id, ssn FROM e7emp WHERE ssn LIKE 'true!-%' ESCAPE '!'`},
		{"substring-of-a-masked-column",
			`SELECT SUBSTRING(ssn FROM 1 FOR 4) AS v FROM e7emp`},
		{"left-of-a-masked-column", `SELECT LEFT(ssn, 5) AS v FROM e7emp`},
		{"overlay-over-a-masked-column",
			`SELECT OVERLAY(ssn PLACING 'X' FROM 1) AS v FROM e7emp`},
		{"normalize-of-a-masked-column", `SELECT NORMALIZE(ssn, NFC) AS v FROM e7emp`},
		{"xor-over-a-masked-numeric", `SELECT acct # 0 AS v FROM e7emp`},
		{"xor-of-a-masked-numeric-in-a-predicate",
			`SELECT id FROM e7emp WHERE acct # 0 = acct`},
		{"between-then-comparison-over-a-masked-numeric",
			`SELECT id FROM e7emp WHERE acct BETWEEN 900000 AND 999999 = true`},
		{"is-not-unknown-over-a-masked-predicate",
			`SELECT id FROM e7emp WHERE (ssn LIKE '%') IS NOT UNKNOWN`},
		{"row-filter-under-a-similar-to",
			`SELECT id, bal FROM e7bal WHERE CAST(bal AS VARCHAR) SIMILAR TO '%'`},
		{"row-filter-under-a-xor", `SELECT id, bal # 0 AS v FROM e7bal`},
		// The DENIED column, named through the rename and through a new
		// spelling: neither may publish it.
		{"denied-column-through-the-rename",
			`SELECT sal FROM e7emp AS e(i, d, s, a, sal, m)`},
		{"denied-column-named-directly", `SELECT salary FROM e7emp`},
		{"denied-column-through-a-function", `SELECT LEFT(CAST(salary AS VARCHAR), 3) FROM e7emp`},
	}

	answered, refused := 0, 0
	perDoor := map[string]int{}
	perCell := map[string]int{}
	for _, cell := range cells {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if err != nil {
				// A refusal is a disposition this gate accepts: the claim is
				// that nothing POLICED reaches a client, not that every shape
				// answers.
				refused++
				continue
			}
			answered++
			perDoor[door.name]++
			perCell[cell.name]++
			for _, c := range got.cols {
				if strings.EqualFold(c, "salary") || strings.EqualFold(c, "sal") {
					t.Errorf("%s / %s: the DENIED column %q is in the published list\n  %s",
						cell.name, door.name, c, cell.sql)
				}
			}
			for _, row := range got.rows {
				for c, v := range row {
					for _, bad := range leaks {
						if strings.Contains(v, bad) {
							t.Errorf("%s / %s: %s=%s is a policed value reaching the client\n  %s",
								cell.name, door.name, c, v, cell.sql)
						}
					}
				}
			}
		}
	}
	if answered == 0 {
		t.Fatalf("no (cell, door) pair answered: this gate's leak test cannot fail, and "+
			"%d shapes over policed relations were refused on every door", len(cells))
	}
	if len(perDoor) != len(rig.doors) {
		t.Errorf("only %d of %d doors answered any cell (%v); a door that answers nothing "+
			"is a door this gate does not cover", len(perDoor), len(rig.doors), perDoor)
	}
	// The RENAME cells are the subject: a cell that refuses on every door
	// measures nothing about whether the mask survived the rename.
	for _, cell := range cells {
		if strings.HasPrefix(cell.name, "alias-list") &&
			cell.name != "alias-list-with-a-name-for-the-denied-column" &&
			perCell[cell.name] == 0 {
			t.Errorf("%s answered on NO door, so this gate says nothing about the mask "+
				"under a rename\n  %s", cell.name, cell.sql)
		}
	}
	t.Logf("%d of %d (cell, door) pairs answered and %d were refused, across %d doors; "+
		"per cell: %v", answered, len(cells)*len(rig.doors), refused, len(rig.doors), perCell)
}
