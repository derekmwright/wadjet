// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A WINDOW'S TERMS AND AN ITEM MIXING AN AGGREGATE AND A WINDOW READ THE
// POLICED COLUMN, ON EVERY DOOR (#1651, #1646).
//
// The projection above a window now applies an item's aggregate slots as well
// as its window slots (#1646), and the grouped check now judges a window's
// terms (#1651). An item that answers must answer the MASKED reading (the
// policy is a plan-time projection at the scan, ADR-0033), a denied column in
// a window term stays unknown, and a row filter keeps its rows out of every
// window and aggregate. The assertion is the masking census's: no true value
// of a policed column in any cell, no denied column in any list, the mask's
// own value where a masked item answers, and a NON-VACUOUS count of
// (cell, door) pairs that answered. At 542b4f37 the masked mixed items
// answered NULL in place of the mask on every door that answered.
func TestArcCWWindowTermsNeverPublishAPolicedValue(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	cells := []struct {
		name, sql string
		// maskCol names the result column whose every answered value must
		// hold the mask.
		maskCol string
		// mustAnswer: every door answers this cell (the non-vacuous half).
		mustAnswer bool
	}{
		{"mixedMasked", `SELECT max(ssn) || CAST(row_number() OVER () AS VARCHAR) AS k FROM e7emp e`, "k", true},
		{"mixedMaskedGrouped", `SELECT dept, max(e.ssn) || CAST(rank() OVER (ORDER BY dept) AS VARCHAR) AS k FROM e7emp e GROUP BY dept`, "k", true},
		{"mixedMaskedNumeric", `SELECT dept, max(acct) + row_number() OVER (ORDER BY dept) AS k FROM e7emp e GROUP BY dept`, "", true},
		{"qualifyMixedMasked", `SELECT dept, max(acct) + row_number() OVER (ORDER BY dept) AS k FROM e7emp e GROUP BY dept QUALIFY k > 0`, "", true},
		{"windowOverMaskedAgg", `SELECT dept, max(max(ssn)) OVER () AS k FROM e7emp e GROUP BY dept`, "k", true},
		{"windowKeyMaskedAgg", `SELECT dept, max(ssn) AS k, rank() OVER (ORDER BY max(ssn), dept) AS r FROM e7emp e GROUP BY dept`, "k", true},
		{"mixedRowFiltered", `SELECT b.id, max(b.bal) + row_number() OVER (ORDER BY b.id) AS k FROM e7bal b GROUP BY b.id`, "", true},
		{"windowSumRowFiltered", `SELECT count(*) + sum(count(*)) OVER () AS k FROM e7bal b`, "", true},
		{"ungroupedMaskedArg", `SELECT dept, sum(acct) OVER () AS w FROM e7emp e GROUP BY dept`, "", false},
		{"ungroupedMaskedKey", `SELECT dept, rank() OVER (ORDER BY ssn) AS w FROM e7emp e GROUP BY dept`, "", false},
		{"deniedWindowArg", `SELECT dept, sum(max(salary)) OVER () AS w FROM e7emp e GROUP BY dept`, "", false},
		{"deniedMixed", `SELECT dept, max(salary) + row_number() OVER (ORDER BY dept) AS w FROM e7emp e GROUP BY dept`, "", false},
	}
	answered := 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if err != nil {
				if cell.mustAnswer {
					t.Errorf("%s / %s: refused %v\n  %s", cell.name, door.name, err, cell.sql)
				}
				continue
			}
			if strings.HasPrefix(cell.name, "denied") {
				t.Errorf("%s / %s: an item over a DENIED column answered %v\n  %s", cell.name, door.name, got.rows, cell.sql)
			}
			if strings.HasPrefix(cell.name, "ungrouped") {
				t.Errorf("%s / %s: an ungrouped window term answered %v where PostgreSQL raises 42803\n  %s",
					cell.name, door.name, got.rows, cell.sql)
			}
			answered++
			for _, c := range got.cols {
				if strings.EqualFold(c, "salary") {
					t.Errorf("%s / %s: the DENIED column %q is in the published list\n  %s", cell.name, door.name, c, cell.sql)
				}
			}
			if len(got.rows) == 0 {
				t.Errorf("%s / %s: no rows — this cell's leak test cannot fail\n  %s", cell.name, door.name, cell.sql)
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
				// acct masks to 0, so max(acct) + row_number() is the row
				// number alone: anything else is a value the mask did not
				// produce (a raw acct is in the 900000s).
				if cell.name == "mixedMaskedNumeric" || cell.name == "qualifyMixedMasked" {
					if v := row["k"]; v != "1" && v != "2" && v != "3" {
						t.Errorf("%s / %s: k=%q is not the mask's 0 plus the row number\n  %s", cell.name, door.name, v, cell.sql)
					}
				}
				if cell.maskCol != "" && !strings.Contains(row[cell.maskCol], pmMaskSSN) {
					t.Errorf("%s / %s: %s=%q is not the mask's reading %q\n  %s",
						cell.name, door.name, cell.maskCol, row[cell.maskCol], pmMaskSSN, cell.sql)
				}
			}
		}
	}
	want := 0
	for _, c := range cells {
		if c.mustAnswer {
			want += len(rig.doors)
		}
	}
	if answered != want {
		t.Errorf("%d (cell, door) pairs answered, want %d (every door on every answering cell)", answered, want)
	}
	t.Logf("%d of %d (cell, door) pairs answered over %d doors; the rest refused",
		answered, len(cells)*len(rig.doors), len(rig.doors))
}
