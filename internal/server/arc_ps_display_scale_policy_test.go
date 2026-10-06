// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A VALUE'S DISPLAY SCALE NEVER CARRIES A POLICED VALUE, ON EVERY DOOR
// (ADR-0024 §1 as amended, arc PS stage 1).
//
// A set operation re-materializes its arms' values and a COALESCE / CASE /
// LEAST answers its chosen value's text: both now hand on each value's own
// display scale, through the single process, the spill run, the .wshf
// exchange and the coordinator's merge. A masked column read through any of
// them must answer the MASK's value, a row-filtered relation must answer its
// visible rows only, and a denied column stays unknown. The assertion is the
// masking census's: no true value of a policed column in any cell, no denied
// column in any list, and a NON-VACUOUS count of (cell, door) pairs that
// answered.
func TestArcPSDisplayScaleNeverPublishesAPolicedValue(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()
	cells := []struct{ name, sql, maskCol string }{
		{"maskedFold", `SELECT COALESCE(CAST(acct AS NUMERIC(12,2)), 1.5) AS k FROM e7emp`, ""},
		{"maskedCaseLiteral", `SELECT CASE WHEN id > 3 THEN CAST(acct AS NUMERIC(12,2)) ELSE 2.50 END AS k FROM e7emp`, ""},
		{"maskedLeast", `SELECT LEAST(CAST(acct AS NUMERIC(12,2)), '0.5') AS k FROM e7emp`, ""},
		{"maskedUnionLiteral", `SELECT CAST(acct AS NUMERIC(12,2)) AS k FROM e7emp UNION ALL SELECT 2.50 FROM e7other`, ""},
		{"maskedUnionDistinct", `SELECT CAST(acct AS NUMERIC(12,2)) AS k FROM e7emp UNION SELECT 1.5 FROM e7other`, ""},
		{"maskedUnionText", `SELECT CAST(k AS TEXT) AS t FROM (SELECT CAST(acct AS NUMERIC(12,2)) AS k FROM e7emp UNION ALL SELECT 2.50 FROM e7other) s`, ""},
		{"maskedFoldGroupKey", `SELECT k, count(*) AS c FROM (SELECT COALESCE(CAST(acct AS NUMERIC(12,2)), 1.5) AS k FROM e7emp) s GROUP BY k`, ""},
		{"maskedSsnBesideAFold", `SELECT ssn, COALESCE(CAST(acct AS NUMERIC(12,2)), 1.5) AS k FROM e7emp`, "ssn"},
		{"rowFilteredFold", `SELECT COALESCE(CAST(bal AS NUMERIC(10,2)), 1.5) AS k FROM e7bal`, ""},
		{"rowFilteredUnion", `SELECT CAST(bal AS NUMERIC(10,2)) AS k FROM e7bal UNION ALL SELECT 2.50 FROM e7other`, ""},
		{"rowFilteredUnionCount", `SELECT count(*) AS c FROM (SELECT CAST(bal AS NUMERIC(10,2)) AS k FROM e7bal UNION ALL SELECT 2.50 FROM e7other) s WHERE CAST(k AS TEXT) LIKE '%0'`, ""},
		{"deniedFold", `SELECT COALESCE(CAST(salary AS NUMERIC(12,2)), 1.5) AS k FROM e7emp`, ""},
		{"deniedUnion", `SELECT CAST(salary AS NUMERIC(12,2)) AS k FROM e7emp UNION ALL SELECT 2.50 FROM e7other`, ""},
	}
	answered := 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if err != nil {
				// A refusal is a disposition this gate accepts (the denied
				// cells must take it); the claim is that nothing POLICED
				// reaches a client.
				continue
			}
			if strings.HasPrefix(cell.name, "denied") {
				t.Errorf("%s / %s: a fold over a DENIED column answered %v\n  %s", cell.name, door.name, got.rows, cell.sql)
			}
			answered++
			for _, c := range got.cols {
				if strings.EqualFold(c, "salary") {
					t.Errorf("%s / %s: the DENIED column %q is in the published list\n  %s", cell.name, door.name, c, cell.sql)
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
				if cell.maskCol != "" && !strings.Contains(row[cell.maskCol], pmMaskSSN) {
					t.Errorf("%s / %s: %s=%q is not the mask's reading %q\n  %s",
						cell.name, door.name, cell.maskCol, row[cell.maskCol], pmMaskSSN, cell.sql)
				}
			}
		}
	}
	if min := 10 * len(rig.doors); answered < min {
		t.Fatalf("%d (cell, door) pairs answered, want at least %d: the leak test is vacuous on a door", answered, min)
	}
	t.Logf("%d of %d (cell, door) pairs answered over %d doors; the rest refused",
		answered, len(cells)*len(rig.doors), len(rig.doors))
}
