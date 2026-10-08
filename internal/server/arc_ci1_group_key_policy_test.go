// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A GROUP BY KEY MATCHED BY BINDING READS THE POLICED COLUMN, ON EVERY DOOR
// (ADR-0047 stage 1, #1524).
//
// The single-process engine now matches a select item, a HAVING term, an
// ORDER BY term and a window term to a GROUP BY key by the column each
// reference RESOLVES to, so `SELECT upper(e.ssn) … GROUP BY upper(ssn)`
// answers where its spellings differ — over a masked column, over a join, over
// a row-filtered relation. A key that answers must answer the MASKED reading
// (the policy is a plan-time projection at the scan, ADR-0033), and a denied
// column stays unknown whatever spelling reaches it. The assertion is the
// masking census's: no true value of a policed column in any cell, no denied
// column in any list, the mask's own value where a masked key answers, and a
// NON-VACUOUS count of (cell, door) pairs that answered.
func TestArcCI1GroupKeyByBindingNeverPublishesAPolicedValue(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	cells := []struct {
		name, sql string
		// maskCol names the result column that carries the masked key, whose
		// every answered value must hold the mask.
		maskCol string
		// mixed marks a key and a term spelled one qualified and one bare in
		// the direction (or over the join) the spelling rule refuses: the
		// embedded engine's doors answer it by binding, every door through
		// the coordinator or the http door's local path keeps the base 42803
		// (ADR-0047, Stages: stage 5) — a refusal there that starts answering fails here.
		mixed bool
	}{
		{"maskedKeyItemQual", `SELECT upper(e.ssn) AS k, count(*) AS c FROM e7emp e GROUP BY upper(ssn)`, "k", false},
		{"maskedKeyKeyQual", `SELECT upper(ssn) AS k, count(*) AS c FROM e7emp e GROUP BY upper(e.ssn)`, "k", true},
		{"maskedKeyOverJoin", `SELECT e.ssn || 'x' AS k, count(*) AS c FROM e7emp e JOIN e7other o ON o.id = e.id GROUP BY ssn || 'x'`, "k", true},
		{"maskedKeyHaving", `SELECT upper(ssn) AS k FROM e7emp e GROUP BY upper(ssn) HAVING upper(e.ssn) <> ''`, "k", false},
		{"maskedKeyOrderBy", `SELECT upper(e.ssn) AS k FROM e7emp e GROUP BY upper(ssn) ORDER BY upper(e.ssn)`, "k", false},
		{"maskedKeyWindow", `SELECT upper(ssn) AS k, rank() OVER (ORDER BY upper(e.ssn)) AS r FROM e7emp e GROUP BY upper(ssn)`, "k", false},
		{"maskedNumericKey", `SELECT e.acct + 1 AS k, count(*) AS c FROM e7emp e GROUP BY acct + 1`, "", false},
		{"rowFilteredKey", `SELECT b.bal + 1 AS k, count(*) AS c FROM e7bal b GROUP BY bal + 1`, "", false},
		{"rowFilteredKeyOverJoin", `SELECT b.bal * 2 AS k, count(*) AS c FROM e7bal b JOIN e7other o ON o.id = b.id GROUP BY bal * 2`, "", true},
		{"deniedKey", `SELECT e.salary + 1 AS k, count(*) AS c FROM e7emp e GROUP BY salary + 1`, "", false},
		{"deniedKeyOverJoin", `SELECT salary + 1 AS k, count(*) AS c FROM e7emp e JOIN e7other o ON o.id = e.id GROUP BY e.salary + 1`, "", false},
	}
	embeddedDoors := map[string]bool{"embedded/single": true, "embedded/spilled": true, "pgwire/single": true}
	answered, mixedAnswered := 0, 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if cell.mixed {
				switch {
				case embeddedDoors[door.name] && err != nil:
					t.Errorf("%s / %s: refused %v — the embedded engine matches the key by binding and PostgreSQL answers\n  %s",
						cell.name, door.name, err, cell.sql)
				case !embeddedDoors[door.name] && (err == nil || !strings.Contains(err.Error(), "must appear in the GROUP BY clause")):
					t.Errorf("%s / %s: %v — this door matches by spelling until ADR-0047 stage 5 and keeps the base 42803\n  %s",
						cell.name, door.name, err, cell.sql)
				case err == nil:
					mixedAnswered++
				}
			}
			if err != nil {
				// A refusal is a disposition this gate accepts (the denied
				// cells must take it); the claim is that nothing POLICED
				// reaches a client.
				continue
			}
			if strings.HasPrefix(cell.name, "denied") {
				t.Errorf("%s / %s: a key over a DENIED column answered %v\n  %s", cell.name, door.name, got.rows, cell.sql)
			}
			answered++
			for _, c := range got.cols {
				if strings.EqualFold(c, "salary") {
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
				if cell.maskCol != "" && !strings.Contains(row[cell.maskCol], pmMaskSSN) {
					t.Errorf("%s / %s: %s=%q is not the mask's reading %q\n  %s",
						cell.name, door.name, cell.maskCol, row[cell.maskCol], pmMaskSSN, cell.sql)
				}
			}
		}
	}
	if mixedAnswered != 3*len(embeddedDoors) {
		t.Errorf("%d (mixed cell, embedded door) pairs answered, want %d", mixedAnswered, 3*len(embeddedDoors))
	}
	if answered == 0 {
		t.Fatalf("no (cell, door) pair answered: this gate's leak test cannot fail, "+
			"and %d shapes over policed relations were refused on every door", len(cells))
	}
	t.Logf("%d of %d (cell, door) pairs answered over %d doors; the rest refused",
		answered, len(cells)*len(rig.doors), len(rig.doors))
}
