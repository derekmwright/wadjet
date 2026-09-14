package server

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A QUALIFIED STAR OVER A STAR-BODIED BLOCK READS THE POLICY'S LIST, NOT THE
// RELATION'S (arc R1 round-2 closure).
//
// The star hunk that let `SELECT r.*` read a recursive CTE's published list
// changed `logical.relationOutputColumns`' NAMED-BLOCK arm, and three shapes
// that were REFUSED before it now answer: `WITH c AS (SELECT * FROM t) SELECT
// c.*`, its joined spelling, and the derived-table twin. `internal/coordinator`
// gates their rows against PostgreSQL 17.11.
//
// This is the other half, and it is the half a row-set gate cannot see: those
// same shapes over a POLICED relation. The list the star publishes comes from
// `publishedScanColumns`, which answers the SECURITY PROJECTION's list wherever
// one stands over the scan — so a star that starts answering must answer the
// MASKED reading, on every door, or refuse. `TestPolicyMaskingIsPlanTimeOnEveryDoor`
// is the census this belongs beside; it carries no star over a star-bodied
// block, which is why this cell exists rather than a new `want` there.
//
// The assertion is the census's own: no true value of a masked column in any
// cell, and no denied column in any list — whatever shape carried it.
func TestArcR1AStarOverAStarBodiedBlockNeverPublishesAPolicedValue(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	cells := []struct{ name, sql string }{
		{"cte-starbody", `WITH c AS (SELECT * FROM e7emp) SELECT c.* FROM c`},
		{"cte-starbody-join", `WITH c AS (SELECT * FROM e7emp) SELECT c.* FROM e7other o JOIN c ON c.id = o.id`},
		{"derived-starbody", `SELECT d.* FROM (SELECT * FROM e7emp) d`},
		{"cte-starbody-rowfiltered", `WITH c AS (SELECT * FROM e7bal) SELECT c.* FROM c`},
	}
	// A GATE WHOSE CELLS ALL REFUSE PROVES NOTHING. The shapes above answer at
	// the tip, and answering is what makes the leak test capable of failing —
	// so the count of (cell, door) pairs that ANSWERED is asserted, and a
	// future change that turns them all back into refusals fails here rather
	// than passing vacuously.
	answered := 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if err != nil {
				// A refusal is a disposition this cell accepts: the claim is
				// that nothing POLICED reaches a client, not that every shape
				// answers.
				continue
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
			}
		}
	}
	if answered == 0 {
		t.Fatalf("no (cell, door) pair answered: this gate's leak test cannot fail, "+
			"and %d shapes over policed relations were refused on every door", len(cells))
	}
	t.Logf("%d of %d (cell, door) pairs answered; the rest refused",
		answered, len(cells)*len(rig.doors))
}
