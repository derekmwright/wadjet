// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A REFERENCE-SCOPE REFUSAL NEVER PUBLISHES A POLICED VALUE — arc RS's
// masking gate, on all nine doors.
//
// The arc changes which scope a QUALIFIED reference is resolved against: an
// ON clause sees its own join, a window key is resolved at all, a qualified
// star names one relation, and one FROM name answers to one relation. Every
// one of those decisions is made in `physical.validateColumns`, which the
// POLICY door re-enters through `ValidateColumnsUnderPolicy` with the schema
// THIS identity may see — so a scope narrowed for the reference rule is a
// scope the mask is applied against, and getting it wrong is not merely a
// wrong error message.
//
// The shapes below are this arc's own, written over the policed relations:
// a join whose ON is scoped, a window key qualified by the policed relation's
// alias, a qualified star over that alias, a self-join of the policed
// relation, and the row-filtered table in each position. The claim is the
// masking matrix's: nothing policed reaches a client, on any door.
//
// A GATE WHOSE CELLS ALL REFUSE PROVES NOTHING, so the count of (cell, door)
// pairs that ANSWERED is asserted too — at its REAL value, per cell and per
// door, so a change that turns any ONE of these shapes into a refusal fails
// here and is named rather than hiding behind the nine pairs of another cell.
func TestArcRSAScopedReferenceNeverPublishesAPolicedValue(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	cells := []struct{ name, sql string }{
		{"on-scoped-join", `SELECT e.ssn AS s FROM e7emp e JOIN e7other o ON e.id = o.id`},
		{"on-scoped-three-way", `SELECT e.ssn AS s FROM e7emp e JOIN e7other o ON e.id = o.id JOIN e7bal b ON b.id = e.id`},
		{"qualified-star-over-alias", `SELECT e.* FROM e7emp e`},
		{"qualified-star-row-filtered", `SELECT b.* FROM e7bal b`},
		{"window-key-on-alias", `SELECT e.ssn AS s, COUNT(*) OVER (PARTITION BY e.dept) AS n FROM e7emp e`},
		{"window-key-row-filtered", `SELECT b.bal AS v, COUNT(*) OVER (PARTITION BY b.id) AS n FROM e7bal b`},
		{"window-arg-on-alias", `SELECT SUM(e.acct) OVER (PARTITION BY e.dept) AS n FROM e7emp e`},
		{"self-join-both-aliased", `SELECT a.ssn AS s FROM e7emp a JOIN e7emp b ON a.id = b.id`},
		{"self-join-one-aliased", `SELECT e7emp.ssn AS s FROM e7emp JOIN e7emp b ON e7emp.id = b.id`},
		{"derived-arm-window-key", `SELECT COUNT(*) OVER (PARTITION BY x.dept) AS n, x.ssn AS s FROM (SELECT dept, ssn FROM e7emp) x`},
	}
	answered := 0
	refused := map[string]int{}
	for _, cell := range cells {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if err != nil {
				refused[cell.name]++
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
	// THE FLOOR IS THE REAL NUMBER, not a tenth of it. It was
	// `len(rig.doors)` — nine — which a change that turned NINE of the ten
	// cells into refusals passes, leaving one cell's nine doors to carry a
	// claim the notes made about ninety (round-1 review, P2). Every cell must
	// answer on every door, and a cell that stops answering is named.
	for _, cell := range cells {
		if n := refused[cell.name]; n > 0 {
			t.Errorf("%s: refused on %d of %d doors — every cell of this gate "+
				"must ANSWER over the policed relations, or its leak test cannot "+
				"fail for that shape\n  %s", cell.name, n, len(rig.doors), cell.sql)
		}
	}
	if want := len(cells) * len(rig.doors); answered != want {
		t.Fatalf("%d of %d (cell, door) pairs answered: this gate's leak test "+
			"is vacuous for the %d that did not", answered, want, want-answered)
	}
	t.Logf("%d of %d (cell, door) pairs answered", answered, len(cells)*len(rig.doors))
}
