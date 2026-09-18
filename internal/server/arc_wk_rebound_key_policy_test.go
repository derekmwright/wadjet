// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A RE-BOUND KEY OVER A POLICED COLUMN READS THE MASK — arc WK, NINE DOORS.
//
// The arc changes which OCCURRENCE a window key and a sort key bind
// (docs/design/window-key-ownership.md), so it must prove the binding it
// produces is still the POLICY's column on every door.
//
// The instrument is `e7bal`, masked to 0 while its stored values are ±100…±800
// (`pmBalFixture`): under the mask every row shares one key, so a window
// partitioned on it has ONE partition of eight where the STORED column gives
// eight SINGLETONS — arithmetic on the value the policy hides (#859 round 2).
// `e7emp`'s `ssn` and `acct` are the same instrument for a string and a second
// numeric mask, and `salary` is DENIED. Every cell qualifies its key over a
// join whose two arms publish that bare name, the shape this arc re-binds;
// `winpart_bal_derived` takes #975's route, and the unpoliced sibling is the
// control.
//
// Per (cell, door): the ANSWER is the mask's exactly and no rendered value
// contains a stored policed value (`pmTrueValues`); over the run, the count of
// pairs that ANSWERED is non-vacuous. The DENIED cell refuses on all nine
// doors with wording that differs per door, so that assertion is the class.
func TestArcWKAReboundKeyOverAPolicedColumnReadsTheMask(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: nine doors over the policed corpus")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	const balOnePartition = "a=1|n=8 | a=2|n=8 | a=3|n=8 | a=4|n=8 | " +
		"a=5|n=8 | a=6|n=8 | a=7|n=8 | a=8|n=8"
	const empOnePartition = "a=10|n=12 | a=11|n=12 | a=12|n=12 | a=1|n=12 | a=2|n=12 | " +
		"a=3|n=12 | a=4|n=12 | a=5|n=12 | a=6|n=12 | a=7|n=12 | a=8|n=12 | a=9|n=12"

	cells := []struct {
		name string
		sql  string
		// want is the MASK's answer, as pmResult.canon renders it (rows
		// lexicographically ordered — this gate asserts the ANSWER, and the
		// seam's own ORDER is TestWKASeamConsumerBindsItsOwnOccurrence's).
		want string
		// refuses says the cell's disposition is a refusal on every door.
		refuses bool
		// stored is what the cell would answer if the key bound the policy's
		// hidden column instead. It is not asserted — it is here so a reader
		// can see the cell is capable of telling the two apart.
		stored string
	}{
		{
			name:   "winpart_bal_contested",
			sql:    `SELECT b.id AS a, COUNT(*) OVER (PARTITION BY b.bal) AS n FROM e7bal b JOIN e7bal c ON c.id = b.id ORDER BY a`,
			want:   balOnePartition,
			stored: "n=1 on every row — eight singleton partitions, one per stored balance",
		},
		{
			name:   "winorder_bal_contested",
			sql:    `SELECT b.id AS a, COUNT(*) OVER (ORDER BY b.bal) AS n FROM e7bal b JOIN e7bal c ON c.id = b.id ORDER BY a`,
			want:   balOnePartition,
			stored: "a rank per row, in the order of the stored signs",
		},
		{
			name:   "winarg_bal_contested",
			sql:    `SELECT b.id AS a, SUM(b.bal) OVER () AS n FROM e7bal b JOIN e7bal c ON c.id = b.id ORDER BY a`,
			want:   "a=1|n=0 | a=2|n=0 | a=3|n=0 | a=4|n=0 | a=5|n=0 | a=6|n=0 | a=7|n=0 | a=8|n=0",
			stored: "the sum of the stored balances",
		},
		{
			name:   "sortkey_bal_contested",
			sql:    `SELECT b.id AS a, FIRST_VALUE(b.id) OVER (ORDER BY b.bal, b.id) AS f FROM e7bal b JOIN e7bal c ON c.id = b.id ORDER BY a`,
			want:   "a=1|f=1 | a=2|f=1 | a=3|f=1 | a=4|f=1 | a=5|f=1 | a=6|f=1 | a=7|f=1 | a=8|f=1",
			stored: "f=7 — the row holding the lowest stored balance, -700",
		},
		{
			name:   "winpart_bal_derived",
			sql:    `SELECT x.id AS a, COUNT(*) OVER (PARTITION BY x.bal) AS n FROM (SELECT id, bal FROM e7bal) x JOIN (SELECT id, bal FROM e7bal) y ON y.id = x.id ORDER BY a`,
			want:   balOnePartition,
			stored: "n=1 on every row",
		},
		{
			name:   "winpart_ssn_contested",
			sql:    `SELECT e.id AS a, COUNT(*) OVER (PARTITION BY e.ssn) AS n FROM e7emp e JOIN e7emp f ON f.id = e.id ORDER BY a`,
			want:   empOnePartition,
			stored: "n=1 on every row — one partition per true-ssn-NN",
		},
		{
			name:   "winpart_acct_contested",
			sql:    `SELECT e.id AS a, COUNT(*) OVER (PARTITION BY e.acct) AS n FROM e7emp e JOIN e7emp f ON f.id = e.id ORDER BY a`,
			want:   empOnePartition,
			stored: "n=1 on every row — one partition per stored account number",
		},
		{
			name:    "winpart_denied_salary",
			sql:     `SELECT e.id AS a, COUNT(*) OVER (PARTITION BY e.salary) AS n FROM e7emp e JOIN e7emp f ON f.id = e.id ORDER BY a`,
			refuses: true,
			stored:  "a partition per salary, which is the denied column itself",
		},
		{
			// THE CONTROL. The same window over an UNPOLICED sibling column
			// answers one row per partition, so `n=8` above is the MASK and
			// not something about the shape.
			name:   "winpart_bal_unpoliced_sibling",
			sql:    `SELECT b.id AS a, COUNT(*) OVER (PARTITION BY o.note) AS n FROM e7bal b JOIN e7other o ON o.id = b.id ORDER BY a`,
			want:   "a=1|n=1 | a=2|n=1 | a=3|n=1",
			stored: "unpoliced: this IS the stored answer",
		},
	}

	answered, refused := 0, 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if err != nil {
				if !cell.refuses {
					t.Errorf("%s / %s: refused a shape this gate asserts an answer for: %v\n  %s",
						cell.name, door.name, err, cell.sql)
				}
				refused++
				continue
			}
			if cell.refuses {
				t.Errorf("%s / %s: ANSWERED a denied column's key — %v\n  %s",
					cell.name, door.name, got.canon(), cell.sql)
				continue
			}
			answered++
			if g := strings.Join(got.canon(), " | "); g != cell.want {
				t.Errorf("%s / %s\n  got  %s\n  want %s  (the mask's answer; the stored column would give %s)\n  %s",
					cell.name, door.name, g, cell.want, cell.stored, cell.sql)
			}
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
	wantAnswered := (len(cells) - 1) * len(rig.doors)
	if answered != wantAnswered {
		t.Errorf("%d of %d (cell, door) pairs answered, want %d: a gate whose cells refuse "+
			"cannot see a leak", answered, len(cells)*len(rig.doors), wantAnswered)
	}
	if refused != len(rig.doors) {
		t.Errorf("the denied-column cell refused on %d doors, want %d", refused, len(rig.doors))
	}
	t.Logf("%d (cell, door) pairs answered the mask, %d refused the denied column, over %d doors",
		answered, refused, len(rig.doors))
}
