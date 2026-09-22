// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A DECORRELATION THAT MOVES A CONDITION MOVES IT ABOVE THE MASK (arc DC).
//
// Arc DC changes WHERE a predicate over a relation's columns is evaluated: a
// condition inside a subquery body that names only the enclosing row is now
// applied to the OUTER rows, and a conjunct of a body JOIN's ON that names the
// enclosing query is lifted into the same classification the WHERE goes
// through. Both are relocations of a predicate ACROSS a relation boundary, and
// a policed relation is where a relocation stops being a performance question:
// a masked column read at the wrong place answers the STORED value, and the
// answer is the ROW SET rather than a cell, so a leak scan over the output
// alone cannot see it (#859 round 2, ADR-0033).
//
// The cells are the two positions this arc moved, over the two policed
// relations the census already stands up, with the row set each door must
// answer written from the MASK's reading:
//
//	e7bal.bal is masked to 0 and STORED as ±100i, so `o.bal < 0` is FALSE for
//	every row through the mask and TRUE for the odd ids through the column.
//	e7emp.ssn is masked to '***' and stored as `true-ssn-NN`, so `o.ssn =
//	'true-ssn-01'` matches NOTHING through the mask and row 1 through the
//	column — and `o.ssn = '***'` matches EVERY row through the mask, which is
//	the cell that proves the relocation fires at all rather than the whole
//	gate passing on refusals.
//
// The salary cell is the third disposition: a DENIED column named from inside
// a body must not become readable by being relocated.
//
// At 6b9c7acf every one of these bodies was planned with its condition pushed
// into the SUBQUERY's relation with the qualifier stripped, so each refused
// ("column does not exist") rather than answering — which is why this gate
// fails at base on every door.
func TestArcDCARelocatedBodyConditionReadsTheMaskOnEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	cells := []struct {
		name string
		sql  string
		want []string // the MASK's row set, canon() order; nil = must refuse
	}{
		// #1104's hoist, over a masked NUMBER. Through the mask `0 < 0` is
		// false for every outer row and the body is empty, so the IN is false
		// everywhere; through the stored column the odd ids survive.
		{"in-outer-only-masked-number",
			`SELECT o.id AS a FROM e7bal o WHERE o.id IN (SELECT t.id FROM e7other t WHERE o.bal < 0) ORDER BY a`,
			[]string{}},
		{"exists-outer-only-masked-number",
			`SELECT o.id AS a FROM e7bal o WHERE EXISTS (SELECT 1 FROM e7other t WHERE t.id = o.id AND o.bal < 0) ORDER BY a`,
			[]string{}},
		// The same hoist over a masked STRING, both ways round: the stored
		// value matches nothing and the mask matches everything.
		{"in-outer-only-true-value",
			`SELECT o.id AS a FROM e7emp o WHERE o.id IN (SELECT t.id FROM e7other t WHERE o.ssn = 'true-ssn-01') ORDER BY a`,
			[]string{}},
		{"in-outer-only-mask-value",
			`SELECT o.id AS a FROM e7emp o WHERE o.id IN (SELECT t.id FROM e7other t WHERE o.ssn = '***') ORDER BY a`,
			[]string{"a=1", "a=2", "a=3"}},
		{"exists-outer-only-mask-value",
			`SELECT o.id AS a FROM e7emp o WHERE EXISTS (SELECT 1 FROM e7other t WHERE t.id = o.id AND o.ssn = '***') ORDER BY a`,
			[]string{"a=1", "a=2", "a=3"}},
		// #1232's lift: the outer reference is in the body's JOIN ON, and it
		// compares a masked OUTER column with a masked INNER one.
		{"in-on-masked-both-sides",
			`SELECT o.id AS a FROM e7bal o WHERE o.id IN (SELECT b.id FROM e7emp b JOIN e7other c ON c.id = b.id AND o.bal < b.acct) ORDER BY a`,
			[]string{}},
		{"exists-on-masked-outer",
			`SELECT o.id AS a FROM e7bal o WHERE EXISTS (SELECT 1 FROM e7emp b JOIN e7other c ON c.id = b.id AND b.id = o.id AND o.bal < 0) ORDER BY a`,
			[]string{}},
		// The same positions spelled WITHOUT a qualifier (round 2, B1). The
		// body's relation has no `bal` / `ssn`, so the name binds to the
		// enclosing row and is now read as an outer reference in every
		// position — the WHERE hoist, the ON lift, and the HAVING and the
		// SELECT list, which decline to the per-row rerun. Each must still
		// read the MASK.
		{"in-outer-only-bare-masked-number",
			`SELECT o.id AS a FROM e7bal o WHERE o.id IN (SELECT t.id FROM e7other t WHERE bal < 0) ORDER BY a`,
			[]string{}},
		{"in-on-bare-masked-outer",
			`SELECT o.id AS a FROM e7bal o WHERE o.id IN (SELECT b.id FROM e7emp b JOIN e7other c ON c.id = b.id AND bal < b.acct) ORDER BY a`,
			[]string{}},
		{"exists-having-bare-mask-value",
			`SELECT o.id AS a FROM e7emp o WHERE EXISTS (SELECT 1 FROM e7other t WHERE t.id = o.id GROUP BY t.id HAVING ssn = '***') ORDER BY a`,
			[]string{"a=1", "a=2", "a=3"}},
		{"in-select-bare-mask-value",
			`SELECT o.id AS a FROM e7emp o WHERE '***' IN (SELECT ssn FROM e7other t WHERE t.id = o.id) ORDER BY a`,
			[]string{"a=1", "a=2", "a=3"}},
		// A DENIED column named from inside a body is not made readable by
		// being relocated: every door refuses.
		{"in-outer-only-denied-column",
			`SELECT o.id AS a FROM e7emp o WHERE o.id IN (SELECT t.id FROM e7other t WHERE o.salary > 0) ORDER BY a`,
			nil},
		{"in-outer-only-bare-denied-column",
			`SELECT o.id AS a FROM e7emp o WHERE o.id IN (SELECT t.id FROM e7other t WHERE salary > 0) ORDER BY a`,
			nil},
	}

	// A GATE WHOSE CELLS ALL REFUSE PROVES NOTHING. Every cell with a non-nil
	// want answers at the tip, so the count of (cell, door) pairs that
	// answered is asserted and a change that turns them into refusals fails
	// here rather than passing vacuously.
	wantAnswer := 0
	for _, c := range cells {
		if c.want != nil {
			wantAnswer++
		}
	}
	answered := 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if cell.want == nil {
				if err == nil {
					t.Errorf("%s / %s: a DENIED column named from inside a subquery body "+
						"answered instead of refusing\n  %s", cell.name, door.name, cell.sql)
				}
				continue
			}
			if err != nil {
				t.Errorf("%s / %s: refused where the mask has an answer: %v\n  %s",
					cell.name, door.name, err, cell.sql)
				continue
			}
			answered++
			if diff := strings.Join(got.canon(), " | "); diff != strings.Join(cell.want, " | ") {
				t.Errorf("%s / %s\n  got  %s\n  want %s (the MASK's reading)\n  %s",
					cell.name, door.name, diff, strings.Join(cell.want, " | "), cell.sql)
			}
			for _, c := range got.cols {
				if strings.EqualFold(c, "salary") {
					t.Errorf("%s / %s: the DENIED column %q is in the published list\n  %s",
						cell.name, door.name, c, cell.sql)
				}
			}
			for i := range got.rows {
				for _, v := range got.cells(i) {
					for _, bad := range leaks {
						if strings.Contains(v, bad) {
							t.Errorf("%s / %s: %s is a policed value reaching the client\n  %s",
								cell.name, door.name, v, cell.sql)
						}
					}
				}
			}
		}
	}
	if answered < wantAnswer*len(rig.doors) {
		t.Errorf("%d of %d (cell, door) pairs answered; every cell with a recorded row set "+
			"must answer on every door, or this gate's reading of the mask is vacuous",
			answered, wantAnswer*len(rig.doors))
	}
	t.Logf("%d of %d (cell, door) pairs answered", answered, wantAnswer*len(rig.doors))
}
