package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// THE GATE that walks the derived-block / slot-identity table (arc O2).
//
// One rule, one table, one gate. A derived block — a subquery in FROM, a CTE,
// a LATERAL — publishes EXACTLY its visible projection, by position and under
// the name the block wrote, to every consumer above it; a slot the planner
// minted for itself (`__sortkey_N`, `__key_N`) dies where the operator that
// minted it ends; and an OUTPUT alias never captures a source column of the
// same name. Every cell is compared against live PostgreSQL 17.11 on FIVE arms
// — single, spilled, dag, dag-shuffled, dag-morsel — because each of the four
// defects this arc closed was right on some arm and wrong on another:
//
//   - #1077 a QUALIFIED star over a block with an unaliased item bound the
//     item by its PUBLISHED name (`?column?`), which is a rendering and not a
//     handle: NULL under a STRING declaration on every arm, where the bare star
//     answered the value.
//   - #991 a block whose own ORDER BY was materialized published `__sortkey_0`
//     — a name no query can spell — to a star above the join, and on the wire.
//   - #1020 the minted correlation slot `__key_0` rode out beside a grouped
//     LATERAL's own column on both DAG arms, because the ordinal it is dropped
//     by was read off a walk that stopped at the block's own Sort.
//   - a QUALIFIED star over a block carrying its own ORDER BY, LIMIT or
//     DISTINCT, and over a LATERAL body, was REFUSED where PostgreSQL answers.
//
// A cell with no recorded PostgreSQL answer FAILS, and so does a PIN that
// starts agreeing: the pins below are the residue, each with the mechanism that
// keeps it open, and deleting one is the proof its fix landed.
func TestArcO2ADerivedBlockPublishesItsVisibleList(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := c1Arms(t, ctx)

	for _, tc := range o2Table() {
		t.Run(tc.name, func(t *testing.T) {
			want, ok := o2Want[tc.name]
			if !ok {
				t.Fatalf("no PostgreSQL 17.11 answer recorded for %q — the table is the "+
					"claim, and a position nobody measured is not one\n  SQL: %s",
					tc.name, tc.sql)
			}
			for _, arm := range arms {
				got, err := arm.run(tc.sql)
				if err != nil {
					got = "ERR: " + err.Error()
				} else {
					if tc.sorted {
						got = o2SortRender(got)
					}
					if len(tc.keyCols) > 0 {
						got += "  KEYS " + o2KeySeq(arm.run, tc)
					}
				}
				armWant := want
				pinned := false
				if p, has := o2Pin[tc.name][arm.name]; has {
					armWant, pinned = p, true
				}
				if got == armWant {
					continue
				}
				if pinned {
					t.Fatalf("%s arm: the PIN no longer describes this cell — it is now\n"+
						"  %s\n  pinned %s\nDeleting the pin is the proof of the fix; "+
						"changing it needs the mechanism beside it.\n  SQL: %s",
						arm.name, got, armWant, tc.sql)
				}
				t.Fatalf("%s arm: %s\n  want %s (PostgreSQL 17.11)\n  SQL: %s",
					arm.name, got, want, tc.sql)
			}
			// NOTHING THE PLANNER MINTED FOR ITSELF REACHES A CLIENT. The
			// reserved-name property tolerates nothing now: #991 and #1020 were
			// the two leaks a `want` string used to carry, and both are closed,
			// so a reserved name in any cell's column list is a failure rather
			// than an expectation (K3's property, back to its full strength).
			for _, arm := range arms {
				got, err := arm.run(tc.sql)
				if err != nil {
					continue
				}
				head, _, _ := strings.Cut(got, "] rows=")
				for _, col := range strings.Fields(strings.TrimPrefix(head, "cols=[")) {
					name, _, _ := strings.Cut(col, ":")
					if strings.HasPrefix(name, "__") {
						t.Fatalf("%s arm published the reserved slot %q to the client\n  SQL: %s",
							arm.name, name, tc.sql)
					}
				}
			}
		})
	}
}
