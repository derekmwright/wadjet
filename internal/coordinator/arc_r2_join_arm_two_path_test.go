// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// THE GATE that walks the join-arm table (arc R2).
//
// One rule, one table, one gate: a join ARM THAT IS NOT A BASE SCAN is keyed,
// shuffled, merged and NAMED on the three DAG arms exactly as on the
// single-process arms, which is exactly as PostgreSQL 17.11 does it. Every
// cell is compared against live PostgreSQL on FIVE arms — single, spilled,
// dag, dag-shuffled, dag-morsel — because each of the four defects this arc
// closed was right on the single arms and wrong on the distributed ones:
//
//   - #1102 a qualified reference into a SET-OPERATION arm bound the OTHER
//     side's column of that name, because the arm's columns reached the join
//     under the SCAN's qualifier — `a.id` read 1|2|3|4 for PostgreSQL's
//     1|1|2|2.
//   - #1099 DISTINCT over a join with a JOIN-BODIED arm answered rows that
//     violate the join condition.
//   - #1095 the ORDER BY above a join over a GROUPED arm ordered by the group
//     KEY where the projection reads the aggregate.
//   - #1096 a NESTED block's rename was lost where the inner block minted a
//     sort key.
//
// A cell with no recorded PostgreSQL answer FAILS, and so does a PIN that
// starts agreeing: the pins in r2Pin are the residue, each with the mechanism
// that keeps it open, and deleting one is the proof its fix landed.
func TestR2AJoinArmIsKeyedAndNamedTheSameOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up three embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := c1Arms(t, ctx)

	for _, tc := range r2Table() {
		t.Run(tc.name, func(t *testing.T) {
			want, ok := r2Want[tc.name]
			if !ok {
				t.Fatalf("no PostgreSQL 17.11 answer recorded for %q — the table is the "+
					"claim, and a position nobody measured is not one\n  SQL: %s",
					tc.name, tc.sql)
			}
			for _, arm := range arms {
				got, err := arm.run(tc.sql)
				if err != nil {
					got = "ERR: " + err.Error()
				} else if tc.sorted {
					got = o2SortRender(got)
				}
				// A REFUSAL is a recorded disposition, not a value: the stable
				// part of the message is asserted and the query id and file
				// name in it are not. A cell that starts ANSWERING fails —
				// it then needs PostgreSQL's answer, not a refusal.
				if refusal, has := r2Refuse[tc.name][arm.name]; has {
					if !strings.HasPrefix(got, "ERR:") {
						t.Errorf("%s arm ANSWERED where this cell is a recorded REFUSAL: %s\n"+
							"  the refusal is the disposition; a shape that starts answering "+
							"needs PostgreSQL's answer beside it\n  SQL: %s",
							arm.name, got, tc.sql)
					} else if !strings.Contains(got, refusal) {
						t.Errorf("%s arm refused for a different reason than the recorded one:\n"+
							"  %s\n  recorded %q\n  SQL: %s", arm.name, got, refusal, tc.sql)
					}
					continue
				}
				armWant := want
				pinned := false
				if p, has := r2Pin[tc.name][arm.name]; has {
					armWant, pinned = p, true
				}
				// K3's property rides along: nothing the planner minted for
				// itself reaches a client, on any arm, in any cell.
				o2RefuseReservedSlots(t, arm.name, got, tc.sql)
				if got == armWant {
					continue
				}
				if pinned {
					t.Errorf("%s arm: the PIN no longer describes this cell — it is now\n"+
						"  %s\n  pinned %s\nDeleting the pin is the proof of the fix; "+
						"changing it needs the mechanism beside it.\n  SQL: %s",
						arm.name, got, armWant, tc.sql)
					continue
				}
				// EVERY arm is reported, not the first that diverges: which
				// arms disagree IS the diagnosis here — all four defects were
				// right on the single-process arms and wrong on the
				// distributed ones, and a gate that stopped at the first
				// failure could not say that.
				t.Errorf("%s arm: %s\n  want %s (PostgreSQL 17.11)\n  SQL: %s",
					arm.name, got, want, tc.sql)
			}
		})
	}
}

// r2SingleArmAgrees is the second claim the table carries, and the cheap one:
// the SINGLE-process arm answers PostgreSQL for every cell. It is asserted
// separately because a distributed divergence is diagnosed against it — the
// single arm is the free oracle this arc leaned on per cell — and a gate that
// only compared five arms to one another could not say which of them moved.
func TestR2TheSingleArmAnswersPostgreSQLForEveryJoinArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate opens two embedded engines over the corpus")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	single := tmdStandalone(t, ctx)

	for _, tc := range r2Table() {
		want, ok := r2Want[tc.name]
		if !ok {
			t.Errorf("%s: no PostgreSQL answer recorded", tc.name)
			continue
		}
		got, err := f1RenderSingle(ctx, single, tc.sql)
		if err != nil {
			got = "ERR: " + err.Error()
		} else if tc.sorted {
			got = o2SortRender(got)
		}
		if p, has := r2Pin[tc.name]["single"]; has {
			want = p
		}
		if got != want {
			t.Errorf("%s\n  got  %s\n  want %s\n  SQL: %s", tc.name, got, want, tc.sql)
		}
	}
}

// r2NoCellIsUnmeasured keeps the table and its oracle in step without standing
// anything up: every cell has a PostgreSQL answer, every recorded answer names
// a cell, and every pin names one too. It runs under -short, which is where a
// table edit that forgot its measurement is caught in a second rather than in
// half an hour.
func TestR2EveryCellHasAMeasuredAnswer(t *testing.T) {
	table := r2Table()
	inTable := make(map[string]bool, len(table))
	for _, tc := range table {
		inTable[tc.name] = true
		if _, ok := r2Want[tc.name]; !ok {
			t.Errorf("cell %q has no recorded PostgreSQL 17.11 answer", tc.name)
		}
		if strings.TrimSpace(tc.sql) == "" {
			t.Errorf("cell %q renders no SQL", tc.name)
		}
	}
	for name := range r2Want {
		if !inTable[name] {
			t.Errorf("recorded answer %q names no cell of the table", name)
		}
	}
	for name := range r2Pin {
		if !inTable[name] {
			t.Errorf("pin %q names no cell of the table", name)
		}
	}
	for name := range r2Refuse {
		if !inTable[name] {
			t.Errorf("recorded refusal %q names no cell of the table", name)
		}
		if _, both := r2Pin[name]; both {
			t.Errorf("cell %q is both pinned and recorded as a refusal", name)
		}
	}
}
