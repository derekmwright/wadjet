// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// A SPILLED RIGHT / FULL JOIN OVER AN EMPTY SIDE PUBLISHES THE POLICY'S VALUE,
// ON EVERY DOOR — arc SJ (#1359), the masking gate COMMON names for a fix in
// the path that partitions a relation. The fix changes how an evicted grace
// partition's preserved rows are shaped when the other side is empty; the
// preserved side here is a POLICED relation — e7emp with ssn masked, e7bal
// with bal masked to 0 from stored values of both signs, also as the join
// key — so a replay that read a stored column, or the empty side's, would
// show. Every arriving build batch forces an eviction (ADR-0027 decision 6),
// asserted on the spilled embedded door; every rendered cell is checked
// against every stored policed value, and the (cell, door) count is asserted
// so the gate cannot pass by refusing.
func TestArcSJAnEmptySidedSpilledJoinPublishesThePolicysValueOnEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up all nine policy doors")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()
	prev := exec.ForceJoinPartitionEvictEvery(1)
	defer exec.ForceJoinPartitionEvictEvery(prev)

	cells := []struct{ name, sql, want string }{
		{"rightEmpOnId",
			`SELECT p.id AS a, p.ssn AS m FROM (SELECT id, ssn FROM e7emp WHERE id < 0) e RIGHT JOIN e7emp p ON e.id = p.id`,
			pmSSNAll()},
		{"fullEmpOnId",
			`SELECT p.id AS a, p.ssn AS m FROM (SELECT id, ssn FROM e7emp WHERE id < 0) e FULL JOIN e7emp p ON e.id = p.id`,
			pmSSNAll()},
		{"rightEmpOnMasked",
			`SELECT p.id AS a, p.ssn AS m FROM (SELECT id, ssn FROM e7emp LIMIT 0) e RIGHT JOIN e7emp p ON e.ssn = p.ssn`,
			pmSSNAll()},
		{"rightBalOnMasked",
			`SELECT p.id AS a, p.bal AS m FROM (SELECT id, bal FROM e7bal WHERE id < 0) e RIGHT JOIN e7bal p ON e.bal = p.bal`,
			pmAllWith(8, "0")},
		{"fullBalOnMasked",
			`SELECT p.id AS a, p.bal AS m FROM (SELECT id, bal FROM e7bal WHERE false) e FULL JOIN e7bal p ON e.bal = p.bal`,
			pmAllWith(8, "0")},
		{"rightBalKeyIsTheEmptySides",
			`SELECT p.id AS a, COALESCE(e.bal, p.bal) AS m FROM (SELECT id, bal FROM e7bal WHERE id < 0) e RIGHT JOIN e7bal p ON e.id = p.id`,
			pmAllWith(8, "0")},
	}
	answered := 0
	for _, c := range cells {
		for _, door := range rig.doors {
			t.Run(c.name+"/"+door.name, func(t *testing.T) {
				before := exec.JoinPartitionsEvicted.Load()
				got, err := door.run(t, "analyst-key", c.sql)
				if door.name == "embedded/spilled" && exec.JoinPartitionsEvicted.Load() == before {
					t.Errorf("no build partition was evicted on the spilled door — the cell compared two in-memory runs")
				}
				if err != nil {
					t.Fatalf("the analyst's query was refused, which this gate cannot "+
						"read as an answer: %v\n  SQL: %s", err, c.sql)
				}
				rendered := strings.Join(got.canon(), " ; ")
				for _, s := range leaks {
					if strings.Contains(rendered, s) {
						t.Fatalf("a masked or denied value reached the client: %q\n  %s\n  SQL: %s", s, rendered, c.sql)
					}
				}
				if pin := sjDAGSameNamePin(c.name, door.name); pin != "" {
					if rendered != pin {
						t.Errorf("%s\n  door %s\n  got  %s\n  pinned %s — an answer that moves deletes this pin (want %s)",
							c.sql, door.name, rendered, pin, c.want)
					}
					answered++
					return
				}
				if rendered != c.want {
					t.Errorf("%s\n  door %s\n  got  %s\n  want %s (the value the policy PUBLISHES)",
						c.sql, door.name, rendered, c.want)
				}
				answered++
			})
		}
	}
	if want := len(cells) * len(rig.doors); answered != want || len(rig.doors) != 9 {
		t.Errorf("%d (cell, door) pairs answered over %d doors, want %d over 9 — the gate is vacuous",
			answered, len(rig.doors), want)
	}
}

// sjDAGSameNamePin is the one cell × door this gate pins rather than asserts:
// on the four stage-DAG doors, `p.id` beside an empty side that also
// publishes `id` (and joins on it) reads NULL for every row — the DAG's
// same-name column resolution (arc CW's N1 / JP's FC-JP-3 family), the same
// at the arc's base with the eviction knob armed or not, and outside this
// arc's seam (the single-process grace replay). No policed value is read:
// `m` is the mask on every door. Filing candidate, distributed.
func sjDAGSameNamePin(cell, door string) string {
	if cell != "rightBalKeyIsTheEmptySides" {
		return ""
	}
	switch door {
	case "embedded/dag", "embedded/dag-shuffled", "pgwire/dag", "http/dag":
		return strings.TrimSuffix(strings.Repeat("a=NULL|m=0 ; ", 8), " ; ")
	}
	return ""
}
