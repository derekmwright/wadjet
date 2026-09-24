// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"strings"
	"testing"
	"time"
)

// ARC PS's MASKING GATE, on all nine doors: a `JOIN … USING` key over a
// POLICED column, and a column-alias list over a policed relation.
//
// The arc REWRITES over a relation's columns in two places, and each is a
// place a security projection could be bypassed:
//
//   - the USING merge is the ONE star item in this engine that is not a plain
//     qualified reference. A FULL join's merged key is a `COALESCE(l.c, r.c)`
//     the star expansion MINTS (logical.mergedUsingItem), so "the star reads
//     the barrier's list" is an argument about the OTHER items;
//   - a column-alias list on a named relation is LOWERED into a derived table
//     in the parser, and a derived body is a second place a scan's policed
//     list has to survive before the positional rename lands on it — a rename
//     over a policed relation renames the POLICED width (`alias_list_*`).
//
// Round 1 argued the gate away in prose and the review measured it instead
// (162 cells, no leak). Per cell: no TRUE value of a masked column reaches any
// door, no DENIED column appears in any output, and a statement naming a
// denied column REFUSES. The table asserts a NON-VACUOUS count of (cell, door)
// pairs that ANSWERED, so turning every shape into a refusal cannot pass.
func TestArcPSAUsingMergedKeyOverAPolicedColumnOnEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three servers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	cells := []struct {
		name string
		sql  string
		// denied names a column this identity may not read; the statement must
		// REFUSE on every door rather than answer any part of it.
		denied string
		// answers marks a cell expected to produce ROWS on every door, so the
		// no-leak assertion over it is not vacuous.
		answers bool
		// dagLoud pins a cell that answers on the single-process doors and is
		// REFUSED on the DAG doors, naming the sentence the refusal carries.
		// A pinned cell that starts ANSWERING on a DAG door fails, which is
		// how the pin gets deleted when the divergence closes (ADR-0013).
		dagLoud string
	}{
		// ---- the merged key IS the masked column --------------------------
		//
		// Over the BASE relation the two arms share every other column name
		// too. That was this arc's own shared-tail-name bound and a refusal;
		// arc SR removed it (#1177), so these ANSWER now — eleven published
		// columns, ten of them from a policed arm — and the no-leak
		// assertions below are what they are for. The cells stay: a shape
		// that changes disposition is exactly the one a masking gate must
		// keep watching.
		{name: "inner_using_a_masked_key", sql: `SELECT * FROM e7emp x JOIN e7emp y USING (ssn)`},
		{name: "full_using_a_masked_key", sql: `SELECT * FROM e7emp x FULL JOIN e7emp y USING (ssn)`},
		// Over DERIVED arms that share the policed name and NOTHING else, the
		// merge IS stated — and for FULL it mints the COALESCE. These are the
		// cells that make the gate non-vacuous.
		{name: "derived_inner_using_a_masked_key", answers: true,
			sql: `SELECT * FROM (SELECT ssn, amt AS m FROM e7emp) x ` +
				`JOIN (SELECT ssn, id AS n FROM e7emp) y USING (ssn)`},
		{name: "derived_right_using_a_masked_key", answers: true,
			sql: `SELECT * FROM (SELECT ssn, amt AS m FROM e7emp) x ` +
				`RIGHT JOIN (SELECT ssn, id AS n FROM e7emp) y USING (ssn)`},
		// The MINTED COALESCE over a masked STRING column. It was LOUD on
		// the three DAG doors — the minted `coalesce(x.ssn, y.ssn)` reached
		// the join stage under a FLOAT64 declaration typed against a walk
		// that stops at the derived arms — until arc CW typed a stage's
		// SELECT list against the child's EMITTED declarations (the
		// single-process walk); the pin was deleted as that fix's proof.
		{name: "derived_full_using_a_minted_coalesce_over_a_masked_key", answers: true,
			sql: `SELECT * FROM (SELECT ssn, amt AS m FROM e7emp) x ` +
				`FULL JOIN (SELECT ssn, id AS n FROM e7emp) y USING (ssn)`},
		{name: "derived_full_using_a_masked_numeric_key", answers: true,
			sql: `SELECT * FROM (SELECT acct, amt AS m FROM e7emp) x ` +
				`FULL JOIN (SELECT acct, id AS n FROM e7emp) y USING (acct)`},
		{name: "derived_full_using_a_masked_key_grouped", answers: true,
			sql: `SELECT x.ssn AS k, COUNT(*) AS c FROM (SELECT ssn, amt AS m FROM e7emp) x ` +
				`FULL JOIN (SELECT ssn, id AS n FROM e7emp) y USING (ssn) GROUP BY x.ssn`},

		// ---- the merged key is UNPOLICED, the tail is not -----------------
		{name: "using_id_across_a_policed_arm", answers: true,
			sql: `SELECT * FROM e7emp JOIN e7other USING (id)`},
		{name: "full_using_id_across_a_policed_arm", answers: true,
			sql: `SELECT * FROM e7emp FULL JOIN e7other USING (id)`},

		// ---- a DENIED column named in the USING list ----------------------
		{name: "using_a_denied_key", denied: "salary",
			sql: `SELECT * FROM e7emp x JOIN e7emp y USING (salary)`},
		{name: "derived_full_using_a_denied_key", denied: "salary",
			sql: `SELECT * FROM (SELECT salary, amt AS m FROM e7emp) x ` +
				`FULL JOIN (SELECT salary, id AS n FROM e7emp) y USING (salary)`},

		// ---- a column-alias list over a policed relation ------------------
		//
		// The list renames the POLICED width, not the catalog's: e7emp
		// publishes five columns to this identity, so a five-name list covers
		// it and every renamed position carries the policed value.
		{name: "alias_list_over_a_policed_relation", answers: true,
			sql: `SELECT * FROM e7emp a(c1, c2, c3, c4, c5)`},
		{name: "qualified_star_over_an_alias_list_on_a_policed_relation", answers: true,
			sql: `SELECT a.* FROM e7emp a(c1, c2, c3, c4, c5)`},
		{name: "alias_list_names_every_position", answers: true,
			sql: `SELECT c1, c2, c3, c4, c5 FROM e7emp a(c1, c2, c3, c4, c5)`},
		{name: "alias_list_renames_the_first_position_then_joins_on_it", answers: true,
			sql: `SELECT * FROM e7emp a(id, c2, c3, c4, c5) JOIN e7other USING (id)`},
	}

	answered := 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			t.Run(cell.name+"/"+door.name, func(t *testing.T) {
				got, err := door.run(t, "analyst-key", cell.sql)
				dagDoor := strings.Contains(door.name, "dag") && !strings.Contains(door.name, "fastpath")
				if err != nil {
					switch {
					case cell.dagLoud != "" && dagDoor:
						if !strings.Contains(err.Error(), cell.dagLoud) {
							t.Errorf("pinned as loud on the DAG for %q, but the refusal is %v\n"+
								"  SQL: %s", cell.dagLoud, err, cell.sql)
						}
					case cell.answers:
						t.Errorf("this cell must ANSWER on every door, so the no-leak assertion "+
							"over it is not vacuous: %v\n  SQL: %s", err, cell.sql)
					}
					return
				}
				if cell.dagLoud != "" && dagDoor {
					t.Errorf("the pinned DAG divergence (%s) no longer happens — delete the pin, "+
						"which is the fix's proof\n  SQL: %s", cell.dagLoud, cell.sql)
				}
				if cell.denied != "" {
					t.Fatalf("answered, but %q is DENIED for this identity\n  SQL: %s\n  got: %s",
						cell.denied, cell.sql, strings.Join(got.canon(), " ; "))
				}
				answered++
				for _, row := range got.rows {
					for c, v := range row {
						for _, bad := range leaks {
							if strings.Contains(v, bad) {
								t.Fatalf("the analyst identity received the TRUE value (%s=%s)\n"+
									"  SQL: %s\n  got: %s", c, v, cell.sql,
									strings.Join(got.canon(), " ; "))
							}
						}
					}
				}
				for _, c := range got.cols {
					if strings.EqualFold(c, "salary") {
						t.Fatalf("the DENIED column salary is present in the output\n"+
							"  SQL: %s\n  got: %s", cell.sql, strings.Join(got.canon(), " ; "))
					}
				}
			})
		}
	}

	// NON-VACUOUS. Ten cells must answer on each of the nine doors; a table
	// that refuses everything proves nothing about masking, which is the
	// failure mode a sentence in a definition of done cannot catch.
	if want := 10 * len(rig.doors); answered < want {
		t.Errorf("only %d (cell, door) pairs ANSWERED; want at least %d — a masking gate over "+
			"refusals is vacuous", answered, want)
	}
}
