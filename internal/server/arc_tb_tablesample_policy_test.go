// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A SAMPLED POLICED RELATION PUBLISHES ONLY THE POLICY'S ROWS AND VALUES, ON
// EVERY DOOR (#1411).
//
// Arc TB reads the argument as an expression and draws the sample where the
// scan runs: the single-process scan and, on the DAG doors, the worker's scan
// fragment, which now carries the sampler (round 2; round 1 routed every
// sampled statement to the coordinator-local pipeline). So a sampled scan
// runs a sampler on the DAG it did not run before, over the same enforced
// plan. This is the masking gate for it: e7emp (ssn masked, salary denied)
// and e7bal (a row filter, bal masked) sampled at 100 % answer exactly what
// the same unsampled statement answers on the same door — the policy's rows
// and the mask — and 0 % answers nothing. An out-of-range percentage is
// refused 2202H on every door, which is how this file fails at 6184761c
// (BERNOULLI (0) answered every row and (101) answered).
//
// Then a DELETE (as admin, through each door) removes one visible row of
// each table, and the sampled statements must still equal the unsampled ones:
// the sample is drawn from the rows the scan SELECTS. Round 1's sampler drew
// from every physical row and returned the deleted one (review r1 B1).
//
// Non-vacuity: every (cell, door) pair of the answering cells must answer;
// a door that refuses one of them fails here rather than passing silently,
// and the deleted row must be gone from the unsampled answer on every door.
func TestArcTBASampledPolicedRelationPublishesOnlyThePolicysRows(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	// The arc's policy, and the admin may write: the DELETE phase removes a
	// row through every door.
	rig := pmRigUpWith(t, ctx, pmProviderWith(t, auth.PolicyRule{
		ID: "tb-admin-writes", EffectStr: "allow", Priority: 10,
		Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "admin"}},
		Actions:  []auth.Action{auth.ActionWrite},
	}))
	leaks := pmTrueValues()

	cells := []struct {
		name, sql string
		// same is the unsampled statement the cell must equal on its door;
		// empty for a cell with its own want.
		same  string
		empty bool   // the cell answers no row
		state string // the cell is refused with this SQLSTATE
		// msg is the refusal's sentence: the HTTP door carries the
		// sentence and not the SQLSTATE.
		msg string
	}{
		{name: "masked-columns-100", sql: `SELECT id, ssn, acct FROM e7emp TABLESAMPLE BERNOULLI (100)`,
			same: `SELECT id, ssn, acct FROM e7emp`},
		{name: "star-100", sql: `SELECT * FROM e7emp TABLESAMPLE SYSTEM (100)`,
			same: `SELECT * FROM e7emp`},
		{name: "row-filter-100", sql: `SELECT id, bal FROM e7bal TABLESAMPLE BERNOULLI (100.000001)`,
			same: `SELECT id, bal FROM e7bal`},
		{name: "row-filter-count-100", sql: `SELECT COUNT(*) AS c FROM e7bal TABLESAMPLE BERNOULLI (CAST(100 AS NUMERIC))`,
			same: `SELECT COUNT(*) AS c FROM e7bal`},
		{name: "joined-100", sql: `SELECT o.id AS a, e7emp.ssn AS s FROM e7other o JOIN e7emp TABLESAMPLE BERNOULLI (100) ON e7emp.id = o.id`,
			same: `SELECT o.id AS a, e7emp.ssn AS s FROM e7other o JOIN e7emp ON e7emp.id = o.id`},
		{name: "masked-columns-0", sql: `SELECT id, ssn FROM e7emp TABLESAMPLE BERNOULLI (0)`, empty: true},
		{name: "row-filter-0", sql: `SELECT id, bal FROM e7bal TABLESAMPLE SYSTEM (0)`, empty: true},
		{name: "out-of-range", sql: `SELECT id, ssn FROM e7emp TABLESAMPLE BERNOULLI (101)`, state: "2202H",
			msg: "sample percentage must be between 0 and 100"},
		{name: "null", sql: `SELECT id, bal FROM e7bal TABLESAMPLE BERNOULLI (NULL)`, state: "2202H",
			msg: "TABLESAMPLE parameter cannot be null"},
	}
	answered := 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if cell.state != "" {
				if err == nil {
					t.Errorf("%s / %s: answered %d rows; want %s\n  %s", cell.name, door.name, len(got.rows), cell.state, cell.sql)
				} else if s := sqlerr.StateOf(err); s != cell.state && !strings.Contains(err.Error(), cell.state) &&
					!strings.Contains(err.Error(), cell.msg) {
					t.Errorf("%s / %s: refused %v; want %s", cell.name, door.name, err, cell.state)
				}
				continue
			}
			if err != nil {
				t.Errorf("%s / %s: refused %v\n  %s", cell.name, door.name, err, cell.sql)
				continue
			}
			answered++
			for _, c := range got.cols {
				if strings.EqualFold(c, "salary") {
					t.Errorf("%s / %s: the DENIED column %q is in the published list", cell.name, door.name, c)
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
			if cell.empty {
				if len(got.rows) != 0 {
					t.Errorf("%s / %s: %d rows at 0 %%; want none", cell.name, door.name, len(got.rows))
				}
				continue
			}
			ref, err := door.run(t, "analyst-key", cell.same)
			if err != nil {
				t.Fatalf("%s / %s: the unsampled statement: %v", cell.name, door.name, err)
			}
			if len(ref.rows) == 0 {
				t.Fatalf("%s / %s: the unsampled statement answers no row; the cell proves nothing", cell.name, door.name)
			}
			if g, w := strings.Join(got.canon(), " ; "), strings.Join(ref.canon(), " ; "); g != w {
				t.Errorf("%s / %s: sampled at 100 %%\n  got  %s\n  want %s (the unsampled statement)", cell.name, door.name, g, w)
			}
		}
	}
	want := 0
	for _, c := range cells {
		if c.state == "" {
			want += len(rig.doors)
		}
	}
	if answered != want || len(rig.doors) < 9 {
		t.Fatalf("%d of %d answering (cell, door) pairs answered over %d doors", answered, want, len(rig.doors))
	}

	// The DELETE phase. Each door runs the DELETE itself (it is idempotent
	// where doors share a store), then the sampled statements over the
	// narrowed tables.
	deleted := []struct{ table, del, probe string }{
		{"e7emp", `DELETE FROM e7emp WHERE id = 5`, `SELECT id FROM e7emp WHERE id = 5`},
		{"e7bal", `DELETE FROM e7bal WHERE id = 4`, `SELECT id FROM e7bal WHERE id = 4`},
	}
	after := []struct{ name, sql, same string }{
		{"deleted-masked-100", `SELECT id, ssn, acct FROM e7emp TABLESAMPLE BERNOULLI (100)`,
			`SELECT id, ssn, acct FROM e7emp`},
		{"deleted-masked-count-100", `SELECT COUNT(*) AS c FROM e7emp TABLESAMPLE BERNOULLI (100)`,
			`SELECT COUNT(*) AS c FROM e7emp`},
		{"deleted-system-100", `SELECT id, ssn FROM e7emp TABLESAMPLE SYSTEM (100)`,
			`SELECT id, ssn FROM e7emp`},
		{"deleted-row-filter-100", `SELECT id, bal FROM e7bal TABLESAMPLE BERNOULLI (100)`,
			`SELECT id, bal FROM e7bal`},
		{"deleted-row-filter-count-100", `SELECT COUNT(*) AS c FROM e7bal TABLESAMPLE SYSTEM (100)`,
			`SELECT COUNT(*) AS c FROM e7bal`},
	}
	// Every door that takes DML runs the DELETE (the coordinator's own
	// ExecuteSQL does not; the pgwire and HTTP doors over its catalog do, so
	// the DAG doors' store is reached through them). Then every door must
	// see the row gone, or the phase proves nothing there.
	for _, door := range rig.doors {
		for _, d := range deleted {
			_, _ = door.run(t, "admin-key", d.del)
		}
	}
	pairs := 0
	for _, door := range rig.doors {
		for _, d := range deleted {
			if got, err := door.run(t, "admin-key", d.probe); err != nil || len(got.rows) != 0 {
				t.Fatalf("%s: after %s, %s answered %d rows (err %v); the DELETE did not reach this door",
					door.name, d.del, d.probe, len(got.rows), err)
			}
		}
		for _, cell := range after {
			got, err := door.run(t, "analyst-key", cell.sql)
			if err != nil {
				t.Errorf("%s / %s: refused %v\n  %s", cell.name, door.name, err, cell.sql)
				continue
			}
			ref, err := door.run(t, "analyst-key", cell.same)
			if err != nil || len(ref.rows) == 0 {
				t.Fatalf("%s / %s: the unsampled statement: %d rows, %v", cell.name, door.name, len(ref.rows), err)
			}
			pairs++
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
			if g, w := strings.Join(got.canon(), " ; "), strings.Join(ref.canon(), " ; "); g != w {
				t.Errorf("%s / %s: sampled at 100 %% after the DELETE\n  got  %s\n  want %s (the unsampled statement)",
					cell.name, door.name, g, w)
			}
		}
	}
	if pairs != len(after)*len(rig.doors) {
		t.Fatalf("%d of %d DELETE-phase (cell, door) pairs answered", pairs, len(after)*len(rig.doors))
	}
}
