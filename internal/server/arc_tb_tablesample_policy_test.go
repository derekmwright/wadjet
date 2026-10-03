// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A SAMPLED POLICED RELATION PUBLISHES ONLY THE POLICY'S ROWS AND VALUES, ON
// EVERY DOOR (#1411).
//
// Arc TB routes a TABLESAMPLE scan off the stage DAG onto the
// coordinator-local pipeline (no stage fragment carries the sampler) and
// reads the argument as an expression. So a sampled scan now RUNS on a path
// it did not run on before on the DAG doors, over the same enforced plan.
// This is the masking gate for that route: e7emp (ssn masked, salary denied)
// and e7bal (a row filter, bal masked) sampled at 100 % answer exactly what
// the same unsampled statement answers on the same door — the policy's rows
// and the mask — and 0 % answers nothing. An out-of-range percentage is
// refused 2202H on every door, which is how this file fails at 6184761c
// (BERNOULLI (0) answered every row and (101) answered).
//
// Non-vacuity: every (cell, door) pair of the answering cells must answer;
// a door that refuses one of them fails here rather than passing silently.
func TestArcTBASampledPolicedRelationPublishesOnlyThePolicysRows(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
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
}
