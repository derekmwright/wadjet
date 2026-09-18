// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"strings"
	"testing"
	"time"
)

// ARC SR's MASKING GATE, on all nine doors: a STAR that publishes one column
// NAME twice, where one of the two is POLICED.
//
// COMMON.md requires it because this arc rewrites what a star PUBLISHES over a
// relation, in three places where a security projection could be bypassed:
//
//   - the `JOIN … USING` merge now states its list wherever the two arms share
//     a column name OUTSIDE the USING list. That decline was the ONLY thing
//     standing between a star over `e7emp a JOIN e7emp b USING (id)` and an
//     answer, so every one of these cells was a plan-time refusal before this
//     arc and is a real published relation after it — eleven columns, five of
//     them from each policed arm.
//   - a SET OPERATION now carries a published name to the client on all five
//     arms, through the sink on the single-process path and through a new
//     gather rename on the DAG. A rename list the gather applies is also a
//     list it DROPS by, so a widened one would publish a column the security
//     projection removed.
//   - a REFERENCE into a block that publishes one name twice is 42702 now.
//     A refusal is a read too (#994): it must not be reachable only after the
//     policed column has been read, and it must not name what it refuses.
//
// The assertion per cell is threefold — no TRUE value of a masked column
// reaches any door, no DENIED column appears in any output, and a statement
// naming a denied column REFUSES — and the whole table asserts a NON-VACUOUS
// count of (cell, door) pairs that actually ANSWERED, so a change that turns
// every shape into a refusal cannot pass by emptiness.
func TestArcSRAStarOverAPolicedArmNeverPublishesTheOtherArmsValue(t *testing.T) {
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
		// refuses marks a cell that must be REFUSED on every door, naming a
		// substring of the sentence it is refused with. A cell that starts
		// answering fails, which is how the entry gets deleted when the
		// divergence closes.
		refuses string
	}{
		// ---- the shape this arc UNLOCKED: a USING star over two policed arms
		//
		// Eleven published columns, ten of them from a policed relation, and
		// the merged key first. Before this arc the merge declined on the
		// shared tail names and the statement was 0A000, so these are the
		// cells that say the new list is a POLICED list.
		{name: "using_id_over_two_policed_arms", answers: true,
			sql: `SELECT * FROM e7emp a JOIN e7emp b USING (id)`},
		{name: "using_id_over_two_policed_arms_left", answers: true,
			sql: `SELECT * FROM e7emp a LEFT JOIN e7emp b USING (id)`},
		{name: "using_the_masked_column_itself", answers: true,
			sql: `SELECT * FROM e7emp a JOIN e7emp b USING (ssn)`},
		{name: "using_the_masked_numeric_itself", answers: true,
			sql: `SELECT * FROM e7emp a JOIN e7emp b USING (acct)`},
		{name: "using_id_beside_a_policed_reference", answers: true,
			sql: `SELECT *, a.ssn FROM e7emp a JOIN e7emp b USING (id)`},
		{name: "using_id_a_qualified_star_left", answers: true,
			sql: `SELECT a.* FROM e7emp a JOIN e7emp b USING (id)`},
		{name: "using_id_a_qualified_star_right", answers: true,
			sql: `SELECT b.* FROM e7emp a JOIN e7emp b USING (id)`},
		{name: "using_id_one_policed_arm_one_not", answers: true,
			sql: `SELECT * FROM e7emp a JOIN e7other b USING (id)`},
		{name: "using_id_over_two_masked_numeric_arms", answers: true,
			sql: `SELECT * FROM e7bal a JOIN e7bal b USING (id)`},
		{name: "using_id_across_two_policed_relations", answers: true,
			sql: `SELECT * FROM e7bal a JOIN e7emp b USING (id)`},
		{name: "using_id_over_two_policed_derived_arms", answers: true,
			sql: `SELECT * FROM (SELECT id, ssn, acct FROM e7emp) a ` +
				`JOIN (SELECT id, ssn, acct FROM e7emp) b USING (id)`},

		// ---- the ON control: the same pair, the spelling that always answered
		{name: "ctl_on_over_two_policed_arms", answers: true,
			sql: `SELECT * FROM e7emp a JOIN e7emp b ON a.id = b.id`},
		{name: "ctl_on_over_two_masked_numeric_arms", answers: true,
			sql: `SELECT * FROM e7bal a JOIN e7bal b ON a.id = b.id`},

		// ---- a DENIED column reached through the new list ------------------
		{name: "using_a_denied_key", denied: "salary",
			sql: `SELECT * FROM e7emp a JOIN e7emp b USING (salary)`},
		{name: "using_id_beside_a_denied_reference", denied: "salary",
			sql: `SELECT *, a.salary FROM e7emp a JOIN e7emp b USING (id)`},

		// ---- the SET OPERATION's published name, over a policed relation ---
		//
		// `acct` masks to 0, so `acct + 1` is 1 for every row: a cell that
		// carried a stored 9000xx would be a leak through the arm the gather
		// renames from.
		{name: "set_op_published_name_over_a_policed_relation", answers: true,
			sql: `WITH c AS (SELECT id, acct + 1 FROM e7emp UNION ALL SELECT id, amt FROM e7emp) ` +
				`SELECT * FROM c`},
		{name: "set_op_published_name_qualified_star", answers: true,
			sql: `WITH c AS (SELECT id, acct + 1 FROM e7emp UNION ALL SELECT id, amt FROM e7emp) ` +
				`SELECT c.* FROM c`},
		{name: "set_op_published_name_under_a_join", answers: true,
			sql: `SELECT * FROM (SELECT id, acct + 1 FROM e7emp UNION ALL ` +
				`SELECT id, amt FROM e7emp) a JOIN e7other b ON a.id = b.id`},
		{name: "set_op_published_name_denied_column", denied: "salary",
			sql: `WITH c AS (SELECT id, salary + 1 FROM e7emp UNION ALL SELECT id, amt FROM e7emp) ` +
				`SELECT * FROM c`},

		// ---- the block that publishes one POLICED name twice ---------------
		//
		// The bare star reads it positionally and must mask BOTH copies; the
		// reference into it is 42702 and must not name a value on the way.
		{name: "dupname_bare_star_over_a_policed_block", answers: true,
			sql: `SELECT * FROM (SELECT a.ssn, b.ssn FROM e7emp a JOIN e7emp b ON a.id = b.id) x`},
		{name: "dupname_reference_into_a_policed_block",
			refuses: `column reference "ssn" is ambiguous`,
			sql:     `SELECT x.ssn FROM (SELECT a.ssn, b.ssn FROM e7emp a JOIN e7emp b ON a.id = b.id) x`},

		// ---- the ZERO-ROW declaration over a policed relation --------------
		//
		// A declaration is a read too (#994): the columns a zero-row star
		// declares are the POLICED width, so `salary` is not among them.
		{name: "zero_row_three_way_over_a_policed_relation",
			sql: `SELECT * FROM e7emp a JOIN e7other b ON a.id = b.id ` +
				`JOIN e7bal c ON c.id = a.id WHERE a.id < 0`},
		{name: "zero_row_using_over_two_policed_arms",
			sql: `SELECT * FROM e7emp a JOIN e7emp b USING (id) WHERE a.id < 0`},
	}

	answered := 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			t.Run(cell.name+"/"+door.name, func(t *testing.T) {
				got, err := door.run(t, "analyst-key", cell.sql)
				if err != nil {
					if cell.refuses != "" && !strings.Contains(err.Error(), cell.refuses) {
						t.Errorf("refused with %v\n  want a refusal naming %q\n  SQL: %s",
							err, cell.refuses, cell.sql)
					}
					if cell.answers {
						t.Errorf("this cell must ANSWER on every door, so the no-leak assertion "+
							"over it is not vacuous: %v\n  SQL: %s", err, cell.sql)
					}
					return
				}
				if cell.refuses != "" {
					t.Fatalf("ANSWERED where the rule refuses (%s)\n  SQL: %s\n  got: %s",
						cell.refuses, cell.sql, strings.Join(got.canon(), " ; "))
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
				// The DENIED column must not be in the OUTPUT LIST either —
				// a zero-row declaration publishes a list with no rows under
				// it, and that list is a read of the relation's shape.
				for _, c := range got.cols {
					if strings.EqualFold(c, "salary") {
						t.Fatalf("the DENIED column salary is present in the output\n"+
							"  SQL: %s\n  got: %s", cell.sql, strings.Join(got.canon(), " ; "))
					}
				}
			})
		}
	}

	// NON-VACUOUS. Sixteen cells must answer on each of the nine doors; a
	// table that refuses everything proves nothing about masking, which is the
	// failure mode a sentence in a definition of done cannot catch.
	if want := 16 * len(rig.doors); answered < want {
		t.Errorf("only %d (cell, door) pairs ANSWERED; want at least %d — a masking gate over "+
			"refusals is vacuous", answered, want)
	}
}
