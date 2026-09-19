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
// relation, in three places where a security projection could be bypassed. The
// `JOIN … USING` merge now states its list wherever the two arms share a name
// OUTSIDE the USING list — that decline was the ONLY thing standing between a
// star over `e7emp a JOIN e7emp b USING (id)` and an answer, so every cell here
// was a plan-time refusal before the arc and is a real published relation after
// it. A SET OPERATION now carries a published name to the client on all five
// arms, through a new gather rename on the DAG, and a rename list the gather
// applies is also a list it DROPS by. And a REFERENCE into a block publishing
// one name twice is 42702 now — a refusal is a read too (#994): it must not be
// reachable only after the policed column has been read, and must not name
// what it refuses.

// THE PROPERTY THE BRIEF NAMED IS "the mask stays on the arm that OWNS it",
// and round 1 measured that the first version of this table could not see it:
// every `using_*` cell joined a policed relation to ITSELF, where a mask that
// migrated between arms renders identically. The `armmask_*` cells are the
// discriminating dimension — two columns of ONE name, one policed and one not,
// in both written orders — and each names the value that must appear on EACH
// side, so a migrated mask changes one of the two.
//
// The assertion per cell is fourfold: no TRUE value of a masked column reaches
// any door, no DENIED column appears in any output, a statement naming a denied
// column REFUSES, and a cell naming its per-arm values gets them POSITIONALLY
// (`pmResult.cells`), because two columns of one name cannot both be read out
// of a row keyed by name. The table asserts a NON-VACUOUS count of (cell, door)
// pairs that actually ANSWERED, so a change turning every shape into a refusal
// cannot pass by emptiness.
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
		// arm is the value each OUTPUT POSITION must carry, per row, read
		// positionally. It is what makes a migrated mask a failure rather than
		// an identical rendering: `***` on the policed side and the other
		// relation's own value on the unpoliced one. An empty entry is a
		// position this cell does not pin.
		arm [][]string
		// cols, when set, is the output column list this cell must publish,
		// in order — asserted where two of them share a name, which a
		// name-keyed reading cannot check.
		cols []string
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

		// ---- THE MASK'S ARM, which is the property the brief named --------
		//
		// e7other is UNPOLICED, so `(SELECT id, note AS ssn FROM e7other)`
		// publishes a column called `ssn` that must NOT be masked, beside
		// e7emp's `ssn`, which must be. A mask that migrated arms shows up as
		// `***` where `n1` belongs or the true `true-ssn-01` where `***`
		// does — either way the cell fails. Both written orders, because
		// which side the plan builds is a cost decision.
		{name: "armmask_policed_first", answers: true,
			sql: `SELECT a.ssn AS asn, b.ssn AS bsn FROM e7emp a ` +
				`JOIN (SELECT id, note AS ssn FROM e7other) b ON a.id = b.id ORDER BY b.ssn`,
			cols: []string{"asn", "bsn"},
			arm:  [][]string{{"***", "n1"}, {"***", "n2"}, {"***", "n3"}}},
		{name: "armmask_unpoliced_first", answers: true,
			sql: `SELECT b.ssn AS bsn, a.ssn AS asn FROM (SELECT id, note AS ssn FROM e7other) b ` +
				`JOIN e7emp a ON a.id = b.id ORDER BY b.ssn`,
			cols: []string{"bsn", "asn"},
			arm:  [][]string{{"n1", "***"}, {"n2", "***"}, {"n3", "***"}}},
		// The masked NUMERIC, whose mask is 0 and whose unpoliced twin is a
		// computed multiple — so a migration is a different number, not a
		// different string.
		{name: "armmask_numeric", answers: true,
			sql: `SELECT a.acct AS aacct, b.acct AS bacct FROM e7emp a ` +
				`JOIN (SELECT id, id * 7 AS acct FROM e7other) b ON a.id = b.id ORDER BY b.acct`,
			cols: []string{"aacct", "bacct"},
			arm:  [][]string{{"0", "7"}, {"0", "14"}, {"0", "21"}}},
		// THE STAR spelling, where the two columns really do share a name:
		// the star publishes e7emp's policed `ssn` and the aliased item
		// carries the unpoliced one. This is the cell the name-keyed reading
		// could not see, and it is read positionally.
		{name: "armmask_star_publishes_the_policed_arm", answers: true,
			sql: `SELECT a.ssn, b.ssn FROM e7emp a ` +
				`JOIN (SELECT id, note AS ssn FROM e7other) b ON a.id = b.id ORDER BY b.ssn`,
			cols: []string{"ssn", "ssn"},
			arm:  [][]string{{"***", "n1"}, {"***", "n2"}, {"***", "n3"}}},
		{name: "armmask_star_publishes_the_unpoliced_arm_first", answers: true,
			sql: `SELECT b.ssn, a.ssn FROM (SELECT id, note AS ssn FROM e7other) b ` +
				`JOIN e7emp a ON a.id = b.id ORDER BY b.ssn`,
			cols: []string{"ssn", "ssn"},
			arm:  [][]string{{"n1", "***"}, {"n2", "***"}, {"n3", "***"}}},
		// The same pair through the MERGE this arc unlocked: the USING key is
		// unpoliced and the shared tail name `ssn` is policed on one side.
		{name: "armmask_using_a_shared_policed_tail_name", answers: true,
			sql: `SELECT * FROM e7emp a JOIN (SELECT id, note AS ssn FROM e7other) b USING (id) ` +
				`ORDER BY b.ssn`},

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
				// POSITIONALLY: a row keyed by NAME holds one of two columns
				// that share one, so a leak scan over the map inspects one
				// value and a migration between them is invisible.
				for i := range got.rows {
					for j, v := range got.cells(i) {
						name := ""
						if j < len(got.cols) {
							name = got.cols[j]
						}
						for _, bad := range leaks {
							if strings.Contains(v, bad) {
								t.Fatalf("the analyst identity received the TRUE value (%s=%s)\n"+
									"  SQL: %s\n  got: %s", name, v, cell.sql,
									strings.Join(got.canon(), " ; "))
							}
						}
					}
				}
				if cell.cols != nil && strings.Join(got.cols, ",") != strings.Join(cell.cols, ",") {
					t.Fatalf("published %v, want %v — the list is what the mask sits on\n  SQL: %s",
						got.cols, cell.cols, cell.sql)
				}
				// THE MASK STAYS ON ITS OWN ARM. Each pinned position names
				// the value that side must carry; a mask that migrated to the
				// other arm changes one of them.
				if cell.arm != nil {
					if got.vals == nil {
						// The HTTP door's JSON body is one object per row, so
						// it cannot carry two columns of one name. A cell
						// whose list has no duplicate is still readable there.
						if hasDup(got.cols) {
							return
						}
					}
					if len(got.rows) != len(cell.arm) {
						t.Fatalf("%d rows, want %d\n  SQL: %s\n  got: %s", len(got.rows),
							len(cell.arm), cell.sql, strings.Join(got.canon(), " ; "))
					}
					for i, want := range cell.arm {
						have := got.cells(i)
						for j, w := range want {
							if w == "" {
								continue
							}
							if j >= len(have) || have[j] != w {
								t.Fatalf("row %d position %d is %q, want %q — the mask must "+
									"stay on the arm that owns it\n  SQL: %s\n  got: %s",
									i, j, strings.Join(have, "|"), w, cell.sql,
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

	// NON-VACUOUS. Twenty-two cells must answer on each of the nine doors; a
	// table that refuses everything proves nothing about masking, which is the
	// failure mode a sentence in a definition of done cannot catch.
	if want := 22 * len(rig.doors); answered < want {
		t.Errorf("only %d (cell, door) pairs ANSWERED; want at least %d — a masking gate over "+
			"refusals is vacuous", answered, want)
	}
}

// hasDup reports whether two of these column names are the same, folded — the
// shape a row keyed by NAME cannot carry.
func hasDup(cols []string) bool {
	seen := make(map[string]bool, len(cols))
	for _, c := range cols {
		lc := strings.ToLower(c)
		if seen[lc] {
			return true
		}
		seen[lc] = true
	}
	return false
}
