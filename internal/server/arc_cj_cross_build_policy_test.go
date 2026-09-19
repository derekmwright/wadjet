// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
)

// A LIFTED FILTER OVER A CROSS JOIN'S BUILD SIDE READS THE POLICY'S ROWS — on
// all nine doors (arc CJ, #1189).
//
// A predicate over one side of a cross join is pushed onto that side's SCAN,
// and a scan under a policy is exactly where the mask and the row filter live
// (ADR-0033). The arc's defect was that the rows such a filter REJECTED came
// back as live build rows once the build spanned more than one batch — and
// under a policy the rejected rows include the ones the POLICY removed. This
// is therefore a disclosure shape and not only a wrong count: at 1c2b4d25, a
// clerk whose row filter leaves three of twelve employees reads all twelve
// through `e7other CROSS JOIN e7emp`.
//
// The fixture makes both readings VISIBLE, which is what a policy gate needs:
//
//   - `bal` is masked to 0 and `ssn` to '***', so a predicate written against
//     the MASK (`b.bal = 0`) accepts every row while the same predicate
//     against the STORED value accepts none. `stored` beside each cell is
//     what an unpoliced identity gets, and it differs from `want` in every
//     mask cell — a cell where the two agreed could not tell a mask read from
//     a value read.
//   - each cell also carries `leaked`, the answer the ROW SET would have if
//     the build republished the rows the filter rejected. It differs from
//     `want` in every cell, which is what makes this gate fail at base rather
//     than pass over an unfixed defect.
//
// e7emp is written with RowGroupSize 4 over twelve rows and e7bal over eight,
// so both builds arrive in more than one batch on the single-process doors:
// the condition the defect needs is present, deliberately, in the fixture
// these cells already use.
//
// At 1c2b4d25 this gate FAILS: cj_author/gate_cj_policy_at_base_FAILS.log.
func TestCJALiftedFilterOverAPolicedBuildPublishesOnlyThePolicysRows(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three servers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	cells := []struct {
		name string
		// key is the identity the cell runs as. Every cell but the control
		// below is the analyst, whose policy masks and denies.
		key    string
		sql    string
		want   []string // the MASK's answer
		stored []string // what an unpoliced identity gets, for non-vacuity
		leaked []string // what the unfixed build's row set answers
		// dagControl is the SAME query with the build column spelled as the
		// PROBE's. Where the four DAG doors diverge it is what they answer,
		// and running it beside the cell on that same door is the mechanism
		// and the proof at once: an answer that is the probe's own column
		// cannot depend on the value the policy hides. See cjPolicyDAGDoors.
		dagControl string
	}{
		{
			// The masked column decides the row set, and the row set is
			// smaller than the relation: three probe rows against build rows
			// 1..4 of eight.
			name: "cross/masked-number-decides-the-row-set",
			sql: `SELECT o.id AS a, b.id AS c FROM e7other o CROSS JOIN e7bal b ` +
				`WHERE b.bal = 0 AND b.id <= 4 ORDER BY 1, 2`,
			want:   cjPairs([]int{1, 2, 3}, []int{1, 2, 3, 4}),
			stored: nil, // `bal = 0` is true of no stored row
			leaked: cjPairs([]int{1, 2, 3}, []int{1, 2, 3, 4, 5, 6, 7, 8}),
			dagControl: `SELECT o.id AS a, o.id AS c FROM e7other o CROSS JOIN e7bal b ` +
				`WHERE b.bal = 0 AND b.id <= 4 ORDER BY 1, 2`,
		},
		{
			name: "cross/masked-string-decides-the-row-set",
			sql: `SELECT o.id AS a, m.id AS c FROM e7other o CROSS JOIN e7emp m ` +
				`WHERE m.ssn = '***' AND m.id <= 5 ORDER BY 1, 2`,
			want:   cjPairs([]int{1, 2, 3}, []int{1, 2, 3, 4, 5}),
			stored: nil,
			leaked: cjPairs([]int{1, 2, 3}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
			dagControl: `SELECT o.id AS a, o.id AS c FROM e7other o CROSS JOIN e7emp m ` +
				`WHERE m.ssn = '***' AND m.id <= 5 ORDER BY 1, 2`,
		},
		{
			// The same executor path spelled as an INNER join whose ON is an
			// EXPRESSION: ADR-0006's routed-probe amendment says the planner
			// lifts that ON into a filter above a CROSS join, so it is the
			// same seam under another name.
			name: "on-expression/masked-number-decides-the-row-set",
			sql: `SELECT o.id AS a, b.id AS c FROM e7other o JOIN e7bal b ` +
				`ON b.bal = 0 AND b.id <= 4 ORDER BY 1, 2`,
			want:   cjPairs([]int{1, 2, 3}, []int{1, 2, 3, 4}),
			stored: nil,
			leaked: cjPairs([]int{1, 2, 3}, []int{1, 2, 3, 4, 5, 6, 7, 8}),
			dagControl: `SELECT o.id AS a, o.id AS c FROM e7other o JOIN e7bal b ` +
				`ON b.bal = 0 AND b.id <= 4 ORDER BY 1, 2`,
		},
		{
			// The comma spelling of the same join.
			name: "comma/masked-string-decides-the-row-set",
			sql: `SELECT o.id AS a, m.id AS c FROM e7other o, e7emp m ` +
				`WHERE m.ssn = '***' AND m.id <= 5 ORDER BY 1, 2`,
			want:   cjPairs([]int{1, 2, 3}, []int{1, 2, 3, 4, 5}),
			stored: nil,
			leaked: cjPairs([]int{1, 2, 3}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
			dagControl: `SELECT o.id AS a, o.id AS c FROM e7other o, e7emp m ` +
				`WHERE m.ssn = '***' AND m.id <= 5 ORDER BY 1, 2`,
		},
		{
			// THE BOUNDARY, and the cell that makes this file fail at base.
			//
			// Measured: a POLICED relation's build side was never at risk.
			// The security projection standing between the scan and the join
			// materialises the filtered rows, so the batches the build stores
			// are dense and carry no selection vector for the merge to lose —
			// `SELECT COUNT(*) FROM e7other o CROSS JOIN e7emp m WHERE m.id
			// <= 5` answers 15 at 1c2b4d25 as the analyst and 24 as the
			// ADMIN, over the same rows, the same relation and the same
			// predicate. The cells above therefore hold at base too, and they
			// are here as the standing assertion that a policy's row set
			// survives this seam, not as the defect's evidence.
			//
			// This cell is the defect's evidence at the same seam: the same
			// query with no policy over it. It runs as the admin, so the leak
			// scan is skipped — the admin is entitled to the values.
			name: "cross/unpoliced-control-over-the-same-relation",
			key:  "admin-key",
			sql: `SELECT o.id AS a, m.id AS c FROM e7other o CROSS JOIN e7emp m ` +
				`WHERE m.id <= 5 ORDER BY 1, 2`,
			want:   cjPairs([]int{1, 2, 3}, []int{1, 2, 3, 4, 5}),
			stored: cjPairs([]int{1, 2, 3}, []int{1, 2, 3, 4, 5}),
			// Row groups of four: `m.id <= 5` prunes the third, and the two
			// that remain are the two batches the merge lost the row set of.
			leaked: cjPairs([]int{1, 2, 3}, []int{1, 2, 3, 4, 5, 6, 7, 8}),
			// NO DAG PIN, and that is a measurement: with no policy over the
			// relation the four DAG doors publish `m.id` correctly. The
			// column-identity divergence the other cells pin is POLICY-bound
			// — see cjPolicyDAGDoors.
		},
	}

	answered := 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			t.Run(cell.name+"/"+door.name, func(t *testing.T) {
				key := cell.key
				if key == "" {
					key = "analyst-key"
				}
				got, err := door.run(t, key, cell.sql)
				if err != nil {
					t.Fatalf("a lifted filter over a policed build must ANSWER on every door: %v\n  %s",
						err, cell.sql)
				}
				answered++
				// The admin control publishes the stored values by right, so
				// the leak scan is the analyst's alone.
				if key == "analyst-key" {
					for _, row := range got.rows {
						for c, v := range row {
							for _, bad := range leaks {
								if strings.Contains(v, bad) {
									t.Errorf("%s=%s is a policed value reaching the client\n  %s",
										c, v, cell.sql)
								}
							}
						}
					}
				}
				g := got.canon()
				want := cell.want
				if cjPolicyDAGDoors[door.name] && cell.dagControl != "" {
					// PINNED, pre-existing and `distributed`: these four doors
					// publish the PROBE's `id` where the query spells the
					// build's. The ROW COUNT is still the policy's — asserted
					// first, because that is the half a disclosure would move
					// — and the rows themselves are the ones the query has
					// when the column is spelled `o.id`, measured on this same
					// door in this same run.
					if len(g) != len(cell.want) {
						t.Fatalf("the pinned door's ROW COUNT is not the policy's\n  sql  %s\n  got  %v\n  want %d rows",
							cell.sql, g, len(cell.want))
					}
					ctl, cerr := door.run(t, key, cell.dagControl)
					if cerr != nil {
						t.Fatalf("the probe-spelled control did not answer: %v", cerr)
					}
					want = ctl.canon()
				}
				if strings.Join(g, ";") != strings.Join(want, ";") {
					what := "the lifted filter did not read the mask"
					if strings.Join(g, ";") == strings.Join(cell.leaked, ";") {
						what = "the build republished the rows the filter rejected"
					}
					t.Errorf("%s\n  sql    %s\n  got    %v\n  want   %v\n  stored %v",
						what, cell.sql, g, want, cell.stored)
				}
			})
		}
	}
	if want := len(cells) * len(rig.doors); answered != want {
		t.Errorf("only %d of %d (cell, door) pairs answered; a gate of refusals proves nothing",
			answered, want)
	}
	t.Logf("%d of %d (cell, door) pairs answered", answered, len(cells)*len(rig.doors))
}

// A ROW FILTER OVER A CROSS JOIN'S BUILD SIDE KEEPS ITS ROWS OUT — nine doors.
//
// The mask cells above turn on a value. This one turns on ROW-LEVEL SECURITY:
// `pmFilterProvider` denies `salary` and filters by it, leaving a clerk the
// three employees whose salary exceeds 700009. That filter reaches the build
// side of a cross join as the same pushed-down predicate every other filter
// does, so the arc's defect published the nine rows the policy removed.
//
// The cells carry no predicate of their own. That is the point: the ONLY
// thing deciding which build rows come back is the policy.
func TestCJARowFilterOverACrossJoinsBuildKeepsItsRowsOut(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three servers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUpWith(t, ctx, pmFilterProvider(t))

	visible := []int{10, 11, 12} // salary = 700000 + id > 700009
	cells := []struct {
		name       string
		sql        string
		want       []string
		dagControl string
	}{
		{
			name: "cross/row-filter-alone-decides-the-build",
			sql:  `SELECT o.id AS a, m.id AS c FROM e7other o CROSS JOIN e7emp m ORDER BY 1, 2`,
			want: cjPairs([]int{1, 2, 3}, visible),
			dagControl: `SELECT o.id AS a, o.id AS c FROM e7other o CROSS JOIN e7emp m ` +
				`ORDER BY 1, 2`,
		},
		{
			name: "comma/row-filter-alone-decides-the-build",
			sql:  `SELECT o.id AS a, m.id AS c FROM e7other o, e7emp m ORDER BY 1, 2`,
			want: cjPairs([]int{1, 2, 3}, visible),
		},
		{
			name: "cross/row-filter-under-a-count",
			sql:  `SELECT COUNT(*) AS n FROM e7other o CROSS JOIN e7emp m`,
			want: []string{fmt.Sprintf("n=%d", 3*len(visible))},
		},
	}

	answered := 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			t.Run(cell.name+"/"+door.name, func(t *testing.T) {
				got, err := door.run(t, "clerk-key", cell.sql)
				if err != nil {
					t.Fatalf("a cross join over a row-filtered relation must ANSWER on every door: %v\n  %s",
						err, cell.sql)
				}
				answered++
				g := got.canon()
				want := cell.want
				if cjPolicyDAGDoors[door.name] && cell.dagControl != "" {
					if len(g) != len(cell.want) {
						t.Fatalf("the pinned door's ROW COUNT is not the row filter's\n  sql  %s\n  got  %v\n  want %d rows",
							cell.sql, g, len(cell.want))
					}
					ctl, cerr := door.run(t, "clerk-key", cell.dagControl)
					if cerr != nil {
						t.Fatalf("the probe-spelled control did not answer: %v", cerr)
					}
					want = ctl.canon()
				}
				if strings.Join(g, ";") != strings.Join(want, ";") {
					t.Errorf("the build published rows the row filter removed\n  sql  %s\n  got  %v\n  want %v",
						cell.sql, g, want)
				}
			})
		}
	}
	if want := len(cells) * len(rig.doors); answered != want {
		t.Errorf("only %d of %d (cell, door) pairs answered", answered, want)
	}
	t.Logf("%d of %d (cell, door) pairs answered", answered, len(cells)*len(rig.doors))
}

// cjPolicyDAGDoors names the four doors on which a CROSS join over a POLICED
// relation answers the PROBE's `id` where the query spells the build's.
//
// Policy-bound, and measured to be: the unpoliced control cell in the same
// table — the same two relations, the same spelling, the same doors, run as
// the admin — publishes the build's `id` correctly on all nine.
//
// PRE-EXISTING and `distributed` under the arm rule, and NOT a disclosure: the
// value published is the probe relation's own unpoliced column, the row COUNT
// is the policy's on every cell (asserted before the pin is applied), and the
// same doors answer the same rows with the column spelled `o.id`. It is arc
// JR's filing candidate FC-7 in another spelling — a join publishing the probe
// arm's column for a build-arm reference — and this arc does not chase it. The
// comma spelling with no WHERE does not diverge, which is measured rather than
// explained: the two spellings reach the stage builder differently.
var cjPolicyDAGDoors = map[string]bool{
	"embedded/dag":          true,
	"embedded/dag-shuffled": true,
	"pgwire/dag":            true,
	"http/dag":              true,
}

// cjPairs renders the cross product of two id lists the way pmResult.canon
// does, so a cell's `want` is a statement about the ROW SET and not a
// transcript nobody can read.
func cjPairs(as, cs []int) []string {
	out := make([]string, 0, len(as)*len(cs))
	for _, a := range as {
		for _, c := range cs {
			out = append(out, fmt.Sprintf("a=%d|c=%d", a, c))
		}
	}
	// canon sorts its rows as STRINGS.
	sortStrings(out)
	return out
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

var _ = auth.ActionRead
