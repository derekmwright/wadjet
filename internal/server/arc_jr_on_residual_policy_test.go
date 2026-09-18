// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"strings"
	"testing"
	"time"

	"context"
)

// AN OUTER JOIN'S ON RESIDUAL READS THE MASK, NOT THE STORED VALUE — on all
// nine doors (arc JR, #1153).
//
// This arc makes an ON clause EVALUATE a general expression over a relation's
// columns at the join, per probe row against each build candidate. That is a
// new predicate site over a policed relation, and a predicate site is where a
// mask has to be read instead of the value the policy hides: the join's ROW
// SET is arithmetic on whatever the residual read. A residual reading the
// stored column would disclose it WITHOUT EVER PUBLISHING IT — a predicate over
// a column masked to 0 answers "which rows are negative" in the shape of which
// probe rows came back padded, and no leak test over the returned VALUES can
// see that.
//
// So each cell asserts the ANSWER, not only the absence of a true value. The
// mask's answer is written beside the STORED answer the same query gives an
// unpoliced identity, and they differ in every cell: a residual reading the
// stored column fails on the first door.
//
// EVERY CELL IS A REAL RESIDUAL. A conjunct that names only the NULL-SUPPLYING
// side of an outer join has a better home — `pushdownPredicates` puts it in
// that side's scan — so a cell spelled that way would gate the SCAN's masking
// and not this arc's seam at all. The LEFT cells are therefore CROSS-SIDE
// (they name the probe too), and the RIGHT and FULL cells name the PRESERVED
// side, which cannot be pushed anywhere.
//
// THE FOUR DAG DOORS DIVERGE, and the divergence is PRE-EXISTING and is not a
// disclosure. See jrPolicyDAGDoors.
func TestJRAnOuterJoinResidualOverAPolicedColumnReadsTheMask(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three servers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	// e7other holds ids 1, 2, 3 and carries no obligation; e7bal holds ids
	// 1..8 with bal = ±100i, masked to 0; e7emp holds ids 1..12 with ssn
	// masked to '***'.
	cells := []struct {
		name string
		sql  string
		// noResidual is the SAME join with the residual conjunct removed. It
		// is what the four DAG doors answer, and running it beside the cell
		// is what proves their divergence discloses nothing: their answer is
		// the one the query has when the predicate is absent, which cannot
		// depend on the value the policy hides.
		noResidual string
		// want is the MASK's answer, on the five single-process doors.
		want []string
		// stored is what an unpoliced identity gets. It is here so the cell
		// is visibly non-vacuous: were it equal to want, the cell could not
		// tell a mask read from a value read.
		stored []string
		// pinDAG, when set, is the four DAG doors' measured answer. It is set
		// only where those doors do not simply answer the residual-free
		// control — the RIGHT and FULL cells, where the residual IS applied
		// and the UNMATCHED FLUSH is what loses the build columns.
		pinDAG []string
	}{
		{
			name:       "left/cross-side-arithmetic",
			sql:        `SELECT o.id AS a, b.id AS c FROM e7other o LEFT JOIN e7bal b ON o.id = b.id AND b.bal < o.id ORDER BY 1`,
			noResidual: `SELECT o.id AS a, b.id AS c FROM e7other o LEFT JOIN e7bal b ON o.id = b.id ORDER BY 1`,
			want:       []string{"a=1|c=1", "a=2|c=2", "a=3|c=3"},
			stored:     []string{"a=1|c=1", "a=2|c=NULL", "a=3|c=3"},
		},
		{
			name:       "left/cross-side-function",
			sql:        `SELECT o.id AS a, b.id AS c FROM e7other o LEFT JOIN e7bal b ON o.id = b.id AND ABS(b.bal) - 250 > o.id ORDER BY 1`,
			noResidual: `SELECT o.id AS a, b.id AS c FROM e7other o LEFT JOIN e7bal b ON o.id = b.id ORDER BY 1`,
			want:       []string{"a=1|c=NULL", "a=2|c=NULL", "a=3|c=NULL"},
			stored:     []string{"a=1|c=NULL", "a=2|c=NULL", "a=3|c=3"},
		},
		{
			name:       "left/cross-side-substr",
			sql:        `SELECT o.id AS a, m.id AS c FROM e7other o LEFT JOIN e7emp m ON o.id = m.id AND SUBSTR(m.ssn, 1, o.id) = 'tru' ORDER BY 1`,
			noResidual: `SELECT o.id AS a, m.id AS c FROM e7other o LEFT JOIN e7emp m ON o.id = m.id ORDER BY 1`,
			want:       []string{"a=1|c=NULL", "a=2|c=NULL", "a=3|c=NULL"},
			stored:     []string{"a=1|c=NULL", "a=2|c=NULL", "a=3|c=3"},
		},
		{
			name:       "left/cross-side-like",
			sql:        `SELECT o.id AS a, m.id AS c FROM e7other o LEFT JOIN e7emp m ON o.id = m.id AND m.ssn LIKE 'true-ssn-0' || CAST(o.id AS VARCHAR) ORDER BY 1`,
			noResidual: `SELECT o.id AS a, m.id AS c FROM e7other o LEFT JOIN e7emp m ON o.id = m.id ORDER BY 1`,
			want:       []string{"a=1|c=NULL", "a=2|c=NULL", "a=3|c=NULL"},
			stored:     []string{"a=1|c=1", "a=2|c=2", "a=3|c=3"},
		},
		{
			// RIGHT: the preserved side is the POLICED one, so its conjunct
			// cannot be pushed into its own scan and the residual also
			// decides the unmatched FLUSH.
			name:       "right/preserved-side-function",
			sql:        `SELECT o.id AS a, b.id AS c FROM e7other o RIGHT JOIN e7bal b ON o.id = b.id AND ABS(b.bal) > 50 ORDER BY 2`,
			noResidual: `SELECT o.id AS a, b.id AS c FROM e7other o RIGHT JOIN e7bal b ON o.id = b.id ORDER BY 2`,
			want: []string{"a=NULL|c=1", "a=NULL|c=2", "a=NULL|c=3", "a=NULL|c=4",
				"a=NULL|c=5", "a=NULL|c=6", "a=NULL|c=7", "a=NULL|c=8"},
			stored: []string{"a=1|c=1", "a=2|c=2", "a=3|c=3", "a=NULL|c=4",
				"a=NULL|c=5", "a=NULL|c=6", "a=NULL|c=7", "a=NULL|c=8"},
			// The residual IS read, and reads the mask — all eight build rows
			// come back unmatched, which is want's disposition exactly. What
			// the DAG doors lose is the build COLUMNS of an unmatched row,
			// with or without a residual (see jrPolicyDAGDoors).
			pinDAG: []string{"a=NULL|c=NULL", "a=NULL|c=NULL", "a=NULL|c=NULL", "a=NULL|c=NULL",
				"a=NULL|c=NULL", "a=NULL|c=NULL", "a=NULL|c=NULL", "a=NULL|c=NULL"},
		},
		{
			name:       "full/preserved-side-function",
			sql:        `SELECT o.id AS a, b.id AS c FROM e7other o FULL JOIN e7bal b ON o.id = b.id AND ABS(b.bal) > 50 ORDER BY 2`,
			noResidual: `SELECT o.id AS a, b.id AS c FROM e7other o FULL JOIN e7bal b ON o.id = b.id ORDER BY 2`,
			want: []string{"a=1|c=NULL", "a=2|c=NULL", "a=3|c=NULL",
				"a=NULL|c=1", "a=NULL|c=2", "a=NULL|c=3", "a=NULL|c=4",
				"a=NULL|c=5", "a=NULL|c=6", "a=NULL|c=7", "a=NULL|c=8"},
			stored: []string{"a=1|c=1", "a=2|c=2", "a=3|c=3", "a=NULL|c=4",
				"a=NULL|c=5", "a=NULL|c=6", "a=NULL|c=7", "a=NULL|c=8"},
			// A FULL join's two halves disagree on these doors: the probe half
			// answers as though the residual were absent (three matched rows)
			// while the flush answers as though it rejected everything (eight
			// unmatched build rows), and the unmatched rows lose their build
			// columns. Eleven rows that cannot all be true at once — and none
			// of them a function of the policed value.
			pinDAG: []string{"a=1|c=1", "a=2|c=2", "a=3|c=3",
				"a=NULL|c=NULL", "a=NULL|c=NULL", "a=NULL|c=NULL", "a=NULL|c=NULL",
				"a=NULL|c=NULL", "a=NULL|c=NULL", "a=NULL|c=NULL", "a=NULL|c=NULL"},
		},
	}

	answered := 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			t.Run(cell.name+"/"+door.name, func(t *testing.T) {
				got, err := door.run(t, "analyst-key", cell.sql)
				if err != nil {
					t.Fatalf("a residual over a policed column must ANSWER on every door: %v\n  %s",
						err, cell.sql)
				}
				answered++
				for _, row := range got.rows {
					for c, v := range row {
						for _, bad := range leaks {
							if strings.Contains(v, bad) {
								t.Errorf("%s=%s is a policed value reaching the client\n  %s", c, v, cell.sql)
							}
						}
					}
				}
				want := cell.want
				if jrPolicyDAGDoors[door.name] && cell.pinDAG != nil {
					want = cell.pinDAG
				} else if jrPolicyDAGDoors[door.name] {
					// PINNED: the DAG doors answer the query WITHOUT its
					// residual, measured on this same door in this same run.
					// That is the mechanism and the proof at once — an answer
					// the predicate did not reach cannot depend on the value
					// the policy hides — and it is why this divergence is a
					// wrong answer rather than a disclosure. Pre-existing at
					// 563aa517: the same three doors already dropped a
					// policy-rewritten residual there, for the residual
					// spellings that path could evaluate at all.
					bare, berr := door.run(t, "analyst-key", cell.noResidual)
					if berr != nil {
						t.Fatalf("the residual-free control did not answer: %v", berr)
					}
					want = bare.canon()
				}
				if strings.Join(got.canon(), ";") != strings.Join(want, ";") {
					t.Errorf("the residual did not read the mask\n  sql    %s\n  got    %v\n  want   %v\n  stored %v",
						cell.sql, got.canon(), want, cell.stored)
				}
			})
		}
	}
	if answered != len(cells)*len(rig.doors) {
		t.Errorf("only %d of %d (cell, door) pairs answered; a gate of refusals proves nothing",
			answered, len(cells)*len(rig.doors))
	}
	t.Logf("%d of %d (cell, door) pairs answered", answered, len(cells)*len(rig.doors))
}

// jrPolicyDAGDoors names the four doors whose answer to a policed outer join is
// the answer with the RESIDUAL ABSENT, not the answer the mask gives.
//
// PRE-EXISTING and `distributed` under the arm rule. Two mechanisms sit behind
// it, and both are visible with no residual in the query at all:
//
//   - A policy-rewritten predicate over the masked column is not applied on
//     these doors. At 563aa517, `LEFT JOIN e7bal b ON o.id = b.id AND b.bal < 0`
//     — arithmetic, which the residual evaluator of the day could evaluate —
//     already answered "every candidate matched" here while the five
//     single-process doors answered the mask's "every probe row padded".
//   - The RIGHT/FULL unmatched FLUSH publishes NULL for the build columns on
//     these doors. `RIGHT JOIN e7bal b ON o.id = b.id`, with no residual and
//     nothing for a policy to rewrite in the ON clause, answers
//     `a=NULL|c=NULL` for the five unmatched build rows here and
//     `a=NULL|c=4..8` on the single-process doors — at 563aa517 as well.
//
// Neither discloses anything: the pinned answer is measured to EQUAL the same
// query with its residual removed, which cannot be a function of the value the
// policy hides. Not chased here (engine-first, Derek 2026-09-16); recorded as a
// filing candidate in the arc's landing notes.
//
// A pin that starts agreeing FAILS — when these doors apply a policed
// residual, this map is what gets deleted.
var jrPolicyDAGDoors = map[string]bool{
	"embedded/dag":          true,
	"embedded/dag-shuffled": true,
	"pgwire/dag":            true,
	"http/dag":              true,
}

// A DENIED COLUMN INSIDE AN ON RESIDUAL DOES NOT DECIDE A ROW SET.
//
// `salary` is denied to the analyst on e7emp. Put in a residual it must refuse,
// on every door: a denied column that quietly evaluated would answer "which
// rows have salary above the bound" in the shape of which probe rows came back
// padded. The admin identity, which the policy does not touch, answers — that
// is the control that keeps the refusal about the POLICY rather than about the
// shape.
func TestJRADeniedColumnInAnOnResidualDoesNotDecideARowSet(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three servers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)

	const sql = `SELECT o.id AS a, m.id AS c FROM e7other o LEFT JOIN e7emp m ` +
		`ON o.id = m.id AND m.salary > o.id + 700001 ORDER BY 1`
	// Off the stored column the answer is id 1 padded and ids 2, 3 matched —
	// three rows that say which salaries exceed the bound.
	const disclosing = "a=1|c=NULL;a=2|c=2;a=3|c=3"

	refused, admin := 0, 0
	for _, door := range rig.doors {
		t.Run(door.name, func(t *testing.T) {
			got, err := door.run(t, "analyst-key", sql)
			if err != nil {
				refused++
				return
			}
			if canon := strings.Join(got.canon(), ";"); canon == disclosing {
				t.Errorf("a DENIED column decided the join's row set: %s\n  %s", canon, sql)
			} else {
				t.Errorf("a DENIED column inside an ON residual answered %s; "+
					"it must refuse\n  %s", canon, sql)
			}
		})
		// The control: the same shape, an identity the policy does not police.
		if _, err := door.run(t, "admin-key", sql); err == nil {
			admin++
		}
	}
	if refused != len(rig.doors) {
		t.Errorf("%d of %d doors refused the denied column in a residual", refused, len(rig.doors))
	}
	if admin == 0 {
		t.Errorf("no door answered the same shape for the admin identity: the refusal " +
			"above is about the SHAPE, not about the policy, and proves nothing")
	}
}
