// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// A NUMERIC VALUE CARRIES ITS DISPLAY SCALE (ADR-0024 §1 as amended
// 2026-10-06, arc PS stage 1). PostgreSQL prints an unconstrained numeric
// value at its own scale — `12.75` beside `12.3456789012345` in one COALESCE
// column, `2.50` beside `1` in one set operation — where one stored scale per
// column printed every value at the widest (`12.7500000000000`, numeric-decimal
// r18, #764) or at the stored scale of a set operation a literal with trailing
// zeros vetoed (`1.0000000000`, set-operations r4, #1647, whose COUNT over the
// text answered 7 for PostgreSQL's 3).
//
// Every statement of testdata/arc_ps_display_scale_cells.tsv answers
// PostgreSQL 17.11's raw text (testdata/arc_ps_display_scale_pg17.tsv,
// measured first) on the eleven arms of TestArcUNInBandMarkEveryArm: single,
// spilled, dag, dag-shuffled, dag-morsel4, dag-eager, dag-skew,
// dag-aggsplit, the fast path, the asynchronous door and the asynchronous
// door with its probe split forced — the display scale crosses the .wshf
// exchange, the spill run, the coordinator's merge and the asynchronous
// result. The rows are compared as text, sorted; the i5_* cells count, they
// do not print, a group or distinct value. The i11_* cells test the TEXT of
// a set operation's output whose arms carry equal values at different
// display scales: PostgreSQL's answer depends on whether its planner pushes
// the predicate into the arms (ADR-0013 item 11), and their PostgreSQL row
// is its answer to the statement with the set operation fenced (`OFFSET
// 0`), which tests the representative the operation chose — the reading
// this engine evaluates on every arm.
func TestArcPSDisplayScaleEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: eleven arms over the display scale")
	}
	cells := wdConsumerTable(t, "testdata/arc_ps_display_scale_cells.tsv")
	pg := map[string]string{}
	for _, a := range wdConsumerTable(t, "testdata/arc_ps_display_scale_pg17.tsv") {
		pg[a[0]] = a[1]
	}
	kept := psDisplayScaleKept()
	armKept := psDisplayScaleArmKept()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := unibArms(t, ctx)
	var gen *os.File
	if p := os.Getenv("PS_DSCALE_GEN"); p != "" {
		var err error
		if gen, err = os.Create(p); err != nil {
			t.Fatal(err)
		}
		defer gen.Close()
	}
	asserted := 0
	for _, c := range cells {
		name, sql := c[0], c[1]
		want, ok := pg[name]
		if !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", name)
		}
		if k, ok := kept[name]; ok {
			want = k
		}
		t.Run(name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(sql)
				got := ""
				if err != nil {
					got = "ERR " + strings.SplitN(err.Error(), "\n", 2)[0]
				} else {
					got = psRender(res)
				}
				if gen != nil {
					fmt.Fprintf(gen, "%s\t%s\t%s\n", name, arm.name, got)
				}
				w := want
				if a, ok := armKept[name][arm.name]; ok {
					w = a
				}
				asserted++
				if msg, ok := strings.CutPrefix(w, "ERR "); ok && err != nil && strings.Contains(err.Error(), msg) {
					continue
				}
				if got != w {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s", sql, arm.name, got, w)
				}
			}
		})
	}
	if asserted != len(cells)*len(arms) {
		t.Fatalf("only %d (cell, arm) answers asserted", asserted)
	}
}

// psDisplayScaleKept is the kept answer, the same on every arm, where it is
// not PostgreSQL's — each one a later stage's, named by its row.
func psDisplayScaleKept() map[string]string {
	return map[string]string{}
}

// psDisplayScaleArmKept is the answer one arm keeps where it is not
// PostgreSQL's — a defect outside the display scale that the base has on the
// same arm (gate_ps_every_arm_at_base_FAILS.log):
//
//   - k_left_join: the shuffled DAG arms refuse a LEFT JOIN to a derived table
//     whose select list renames a column (the join's output files name the
//     column two ways; a filing candidate of arc PS1).
//   - k_scalar: the asynchronous door has no distributed lowering for a scalar
//     subquery in a SELECT-list item.
//   - k_big_*: the asynchronous door with its probe split forced answers
//     rv_big's rows more than once (#1615), as the base does; the display
//     scales of the rows it answers are PostgreSQL's.
func psDisplayScaleArmKept() map[string]map[string]string {
	shuffled := "ERR where an earlier file of the same stage input named it"
	scalar := "ERR scalar subquery in a SELECT-list item has no distributed lowering"
	return map[string]map[string]string{
		"k_left_join":            {"dag-shuffled": shuffled, "dag-eager": shuffled, "dag-skew": shuffled},
		"k_scalar":               {"async": scalar, "async-probesplit": scalar},
		"k_big_union":            {"async-probesplit": "rows=1 37200"},
		"k_big_window":           {"async-probesplit": "rows=1 11238"},
		"k_big_sorted":           {"async-probesplit": "rows=1 10250"},
		"k_big_groupkey_count":   {"async-probesplit": "rows=1 9"},
		"k_big_distinct":         {"async-probesplit": "rows=12 1.00 | 1.00 | 1.00 | 1.50 | 1.50 | 1.50 | 2.5 | 2.5 | 2.5 | 7.25 | 7.25 | 7.25"},
		"k_big_group_key_marked": {"async-probesplit": "rows=6 0.0000000001,600 | 1,600 | 1.5,600 | 2.50,9 | 7,600 | NULL,600"},
	}
}

// psRender is the result as raw text, rows sorted: each cell exactly as the
// door boxed it, NULL as NULL — never trimmed, so a display scale the arm
// lost shows.
func psRender(res wdResult) string {
	rows := make([]string, 0, len(res.rows))
	for _, r := range res.rows {
		cells := make([]string, len(r))
		for j, v := range r {
			if v == nil {
				cells[j] = "NULL"
			} else {
				cells[j] = fmt.Sprint(v)
			}
		}
		rows = append(rows, strings.Join(cells, ","))
	}
	sort.Strings(rows)
	return fmt.Sprintf("rows=%d %s", len(rows), strings.Join(rows, " | "))
}
