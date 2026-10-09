// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// AN EXPRESSION SUBQUERY'S BODY, PLANNED IN ITS OWN WITH CHAIN, READS THE
// POLICED COLUMN AS THE POLICY PUBLISHES IT, ON EVERY DOOR (ADR-0047 stage 3,
// ADR-0033).
//
// The binder now binds an expression subquery's memoized body under a
// MASK-ONLY policy too — at 542b4f37 it saw a subquery body only when a
// column was DENIED — and the planners plan that body in the WITH chain
// where it is written, so a subquery in a nested block reads the block's WITH
// item and a nested item that reuses a name shadows the statement's. Each
// cell reads a policed column (e7emp's masked ssn and acct, denied salary;
// e7bal's masked bal) through such a body. The assertion is the masking
// census's — no true value of a policed column in any cell, no denied column
// in any list, a denied reference never answered — plus the MASK's own
// answer where a cell answers (the mask's text, or the value the masked
// column computes to), and a NON-VACUOUS count of (cell, door) pairs that
// answered. At 542b4f37 the nested-scope cells answered NULL, the statement's
// item or zero rows instead of the mask's reading.
func TestArcCI3SubqueryBodyNeverPublishesAPolicedValue(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	cells := []struct {
		name, sql string
		// maskCol names a result column whose every answered value holds the
		// ssn mask; want is the whole answer ("col=v|…" per row, sorted) a
		// cell must give where it answers; denied cells must not answer.
		maskCol, want string
		denied        bool
	}{
		{name: "maskNestedScalar", sql: `SELECT x.k FROM (WITH t AS (SELECT ssn FROM e7emp) SELECT (SELECT max(ssn) FROM t) AS k) x`, maskCol: "k"},
		{name: "maskShadowsTheStatementItem", sql: `WITH t AS (SELECT 'plain' AS ssn) SELECT x.k FROM (WITH t AS (SELECT ssn FROM e7emp) SELECT (SELECT max(ssn) FROM t) AS k) x`, maskCol: "k"},
		{name: "maskSubqueryOwnWith", sql: `SELECT (WITH t AS (SELECT ssn FROM e7emp) SELECT max(ssn) FROM t) AS k`, maskCol: "k"},
		{name: "maskAggregateArgumentOverCTE", sql: `WITH s AS (SELECT id, acct FROM e7emp) SELECT sum((SELECT max(acct) FROM s WHERE s.id >= e.id)) AS k FROM e7emp e`, want: "k=0"},
		{name: "maskCorrelatedOuterValue", sql: `SELECT e.id, (SELECT max(o.note) || e.ssn FROM e7other o WHERE o.id = e.id) AS k FROM e7emp e WHERE e.id <= 3`, maskCol: "k"},
		{name: "maskExistsInNestedBlock", sql: `SELECT count(*) AS n FROM (WITH t AS (SELECT ssn FROM e7emp) SELECT id FROM e7other o WHERE EXISTS (SELECT 1 FROM t WHERE t.ssn = '***')) x`, want: "n=3"},
		{name: "maskTrueValueNeverMatches", sql: `SELECT count(*) AS n FROM (WITH t AS (SELECT ssn FROM e7emp) SELECT id FROM e7other o WHERE EXISTS (SELECT 1 FROM t WHERE t.ssn = 'true-ssn-01')) x`, want: "n=0"},
		{name: "maskInSetInNestedBlock", sql: `SELECT count(*) AS n FROM (WITH t AS (SELECT acct FROM e7emp) SELECT id FROM e7other WHERE 0 IN (SELECT acct FROM t)) x`, want: "n=3"},
		{name: "maskReadPerRunOnce", sql: `WITH s AS (SELECT ssn, random() AS r FROM e7emp WHERE id = 1) SELECT count(DISTINCT (SELECT r + e.id * 0 FROM s)) AS n, max((SELECT ssn FROM s WHERE e.id > 0)) AS k FROM e7emp e`, maskCol: "k", want: "k=***|n=1"},
		{name: "balNestedScalar", sql: `SELECT x.k FROM (WITH t AS (SELECT bal FROM e7bal) SELECT (SELECT max(bal) FROM t) AS k) x`, want: "k=0"},
		{name: "deniedNestedScalar", sql: `SELECT x.k FROM (WITH t AS (SELECT salary FROM e7emp) SELECT (SELECT max(salary) FROM t) AS k) x`, denied: true},
		{name: "deniedCorrelatedBody", sql: `SELECT e.id, (SELECT max(salary) FROM e7emp i WHERE i.id = e.id) AS k FROM e7other e`, denied: true},
		{name: "deniedShadowed", sql: `WITH t AS (SELECT 1 AS salary) SELECT x.k FROM (WITH t AS (SELECT salary FROM e7emp) SELECT (SELECT max(salary) FROM t) AS k) x`, denied: true},
	}
	var dump strings.Builder
	answered := 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if err != nil {
				fmt.Fprintf(&dump, "%s\t%s\tERR %s\n", cell.name, door.name, strings.Join(strings.Fields(err.Error()), " "))
				// A refusal is a disposition this gate accepts; the claim is
				// that nothing POLICED reaches a client.
				continue
			}
			rendered := ci3PolicyRows(got)
			fmt.Fprintf(&dump, "%s\t%s\t%s\n", cell.name, door.name, rendered)
			if cell.denied {
				t.Errorf("%s / %s: a reference to a DENIED column answered %s\n  %s", cell.name, door.name, rendered, cell.sql)
				continue
			}
			answered++
			for _, c := range got.cols {
				if strings.EqualFold(c, "salary") {
					t.Errorf("%s / %s: the DENIED column %q is in the published list\n  %s", cell.name, door.name, c, cell.sql)
				}
			}
			for _, row := range got.rows {
				for c, v := range row {
					for _, bad := range leaks {
						if strings.Contains(v, bad) {
							t.Errorf("%s / %s: %s=%s is a policed value reaching the client\n  %s", cell.name, door.name, c, v, cell.sql)
						}
					}
				}
				if cell.maskCol != "" && !strings.Contains(row[cell.maskCol], pmMaskSSN) {
					t.Errorf("%s / %s: %s=%q is not the mask's reading %q\n  %s", cell.name, door.name, cell.maskCol, row[cell.maskCol], pmMaskSSN, cell.sql)
				}
			}
			if cell.maskCol != "" && len(got.rows) == 0 {
				t.Errorf("%s / %s: no row where the mask's reading is one\n  %s", cell.name, door.name, cell.sql)
			}
			if cell.want != "" && rendered != cell.want {
				t.Errorf("%s / %s: %s, want the masked column's %s\n  %s", cell.name, door.name, rendered, cell.want, cell.sql)
			}
		}
	}
	if p := os.Getenv("CI3P_DUMP"); p != "" {
		_ = os.WriteFile(p, []byte(dump.String()), 0o644)
	}
	if answered < 8*len(rig.doors) {
		t.Fatalf("%d (cell, door) pairs answered over %d doors, want at least %d: this gate's leak and mask "+
			"assertions must be capable of failing", answered, len(rig.doors), 8*len(rig.doors))
	}
	t.Logf("%d of %d (cell, door) pairs answered over %d doors; the rest refused",
		answered, len(cells)*len(rig.doors), len(rig.doors))
}

// ci3PolicyRows renders a result as "col=v|col=v" per row, rows sorted and
// joined by "; ".
func ci3PolicyRows(r pmResult) string {
	rows := make([]string, 0, len(r.rows))
	for _, row := range r.rows {
		keys := make([]string, 0, len(row))
		for k := range row {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		f := make([]string, len(keys))
		for i, k := range keys {
			f[i] = k + "=" + row[k]
		}
		rows = append(rows, strings.Join(f, "|"))
	}
	sort.Strings(rows)
	return strings.Join(rows, "; ")
}
