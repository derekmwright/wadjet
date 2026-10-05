// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A VOLATILE CTE'S ONE EVALUATION READS THE POLICED RELATION (arc CM, #1531;
// COMMON's masking gate).
//
// The single-process planner now evaluates a volatile WITH item's body once
// into a collector and serves every reference from it (ADR-0021 §2d) — a
// materialization over the relation the body reads. That evaluation must read
// the MASKED relation: the body is planned through the same subquery path,
// under the statement's identity, as every other block. Each cell reads a
// volatile body over e7emp (ssn masked, acct masked, salary denied) or e7bal
// (bal masked) at least twice, in the positions the rule serves: a FROM
// self-join, two scalar subqueries, a nested block, a set-operation root and
// a sampled body. On every door: no true value reaches a client, no denied
// column is published, every value of a masked output column is the MASK's,
// and the number of (cell, door) pairs that answered is asserted non-zero.
func TestArcCMVolatileCTEOverAPolicedRelationReadsTheMask(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	cells := []struct {
		name, sql string
		masked    map[string]string // output column → the mask every value must read
	}{
		{"from-selfjoin", `WITH c AS (SELECT id, ssn, acct, random() AS r FROM e7emp) ` +
			`SELECT a.ssn AS s, b.acct AS a FROM c a JOIN c b ON a.id = b.id WHERE a.r = b.r`,
			map[string]string{"s": pmMaskSSN, "a": pmMaskAcct}},
		{"two-scalars", `WITH c AS (SELECT id, ssn, random() AS r FROM e7emp) ` +
			`SELECT (SELECT max(ssn) FROM c) AS hi, (SELECT min(ssn) FROM c) AS lo`,
			map[string]string{"hi": pmMaskSSN, "lo": pmMaskSSN}},
		{"where-scalars", `WITH c AS (SELECT id, ssn, random() AS r FROM e7emp) ` +
			`SELECT ssn AS s FROM e7emp WHERE (SELECT max(r) FROM c) = (SELECT max(r) FROM c)`,
			map[string]string{"s": pmMaskSSN}},
		{"nested-block", `SELECT x.s FROM (WITH c AS (SELECT id, ssn, random() AS r FROM e7emp) ` +
			`SELECT a.ssn AS s FROM c a JOIN c b ON a.id = b.id WHERE a.r = b.r) x`,
			map[string]string{"s": pmMaskSSN}},
		{"setop-root", `WITH c AS (SELECT id, ssn, random() AS r FROM e7emp) ` +
			`SELECT ssn AS s FROM c UNION ALL SELECT ssn AS s FROM c`,
			map[string]string{"s": pmMaskSSN}},
		{"sampled", `WITH c AS (SELECT id, ssn FROM e7emp TABLESAMPLE BERNOULLI (100)) ` +
			`SELECT (SELECT max(ssn) FROM c) AS hi, (SELECT count(*) FROM c) AS n`,
			map[string]string{"hi": pmMaskSSN}},
		{"bal-two-scalars", `WITH c AS (SELECT id, bal, random() AS r FROM e7bal) ` +
			`SELECT (SELECT max(bal) FROM c) AS hi, (SELECT min(bal) FROM c) AS lo`,
			map[string]string{"hi": "0", "lo": "0"}},
		// A denied column read through the one evaluation refuses or omits.
		{"denied", `WITH c AS (SELECT id, salary, random() AS r FROM e7emp) ` +
			`SELECT (SELECT max(salary) FROM c) AS m, (SELECT min(salary) FROM c) AS n`, nil},
	}
	answered := 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if err != nil {
				continue // a refusal: nothing policed reaches the client
			}
			answered++
			for _, c := range got.cols {
				if strings.EqualFold(c, "salary") {
					t.Errorf("%s / %s: the DENIED column %q is published\n  %s", cell.name, door.name, c, cell.sql)
				}
			}
			for _, row := range got.rows {
				for c, v := range row {
					for _, bad := range leaks {
						if strings.Contains(v, bad) {
							t.Errorf("%s / %s: %s=%s is a policed value reaching the client\n  %s",
								cell.name, door.name, c, v, cell.sql)
						}
					}
					if want, ok := cell.masked[c]; ok && v != want {
						t.Errorf("%s / %s: %s=%q, want the mask %q\n  %s", cell.name, door.name, c, v, want, cell.sql)
					}
				}
			}
		}
	}
	if answered == 0 {
		t.Fatalf("no (cell, door) pair answered: the leak test cannot fail")
	}
	t.Logf("%d of %d (cell, door) pairs answered; the rest refused", answered, len(cells)*len(rig.doors))
}
