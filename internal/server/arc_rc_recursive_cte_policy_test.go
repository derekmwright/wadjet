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

// A RECURSIVE CTE OVER A POLICED RELATION PUBLISHES ONLY THE POLICY'S READING,
// on every door (arc RC).
//
// Arc RC rebuilt the recursive CTE's materialization: the seed and every
// iteration of the recursive term are now planned and run as their own
// pipelines, columnar, and the result is held in a spill-backed collector the
// references replay. A materialization is exactly where a policy can be
// stepped around — a pipeline built outside the statement's security
// projection reads the stored column — so this gate runs the shapes the
// rebuild changed over e7emp (ssn and acct masked, salary denied) and e7bal
// (bal masked) on every door the census stands up, and asserts the census's
// own rule: no true value of a masked column in any cell, no denied column in
// any list, and a predicate over a masked column sees the MASK.
//
// The DAG doors cannot run a recursive CTE at all (#1042) and refuse; a
// refusal is a disposition this gate accepts. The count of (cell, door) pairs
// that ANSWERED is asserted, so the leak test cannot pass vacuously — and it
// was vacuous at 16b924d1, because every recursive CTE under a policy was
// refused there: the policy layer read the CTE reference's NAME as a table and
// default-denied it (`permission denied for table "r"`).
func TestArcRCARecursiveCTEOverAPolicedRelationNeverPublishesAPolicedValue(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	cells := []struct {
		name, sql string
		// want, when set, is the one row an answering door must produce: the
		// reading of the MASK, not of the stored value.
		want map[string]string
	}{
		{name: "masked text through seed and term",
			sql: `WITH RECURSIVE r(id, s) AS (SELECT id, ssn FROM e7emp WHERE id = 1 ` +
				`UNION ALL SELECT e.id, e.ssn FROM e7emp e JOIN r ON e.id = r.id + 1) SELECT id, s FROM r`},
		{name: "masked number accumulated across iterations",
			sql: `WITH RECURSIVE r(id, a) AS (SELECT id, acct FROM e7emp WHERE id = 1 ` +
				`UNION ALL SELECT e.id, r.a + e.acct FROM e7emp e JOIN r ON e.id = r.id + 1) SELECT id, a FROM r`},
		{name: "masked number carried by the working table",
			sql: `WITH RECURSIVE r(id, b, k) AS (SELECT id, bal, 1 FROM e7bal ` +
				`UNION ALL SELECT id, b, k + 1 FROM r WHERE k < 3) SELECT id, b FROM r`},
		{name: "a predicate on the true value in the seed and the term",
			sql: `WITH RECURSIVE r(id) AS (SELECT id FROM e7emp WHERE ssn = 'true-ssn-01' ` +
				`UNION ALL SELECT e.id FROM e7emp e JOIN r ON e.id = r.id + 1 WHERE e.ssn = 'true-ssn-02') ` +
				`SELECT COUNT(*) AS c FROM r`,
			want: map[string]string{"c": "0"}},
		{name: "a predicate on the masked number's sign in the term",
			sql: `WITH RECURSIVE r(id, k) AS (SELECT id, 1 FROM e7bal WHERE id = 1 ` +
				`UNION ALL SELECT e.id, r.k + 1 FROM e7bal e JOIN r ON e.id = r.id + 1 WHERE e.bal > 0) ` +
				`SELECT COUNT(*) AS c FROM r`,
			want: map[string]string{"c": "1"}},
		{name: "the denied column in the recursive term",
			sql: `WITH RECURSIVE r(id, s) AS (SELECT id, amt FROM e7emp WHERE id = 1 ` +
				`UNION ALL SELECT e.id, e.salary FROM e7emp e JOIN r ON e.id = r.id + 1) SELECT id, s FROM r`},
		{name: "a star over the masked relation in the seed",
			sql: `WITH RECURSIVE r AS (SELECT * FROM e7bal WHERE id = 1 ` +
				`UNION ALL SELECT e.* FROM e7bal e JOIN r ON e.id = r.id + 1) SELECT * FROM r`},
	}
	answered := 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if err != nil {
				continue
			}
			answered++
			for _, c := range got.cols {
				if strings.EqualFold(c, "salary") {
					t.Errorf("%s / %s: the DENIED column %q is in the published list\n  %s",
						cell.name, door.name, c, cell.sql)
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
				}
			}
			if cell.want != nil {
				if len(got.rows) != 1 {
					t.Errorf("%s / %s: %d rows, want one\n  %s", cell.name, door.name, len(got.rows), cell.sql)
					continue
				}
				for c, v := range cell.want {
					if got.rows[0][c] != v {
						t.Errorf("%s / %s: %s=%q, want %q — the predicate read the stored value, not the mask\n  %s",
							cell.name, door.name, c, got.rows[0][c], v, cell.sql)
					}
				}
			}
		}
	}
	if answered == 0 {
		t.Fatalf("no (cell, door) pair answered: this gate's leak test cannot fail, "+
			"and %d recursive shapes over policed relations were refused on every door", len(cells))
	}
	t.Logf("%d of %d (cell, door) pairs answered; the rest refused",
		answered, len(cells)*len(rig.doors))
}

// …AND A RELATION THE IDENTITY MAY NOT READ IS STILL REFUSED THROUGH ONE.
//
// The CTE reference stopped being policed as a table (it is the closure of
// its arms, not a relation), so the arms are where access is decided. Under a
// policy that grants e7emp and nothing else, a recursive CTE whose seed or
// recursive term reads e7other is 42501 on every door that plans it, and one
// that reads only e7emp answers — the control that the refusal is about the
// relation and not about recursion.
func TestArcRCARecursiveCTEArmReadingAnUnreadableRelationIsRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "e7-emp-only", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{{
			ID: "analyst-emp", EffectStr: "allow", Priority: 10,
			Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
			Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: pmTable}},
			Actions:   []auth.Action{auth.ActionRead},
			Obligations: []auth.Obligation{
				{Type: "mask_column", Target: "ssn", Value: "'" + pmMaskSSN + "'"},
			},
		}},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "analyst-key", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, evaluator)
	rig := pmRigUpWith(t, ctx, provider)

	refused := []struct{ name, sql string }{
		{"the seed reads it", `WITH RECURSIVE r(id, k) AS (SELECT id, 1 FROM e7other ` +
			`UNION ALL SELECT id, k + 1 FROM r WHERE k < 2) SELECT id FROM r`},
		{"the recursive term reads it", `WITH RECURSIVE r(id, k) AS (SELECT id, 1 FROM e7emp WHERE id = 1 ` +
			`UNION ALL SELECT o.id, r.k + 1 FROM e7other o JOIN r ON o.id = r.id + 1) SELECT id FROM r`},
		{"a nested recursive CTE reads it", `SELECT q.id FROM (WITH RECURSIVE r(id, k) AS (SELECT id, 1 FROM e7other ` +
			`UNION ALL SELECT id, k + 1 FROM r WHERE k < 2) SELECT id FROM r) q`},
	}
	for _, cell := range refused {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if err == nil {
				t.Errorf("%s / %s: a relation the identity may not read ANSWERED %v\n  %s",
					cell.name, door.name, got.rows, cell.sql)
				continue
			}
			if !strings.Contains(err.Error(), `permission denied for table "e7other"`) &&
				sqlerr.StateOf(err) != "42501" {
				// A DAG door may refuse first for its own reason (#1042);
				// what it may never do is answer.
				if !strings.Contains(err.Error(), "has no dependencies and no ScanFiles") {
					t.Errorf("%s / %s: want 42501 naming e7other, got %v\n  %s",
						cell.name, door.name, err, cell.sql)
				}
			}
		}
	}
	answered := 0
	control := `WITH RECURSIVE r(id, s) AS (SELECT id, ssn FROM e7emp WHERE id = 1 ` +
		`UNION ALL SELECT e.id, e.ssn FROM e7emp e JOIN r ON e.id = r.id + 1) SELECT id, s FROM r`
	for _, door := range rig.doors {
		got, err := door.run(t, "analyst-key", control)
		if err != nil {
			continue
		}
		answered++
		for _, row := range got.rows {
			if strings.HasPrefix(row["s"], "true-") {
				t.Errorf("control / %s: s=%s is the stored value", door.name, row["s"])
			}
		}
	}
	if answered == 0 {
		t.Fatalf("the control answered on no door: the refusals above prove nothing about the relation")
	}
}
