package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// A scalar expression subquery reads a relation through the physical planner's
// OWN plan, and that plan asks the same table decision every other relation
// asks (#945, ADR-0034 item 3).
//
// `auth.EnforcePlanPolicies` polices what `policedRelations` finds in the
// LOGICAL PLAN. A scalar subquery in the SELECT list is not in it: it is SQL
// TEXT inside an expression, planned separately and later by
// `physical.buildSubqueryPipelineFor` (and by `emitScalarProducerStagesTyped`
// on the DAG). Neither asked the context lookup, so
// `SELECT (SELECT MAX(id) FROM other)` returned the value on every door, under
// BOTH provider shapes, for an identity whose role does not list `other` and
// for one an ABAC policy denies it to.
//
// The fix is at the site that turns the text into a plan, not at a walk of the
// text — which is why the spelling is irrelevant here and the table below
// varies it anyway: a SELECT-list item, an alias, a WHERE, a CASE, a CTE body
// and a correlated comparison all arrive at the same builder.

// sec5DeniedShapes are the spellings that reach `e7other` only through an
// expression subquery. Every one of them must refuse; the `IN` spelling is the
// CONTROL, the shape SEC1 already closed through the same lookup.
var sec5DeniedShapes = []struct{ name, sql string }{
	{"select_list", "SELECT (SELECT MAX(id) FROM " + pmOther + ") AS m"},
	{"select_list_beside_a_permitted_scan",
		"SELECT id, (SELECT MAX(id) FROM " + pmOther + ") AS m FROM " + pmTable},
	{"where", "SELECT id FROM " + pmTable + " WHERE id = (SELECT MAX(id) FROM " + pmOther + ")"},
	{"case", "SELECT CASE WHEN (SELECT MAX(id) FROM " + pmOther + ") > 0 THEN 1 ELSE 0 END AS m"},
	{"cte_body", "WITH c AS (SELECT (SELECT MAX(id) FROM " + pmOther +
		") AS m) SELECT m FROM c"},
	{"correlated", "SELECT t.id FROM " + pmTable + " t WHERE t.id = (SELECT MAX(o.id) FROM " +
		pmOther + " o WHERE o.id = t.id)"},
	{"in_subquery_control", "SELECT id FROM " + pmTable + " WHERE id IN (SELECT id FROM " +
		pmOther + ")"},
}

// sec5LegacyProvider is the `roles:`-only shape: `reader` lists e7emp and
// e7bal, and NOT e7other. No evaluator at all.
func sec5LegacyProvider(t *testing.T) *auth.Provider {
	t.Helper()
	authn, authz, err := auth.Build(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "reader-key", Name: "reader", Role: "reader"},
			{Key: "wide-key", Name: "wide", Role: "wide"},
		},
		Roles: []auth.RoleConfig{
			{Name: "reader", Tables: []string{pmTable, pmBal}, Allow: []string{"read"}},
			{Name: "wide", Tables: []string{"*"}, Allow: []string{"read"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p := auth.NewProvider(authn, authz, nil, nil)
	if p.Evaluator() != nil {
		t.Fatal("test setup: this provider must have NO ABAC evaluator")
	}
	return p
}

// sec5ABACProvider is the evaluator shape: a broad allow with an explicit deny
// on e7other, the shape a roles-to-ABAC migration leaves behind.
func sec5ABACProvider(t *testing.T) *auth.Provider {
	t.Helper()
	authn, authz, err := auth.Build(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "reader-key", Name: "reader", Role: "reader"},
			{Key: "wide-key", Name: "wide", Role: "wide"},
		},
		Roles: []auth.RoleConfig{
			{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "wide", Tables: []string{"*"}, Allow: []string{"read"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "sec5", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{ID: "broad-allow", EffectStr: "allow", Priority: 100,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "in",
					Value: []any{"reader", "wide"}}},
				Actions: []auth.Action{auth.ActionRead}},
			{ID: "secret-internal-rule-name", EffectStr: "deny", Priority: 10,
				Subjects:    []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "reader"}},
				Actions:     []auth.Action{auth.ActionRead},
				Description: "an operator note the refused caller has no business reading",
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq",
					Value: pmOther}}},
		},
	}}))
	return p
}

// TestAScalarSubqueryAsksTheTableDecisionOnEveryDoor is the door census for
// #945: eight doors (embedded single and spilled, the DAG and the shuffled
// DAG, pgwire on both, HTTP on both) × two provider shapes × seven spellings,
// from BOTH sides — the denied identity is refused with the shared decision's
// own text and no rows, and an identity that MAY read the relation still gets
// the value on every one of them.
func TestAScalarSubqueryAsksTheTableDecisionOnEveryDoor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	for _, shape := range []struct {
		name     string
		provider func(*testing.T) *auth.Provider
	}{
		{"legacy", sec5LegacyProvider},
		{"abac", sec5ABACProvider},
	} {
		t.Run(shape.name, func(t *testing.T) {
			rig := pmRigUpWith(t, ctx, shape.provider(t))
			want := fmt.Sprintf("permission denied for table %q", pmOther)
			for _, door := range rig.doors {
				for _, tc := range sec5DeniedShapes {
					t.Run(door.name+"/"+tc.name, func(t *testing.T) {
						res, err := door.run(t, "reader-key", tc.sql)
						if err == nil {
							t.Fatalf("the relation the identity may not read was SERVED: %s\n  %v",
								tc.sql, res.rows)
						}
						// The shared decision's own text, verbatim: the
						// relation and nothing else. Not the rule that denied,
						// not its description, and not a site-local prefix
						// naming where the check fired (ADR-0034 item 6).
						if !strings.Contains(err.Error(), want) {
							t.Errorf("refusal does not carry the shared decision's text\n"+
								"  sql:  %s\n  want: %s\n  got:  %v", tc.sql, want, err)
						}
						for _, leak := range []string{"secret-internal-rule-name",
							"denied by rule", "an operator note",
							"subquery could not be executed"} {
							if strings.Contains(err.Error(), leak) {
								t.Errorf("refusal discloses %q: %v", leak, err)
							}
						}
						if len(res.rows) != 0 {
							t.Errorf("a refused statement still produced %d rows", len(res.rows))
						}
					})
				}
				// The other side of the boundary: an identity that MAY read
				// e7other reads it through every one of those spellings.
				for _, tc := range sec5DeniedShapes {
					t.Run(door.name+"/allowed/"+tc.name, func(t *testing.T) {
						if _, err := door.run(t, "wide-key", tc.sql); err != nil {
							t.Fatalf("an identity that MAY read the relation was refused: %s\n  %v",
								tc.sql, err)
						}
					})
				}
			}
		})
	}
}

// TestADeniedScalarSubqueryRefusesBeforeAnyStageIsDispatched — the DAG arm's
// half of the boundary claim.
//
// The DAG plans a deferred scalar subquery into PRODUCER STAGES
// (`emitScalarProducerStagesTyped`), which is a second place SQL text becomes
// a plan after enforcement ran. The refusal has to happen there, at stage
// emission, and not when a worker opens the file: every stage's output
// materializes under `queries/<id>/` in the object store, so an empty listing
// is the evidence that nothing was dispatched.
func TestADeniedScalarSubqueryRefusesBeforeAnyStageIsDispatched(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUpWith(t, ctx, sec5ABACProvider(t))

	before, err := rig.store.List(ctx, "test", objstore.ListOptions{Prefix: "queries/"})
	if err != nil {
		t.Fatal(err)
	}
	for _, door := range rig.doors {
		if !strings.Contains(door.name, "dag") {
			continue
		}
		for _, tc := range sec5DeniedShapes {
			if _, qerr := door.run(t, "reader-key", tc.sql); qerr == nil {
				t.Fatalf("%s: the denied relation was served: %s", door.name, tc.sql)
			}
		}
	}
	after, err := rig.store.List(ctx, "test", objstore.ListOptions{Prefix: "queries/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Errorf("a refused statement wrote %d stage outputs to the store; "+
			"the refusal must precede dispatch", len(after)-len(before))
	}
}

// TestAScalarSubqueryCarriesTheObligationsToo — the same seam, the other two
// things a policy does to a relation.
//
// The pass that was missing did not only skip the ACCESS decision. A ROW
// FILTER bound to a relation reached ONLY from a subquery restricted nothing —
// `SELECT (SELECT MAX(id) FROM t)` answered the unfiltered maximum where
// `SELECT MAX(id) FROM t` answered the filtered one, so moving the constant
// reads the hidden rows off the answer. And a MASKED relation reached only
// from a subquery could not be answered at all: no projection was ever
// injected, so the plan-order invariant refused a query it should have
// answered with the mask.
func TestAScalarSubqueryCarriesTheObligationsToo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	t.Run("a row filter reaches the subquery's own plan", func(t *testing.T) {
		authn, authz, err := auth.Build(auth.Config{
			Enabled: true,
			APIKeys: []auth.APIKeyDef{{Key: "clerk-key", Name: "clerk", Role: "clerk"}},
			Roles:   []auth.RoleConfig{{Name: "clerk", Tables: []string{"*"}, Allow: []string{"read"}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		p := auth.NewProvider(authn, authz, nil, nil)
		p.UpdateWithEvaluator(authn, authz, nil, auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
			Name: "sec5-rf", Version: 1, Enabled: true,
			Rules: []auth.PolicyRule{
				{ID: "clerk-emp", EffectStr: "allow", Priority: 10,
					Subjects:    []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "clerk"}},
					Resources:   []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: pmTable}},
					Actions:     []auth.Action{auth.ActionRead},
					Obligations: []auth.Obligation{{Type: "row_filter", Value: "id < 3"}}},
				{ID: "clerk-rest", EffectStr: "allow", Priority: 10,
					Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "clerk"}},
					Resources: []auth.Condition{{Attribute: "resource.name", Op: "neq", Value: pmTable}},
					Actions:   []auth.Action{auth.ActionRead}},
			},
		}}))
		db := pmEmbeddedDB(t, ctx, 0)
		if err := db.SetAuthProvider(p); err != nil {
			t.Fatal(err)
		}
		id, err := p.Authenticator().AuthenticateToken("clerk-key")
		if err != nil {
			t.Fatal(err)
		}
		idCtx := auth.ContextWithIdentity(ctx, id)
		// The direct read is the reference: the filter keeps ids 1 and 2.
		direct, err := db.Query(idCtx, "SELECT MAX(id) AS m FROM "+pmTable)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprint(direct.Rows[0]["m"]); got != "2" {
			t.Fatalf("test setup: the filtered direct read answered %s, want 2", got)
		}
		for _, sql := range []string{
			"SELECT (SELECT MAX(id) FROM " + pmTable + ") AS m",
			"SELECT o.id, (SELECT MAX(id) FROM " + pmTable + ") AS m FROM " + pmOther + " o",
		} {
			res, qerr := db.Query(idCtx, sql)
			if qerr != nil {
				t.Fatalf("%s: %v", sql, qerr)
			}
			for _, row := range res.Rows {
				if got := fmt.Sprint(row["m"]); got != "2" {
					t.Errorf("the row filter did not reach the subquery's plan:\n  %s\n"+
						"  m = %s, want 2 (12 is the unfiltered maximum)", sql, got)
				}
			}
		}
	})

	t.Run("a mask reaches a relation named only in a subquery", func(t *testing.T) {
		p := pmProvider(t)
		db := pmEmbeddedDB(t, ctx, 0)
		if err := db.SetAuthProvider(p); err != nil {
			t.Fatal(err)
		}
		id, err := p.Authenticator().AuthenticateToken("analyst-key")
		if err != nil {
			t.Fatal(err)
		}
		idCtx := auth.ContextWithIdentity(ctx, id)
		for _, sql := range []string{
			"SELECT (SELECT MAX(ssn) FROM " + pmTable + ") AS m",
			"SELECT o.id, (SELECT MAX(ssn) FROM " + pmTable + ") AS m FROM " + pmOther + " o",
		} {
			res, qerr := db.Query(idCtx, sql)
			if qerr != nil {
				t.Fatalf("a masked relation reached from a subquery refused instead of "+
					"answering the mask:\n  %s\n  %v", sql, qerr)
			}
			for _, row := range res.Rows {
				if got := fmt.Sprint(row["m"]); got != pmMaskSSN {
					t.Errorf("%s: m = %s, want the mask %s", sql, got, pmMaskSSN)
				}
			}
		}
	})
}

// TestAScalarSubqueryIsUnchangedWithoutAuth — the other half of every
// security change: with no provider nothing is enforced and nothing moves.
func TestAScalarSubqueryIsUnchangedWithoutAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	db := pmEmbeddedDB(t, ctx, 0)
	for _, tc := range sec5DeniedShapes {
		if _, err := db.Query(ctx, tc.sql); err != nil {
			t.Errorf("a database with no auth provider refused %s: %v", tc.name, err)
		}
	}
}

// sec5PgRig is the embedded door and a pgwire server over one provider.
func sec5PgRig(t *testing.T, ctx context.Context, p *auth.Provider) (*wadjet.DB, *auth.Provider, string) {
	t.Helper()
	db := pmEmbeddedDB(t, ctx, 0)
	if err := db.SetAuthProvider(p); err != nil {
		t.Fatal(err)
	}
	pg := pgwire.NewServer(db, pgwire.Config{AuthProvider: p}, nil)
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Shutdown)
	return db, p, pg.Addr()
}

// TestADeniedScalarSubqueryLeavesPgwireAs42501 — the class, on the wire.
//
// The refusal is raised while the outer pipeline RUNS (a scalar subquery's
// relations are decided when its own plan is built, and that is at evaluation
// time on the single-process arm), and a run-time failure used to be wrapped
// twice on the way out: `executing query: scalar subquery could not be
// executed: …`. Both wrappers hid the decision behind the site, and the second
// one dropped nothing but still made the same operation carry two different
// messages depending on which arm answered.
func TestADeniedScalarSubqueryLeavesPgwireAs42501(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	p := sec5ABACProvider(t)
	_, _, addr := sec5PgRig(t, ctx, p)
	conn, cerr := pgconn.Connect(ctx, fmt.Sprintf(
		"postgres://wadjet:reader-key@%s/wadjet?sslmode=disable", addr))
	if cerr != nil {
		t.Fatal(cerr)
	}
	defer conn.Close(ctx)

	want := fmt.Sprintf("permission denied for table %q", pmOther)
	for _, tc := range sec5DeniedShapes {
		res := conn.ExecParams(ctx, tc.sql, nil, nil, nil, nil).Read()
		if res.Err == nil {
			t.Errorf("%s: the denied relation was served", tc.sql)
			continue
		}
		var pgErr *pgconn.PgError
		if !errors.As(res.Err, &pgErr) {
			t.Errorf("%s: refusal is not a PostgreSQL error: %v", tc.sql, res.Err)
			continue
		}
		if pgErr.Code != "42501" {
			t.Errorf("%s: SQLSTATE %s, want 42501", tc.sql, pgErr.Code)
		}
		if pgErr.Message != want {
			t.Errorf("%s: message = %q, want exactly %q", tc.sql, pgErr.Message, want)
		}
	}
}
