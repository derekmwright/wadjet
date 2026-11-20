// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
)

// TestAStatementIsDecidedUnderOneStateAcrossItsPlans is ADR-0050 §1
// Acceptance 8 for section 2: a policy reloaded between the veto and L
// ("vetoed"), and between L and D ("dag-planning"), does not change what the
// statement answers — tightened, loosened, a mask removed, a reload that fails
// to bind — on the stage DAG and on a statement that plans and runs L and then
// goes on to D. A cell whose next statement answers the same under the reload
// is no evidence and is skipped; at least one cell must be tested.
func TestAStatementIsDecidedUnderOneStateAcrossItsPlans(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	f := newPinFixture(t)
	authn, authz := f.provider.Authenticator(), f.provider.Authorizer()
	// Every local result bails out over the budget: L is planned and run,
	// then the statement goes on to D.
	fall := f.fast
	fall.localResultBudgetOverride = 1
	accepted := func() { f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, true, true, true, false)) }
	reloads := map[string]func(){
		"tightened":     func() { f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, true, true, false, false)) },
		"tightened_t":   func() { f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(false, false, false, true, false)) },
		"loosened":      func() { f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, true, false, true, false)) },
		"mask_removed":  func() { f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, false, true, true, false)) },
		"fails_to_bind": func() { f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, false, false, true, true)) },
	}
	statements := []struct{ name, sql string }{
		{"plain", "SELECT id, owner FROM pin_t"},
		{"decorrelated_in", "SELECT id, owner FROM pin_t WHERE id IN (SELECT id FROM pin_u)"},
	}
	arms := []struct {
		name  string
		c     *Coordinator
		steps []string
	}{
		{"dag", f.dag, []string{"vetoed", "dag-planning"}},
		{"l_then_d", fall, []string{"vetoed", "dag-planning"}},
	}
	moved, tested := 0, 0
	for _, st := range statements {
		for _, arm := range arms {
			accepted()
			want := pinAnswer(arm.c, f.ctx, st.sql)
			if strings.HasPrefix(want, "ERROR") || want == "" {
				t.Fatalf("%s/%s: the answer under the accepted state is %q", st.name, arm.name, want)
			}
			for rname, reload := range reloads {
				reload()
				next := pinAnswer(arm.c, f.ctx, st.sql)
				accepted()
				if next == want {
					continue
				}
				for _, step := range arm.steps {
					var got string
					fired := withStepReload(step, reload, func() { got = pinAnswer(arm.c, f.ctx, st.sql) })
					accepted()
					if !fired {
						t.Fatalf("%s/%s: step %q never ran", st.name, arm.name, step)
					}
					tested++
					if got != want {
						moved++
						t.Errorf("%s/%s: a %s reload at %q changed the statement's answer\n   accepted: %s\n   got:      %s",
							st.name, arm.name, rname, step, want, got)
					}
				}
			}
		}
	}
	if tested == 0 {
		t.Fatal("no reload changed a next statement's answer: the grid tests nothing")
	}
	t.Logf("%d cells, %d decided under another state", tested, moved)
}

// TestEveryStatementRefusesAnotherIncarnationAcrossItsPlans is ADR-0050 §1
// Acceptance 8 for section 3: inc_t dropped and re-created between the veto
// and L ("vetoed") and between L and D ("dag-planning"), with a provider
// enabled, disabled and absent, refuses 40001 and never answers rows of two
// incarnations; the next statement answers as before.
func TestEveryStatementRefusesAnotherIncarnationAcrossItsPlans(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	sqls := []string{
		"SELECT * FROM inc_t",
		"SELECT id, owner FROM inc_t",
		"SELECT a.id, a.owner, b.id AS bid FROM inc_t a JOIN inc_u b ON a.id = b.id",
		"SELECT id FROM inc_u WHERE id IN (SELECT id FROM inc_t WHERE owner <> 'o-2')",
	}
	for _, mode := range incProviderModes {
		t.Run(mode, func(t *testing.T) {
			f := newIncFixtureMode(t, nil, mode)
			fall := tmdCoordinator(t, context.Background(), f.infra, func(c *Config) { c.LocalFastPathBytes = DefaultLocalFastPathBytes })
			fall.localResultBudgetOverride = 1
			if f.provider != nil {
				if err := fall.SetAuthProvider(f.provider); err != nil {
					t.Fatal(err)
				}
			}
			for i, sql := range sqls {
				for _, arm := range []struct {
					name string
					c    *Coordinator
					step string
				}{
					{"fastpath", f.fast, "vetoed"}, {"dag", f.dag, "vetoed"},
					{"l_then_d", fall, "vetoed"}, {"l_then_d", fall, "dag-planning"},
				} {
					t.Run(arm.name+"/"+arm.step+"/"+string(rune('a'+i)), func(t *testing.T) {
						want := pinAnswer(arm.c, f.ctx, sql)
						if strings.HasPrefix(want, "ERROR") || want == "" {
							t.Fatalf("%s: the answer with nothing changed is %q", sql, want)
						}
						var got string
						fired := atStep(arm.step, func() { f.recreateAs(t, incSchemaC) }, func() { got = pinAnswer(arm.c, f.ctx, sql) })
						f.restoreA(t)
						if !fired {
							t.Fatalf("%s: step %q never ran", sql, arm.step)
						}
						if got != incRefusal {
							t.Fatalf("%s racing a re-create of inc_t answered\n   nothing changed: %s\n   got:             %s", sql, want, got)
						}
						if again := pinAnswer(arm.c, f.ctx, sql); again != want {
							t.Errorf("%s: after the refusal the statement answers %q, want %q", sql, again, want)
						}
					})
				}
			}
		})
	}
}
