package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
)

// A `query_limit` obligation was accepted by the policy loader and then
// dropped by the evaluator — `abac_eval.go`'s switch had `case "query_limit":`
// with a comment and no body — so an operator who wrote one had no ceiling at
// all. docs/security.md's obligation table said "**Not enforced**" beside it,
// which is a fact about the code that should never have been a fact about a
// security control (arc E7's deferral 2 / this arc's E7-D1).
//
// It is enforced now, through the SAME cost guard `query_limits:` uses: the
// obligation is read into a *config.QueryLimits on the table decision,
// EnforcePlanPolicies narrows the statement's ceiling with every policed
// relation's, and Planner.enforceQueryLimits takes the tighter of that and the
// deployment's. One enforcement point, so every door and every arm carries it.

// pmLimitProvider is pmProvider's shape with query_limit obligations instead of
// masks: one role capped on rows, one on bytes, one on files, one uncapped.
func pmLimitProvider(t *testing.T) *auth.Provider {
	t.Helper()
	capRule := func(id, role, target, value string) auth.PolicyRule {
		return auth.PolicyRule{
			ID: id, EffectStr: "allow", Priority: 10,
			Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: role}},
			Actions:  []auth.Action{auth.ActionRead},
			Obligations: []auth.Obligation{
				{Type: "query_limit", Target: target, Value: value},
			},
		}
	}
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "f5-limits", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			capRule("rows", "rowcap", "", "1"),
			capRule("bytes", "bytecap", auth.QueryLimitBytes, "1"),
			capRule("files", "filecap", auth.QueryLimitFiles, "1"),
			{
				ID: "open", EffectStr: "allow", Priority: 10,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "open"}},
				Actions:  []auth.Action{auth.ActionRead},
			},
		},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "rowcap-key", Name: "rowcap", Role: "rowcap"},
			{Key: "bytecap-key", Name: "bytecap", Role: "bytecap"},
			{Key: "filecap-key", Name: "filecap", Role: "filecap"},
			{Key: "open-key", Name: "open", Role: "open"},
		},
		Roles: []auth.RoleConfig{
			{Name: "rowcap", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "bytecap", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "filecap", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "open", Tables: []string{"*"}, Allow: []string{"read"}},
		},
	})
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, evaluator)
	return p
}

// TestAQueryLimitObligationIsEnforcedOnEveryDoor is E7-D1's gate.
func TestAQueryLimitObligationIsEnforcedOnEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("stands up embedded NATS and three workers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUpWith(t, ctx, pmLimitProvider(t))

	for _, tc := range []struct {
		name, key, sql, wants string
	}{
		// The fixture has twelve rows in one file per table.
		{name: "rows", key: "rowcap-key", sql: `SELECT id FROM e7emp`,
			wants: "exceeding limit of 1 rows"},
		{name: "bytes", key: "bytecap-key", sql: `SELECT id FROM e7emp`,
			wants: "exceeding limit of 1B"},
		{name: "files", key: "filecap-key",
			sql:   `SELECT a.id FROM e7emp a JOIN e7other b ON a.id = b.id`,
			wants: "exceeding limit of 1"},
	} {
		for _, door := range rig.doors {
			t.Run(tc.name+"/"+door.name, func(t *testing.T) {
				_, err := door.run(t, tc.key, tc.sql)
				if err == nil {
					t.Fatalf("%q answered for an identity whose policy caps it; "+
						"a query_limit obligation that is not enforced is a ceiling "+
						"an operator believes in and does not have", tc.sql)
				}
				if !strings.Contains(err.Error(), tc.wants) {
					t.Errorf("%q refused with %v\n  want a cost-guard refusal saying %q",
						tc.sql, err, tc.wants)
				}
			})
		}
	}

	// The control, on every door: the same statements for an identity whose
	// policy sets no ceiling. A guard that refused these would be a
	// right→loud move, which is the failure mode a ceiling on the wrong
	// context key would produce.
	for _, sql := range []string{
		`SELECT id FROM e7emp`,
		`SELECT a.id FROM e7emp a JOIN e7other b ON a.id = b.id`,
	} {
		for _, door := range rig.doors {
			t.Run("uncapped/"+door.name+"/"+sql[:12], func(t *testing.T) {
				if _, err := door.run(t, "open-key", sql); err != nil {
					t.Errorf("an identity with no query_limit obligation was refused %q: %v",
						sql, err)
				}
			})
		}
	}
}

// TestAQueryLimitObligationThatCannotBeEnforcedRefusesToLoad — the other half
// of the doctrine (ADR-0033 decision 4, the #802 config-load rule): a control
// that cannot be applied does not load, because the alternative is an operator
// who believes there is a ceiling.
func TestAQueryLimitObligationThatCannotBeEnforcedRefusesToLoad(t *testing.T) {
	pol := func(ob auth.Obligation) []auth.AccessControlPolicy {
		return []auth.AccessControlPolicy{{
			Name: "p", Version: 1, Enabled: true,
			Rules: []auth.PolicyRule{{
				ID: "r", EffectStr: "allow",
				Actions:     []auth.Action{auth.ActionRead},
				Obligations: []auth.Obligation{ob},
			}},
		}}
	}
	for _, tc := range []struct {
		name string
		ob   auth.Obligation
		want string
	}{
		{"no_value", auth.Obligation{Type: "query_limit"}, "carries no value"},
		{"not_a_number", auth.Obligation{Type: "query_limit", Value: "a million"},
			"is not a number"},
		{"zero", auth.Obligation{Type: "query_limit", Value: "0"}, "would refuse every query"},
		{"negative", auth.Obligation{Type: "query_limit", Value: "-5"}, "would refuse every query"},
		{"unknown_target", auth.Obligation{Type: "query_limit", Target: "max_result_rows", Value: "10"},
			"not a query cost ceiling"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := auth.ValidateABACPolicies(pol(tc.ob))
			if err == nil {
				t.Fatalf("a query_limit obligation spelled %+v loaded; it cannot be enforced, "+
					"so it must refuse", tc.ob)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("refusal is %v\n  want a message containing %q", err, tc.want)
			}
		})
	}

	// And the spellings that DO load — including the documented one, whose
	// target is empty and whose value is a row count.
	for _, ob := range []auth.Obligation{
		{Type: "query_limit", Value: "1000000"},
		{Type: "query_limit", Target: "max_scan_rows", Value: "1"},
		{Type: "query_limit", Target: "MAX_SCAN_BYTES", Value: "1073741824"},
		{Type: "query_limit", Target: "max_scan_files", Value: "200"},
	} {
		if err := auth.ValidateABACPolicies(pol(ob)); err != nil {
			t.Errorf("a valid query_limit obligation %+v was refused: %v", ob, err)
		}
	}
}

// TestTheTighterOfTheTwoCeilingsWins — the deployment's guard and the
// identity's meet in ONE place, and a policy can only NARROW.
//
// The round-1 review found the first version of this test installing no
// deployment `query_limits:` at all, so it showed only that a wide obligation
// does not refuse a cheap query — the arc's headline sentence ("an obligation
// naming a bigger number than the deployment allows does not widen the
// deployment's guard") had no fixture. It does now: one DB with a deployment
// ceiling, four identities, and the four corners of the merge.
func TestTheTighterOfTheTwoCeilingsWins(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	rule := func(role, value string) auth.PolicyRule {
		r := auth.PolicyRule{
			ID: role, EffectStr: "allow", Priority: 10,
			Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: role}},
			Actions:  []auth.Action{auth.ActionRead},
		}
		if value != "" {
			r.Obligations = []auth.Obligation{{Type: "query_limit", Value: value}}
		}
		return r
	}
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "f5-two-ceilings", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			// Far above anything this twelve-row fixture reaches.
			rule("wide", "1000000000"),
			// Below it.
			rule("narrow", "1"),
			// No obligation at all: the deployment's guard is the only one.
			rule("none", ""),
		},
	}})
	keys := []auth.APIKeyDef{
		{Key: "wide-key", Name: "wide", Role: "wide"},
		{Key: "narrow-key", Name: "narrow", Role: "narrow"},
		{Key: "none-key", Name: "none", Role: "none"},
	}
	roles := []auth.RoleConfig{
		{Name: "wide", Tables: []string{"*"}, Allow: []string{"read"}},
		{Name: "narrow", Tables: []string{"*"}, Allow: []string{"read"}},
		{Name: "none", Tables: []string{"*"}, Allow: []string{"read"}},
	}
	authn, authz := auth.New(auth.Config{Enabled: true, APIKeys: keys, Roles: roles})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, evaluator)

	idCtx := func(key string) context.Context {
		id, err := provider.Authenticator().AuthenticateToken(key)
		if err != nil {
			t.Fatalf("authenticate %q: %v", key, err)
		}
		return auth.ContextWithIdentity(ctx, id)
	}

	// Two DBs over the same fixture: one with a deployment ceiling the fixture
	// exceeds, one with none. Four corners, and the pair on the same identity
	// is what shows which ceiling decided.
	const sql = `SELECT id FROM e7emp`
	for _, dep := range []struct {
		name   string
		limits *config.QueryLimits
		// refuses lists the identities this deployment refuses.
		refuses map[string]bool
	}{
		{name: "no_deployment_guard", limits: nil,
			// Only the policy can refuse, and only the narrow one does.
			refuses: map[string]bool{"narrow-key": true}},
		{name: "deployment_guard_of_one_row",
			limits: &config.QueryLimits{MaxScanRows: 1},
			// The deployment refuses everyone, INCLUDING the identity whose
			// obligation names a billion rows: a policy cannot widen.
			refuses: map[string]bool{"wide-key": true, "narrow-key": true, "none-key": true}},
	} {
		t.Run(dep.name, func(t *testing.T) {
			db := pmEmbeddedDB(t, ctx, 0)
			db.SetAuthProvider(provider)
			db.SetQueryLimits(dep.limits, nil)
			for _, key := range []string{"wide-key", "narrow-key", "none-key"} {
				t.Run(key, func(t *testing.T) {
					_, err := db.Query(idCtx(key), sql)
					if dep.refuses[key] {
						if err == nil {
							t.Fatalf("%s answered for %s; the tighter of the deployment's "+
								"ceiling and the identity's must apply", sql, key)
						}
						if !strings.Contains(err.Error(), "exceeding limit of 1 rows") {
							t.Errorf("%s refused with %v; want the cost guard's own refusal", key, err)
						}
						return
					}
					if err != nil {
						t.Errorf("%s was refused %s: %v — neither ceiling is below its cost",
							key, sql, err)
					}
				})
			}
		})
	}
}

// TestAPolicyCeilingCannotWidenTheDeploymentsGuard states the merge directly,
// without a query. tightestLimits lives in internal/planner/physical and has
// its own unit test there; this is the auth-side half — that
// NarrowQueryLimits keeps the SMALLER of every ceiling and mutates neither
// argument, which is what makes two policed relations in one statement safe.
func TestAPolicyCeilingCannotWidenTheDeploymentsGuard(t *testing.T) {
	tight := &config.QueryLimits{MaxScanRows: 50, MaxScanBytes: 100, MaxScanFiles: 2}
	wide := &config.QueryLimits{MaxScanRows: 1e9, MaxScanBytes: 1 << 40, MaxScanFiles: 9999}

	for _, tc := range []struct {
		name string
		a, b *config.QueryLimits
	}{
		{"tight_then_wide", tight, wide},
		{"wide_then_tight", wide, tight},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeA, beforeB := *tc.a, *tc.b
			got := auth.NarrowQueryLimits(tc.a, tc.b)
			if got.MaxScanRows != 50 || got.MaxScanBytes != 100 || got.MaxScanFiles != 2 {
				t.Errorf("narrowed to rows=%d bytes=%d files=%d, want 50/100/2 — the SMALLER "+
					"of every ceiling either side sets", got.MaxScanRows, got.MaxScanBytes,
					got.MaxScanFiles)
			}
			if *tc.a != beforeA || *tc.b != beforeB {
				t.Error("NarrowQueryLimits mutated an argument; a decision is cached per " +
					"statement and a mutated one leaks its ceiling to the next relation")
			}
		})
	}
	// A nil side contributes nothing and is not dereferenced.
	if got := auth.NarrowQueryLimits(nil, tight); got == nil || got.MaxScanRows != 50 {
		t.Errorf("NarrowQueryLimits(nil, tight) = %+v", got)
	}
	if got := auth.NarrowQueryLimits(tight, nil); got != tight {
		t.Errorf("NarrowQueryLimits(tight, nil) = %+v, want the first argument unchanged", got)
	}
}
