package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
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
// identity's meet in one place, and a policy can only NARROW: an obligation
// naming a bigger number does not widen `query_limits:`.
func TestTheTighterOfTheTwoCeilingsWins(t *testing.T) {
	if testing.Short() {
		t.Skip("stands up embedded NATS and three workers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	// A policy whose ceiling is far ABOVE anything this fixture reaches.
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "f5-wide", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{{
			ID: "wide", EffectStr: "allow", Priority: 10,
			Subjects:    []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "wide"}},
			Actions:     []auth.Action{auth.ActionRead},
			Obligations: []auth.Obligation{{Type: "query_limit", Value: "1000000000"}},
		}},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "wide-key", Name: "wide", Role: "wide"}},
		Roles:   []auth.RoleConfig{{Name: "wide", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, evaluator)

	rig := pmRigUpWith(t, ctx, provider)
	for _, door := range rig.doors {
		t.Run(door.name, func(t *testing.T) {
			if _, err := door.run(t, "wide-key", `SELECT id FROM e7emp`); err != nil {
				t.Errorf("a query_limit obligation ABOVE the query's cost refused it: %v", err)
			}
		})
	}
}
