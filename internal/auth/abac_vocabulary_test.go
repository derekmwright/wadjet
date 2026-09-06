package auth

import (
	"strings"
	"testing"
)

// vocabPolicy wraps one rule the way a config load hands it over.
func vocabPolicy(rule PolicyRule) []AccessControlPolicy {
	return []AccessControlPolicy{{Name: "review", Enabled: true, Rules: []PolicyRule{rule}}}
}

// TestPolicySchemaRejectsUnknownSecurityWords is the issue's gate, extended to
// every field that defines security behavior (#932).
//
// Each of these used to load clean. An unresolvable effect became an ALLOW
// that granted the action the operator wrote a deny for; an unknown obligation
// type was dropped and the column came back in plaintext; an unknown action,
// operator or attribute made its rule match nothing, and a rule that matches
// nothing is a grant beside a broad allow.
func TestPolicySchemaRejectsUnknownSecurityWords(t *testing.T) {
	for _, tc := range []struct {
		name string
		rule PolicyRule
		want string // a fragment the refusal must name
	}{
		{"misspelled effect", PolicyRule{
			ID: "deny-secret", EffectStr: "dney", Actions: []Action{ActionRead}}, "effect"},
		{"empty effect", PolicyRule{
			ID: "no-effect", Actions: []Action{ActionRead}}, "effect"},
		{"misspelled obligation type", PolicyRule{
			ID: "hide-secret", EffectStr: "allow", Actions: []Action{ActionRead},
			Obligations: []Obligation{{Type: "deny_colum", Target: "secret"}}}, "obligation type"},
		{"empty obligation type", PolicyRule{
			ID: "no-type", EffectStr: "allow", Actions: []Action{ActionRead},
			Obligations: []Obligation{{Target: "secret"}}}, "obligation type"},
		{"misspelled action", PolicyRule{
			ID: "bad-action", EffectStr: "deny", Actions: []Action{Action("raed")}}, "action"},
		{"misspelled operator", PolicyRule{
			ID: "bad-op", EffectStr: "deny", Actions: []Action{ActionRead},
			Subjects: []Condition{{Attribute: "subject.role", Op: "equals", Value: "x"}}}, "operator"},
		{"empty operator", PolicyRule{
			ID: "no-op", EffectStr: "deny", Actions: []Action{ActionRead},
			Subjects: []Condition{{Attribute: "subject.role", Value: "x"}}}, "operator"},
		{"unnamespaced attribute", PolicyRule{
			ID: "bad-ns", EffectStr: "deny", Actions: []Action{ActionRead},
			Subjects: []Condition{{Attribute: "role", Op: "eq", Value: "x"}}}, "namespace"},
		{"unknown resource attribute", PolicyRule{
			ID: "bad-resource", EffectStr: "deny", Actions: []Action{ActionRead},
			Resources: []Condition{{Attribute: "resource.owner", Op: "eq", Value: "x"}}}, "resource.name"},
		{"unknown environment attribute", PolicyRule{
			ID: "bad-env", EffectStr: "deny", Actions: []Action{ActionRead},
			Environment: []Condition{{Attribute: "env.weekday", Op: "eq", Value: "mon"}}}, "env.hour"},
		{"empty subject attribute name", PolicyRule{
			ID: "bare-ns", EffectStr: "deny", Actions: []Action{ActionRead},
			Subjects: []Condition{{Attribute: "subject.", Op: "exists"}}}, "subject attribute"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateABACPolicies(vocabPolicy(tc.rule))
			if err == nil {
				t.Fatal("policy loader accepted an unknown security keyword")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refusal %q does not name %q", err.Error(), tc.want)
			}
		})
	}
}

// The other side of the claim: every word the evaluator DOES implement loads.
// A refusal that swallows valid policies would take a working deployment down.
func TestPolicySchemaAcceptsTheWholeImplementedVocabulary(t *testing.T) {
	for _, effect := range []string{"allow", "deny", "ALLOW", "Deny", " allow "} {
		if err := ValidateABACPolicies(vocabPolicy(PolicyRule{
			ID: "e", EffectStr: effect, Actions: []Action{ActionRead},
		})); err != nil {
			t.Errorf("effect %q refused: %v", effect, err)
		}
	}
	for _, action := range []Action{ActionRead, ActionWrite, ActionAdmin, ActionCreate, ActionDrop, ActionDescribe} {
		if err := ValidateABACPolicies(vocabPolicy(PolicyRule{
			ID: "a", EffectStr: "allow", Actions: []Action{action},
		})); err != nil {
			t.Errorf("action %q refused: %v", action, err)
		}
	}
	for _, op := range []string{"eq", "neq", "in", "not_in", "gt", "lt", "gte", "lte",
		"contains", "regex", "exists", "not_exists"} {
		if err := ValidateABACPolicies(vocabPolicy(PolicyRule{
			ID: "o", EffectStr: "allow", Actions: []Action{ActionRead},
			Subjects: []Condition{{Attribute: "subject.role", Op: op, Value: "x"}},
		})); err != nil {
			t.Errorf("operator %q refused: %v", op, err)
		}
	}
	for _, attr := range []string{"subject.role", "subject.name", "subject.method",
		"subject.clearance", "subject.anything_a_jwt_carries",
		"resource.type", "resource.name",
		"env.time", "env.hour", "env.source_ip", "env.protocol"} {
		if err := ValidateABACPolicies(vocabPolicy(PolicyRule{
			ID: "c", EffectStr: "allow", Actions: []Action{ActionRead},
			Subjects: []Condition{{Attribute: attr, Op: "exists"}},
		})); err != nil {
			t.Errorf("attribute %q refused: %v", attr, err)
		}
	}
	for _, ob := range []Obligation{
		{Type: "deny_column", Target: "ssn"},
		{Type: "mask_column", Target: "ssn", Value: "'REDACTED'"},
		{Type: "mask_column", Target: "ssn", MaskFunc: "redact"},
		{Type: "row_filter", Value: "dept = 'eng'"},
		{Type: "query_limit", Target: "max_scan_rows", Value: "1000"},
	} {
		if err := ValidateABACPolicies(vocabPolicy(PolicyRule{
			ID: "ob", EffectStr: "allow", Actions: []Action{ActionRead},
			Obligations: []Obligation{ob},
		})); err != nil {
			t.Errorf("obligation %+v refused: %v", ob, err)
		}
	}
}

// The RUNTIME cell: a caller who never ran the validator still cannot turn a
// misspelled deny into a grant. `dney` resolves to DENY, not ALLOW.
func TestUnknownEffectResolvesToDenyNotAllow(t *testing.T) {
	for _, effect := range []string{"dney", "", "DENIED", "no"} {
		pe := NewPolicyEvaluator([]AccessControlPolicy{{
			Name: "review", Enabled: true,
			Rules: []PolicyRule{{
				ID: "deny-secret", EffectStr: effect,
				Subjects: []Condition{{Attribute: "subject.role", Op: "eq", Value: "contractor"}},
				Actions:  []Action{ActionRead},
			}},
		}})
		d := pe.Evaluate(
			Subject{Attributes: Attributes{"role": "contractor"}},
			Resource{Type: "table", Name: "classified"}, ActionRead, Environment{})
		if d.Allowed {
			t.Errorf("effect %q granted the action: %s", effect, d.Reason)
		}
	}
	// The word that means allow still means allow.
	pe := NewPolicyEvaluator([]AccessControlPolicy{{
		Name: "review", Enabled: true,
		Rules: []PolicyRule{{
			ID: "allow-read", EffectStr: "allow",
			Subjects: []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
			Actions:  []Action{ActionRead},
		}},
	}})
	if d := pe.Evaluate(Subject{Attributes: Attributes{"role": "analyst"}},
		Resource{Type: "table", Name: "events"}, ActionRead, Environment{}); !d.Allowed {
		t.Fatalf("a well-spelled allow stopped granting: %s", d.Reason)
	}
}

// The second runtime cell: an obligation the evaluator cannot apply REFUSES
// the table rather than returning an allow with the restriction missing.
func TestUnknownObligationRefusesTheTable(t *testing.T) {
	pe := NewPolicyEvaluator([]AccessControlPolicy{{
		Name: "review", Enabled: true,
		Rules: []PolicyRule{{
			ID: "hide-secret", EffectStr: "allow",
			Subjects:    []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
			Actions:     []Action{ActionRead},
			Obligations: []Obligation{{Type: "deny_colum", Target: "ssn"}},
		}},
	}})
	td := pe.EvaluateTableAccess(
		Subject{Attributes: Attributes{"role": "analyst"}}, "emp", ActionRead, Environment{})
	if td.Allowed {
		t.Fatal("an unknown obligation vanished; the column would be served in plaintext")
	}
	if !strings.Contains(td.Reason, "deny_colum") {
		t.Fatalf("refusal %q does not name the obligation that could not be enforced", td.Reason)
	}
}

// A hot reload carrying an unknown word refuses the WHOLE load and swaps
// nothing — the same contract an unreadable mask already had (#802).
func TestHotReloadRefusesAnUnknownSecurityWordAndKeepsTheRunningSet(t *testing.T) {
	good := []AccessControlPolicy{{
		Name: "good", Enabled: true,
		Rules: []PolicyRule{{
			ID: "allow-analyst", EffectStr: "allow",
			Subjects: []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
			Actions:  []Action{ActionRead},
		}},
	}}
	cfg := Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
		Roles:   []RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
	}
	p := NewProvider(nil, nil, nil, nil)
	if err := p.UpdateFromConfig(cfg, nil, good...); err != nil {
		t.Fatalf("the good set did not install: %v", err)
	}
	installed := p.Evaluator()

	bad := []AccessControlPolicy{{
		Name: "bad", Enabled: true,
		Rules: []PolicyRule{{ID: "typo", EffectStr: "dney", Actions: []Action{ActionRead}}},
	}}
	if err := p.UpdateFromConfig(cfg, nil, bad...); err == nil {
		t.Fatal("a policy set with an unknown effect was installed")
	}
	if p.Evaluator() != installed {
		t.Fatal("the refused set replaced the running one")
	}
	// The set that was running is still the set that decides.
	d := p.Evaluator().Evaluate(Subject{Attributes: Attributes{"role": "analyst"}},
		Resource{Type: "table", Name: "events"}, ActionRead, Environment{})
	if !d.Allowed {
		t.Fatalf("the surviving set stopped granting: %s", d.Reason)
	}
}

// The migration's own output must pass the vocabulary it is validated against
// — a `roles:`-only deployment loads through exactly this path.
func TestMigratedRBACPassesTheVocabulary(t *testing.T) {
	migrated, err := MigrateRBACToABAC(
		[]RoleConfig{
			{Name: "reader", Tables: []string{"events"}, Allow: []string{"read"}},
			{Name: "writer", Tables: []string{"*"}, Allow: []string{"write"}},
			{Name: "root", Tables: []string{"*"}, Allow: []string{"admin"}},
		},
		[]PolicyConfig{
			{Table: "events", Role: "reader", Columns: map[string]string{"ssn": "mask", "raw": "deny"},
				RowFilter: "dept = 'eng'"},
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateABACPolicies(migrated); err != nil {
		t.Fatalf("the RBAC auto-migration emits a policy set its own validator refuses: %v", err)
	}
}
