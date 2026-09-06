package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
)

// TestConfigABACConditionsKeepTheirNamespace.
//
// The evaluator's attribute map is keyed by the NAMESPACED spelling —
// `subject.role`, `resource.name`, `env.hour` — and that is the spelling
// docs/security.md and docs/configuration.md tell the operator to write. The
// config conversion used to slice the namespace off before storing the
// condition, so every documented condition looked up an attribute that does
// not exist. A conditional allow then failed closed; a conditional DENY beside
// a broad allow failed OPEN — the deny disappeared and the grant won (#930).
//
// The assertion is a DECISION, not a field: the stored spelling only matters
// because of what it does to the answer.
func TestConfigABACConditionsKeepTheirNamespace(t *testing.T) {
	policies, err := buildABACPolicies([]config.ABACPolicy{{
		Name: "review",
		Rules: []config.ABACRule{
			{Effect: "allow"}, // broad fallback
			{
				Effect: "deny",
				Conditions: []config.ABACCondition{
					{Attribute: "subject.role", Operator: "eq", Value: "contractor"},
					{Attribute: "resource.name", Operator: "eq", Value: "classified_events"},
				},
			},
		},
	}})
	if err != nil {
		t.Fatalf("buildABACPolicies: %v", err)
	}

	if got := policies[0].Rules[1].Subjects[0].Attribute; got != "subject.role" {
		t.Errorf("config conversion rewrote subject.role to %q", got)
	}
	if got := policies[0].Rules[1].Resources[0].Attribute; got != "resource.name" {
		t.Errorf("config conversion rewrote resource.name to %q", got)
	}
	d := auth.NewPolicyEvaluator(policies).Evaluate(
		auth.Subject{Attributes: auth.Attributes{"role": "contractor"}},
		auth.Resource{Type: "table", Name: "classified_events"},
		auth.ActionRead, auth.Environment{})
	if d.Allowed {
		t.Fatal("documented contractor deny did not match; unconditional allow won")
	}
}

// The environment namespace is the one the plan path could never reach before
// #933, and it is converted by the same switch.
func TestConfigABACEnvironmentConditionsKeepTheirNamespace(t *testing.T) {
	policies, err := buildABACPolicies([]config.ABACPolicy{{
		Name: "after-hours",
		Rules: []config.ABACRule{
			{Effect: "allow"},
			{Effect: "deny", Conditions: []config.ABACCondition{
				{Attribute: "env.source_ip", Operator: "eq", Value: "10.0.0.9"},
			}},
		},
	}})
	if err != nil {
		t.Fatalf("buildABACPolicies: %v", err)
	}
	if got := policies[0].Rules[1].Environment[0].Attribute; got != "env.source_ip" {
		t.Fatalf("config conversion rewrote env.source_ip to %q", got)
	}
	d := auth.NewPolicyEvaluator(policies).Evaluate(
		auth.Subject{Attributes: auth.Attributes{"role": "analyst"}},
		auth.Resource{Type: "table", Name: "events"}, auth.ActionRead,
		auth.Environment{SourceIP: "10.0.0.9", Protocol: "http"})
	if d.Allowed {
		t.Fatal("documented env.source_ip deny did not match; unconditional allow won")
	}
}

// TestABACPolicyFromYAMLFileDenies drives a REAL config file through the real
// loader to a decision — the level the issue asked for, because a unit test on
// the converter alone cannot see a YAML key that never reaches it.
func TestABACPolicyFromYAMLFileDenies(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wadjet.yaml")
	yaml := `
auth:
  enabled: true
  api_keys:
    - key: contractor-key
      name: contractor
      role: contractor
  roles:
    - name: contractor
      tables: ["*"]
      allow: ["read"]
  abac_policies:
    - name: broad-allow
      priority: 100
      rules:
        - effect: allow
          conditions:
            - attribute: subject.role
              operator: eq
              value: "contractor"
    - name: deny-contractors-classified
      priority: 5
      rules:
        - effect: deny
          conditions:
            - attribute: subject.role
              operator: eq
              value: "contractor"
            - attribute: resource.name
              operator: eq
              value: "classified_events"
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	policies, err := buildABACPolicies(cfg.Auth.ABACPolicies)
	if err != nil {
		t.Fatalf("buildABACPolicies: %v", err)
	}
	ev := auth.NewPolicyEvaluator(policies)
	subject := auth.Subject{Attributes: auth.Attributes{"role": "contractor"}}

	denied := ev.EvaluateTableAccess(subject, "classified_events", auth.ActionRead, auth.Environment{})
	if denied.Allowed {
		t.Fatalf("the YAML deny did not reach the decision: %s", denied.Reason)
	}
	allowed := ev.EvaluateTableAccess(subject, "events", auth.ActionRead, auth.Environment{})
	if !allowed.Allowed {
		t.Fatalf("the broad allow stopped working: %s", allowed.Reason)
	}
}

// An attribute with no namespace is a LOAD ERROR, not a subject attribute by
// default: `attribute: hour` reads a subject attribute nothing populates, so
// the rule silently never matches — which is the shape of the defect itself.
func TestConfigABACRefusesAnUnnamespacedAttribute(t *testing.T) {
	for _, attribute := range []string{"role", "hour", "name", "", "subject", "subjectrole"} {
		_, err := buildABACPolicies([]config.ABACPolicy{{
			Name: "typo",
			Rules: []config.ABACRule{{
				Effect:     "deny",
				Conditions: []config.ABACCondition{{Attribute: attribute, Operator: "eq", Value: "x"}},
			}},
		}})
		if err == nil {
			t.Errorf("attribute %q was accepted; want a load refusal", attribute)
			continue
		}
		if !strings.Contains(err.Error(), "namespace") {
			t.Errorf("attribute %q refused with %q; want the message to name the namespace",
				attribute, err.Error())
		}
	}
}

// A `subject.<custom>` attribute is legal — JWT claims and mTLS certificate
// fields arrive under names the product does not know.
func TestConfigABACAcceptsCustomSubjectAttributes(t *testing.T) {
	policies, err := buildABACPolicies([]config.ABACPolicy{{
		Name: "clearance",
		Rules: []config.ABACRule{{
			Effect: "allow",
			Conditions: []config.ABACCondition{
				{Attribute: "subject.clearance", Operator: "in", Value: "TOP_SECRET,SECRET"},
				{Attribute: "resource.name", Operator: "eq", Value: "classified_events"},
				{Attribute: "env.hour", Operator: "gte", Value: "9"},
			},
		}},
	}})
	if err != nil {
		t.Fatalf("a documented policy was refused: %v", err)
	}
	r := policies[0].Rules[0]
	if len(r.Subjects) != 1 || len(r.Resources) != 1 || len(r.Environment) != 1 {
		t.Fatalf("conditions grouped as subjects=%d resources=%d env=%d, want 1/1/1",
			len(r.Subjects), len(r.Resources), len(r.Environment))
	}
}

// The RBAC auto-migration writes the same namespaced spelling, so a
// `roles:`-only deployment and an `abac_policies:` one are read by one rule.
func TestMigratedRBACUsesTheNamespacedSpelling(t *testing.T) {
	migrated, err := auth.MigrateRBACToABAC(
		[]auth.RoleConfig{{Name: "reader", Tables: []string{"events"}, Allow: []string{"read"}}},
		[]auth.PolicyConfig{{Table: "events", Role: "reader", Columns: map[string]string{"ssn": "mask"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, pol := range migrated {
		for _, rule := range pol.Rules {
			for _, c := range append(append([]auth.Condition{}, rule.Subjects...), rule.Resources...) {
				if !strings.HasPrefix(c.Attribute, "subject.") && !strings.HasPrefix(c.Attribute, "resource.") {
					t.Errorf("migrated policy %q rule %q emitted un-namespaced attribute %q",
						pol.Name, rule.ID, c.Attribute)
				}
			}
		}
	}
}
