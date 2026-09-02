package auth

import (
	"testing"
	"time"
)

func testEvaluator() *PolicyEvaluator {
	return NewPolicyEvaluator([]AccessControlPolicy{
		{
			Name:    "clearance-policy",
			Version: 1,
			Enabled: true,
			Rules: []PolicyRule{
				{
					ID:        "top-secret-full-access",
					EffectStr: "allow",
					Priority:  10,
					Subjects: []Condition{
						{Attribute: "subject.clearance", Op: "eq", Value: "TOP_SECRET"},
					},
					Actions: []Action{ActionRead, ActionWrite},
				},
				{
					ID:        "secret-read-with-mask",
					EffectStr: "allow",
					Priority:  20,
					Subjects: []Condition{
						{Attribute: "subject.clearance", Op: "in", Value: []any{"SECRET", "TOP_SECRET"}},
					},
					Resources: []Condition{
						{Attribute: "resource.name", Op: "eq", Value: "intelligence"},
					},
					Actions: []Action{ActionRead},
					Obligations: []Obligation{
						{Type: "mask_column", Target: "source", MaskFunc: "redact"},
						{Type: "row_filter", Value: "classification IN ('UNCLASSIFIED', 'SECRET')"},
					},
				},
				{
					ID:        "unclassified-restricted",
					EffectStr: "allow",
					Priority:  30,
					Subjects: []Condition{
						{Attribute: "subject.clearance", Op: "eq", Value: "UNCLASSIFIED"},
					},
					Resources: []Condition{
						{Attribute: "resource.name", Op: "eq", Value: "intelligence"},
					},
					Actions: []Action{ActionRead},
					Obligations: []Obligation{
						{Type: "deny_column", Target: "source"},
						{Type: "deny_column", Target: "content"},
						{Type: "row_filter", Value: "classification = 'UNCLASSIFIED'"},
					},
				},
				{
					ID:        "deny-write-non-admin",
					EffectStr: "deny",
					Priority:  5,
					Subjects: []Condition{
						{Attribute: "subject.role", Op: "neq", Value: "admin"},
					},
					Actions: []Action{ActionWrite},
				},
			},
		},
		{
			Name:    "department-policy",
			Version: 1,
			Enabled: true,
			Rules: []PolicyRule{
				{
					ID:        "cyber-team-events",
					EffectStr: "allow",
					Priority:  50,
					Subjects: []Condition{
						{Attribute: "subject.department", Op: "eq", Value: "cyber"},
					},
					Resources: []Condition{
						{Attribute: "resource.name", Op: "eq", Value: "events"},
					},
					Actions: []Action{ActionRead},
				},
			},
		},
	})
}

func TestABACDenyOverrides(t *testing.T) {
	pe := testEvaluator()

	// Non-admin trying to write — should be denied by priority-5 deny rule
	subject := Subject{
		Attributes: Attributes{
			"role":      "analyst",
			"clearance": "TOP_SECRET",
		},
	}
	resource := Resource{Type: "table", Name: "intelligence"}
	env := Environment{}

	d := pe.Evaluate(subject, resource, ActionWrite, env)
	if d.Allowed {
		t.Fatal("non-admin write should be denied")
	}
	if d.MatchedRule != "deny-write-non-admin" {
		t.Fatalf("expected deny-write-non-admin, got %q", d.MatchedRule)
	}
}

func TestABACTopSecretFullAccess(t *testing.T) {
	pe := testEvaluator()

	subject := Subject{
		Attributes: Attributes{
			"role":      "admin",
			"clearance": "TOP_SECRET",
		},
	}
	resource := Resource{Type: "table", Name: "intelligence"}

	// Admin with TOP_SECRET can write (deny rule doesn't match admin)
	d := pe.Evaluate(subject, resource, ActionWrite, Environment{})
	if !d.Allowed {
		t.Fatalf("admin TOP_SECRET write should be allowed: %s", d.Reason)
	}

	// Read should also work, with obligations from secret-read-with-mask
	d2 := pe.Evaluate(subject, resource, ActionRead, Environment{})
	if !d2.Allowed {
		t.Fatal("admin TOP_SECRET read should be allowed")
	}
}

func TestABACSecretWithObligations(t *testing.T) {
	pe := testEvaluator()

	subject := Subject{
		Attributes: Attributes{
			"role":      "analyst",
			"clearance": "SECRET",
		},
	}

	td := pe.EvaluateTableAccess(subject, "intelligence", ActionRead, Environment{})
	if !td.Allowed {
		t.Fatalf("SECRET clearance should allow read: %s", td.Reason)
	}

	// Should have mask_column and row_filter obligations
	if td.RowFilter == "" {
		t.Fatal("expected row filter for SECRET clearance")
	}
	if td.RowFilter != "classification IN ('UNCLASSIFIED', 'SECRET')" {
		t.Fatalf("unexpected row filter: %s", td.RowFilter)
	}

	hasMask := false
	for _, col := range td.Columns {
		if col.Column == "source" && col.MaskFunc == "redact" {
			hasMask = true
		}
	}
	if !hasMask {
		t.Fatal("expected source column to be masked")
	}
}

func TestABACUnclassifiedDeniedColumns(t *testing.T) {
	pe := testEvaluator()

	subject := Subject{
		Attributes: Attributes{
			"clearance": "UNCLASSIFIED",
		},
	}

	td := pe.EvaluateTableAccess(subject, "intelligence", ActionRead, Environment{})
	if !td.Allowed {
		t.Fatalf("UNCLASSIFIED should allow read with restrictions: %s", td.Reason)
	}

	if td.RowFilter != "classification = 'UNCLASSIFIED'" {
		t.Fatalf("expected UNCLASSIFIED row filter, got %q", td.RowFilter)
	}

	denied := make(map[string]bool)
	for _, col := range td.Columns {
		if !col.Allowed {
			denied[col.Column] = true
		}
	}
	if !denied["source"] {
		t.Fatal("source should be denied for UNCLASSIFIED")
	}
	if !denied["content"] {
		t.Fatal("content should be denied for UNCLASSIFIED")
	}
}

func TestABACDefaultDeny(t *testing.T) {
	pe := testEvaluator()

	// Unknown user with no matching attributes
	subject := Subject{
		Attributes: Attributes{"role": "unknown"},
	}
	resource := Resource{Type: "table", Name: "events"}

	d := pe.Evaluate(subject, resource, ActionRead, Environment{})
	if d.Allowed {
		t.Fatal("unknown user should be denied by default")
	}
}

func TestABACDepartmentAccess(t *testing.T) {
	pe := testEvaluator()

	subject := Subject{
		Attributes: Attributes{
			"department": "cyber",
			"role":       "analyst",
		},
	}
	resource := Resource{Type: "table", Name: "events"}

	d := pe.Evaluate(subject, resource, ActionRead, Environment{})
	if !d.Allowed {
		t.Fatalf("cyber department should access events: %s", d.Reason)
	}
}

func TestABACNilEvaluator(t *testing.T) {
	var pe *PolicyEvaluator
	d := pe.Evaluate(Subject{}, Resource{}, ActionRead, Environment{})
	if !d.Allowed {
		t.Fatal("nil evaluator should allow (no policy enforcement)")
	}
}

func TestABACDisabledPolicy(t *testing.T) {
	pe := NewPolicyEvaluator([]AccessControlPolicy{
		{
			Name:    "disabled",
			Enabled: false,
			Rules: []PolicyRule{
				{
					ID:        "should-not-match",
					EffectStr: "allow",
					Subjects:  []Condition{{Attribute: "subject.role", Op: "eq", Value: "reader"}},
					Actions:   []Action{ActionRead},
				},
			},
		},
	})

	subject := Subject{Attributes: Attributes{"role": "reader"}}
	d := pe.Evaluate(subject, Resource{Type: "table", Name: "t"}, ActionRead, Environment{})
	if d.Allowed {
		t.Fatal("disabled policy should not match")
	}
}

// --- Condition operator tests ---

func TestConditionEq(t *testing.T) {
	attrs := map[string]any{"subject.role": "admin"}
	if !matchCondition(Condition{Attribute: "subject.role", Op: "eq", Value: "admin"}, attrs) {
		t.Fatal("eq should match")
	}
	if matchCondition(Condition{Attribute: "subject.role", Op: "eq", Value: "reader"}, attrs) {
		t.Fatal("eq should not match different value")
	}
}

func TestConditionNeq(t *testing.T) {
	attrs := map[string]any{"subject.role": "admin"}
	if !matchCondition(Condition{Attribute: "subject.role", Op: "neq", Value: "reader"}, attrs) {
		t.Fatal("neq should match different value")
	}
	if matchCondition(Condition{Attribute: "subject.role", Op: "neq", Value: "admin"}, attrs) {
		t.Fatal("neq should not match same value")
	}
}

func TestConditionIn(t *testing.T) {
	attrs := map[string]any{"subject.clearance": "SECRET"}
	if !matchCondition(Condition{Attribute: "subject.clearance", Op: "in", Value: []any{"SECRET", "TOP_SECRET"}}, attrs) {
		t.Fatal("in should match")
	}
	if matchCondition(Condition{Attribute: "subject.clearance", Op: "in", Value: []any{"UNCLASSIFIED"}}, attrs) {
		t.Fatal("in should not match")
	}
}

func TestConditionNotIn(t *testing.T) {
	attrs := map[string]any{"subject.clearance": "SECRET"}
	if !matchCondition(Condition{Attribute: "subject.clearance", Op: "not_in", Value: []any{"UNCLASSIFIED"}}, attrs) {
		t.Fatal("not_in should match when value not in set")
	}
	if matchCondition(Condition{Attribute: "subject.clearance", Op: "not_in", Value: []any{"SECRET", "TOP_SECRET"}}, attrs) {
		t.Fatal("not_in should not match when value in set")
	}
}

func TestConditionGtLt(t *testing.T) {
	attrs := map[string]any{"env.hour": 14}
	if !matchCondition(Condition{Attribute: "env.hour", Op: "gt", Value: 8}, attrs) {
		t.Fatal("14 > 8")
	}
	if !matchCondition(Condition{Attribute: "env.hour", Op: "lt", Value: 18}, attrs) {
		t.Fatal("14 < 18")
	}
	if matchCondition(Condition{Attribute: "env.hour", Op: "gt", Value: 20}, attrs) {
		t.Fatal("14 not > 20")
	}
}

func TestConditionExists(t *testing.T) {
	attrs := map[string]any{"subject.mfa": true}
	if !matchCondition(Condition{Attribute: "subject.mfa", Op: "exists"}, attrs) {
		t.Fatal("exists should match")
	}
	if matchCondition(Condition{Attribute: "subject.missing", Op: "exists"}, attrs) {
		t.Fatal("exists should not match missing key")
	}
}

func TestConditionContains(t *testing.T) {
	attrs := map[string]any{"subject.email": "alice@example.com"}
	if !matchCondition(Condition{Attribute: "subject.email", Op: "contains", Value: "@example.com"}, attrs) {
		t.Fatal("contains should match")
	}
}

func TestConditionRegex(t *testing.T) {
	attrs := map[string]any{"subject.email": "alice@example.com"}
	if !matchCondition(Condition{Attribute: "subject.email", Op: "regex", Value: `^[a-z]+@example\.com$`}, attrs) {
		t.Fatal("regex should match")
	}
	if matchCondition(Condition{Attribute: "subject.email", Op: "regex", Value: `^bob@`}, attrs) {
		t.Fatal("regex should not match")
	}
}

// --- Environment conditions ---

func TestABACEnvironmentConditions(t *testing.T) {
	pe := NewPolicyEvaluator([]AccessControlPolicy{
		{
			Name:    "business-hours",
			Enabled: true,
			Rules: []PolicyRule{
				{
					ID:        "allow-business-hours",
					EffectStr: "allow",
					Priority:  10,
					Subjects:  []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
					Actions:   []Action{ActionRead},
					Environment: []Condition{
						{Attribute: "env.hour", Op: "gte", Value: 8},
						{Attribute: "env.hour", Op: "lt", Value: 18},
					},
				},
			},
		},
	})

	subject := Subject{Attributes: Attributes{"role": "analyst"}}
	resource := Resource{Type: "table", Name: "events"}

	// 10am — within business hours
	env := Environment{Time: time.Date(2026, 3, 17, 10, 0, 0, 0, time.UTC)}
	d := pe.Evaluate(subject, resource, ActionRead, env)
	if !d.Allowed {
		t.Fatalf("should be allowed during business hours: %s", d.Reason)
	}

	// 2am — outside business hours
	env2 := Environment{Time: time.Date(2026, 3, 17, 2, 0, 0, 0, time.UTC)}
	d2 := pe.Evaluate(subject, resource, ActionRead, env2)
	if d2.Allowed {
		t.Fatal("should be denied outside business hours")
	}
}

// --- Multiple row filters AND'd ---

func TestABACMultipleRowFilters(t *testing.T) {
	pe := NewPolicyEvaluator([]AccessControlPolicy{
		{
			Name:    "row-filters",
			Enabled: true,
			Rules: []PolicyRule{
				{
					ID:        "dept-filter",
					EffectStr: "allow",
					Priority:  10,
					Subjects:  []Condition{{Attribute: "subject.department", Op: "eq", Value: "engineering"}},
					Actions:   []Action{ActionRead},
					Obligations: []Obligation{
						{Type: "row_filter", Value: "department = 'engineering'"},
					},
				},
				{
					ID:        "region-filter",
					EffectStr: "allow",
					Priority:  20,
					Subjects:  []Condition{{Attribute: "subject.region", Op: "eq", Value: "us-east"}},
					Actions:   []Action{ActionRead},
					Obligations: []Obligation{
						{Type: "row_filter", Value: "region = 'us-east'"},
					},
				},
			},
		},
	})

	subject := Subject{
		Attributes: Attributes{
			"department": "engineering",
			"region":     "us-east",
		},
	}

	td := pe.EvaluateTableAccess(subject, "events", ActionRead, Environment{})
	if !td.Allowed {
		t.Fatal("should be allowed")
	}
	// Both filters should be AND'd
	expected := "(department = 'engineering') AND (region = 'us-east')"
	if td.RowFilter != expected {
		t.Fatalf("expected combined filter %q, got %q", expected, td.RowFilter)
	}
}

// --- Identity.ToSubject ---

func TestIdentityToSubject(t *testing.T) {
	id := &Identity{
		Name:   "alice",
		Role:   "analyst",
		Method: "jwt",
		Attributes: Attributes{
			"clearance":  "SECRET",
			"department": "cyber",
		},
	}

	s := id.ToSubject()
	if s.Attributes.GetString("role") != "analyst" {
		t.Fatal("expected role attribute")
	}
	if s.Attributes.GetString("clearance") != "SECRET" {
		t.Fatal("expected clearance attribute")
	}
	if s.Attributes.GetString("department") != "cyber" {
		t.Fatal("expected department attribute")
	}
	if s.Attributes.GetString("method") != "jwt" {
		t.Fatal("expected method attribute")
	}
}

// --- RBAC Migration ---

func TestMigrateRBACToABAC(t *testing.T) {
	roles := []RoleConfig{
		{Name: "admin", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}},
		{Name: "reader", Tables: []string{"events", "metrics"}, Allow: []string{"read"}},
	}
	cellPolicies := []PolicyConfig{
		{
			Table:     "users",
			Role:      "reader",
			Columns:   map[string]string{"email": "mask", "ssn": "deny"},
			RowFilter: "department = 'public'",
		},
	}

	policies, err := MigrateRBACToABAC(roles, cellPolicies)
	if err != nil {
		t.Fatalf("MigrateRBACToABAC: %v", err)
	}

	pe := NewPolicyEvaluator(policies)

	// Admin should access anything
	admin := Subject{Attributes: Attributes{"role": "admin"}}
	d := pe.Evaluate(admin, Resource{Type: "table", Name: "anything"}, ActionRead, Environment{})
	if !d.Allowed {
		t.Fatalf("admin should be allowed: %s", d.Reason)
	}

	// Reader should access events
	reader := Subject{Attributes: Attributes{"role": "reader"}}
	d2 := pe.Evaluate(reader, Resource{Type: "table", Name: "events"}, ActionRead, Environment{})
	if !d2.Allowed {
		t.Fatalf("reader should access events: %s", d2.Reason)
	}

	// Reader should not access secrets table
	d3 := pe.Evaluate(reader, Resource{Type: "table", Name: "secrets"}, ActionRead, Environment{})
	if d3.Allowed {
		t.Fatal("reader should not access secrets")
	}

	// Reader accessing users should get obligations (mask email, deny ssn, row filter)
	td := pe.EvaluateTableAccess(reader, "users", ActionRead, Environment{})
	if !td.Allowed {
		t.Fatalf("reader should access users (with obligations): %s", td.Reason)
	}
	if td.RowFilter != "department = 'public'" {
		t.Fatalf("expected row filter, got %q", td.RowFilter)
	}

	hasMaskEmail := false
	hasDenySSN := false
	for _, col := range td.Columns {
		if col.Column == "email" && col.MaskFunc != "" {
			hasMaskEmail = true
		}
		if col.Column == "ssn" && !col.Allowed {
			hasDenySSN = true
		}
	}
	if !hasMaskEmail {
		t.Fatal("expected email mask obligation")
	}
	if !hasDenySSN {
		t.Fatal("expected ssn deny obligation")
	}
}

// --- Provider with ABAC ---

func TestProviderEvaluator(t *testing.T) {
	cfg := Config{
		Enabled: true,
		APIKeys: []APIKeyDef{
			{Key: "key1", Name: "test", Role: "reader"},
		},
		Roles: []RoleConfig{
			{Name: "reader", Tables: []string{"events"}, Allow: []string{"read"}},
		},
	}
	authn, authz := New(cfg)
	p := NewProvider(authn, authz, nil, nil)

	// No evaluator initially
	if p.Evaluator() != nil {
		t.Fatal("expected nil evaluator initially")
	}

	// UpdateFromConfig with roles should auto-migrate
	if err := p.UpdateFromConfig(cfg, nil); err != nil {
		t.Fatalf("UpdateFromConfig: %v", err)
	}
	if p.Evaluator() == nil {
		t.Fatal("expected evaluator after UpdateFromConfig with roles")
	}

	// Verify migrated policy works
	subject := Subject{Attributes: Attributes{"role": "reader"}}
	d := p.Evaluator().Evaluate(subject, Resource{Type: "table", Name: "events"}, ActionRead, Environment{})
	if !d.Allowed {
		t.Fatalf("reader should access events: %s", d.Reason)
	}
}
