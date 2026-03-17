package auth

import (
	"testing"
	"time"
)

func TestMatchConditionNotExists(t *testing.T) {
	attrs := map[string]any{"subject.role": "admin"}
	if !matchCondition(Condition{Attribute: "subject.missing", Op: "not_exists"}, attrs) {
		t.Error("not_exists should match for missing key")
	}
	if matchCondition(Condition{Attribute: "subject.role", Op: "not_exists"}, attrs) {
		t.Error("not_exists should not match for present key")
	}
}

func TestMatchConditionGteLte(t *testing.T) {
	attrs := map[string]any{"env.hour": 14}
	if !matchCondition(Condition{Attribute: "env.hour", Op: "gte", Value: 14}, attrs) {
		t.Error("14 >= 14 should be true")
	}
	if !matchCondition(Condition{Attribute: "env.hour", Op: "lte", Value: 14}, attrs) {
		t.Error("14 <= 14 should be true")
	}
	if matchCondition(Condition{Attribute: "env.hour", Op: "gte", Value: 15}, attrs) {
		t.Error("14 >= 15 should be false")
	}
	if matchCondition(Condition{Attribute: "env.hour", Op: "lte", Value: 13}, attrs) {
		t.Error("14 <= 13 should be false")
	}
}

func TestMatchConditionUnknownOp(t *testing.T) {
	attrs := map[string]any{"x": "y"}
	if matchCondition(Condition{Attribute: "x", Op: "unknown_operator", Value: "y"}, attrs) {
		t.Error("unknown operator should return false")
	}
}

func TestMatchConditionRegexInvalid(t *testing.T) {
	attrs := map[string]any{"subject.email": "test@example.com"}
	// Invalid regex pattern should return false, not panic
	if matchCondition(Condition{Attribute: "subject.email", Op: "regex", Value: "[invalid"}, attrs) {
		t.Error("invalid regex should return false")
	}
}

func TestMatchConditionMissingAttribute(t *testing.T) {
	attrs := map[string]any{}
	// eq on missing attribute should be false
	if matchCondition(Condition{Attribute: "missing", Op: "eq", Value: "anything"}, attrs) {
		t.Error("eq on missing should be false")
	}
	// neq on missing attribute should be true (value does not exist, so not equal)
	if !matchCondition(Condition{Attribute: "missing", Op: "neq", Value: "anything"}, attrs) {
		t.Error("neq on missing should be true")
	}
	// not_in on missing attribute should be true
	if !matchCondition(Condition{Attribute: "missing", Op: "not_in", Value: []any{"x"}}, attrs) {
		t.Error("not_in on missing should be true")
	}
}

func TestCompareInStringSlice(t *testing.T) {
	// Test with []string (not []any)
	if !compareIn("admin", []string{"admin", "reader"}) {
		t.Error("should find admin in string slice")
	}
	if compareIn("unknown", []string{"admin", "reader"}) {
		t.Error("should not find unknown in string slice")
	}
}

func TestCompareInNoMatch(t *testing.T) {
	// Non-slice set should return false
	if compareIn("x", "not a slice") {
		t.Error("non-slice set should return false")
	}
}

func TestCompareOrdStringFallback(t *testing.T) {
	// Non-numeric values should fall back to string comparison
	result := compareOrd("banana", "apple")
	if result <= 0 {
		t.Error("banana > apple in string comparison")
	}
	result2 := compareOrd("apple", "banana")
	if result2 >= 0 {
		t.Error("apple < banana in string comparison")
	}
	result3 := compareOrd("same", "same")
	if result3 != 0 {
		t.Error("same == same should be 0")
	}
}

func TestToFloatVariousTypes(t *testing.T) {
	if toFloat(int32(42)) != 42 {
		t.Error("int32 conversion failed")
	}
	if toFloat(int64(100)) != 100 {
		t.Error("int64 conversion failed")
	}
	if toFloat(float32(3.14)) != float64(float32(3.14)) {
		t.Error("float32 conversion failed")
	}
	if toFloat(float64(2.71)) != 2.71 {
		t.Error("float64 conversion failed")
	}
	if toFloat("not a number") != 0 {
		t.Error("string should return 0")
	}
	if toFloat(int(7)) != 7 {
		t.Error("int conversion failed")
	}
}

func TestBuildAttrMapEnvFields(t *testing.T) {
	pe := NewPolicyEvaluator(nil)
	subject := Subject{Attributes: Attributes{"role": "admin"}}
	resource := Resource{Type: "table", Name: "events", Attributes: Attributes{"classification": "SECRET"}}
	env := Environment{
		Time:     time.Date(2026, 3, 17, 14, 30, 0, 0, time.UTC),
		SourceIP: "192.168.1.1",
		Protocol: "pgwire",
		Custom:   Attributes{"region": "us-east"},
	}

	attrs := pe.buildAttrMap(subject, resource, ActionRead, env)

	if attrs["subject.role"] != "admin" {
		t.Error("expected subject.role")
	}
	if attrs["resource.type"] != "table" {
		t.Error("expected resource.type")
	}
	if attrs["resource.name"] != "events" {
		t.Error("expected resource.name")
	}
	if attrs["resource.classification"] != "SECRET" {
		t.Error("expected resource.classification")
	}
	if attrs["action"] != "read" {
		t.Error("expected action")
	}
	if attrs["env.source_ip"] != "192.168.1.1" {
		t.Error("expected env.source_ip")
	}
	if attrs["env.protocol"] != "pgwire" {
		t.Error("expected env.protocol")
	}
	if attrs["env.region"] != "us-east" {
		t.Error("expected env.region")
	}
	if attrs["env.hour"] != 14 {
		t.Error("expected env.hour=14")
	}
}

func TestEvaluateMultipleDenyHighestPriority(t *testing.T) {
	pe := NewPolicyEvaluator([]AccessControlPolicy{
		{
			Name:    "multi-deny",
			Enabled: true,
			Rules: []PolicyRule{
				{
					ID:        "deny-low-priority",
					EffectStr: "deny",
					Priority:  100,
					Subjects:  []Condition{{Attribute: "subject.role", Op: "eq", Value: "intern"}},
					Actions:   []Action{ActionRead},
				},
				{
					ID:        "deny-high-priority",
					EffectStr: "deny",
					Priority:  5,
					Subjects:  []Condition{{Attribute: "subject.role", Op: "eq", Value: "intern"}},
					Actions:   []Action{ActionRead},
				},
			},
		},
	})

	subject := Subject{Attributes: Attributes{"role": "intern"}}
	d := pe.Evaluate(subject, Resource{Type: "table", Name: "secret"}, ActionRead, Environment{})
	if d.Allowed {
		t.Fatal("should be denied")
	}
	if d.MatchedRule != "deny-high-priority" {
		t.Fatalf("expected highest priority deny rule, got %q", d.MatchedRule)
	}
}

func TestEvaluateTableAccessDenied(t *testing.T) {
	pe := NewPolicyEvaluator([]AccessControlPolicy{
		{
			Name:    "deny-all",
			Enabled: true,
			Rules: []PolicyRule{
				{
					ID:        "deny-everything",
					EffectStr: "deny",
					Priority:  1,
					Actions:   []Action{ActionRead},
				},
			},
		},
	})

	subject := Subject{Attributes: Attributes{"role": "reader"}}
	td := pe.EvaluateTableAccess(subject, "events", ActionRead, Environment{})
	if td.Allowed {
		t.Fatal("expected denied")
	}
	if td.RuleID != "deny-everything" {
		t.Fatalf("expected deny-everything rule ID, got %q", td.RuleID)
	}
}

func TestEvaluateTableAccessQueryLimitObligation(t *testing.T) {
	pe := NewPolicyEvaluator([]AccessControlPolicy{
		{
			Name:    "with-query-limit",
			Enabled: true,
			Rules: []PolicyRule{
				{
					ID:        "limited-access",
					EffectStr: "allow",
					Priority:  10,
					Subjects:  []Condition{{Attribute: "subject.role", Op: "eq", Value: "intern"}},
					Actions:   []Action{ActionRead},
					Obligations: []Obligation{
						{Type: "query_limit", Value: "1000"},
					},
				},
			},
		},
	})

	subject := Subject{Attributes: Attributes{"role": "intern"}}
	td := pe.EvaluateTableAccess(subject, "events", ActionRead, Environment{})
	if !td.Allowed {
		t.Fatal("should be allowed")
	}
	// query_limit obligation handled externally, no columns/row_filter
	if td.RowFilter != "" {
		t.Error("expected no row filter for query_limit obligation")
	}
}

func TestMigrateRBACWildcardTable(t *testing.T) {
	// Wildcard table should not add resource conditions
	roles := []RoleConfig{
		{Name: "admin", Tables: []string{"*"}, Allow: []string{"admin"}},
	}
	policies := MigrateRBACToABAC(roles, nil)

	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}
	rule := policies[0].Rules[0]
	if len(rule.Resources) != 0 {
		t.Error("wildcard table should have no resource conditions")
	}
}

func TestMigrateRBACCellPolicyWildcardTable(t *testing.T) {
	cellPolicies := []PolicyConfig{
		{
			Table:   "*",
			Role:    "intern",
			Columns: map[string]string{"salary": "deny"},
		},
	}
	policies := MigrateRBACToABAC(nil, cellPolicies)

	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}
	rule := policies[0].Rules[0]
	if len(rule.Resources) != 0 {
		t.Error("wildcard cell policy should have no resource conditions")
	}
}

func TestMigrateRBACCellPolicyAllowColumn(t *testing.T) {
	// "allow" column action should produce no obligation
	cellPolicies := []PolicyConfig{
		{
			Table:   "events",
			Role:    "reader",
			Columns: map[string]string{"id": "allow", "name": "allow"},
		},
	}
	policies := MigrateRBACToABAC(nil, cellPolicies)
	// All columns are "allow" and no row filter -> no obligations -> skipped
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies (all allow, no obligations), got %d", len(policies))
	}
}

func TestPolicySetNilLookup(t *testing.T) {
	var ps *PolicySet
	if ps.Lookup("events", "admin") != nil {
		t.Error("nil PolicySet lookup should return nil")
	}
}

func TestDefaultMaskNilValue(t *testing.T) {
	result := defaultMask(nil)
	if result != nil {
		t.Errorf("expected nil for nil value, got %v", result)
	}
}

func TestDefaultMaskUnknownType(t *testing.T) {
	result := defaultMask([]int{1, 2, 3})
	if result != "***" {
		t.Errorf("expected *** for unknown type, got %v", result)
	}
}

func TestDefaultMaskIntType(t *testing.T) {
	result := defaultMask(42)
	if result != int64(0) {
		t.Errorf("expected 0 for int, got %v (%T)", result, result)
	}
}

func TestApplyToRowsNilPolicy(t *testing.T) {
	var p *AccessPolicy
	rows := []map[string]any{{"a": 1}}
	result := p.ApplyToRows(rows)
	if len(result) != 1 || result[0]["a"] != 1 {
		t.Error("nil policy should pass rows through unchanged")
	}
}

func TestApplyToRowsEmptyColumns(t *testing.T) {
	p := &AccessPolicy{Columns: map[string]ColumnPolicy{}}
	rows := []map[string]any{{"a": 1}, {"a": 2}}
	result := p.ApplyToRows(rows)
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
}

func TestParsePoliciesDefaultAction(t *testing.T) {
	configs := []PolicyConfig{
		{
			Table:   "events",
			Role:    "reader",
			Columns: map[string]string{"col": "unknown_action"},
		},
	}
	ps := ParsePolicies(configs)
	p := ps.Lookup("events", "reader")
	if p == nil {
		t.Fatal("expected policy")
	}
	// Unknown action should default to ColumnAllow
	if p.Columns["col"] != ColumnAllow {
		t.Errorf("expected ColumnAllow for unknown action, got %d", p.Columns["col"])
	}
}

func TestUpdateFromConfigWithABACPolicies(t *testing.T) {
	p := NewProvider(nil, nil, nil, nil)

	cfg := Config{
		Enabled: true,
		APIKeys: []APIKeyDef{
			{Key: "k1", Name: "test", Role: "reader"},
		},
		Roles: []RoleConfig{
			{Name: "reader", Tables: []string{"events"}, Allow: []string{"read"}},
		},
	}

	abacPolicies := []AccessControlPolicy{
		{
			Name:    "custom-abac",
			Enabled: true,
			Rules: []PolicyRule{
				{
					ID:        "custom-rule",
					EffectStr: "allow",
					Priority:  10,
					Subjects:  []Condition{{Attribute: "subject.role", Op: "eq", Value: "reader"}},
					Actions:   []Action{ActionRead},
				},
			},
		},
	}

	p.UpdateFromConfig(cfg, nil, abacPolicies...)
	if p.Evaluator() == nil {
		t.Fatal("expected evaluator with ABAC policies")
	}

	subject := Subject{Attributes: Attributes{"role": "reader"}}
	d := p.Evaluator().Evaluate(subject, Resource{Type: "table", Name: "events"}, ActionRead, Environment{})
	if !d.Allowed {
		t.Fatal("expected allowed by ABAC policy")
	}
}
