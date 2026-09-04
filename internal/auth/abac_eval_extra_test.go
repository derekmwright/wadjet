package auth

import (
	"strings"
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
	policies, err := MigrateRBACToABAC(roles, nil)
	if err != nil {
		t.Fatalf("MigrateRBACToABAC: %v", err)
	}

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
	policies, err := MigrateRBACToABAC(nil, cellPolicies)
	if err != nil {
		t.Fatalf("MigrateRBACToABAC: %v", err)
	}

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
	policies, err := MigrateRBACToABAC(nil, cellPolicies)
	if err != nil {
		t.Fatalf("MigrateRBACToABAC: %v", err)
	}
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

// TestParsePoliciesRefusesUnknownColumnAction is the unit half of #802's gate.
// This test used to be TestParsePoliciesDefaultAction and asserted the exact
// defect: an unrecognised action returned ColumnAllow, so a `columns:` entry
// that could not be read granted the column in full.
func TestParsePoliciesRefusesUnknownColumnAction(t *testing.T) {
	configs := []PolicyConfig{
		{
			Table:   "events",
			Role:    "reader",
			Columns: map[string]string{"col": "***REDACTED***"},
		},
	}
	ps, err := ParsePolicies(configs)
	if err == nil {
		t.Fatalf("expected an error for an unrecognised column action, got policy set %v", ps)
	}
	if ps != nil {
		t.Errorf("a refused parse must return no policy set, got %v", ps)
	}
	// The message has to be actionable: which column, and what it could not read.
	for _, want := range []string{`"col"`, `"***REDACTED***"`, `"events"`, `"reader"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
}

// TestParseColumnActionVocabulary pins the accepted spellings on both sides:
// the three actions parse (case- and space-insensitively), and anything else
// is an error rather than a default.
func TestParseColumnActionVocabulary(t *testing.T) {
	good := map[string]ColumnPolicy{
		"allow": ColumnAllow, "ALLOW": ColumnAllow, " allow ": ColumnAllow,
		"mask": ColumnMask, "Mask": ColumnMask,
		"deny": ColumnDeny, "DENY": ColumnDeny,
	}
	for in, want := range good {
		got, err := ParseColumnAction("c", in)
		if err != nil {
			t.Errorf("ParseColumnAction(%q): unexpected error %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseColumnAction(%q) = %d, want %d", in, got, want)
		}
	}
	for _, in := range []string{"", "***REDACTED***", "redact", "hide", "null", "true", "ALLOWED", "masked"} {
		if _, err := ParseColumnAction("c", in); err == nil {
			t.Errorf("ParseColumnAction(%q): expected an error, got none", in)
		}
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

	if err := p.UpdateFromConfig(cfg, nil, abacPolicies...); err != nil {
		t.Fatalf("UpdateFromConfig: %v", err)
	}
	if p.Evaluator() == nil {
		t.Fatal("expected evaluator with ABAC policies")
	}

	subject := Subject{Attributes: Attributes{"role": "reader"}}
	d := p.Evaluator().Evaluate(subject, Resource{Type: "table", Name: "events"}, ActionRead, Environment{})
	if !d.Allowed {
		t.Fatal("expected allowed by ABAC policy")
	}
}
