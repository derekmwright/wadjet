package auth

import "testing"

// A capability is granted only by a rule that NAMES it (ADR-0034 item 11).
//
// A table function — `read_csv`, `read_parquet`, `postgres_scan` — presents a
// resource whose type is `table_function`, and it reads the server's own
// filesystem and opens outbound connections. Under ordinary deny-overrides
// matching an unscoped `allow` written about TABLES matched that resource
// too, so a role granted `read` on some tables silently also held "read any
// file this process can open".

// capPolicies: a broad allow for `analyst` (no resource scope at all), plus,
// optionally, a rule that names the capability.
func capPolicies(extra ...PolicyRule) *PolicyEvaluator {
	rules := []PolicyRule{{
		ID: "broad-allow", EffectStr: "allow", Priority: 100,
		Subjects: []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
		Actions:  []Action{ActionRead},
	}}
	rules = append(rules, extra...)
	return NewPolicyEvaluator([]AccessControlPolicy{{
		Name: "cap", Version: 1, Enabled: true, Rules: rules,
	}})
}

var capSubject = Subject{Attributes: Attributes{"role": "analyst"}}

func TestAnUnscopedAllowDoesNotGrantACapability(t *testing.T) {
	pe := capPolicies()

	// The table it was written for: still allowed. A capability gate that
	// broke ordinary table access would be the worse defect.
	if d := pe.Evaluate(capSubject, Resource{Type: "table", Name: "events"},
		ActionRead, Environment{}); !d.Allowed {
		t.Fatalf("the broad allow stopped granting TABLE access: %s", d.Reason)
	}

	// The capability it was NOT written for: refused.
	d := pe.Evaluate(capSubject,
		Resource{Type: "table_function", Name: "read_csv",
			Attributes: Attributes{"path": "/etc/passwd"}},
		ActionRead, Environment{})
	if d.Allowed {
		t.Fatal("an unscoped allow granted a table_function: a role with read on " +
			"some tables could read any file the server process can open")
	}
}

func TestARuleThatNamesTheCapabilityGrantsIt(t *testing.T) {
	for _, tc := range []struct {
		name string
		cond Condition
	}{
		{"eq", Condition{Attribute: "resource.type", Op: "eq", Value: "table_function"}},
		{"in", Condition{Attribute: "resource.type", Op: "in",
			Value: []any{"table", "table_function"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pe := capPolicies(PolicyRule{
				ID: "capability", EffectStr: "allow", Priority: 50,
				Subjects:  []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []Condition{tc.cond},
				Actions:   []Action{ActionRead},
			})
			d := pe.Evaluate(capSubject,
				Resource{Type: "table_function", Name: "read_csv",
					Attributes: Attributes{"path": "/data/x.csv"}},
				ActionRead, Environment{})
			if !d.Allowed {
				t.Fatalf("a rule naming the capability did not grant it: %s", d.Reason)
			}
		})
	}
}

// A capability rule can be scoped further — to a destination — and the scope
// still decides once the type has been named.
func TestACapabilityRuleStillHonoursItsOtherConditions(t *testing.T) {
	pe := capPolicies(PolicyRule{
		ID: "capability", EffectStr: "allow", Priority: 50,
		Subjects: []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
		Resources: []Condition{
			{Attribute: "resource.type", Op: "eq", Value: "table_function"},
			{Attribute: "resource.path", Op: "contains", Value: "/data/"},
		},
		Actions: []Action{ActionRead},
	})
	allowed := pe.Evaluate(capSubject,
		Resource{Type: "table_function", Name: "read_csv",
			Attributes: Attributes{"path": "/data/x.csv"}}, ActionRead, Environment{})
	if !allowed.Allowed {
		t.Fatalf("the in-scope destination was refused: %s", allowed.Reason)
	}
	refused := pe.Evaluate(capSubject,
		Resource{Type: "table_function", Name: "read_csv",
			Attributes: Attributes{"path": "/etc/passwd"}}, ActionRead, Environment{})
	if refused.Allowed {
		t.Fatal("a destination outside the rule's scope was granted")
	}
}

// The gate is on ALLOW rules only. A rule that takes access away must reach
// FURTHER than one that grants it, never less far: an unscoped deny that
// stopped matching a capability would be a widening dressed as a restriction.
func TestAnUnscopedDenyStillReachesACapability(t *testing.T) {
	pe := capPolicies(
		PolicyRule{
			ID: "capability", EffectStr: "allow", Priority: 50,
			Subjects:  []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
			Resources: []Condition{{Attribute: "resource.type", Op: "eq", Value: "table_function"}},
			Actions:   []Action{ActionRead},
		},
		PolicyRule{
			ID: "unscoped-deny", EffectStr: "deny", Priority: 10,
			Subjects: []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
			Actions:  []Action{ActionRead},
		})
	if d := pe.Evaluate(capSubject,
		Resource{Type: "table_function", Name: "read_csv"},
		ActionRead, Environment{}); d.Allowed {
		t.Fatal("an unscoped deny stopped reaching a capability")
	}
}

// `neq` and `not_in` EXCLUDE a type; excluding one is not naming it. A rule
// carrying `resource.type neq table_function` — which is what the RBAC
// migration emits on a role's table rule — must not thereby grant the
// capability.
func TestExcludingATypeIsNotNamingIt(t *testing.T) {
	pe := capPolicies(PolicyRule{
		ID: "tables-only", EffectStr: "allow", Priority: 50,
		Subjects:  []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
		Resources: []Condition{{Attribute: "resource.type", Op: "neq", Value: "table_function"}},
		Actions:   []Action{ActionRead},
	})
	if d := pe.Evaluate(capSubject,
		Resource{Type: "table_function", Name: "read_csv"},
		ActionRead, Environment{}); d.Allowed {
		t.Fatal("a rule that EXCLUDES table_function granted it")
	}
	if d := pe.Evaluate(capSubject, Resource{Type: "table", Name: "events"},
		ActionRead, Environment{}); !d.Allowed {
		t.Fatalf("the same rule stopped granting tables: %s", d.Reason)
	}
}

// A resource with no type at all (a programmatic caller's zero Resource) is
// not treated as a capability — the gate keys on a type that is present and
// is not `table`.
func TestAnUntypedResourceIsNotACapability(t *testing.T) {
	if d := capPolicies().Evaluate(capSubject, Resource{Name: "events"},
		ActionRead, Environment{}); !d.Allowed {
		t.Fatalf("an untyped resource was treated as a capability: %s", d.Reason)
	}
}

// The validator accepts the attributes a table-function resource carries, so a
// capability policy scoped to a destination LOADS.
func TestTheValidatorAcceptsTableFunctionResourceAttributes(t *testing.T) {
	for _, attr := range []string{"resource.type", "resource.name", "resource.path",
		"resource.url", "resource.host", "resource.arg_delimiter", "resource.arg_header"} {
		if err := ValidateABACPolicies(vocabPolicy(PolicyRule{
			ID: "cap", EffectStr: "allow", Actions: []Action{ActionRead},
			Resources: []Condition{{Attribute: attr, Op: "eq", Value: "x"}},
		})); err != nil {
			t.Errorf("attribute %q refused: %v", attr, err)
		}
	}
	// A bare `resource.arg_` names no argument.
	if err := ValidateABACPolicies(vocabPolicy(PolicyRule{
		ID: "cap", EffectStr: "allow", Actions: []Action{ActionRead},
		Resources: []Condition{{Attribute: "resource.arg_", Op: "eq", Value: "x"}},
	})); err == nil {
		t.Error("`resource.arg_` with no argument name was accepted")
	}
}
