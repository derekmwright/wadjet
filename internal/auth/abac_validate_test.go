package auth

import (
	"strings"
	"testing"
)

func maskRule(obs ...Obligation) []AccessControlPolicy {
	return []AccessControlPolicy{{
		Name: "p", Version: 1, Enabled: true,
		Rules: []PolicyRule{{
			ID: "r", EffectStr: "allow", Priority: 10,
			Actions:     []Action{ActionRead},
			Obligations: obs,
		}},
	}}
}

// TestValidateABACPoliciesRefusesAMaskThatCannotBeApplied is the LOAD half of
// ADR-0033 decision 4. Each spelling below produced, before #859's round-1
// pass, a policy that loaded happily and returned the column in the clear or
// under a value the operator did not write.
func TestValidateABACPoliciesRefusesAMaskThatCannotBeApplied(t *testing.T) {
	for _, tc := range []struct {
		name string
		obs  []Obligation
		want string
	}{
		{
			// The one that silently returned the STORED value on every door:
			// the enforcement path dropped the obligation entirely.
			name: "neither value nor mask_func",
			obs:  []Obligation{{Type: "mask_column", Target: "ssn"}},
			want: "neither a value nor a mask_func",
		},
		{
			// The spelling docs/configuration.md shipped for twelve releases.
			// It is not a SQL expression, and the fallback redefined the mask.
			// It PARSES — as the expression `* * *`, with the word REDACTED
			// dropped — and every masked column came back as 0. The signal is
			// not a parse error; it is that the operator's text did not
			// survive into the expression.
			name: "value the parser does not read as written",
			obs:  []Obligation{{Type: "mask_column", Target: "ssn", Value: "***REDACTED***"}},
			want: "does not read as written",
		},
		{
			name: "value is not a SQL expression at all",
			obs:  []Obligation{{Type: "mask_column", Target: "ssn", Value: "[MASKED]"}},
			want: "not a SQL expression",
		},
		{
			// A grant written as a mask: the expression runs against the row
			// as stored, so it publishes exactly what the rule takes away.
			name: "mask reads the column it masks",
			obs:  []Obligation{{Type: "mask_column", Target: "ssn", Value: "ssn"}},
			want: "which this rule also restricts",
		},
		{
			name: "mask reads another column the same rule denies",
			obs: []Obligation{
				{Type: "deny_column", Target: "salary"},
				{Type: "mask_column", Target: "ssn", Value: "salary"},
			},
			want: "which this rule also restricts",
		},
		{
			name: "no target",
			obs:  []Obligation{{Type: "mask_column", Value: "'***'"}},
			want: "names no target column",
		},
		{
			name: "deny with no target",
			obs:  []Obligation{{Type: "deny_column"}},
			want: "names no target column",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateABACPolicies(maskRule(tc.obs...))
			if err == nil {
				t.Fatalf("the policy set LOADED; a control that cannot be applied must refuse")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v\n  want one containing %q", err, tc.want)
			}
		})
	}
}

// TestValidateABACPoliciesAcceptsTheSpellingsThatWork — the other side of the
// claim. `mask_func` with no value is the LEGACY `columns: {col: mask}` form
// that MigrateRBACToABAC emits, and it takes the type-derived placeholder.
func TestValidateABACPoliciesAcceptsTheSpellingsThatWork(t *testing.T) {
	for _, tc := range []struct {
		name string
		obs  []Obligation
	}{
		{"explicit string value", []Obligation{{Type: "mask_column", Target: "ssn", Value: "'***'"}}},
		{"explicit numeric value", []Obligation{{Type: "mask_column", Target: "acct", Value: "0"}}},
		{"mask_func only (the legacy form)", []Obligation{{Type: "mask_column", Target: "ssn", MaskFunc: "redact"}}},
		{"an expression over an UNRESTRICTED column", []Obligation{
			{Type: "mask_column", Target: "ssn", Value: "'redacted-' || dept"},
		}},
		{"deny", []Obligation{{Type: "deny_column", Target: "salary"}}},
		{"row filter", []Obligation{{Type: "row_filter", Value: "dept = 'd1'"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateABACPolicies(maskRule(tc.obs...)); err != nil {
				t.Fatalf("refused a working spelling: %v", err)
			}
		})
	}
}

// TestUpdateFromConfigRefusesAnUnenforceablePolicySet: the refusal is wired
// where #802's is — at config load and at hot reload — and a refused set swaps
// NOTHING, so a running server keeps the policies it had.
func TestUpdateFromConfigRefusesAnUnenforceablePolicySet(t *testing.T) {
	authn, authz := New(Config{Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "a", Role: "analyst"}},
		Roles:   []RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}}})
	p := NewProvider(authn, authz, nil, nil)
	good := maskRule(Obligation{Type: "mask_column", Target: "ssn", Value: "'***'"})
	if err := p.UpdateFromConfig(Config{Enabled: true}, nil, good...); err != nil {
		t.Fatalf("good policy set: %v", err)
	}
	before := p.Evaluator()
	if before == nil {
		t.Fatal("no evaluator after a good load")
	}

	bad := maskRule(Obligation{Type: "mask_column", Target: "ssn"})
	if err := p.UpdateFromConfig(Config{Enabled: true}, nil, bad...); err == nil {
		t.Fatal("an unenforceable policy set was installed")
	}
	if p.Evaluator() != before {
		t.Fatal("the refused set swapped the evaluator; a refusal must keep the previous policies")
	}
}
