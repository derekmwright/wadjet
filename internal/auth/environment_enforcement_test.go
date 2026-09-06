package auth

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// timeGatedPolicies is the issue's shape: a broad allow for `analyst` and a
// deny that matches at every real hour. It can only deny if the enforcement
// path put a clock on the environment.
func timeGatedPolicies(actions ...Action) []AccessControlPolicy {
	if len(actions) == 0 {
		actions = []Action{ActionRead}
	}
	return []AccessControlPolicy{{
		Name: "time-gate", Version: 1, Enabled: true,
		Rules: []PolicyRule{
			{ID: "allow", EffectStr: "allow",
				Subjects: []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Actions:  []Action{ActionRead, ActionWrite}},
			{ID: "deny-at-any-real-hour", EffectStr: "deny",
				Subjects:    []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Actions:     actions,
				Environment: []Condition{{Attribute: "env.hour", Op: "gte", Value: 0}}},
		},
	}}
}

func envProvider(t *testing.T, policies []AccessControlPolicy) *Provider {
	t.Helper()
	authn, authz := New(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
		Roles:   []RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read", "write"}}},
	})
	p := NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(policies))
	return p
}

// TestEnforcePlanPoliciesCarriesTheRequestTime is the issue's reproducer
// (#933): the evaluator enforces a time-conditioned deny when it is given a
// real time, and the production plan path used to pass only Protocol.
func TestEnforcePlanPoliciesCarriesTheRequestTime(t *testing.T) {
	ctx := context.Background()
	cat := peCatalog(t, ctx)

	// Control: direct evaluation with Time set denies.
	subject := Subject{Attributes: Attributes{"role": "analyst"}}
	d := NewPolicyEvaluator(timeGatedPolicies()).Evaluate(
		subject, Resource{Type: "table", Name: "pe_emp"}, ActionRead,
		Environment{Time: time.Now()})
	if d.Allowed {
		t.Fatal("bad test setup: the deny should match when Time is set")
	}

	p := envProvider(t, timeGatedPolicies())
	idCtx := ContextWithIdentity(ctx, &Identity{Name: "analyst", Role: "analyst", Method: "apikey",
		Tables: []string{"*"}, Perms: []string{"read", "write"}})
	if _, _, err := envEnforce(t, idCtx, p, cat, "SELECT id FROM pe_emp"); err == nil {
		t.Fatal("time-conditioned deny vanished on the shared plan path")
	}
}

// The DML path builds its own environment and is its own cell.
func TestEnforceDMLPoliciesCarriesTheRequestTime(t *testing.T) {
	ctx := context.Background()
	cat := peCatalog(t, ctx)
	p := envProvider(t, timeGatedPolicies(ActionWrite))
	idCtx := ContextWithIdentity(ctx, &Identity{Name: "analyst", Role: "analyst", Method: "apikey",
		Tables: []string{"*"}, Perms: []string{"read", "write"}})

	parsed, err := plansql.Parse("DELETE FROM pe_emp WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if derr := EnforceDMLPolicies(idCtx, p, cat, parsed, "http"); derr == nil {
		t.Fatal("time-conditioned deny vanished on the shared DML path")
	}
}

// ValidateStatementColumns resolves column policies through the same resolver,
// so it needs the same environment: a mask whose rule is environment-gated has
// to be gated there too, or the binder and the plan disagree about what the
// identity can see.
func TestValidateStatementColumnsCarriesTheEnvironment(t *testing.T) {
	ctx := context.Background()
	cat := peCatalog(t, ctx)
	// The deny is over the whole table, so the column binder resolves no
	// policy for it and the plan path refuses; both must agree.
	p := envProvider(t, timeGatedPolicies())
	idCtx := ContextWithIdentity(ctx, &Identity{Name: "analyst", Role: "analyst", Method: "apikey",
		Tables: []string{"*"}, Perms: []string{"read", "write"}})

	parsed, err := plansql.Parse("SELECT ssn FROM pe_emp")
	if err != nil {
		t.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	// It must not panic or refuse a legal column; the refusal belongs to the
	// plan path, which the previous test asserts.
	if verr := ValidateStatementColumns(idCtx, p, cat, info, "embedded"); verr != nil {
		t.Fatalf("column binding under an environment-gated policy: %v", verr)
	}
}

// A socket address arrives as host:port on every door; `env.source_ip` is
// documented and written as an IP. The attach normalizes it once, so no door
// has to remember.
func TestTheAttachedSourceAddressLosesItsPort(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"127.0.0.1:54321", "127.0.0.1"},
		{"10.0.0.9:5432", "10.0.0.9"},
		{"[::1]:5432", "::1"},
		{"192.168.1.4", "192.168.1.4"}, // already bare
		{"::1", "::1"},                 // bare IPv6
		{"bufconn", "bufconn"},         // not an address at all
		{"", ""},
	} {
		ctx := ContextWithEnvironment(context.Background(), Environment{SourceIP: tc.in})
		if got := EnvironmentFromContext(ctx).SourceIP; got != tc.want {
			t.Errorf("SourceIP %q attached as %q, want %q", tc.in, got, tc.want)
		}
	}
}

// The decision's environment: the boundary's, with the decision's clock, and
// the call-site label only as a fallback.
func TestDecisionEnvironmentPrefersTheBoundary(t *testing.T) {
	// Nothing attached: the label is used and the clock is stamped.
	got := DecisionEnvironment(context.Background(), "embedded")
	if got.Protocol != "embedded" {
		t.Errorf("protocol = %q, want the call-site label", got.Protocol)
	}
	if got.Time.IsZero() {
		t.Error("Time was not stamped at decision time")
	}

	// A boundary attached one: it wins, because `env.protocol` means the door
	// the client used, not the execution path below it.
	ctx := ContextWithEnvironment(context.Background(),
		Environment{SourceIP: "10.0.0.9:1234", Protocol: "pgwire"})
	got = DecisionEnvironment(ctx, "embedded")
	if got.Protocol != "pgwire" {
		t.Errorf("protocol = %q, want the boundary's %q", got.Protocol, "pgwire")
	}
	if got.SourceIP != "10.0.0.9" {
		t.Errorf("source ip = %q, want the port stripped", got.SourceIP)
	}
	if got.Time.IsZero() {
		t.Error("Time was not stamped at decision time")
	}

	// A boundary that pinned a Time keeps it.
	pinned := time.Date(2026, 3, 17, 2, 0, 0, 0, time.UTC)
	ctx = ContextWithEnvironment(context.Background(), Environment{Time: pinned})
	if got := DecisionEnvironment(ctx, "http"); !got.Time.Equal(pinned) {
		t.Errorf("Time = %v, want the pinned %v", got.Time, pinned)
	}
}

func envEnforce(t *testing.T, ctx context.Context, p *Provider, cat *catalog.Catalog, sql string) (context.Context, *logical.Node, error) {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatal(err)
	}
	return EnforcePlanPolicies(ctx, p, cat, info, plan, "embedded")
}
