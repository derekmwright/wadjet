package auth

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// taIdentity is the authenticated caller every cell below runs as: a role that
// the legacy authorizer knows and that the ABAC policies name.
func taIdentity(role string) *Identity {
	return &Identity{Name: "caller", Role: role, Method: "apikey"}
}

// taProvider builds a provider whose Authenticator is enabled (one API key),
// whose Authorizer holds `roles`, and whose evaluator is `policies` — or none,
// which is the LEGACY arm.
func taProvider(t *testing.T, roles []RoleConfig, policies []AccessControlPolicy) *Provider {
	t.Helper()
	authn, authz := New(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "caller", Role: "reader"}},
		Roles:   roles,
	})
	p := NewProvider(authn, authz, nil, nil)
	if !p.Enabled() {
		t.Fatal("setup: the provider should report enabled")
	}
	if policies != nil {
		p.UpdateWithEvaluator(authn, authz, nil, NewPolicyEvaluator(policies))
	}
	return p
}

// taABAC is the ABAC arm's policy set: `reader` may read `events` and write
// `scratch`; `events` is EXPLICITLY denied to `contractor`; every other
// (subject, table, action) triple matches no rule at all and lands on the
// evaluator's default deny.
func taABAC() []AccessControlPolicy {
	return []AccessControlPolicy{{
		Name: "ta", Version: 1, Enabled: true,
		Rules: []PolicyRule{
			{ID: "reader-read-events", EffectStr: "allow", Priority: 100,
				Subjects:  []Condition{{Attribute: "subject.role", Op: "eq", Value: "reader"}},
				Resources: []Condition{{Attribute: "resource.name", Op: "eq", Value: "events"}},
				Actions:   []Action{ActionRead, ActionDescribe}},
			{ID: "reader-write-scratch", EffectStr: "allow", Priority: 100,
				Subjects:  []Condition{{Attribute: "subject.role", Op: "eq", Value: "reader"}},
				Resources: []Condition{{Attribute: "resource.name", Op: "eq", Value: "scratch"}},
				Actions:   []Action{ActionWrite}},
			{ID: "contractor-broad-allow", EffectStr: "allow", Priority: 100,
				Subjects: []Condition{{Attribute: "subject.role", Op: "eq", Value: "contractor"}},
				Actions:  []Action{ActionRead, ActionWrite}},
			{ID: "contractor-denied-events", EffectStr: "deny", Priority: 10,
				Subjects:  []Condition{{Attribute: "subject.role", Op: "eq", Value: "contractor"}},
				Resources: []Condition{{Attribute: "resource.name", Op: "eq", Value: "events"}},
				Actions:   []Action{ActionRead, ActionWrite}},
		},
	}}
}

// taRoles is the LEGACY arm's equivalent: `reader` may read `events` and
// `scratch`, `writer` may write them, `contractor` is scoped to `public`
// (which is how the legacy model spells "not this relation"), and
// `stranger` is a role the configuration does not define at all.
func taRoles() []RoleConfig {
	return []RoleConfig{
		{Name: "reader", Tables: []string{"events", "scratch"}, Allow: []string{"read"}},
		{Name: "writer", Tables: []string{"events", "scratch"}, Allow: []string{"read", "write"}},
		{Name: "contractor", Tables: []string{"public"}, Allow: []string{"read", "write"}},
	}
}

// legacyIdentity resolves an identity the way the Authenticator would: an
// unknown role carries no tables and no permissions.
func legacyIdentity(role string) *Identity {
	id := taIdentity(role)
	for _, r := range taRoles() {
		if r.Name == role {
			id.Tables, id.Perms = r.Tables, r.Allow
		}
	}
	return id
}

// TestTableAccessDecidesEveryCell drives the matrix the shared decision has to
// answer: {evaluator, legacy} × {allow, explicit deny, no rule, nil identity,
// provider disabled} × {read, write}.
//
// A refusal is asserted as a REFUSAL WITH A CLASS — SQLSTATE 42501 and the
// PostgreSQL message shape — because the doors render exactly that (pgwire
// 42501, HTTP 403, gRPC PermissionDenied), and a door census compares the
// class, not the prose.
func TestTableAccessDecidesEveryCell(t *testing.T) {
	abacProvider := taProvider(t, taRoles(), taABAC())
	legacyProvider := taProvider(t, taRoles(), nil)
	// A provider whose operator configured NO credential mechanism: auth is
	// off, and the shared decision must change nothing for it.
	disabledAuthn, disabledAuthz := New(Config{Roles: taRoles()})
	disabledProvider := NewProvider(disabledAuthn, disabledAuthz, nil, nil)
	if disabledProvider.Enabled() {
		t.Fatal("setup: a provider with no mechanism should report disabled")
	}

	tests := []struct {
		name     string
		provider *Provider
		identity *Identity
		table    string
		action   Action
		wantErr  bool
	}{
		// ---- evaluator arm -------------------------------------------------
		{"evaluator/allow/read", abacProvider, taIdentity("reader"), "events", ActionRead, false},
		{"evaluator/allow/write", abacProvider, taIdentity("reader"), "scratch", ActionWrite, false},
		{"evaluator/explicit-deny/read", abacProvider, taIdentity("contractor"), "events", ActionRead, true},
		{"evaluator/explicit-deny/write", abacProvider, taIdentity("contractor"), "events", ActionWrite, true},
		{"evaluator/no-rule/read", abacProvider, taIdentity("stranger"), "events", ActionRead, true},
		{"evaluator/no-rule/write", abacProvider, taIdentity("reader"), "events", ActionWrite, true},
		{"evaluator/nil-identity/read", abacProvider, nil, "events", ActionRead, true},
		{"evaluator/nil-identity/write", abacProvider, nil, "events", ActionWrite, true},
		{"evaluator/disabled/read", disabledProvider, nil, "events", ActionRead, false},
		{"evaluator/disabled/write", disabledProvider, nil, "events", ActionWrite, false},

		// ---- legacy arm ----------------------------------------------------
		{"legacy/allow/read", legacyProvider, legacyIdentity("reader"), "events", ActionRead, false},
		{"legacy/allow/write", legacyProvider, legacyIdentity("writer"), "scratch", ActionWrite, false},
		// The relation is not in the role's table list: the permission alone
		// is not access to THIS relation.
		{"legacy/explicit-deny/read", legacyProvider, legacyIdentity("contractor"), "events", ActionRead, true},
		{"legacy/explicit-deny/write", legacyProvider, legacyIdentity("contractor"), "events", ActionWrite, true},
		// The role is not configured at all — no tables, no permissions.
		{"legacy/no-rule/read", legacyProvider, legacyIdentity("stranger"), "events", ActionRead, true},
		// The relation IS in the role's list, but `read` is not `write`.
		{"legacy/no-rule/write", legacyProvider, legacyIdentity("reader"), "events", ActionWrite, true},
		{"legacy/nil-identity/read", legacyProvider, nil, "events", ActionRead, true},
		{"legacy/nil-identity/write", legacyProvider, nil, "events", ActionWrite, true},
		{"legacy/disabled/read", disabledProvider, nil, "events", ActionRead, false},
		{"legacy/disabled/write", disabledProvider, nil, "events", ActionWrite, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.identity != nil {
				ctx = ContextWithIdentity(ctx, tc.identity)
			}
			err := TableAccess(ctx, tc.provider, tc.table, tc.action)
			if tc.wantErr && err == nil {
				t.Fatalf("TableAccess(%q, %s) allowed; want a refusal", tc.table, tc.action)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("TableAccess(%q, %s) refused: %v; want it allowed", tc.table, tc.action, err)
			}
			if err == nil {
				return
			}
			if got := sqlerr.StateOf(err); got != "42501" {
				t.Errorf("refusal SQLSTATE = %q, want 42501 (insufficient_privilege)", got)
			}
			if !strings.Contains(err.Error(), `permission denied for table "`+tc.table+`"`) {
				t.Errorf("refusal message = %q, want it to carry "+
					`permission denied for table "%s"`, err.Error(), tc.table)
			}
		})
	}
}

// A nil provider is the embedded caller who never called SetAuthProvider:
// nothing is enforced and nothing changes.
func TestTableAccessWithNoProviderEnforcesNothing(t *testing.T) {
	for _, action := range []Action{ActionRead, ActionWrite, ActionAdmin, ActionCreate, ActionDrop, ActionDescribe} {
		if err := TableAccess(context.Background(), nil, "events", action); err != nil {
			t.Fatalf("nil provider, action %s: %v; want nil", action, err)
		}
	}
}

// The missing identity says WHY, so an operator reading a 42501 can tell an
// unauthenticated call from an unauthorized one.
func TestTableAccessNamesTheMissingIdentity(t *testing.T) {
	p := taProvider(t, taRoles(), taABAC())
	err := TableAccess(context.Background(), p, "events", ActionRead)
	if err == nil {
		t.Fatal("auth enabled with no identity was allowed")
	}
	if !strings.Contains(err.Error(), "authentication required") {
		t.Fatalf("refusal = %q, want it to name the missing authentication", err.Error())
	}
	if got := sqlerr.StateOf(err); got != "42501" {
		t.Fatalf("SQLSTATE = %q, want 42501", got)
	}
}

// A policy set that could not be bound to the catalog refuses every table,
// rather than enforcing a set whose scoped rules match nothing (#882).
func TestTableAccessRefusesWhenThePolicySetIsUnbound(t *testing.T) {
	p := taProvider(t, taRoles(), taABAC())
	boom := context.DeadlineExceeded
	p.bindErr.Store(&boom)
	ctx := ContextWithIdentity(context.Background(), taIdentity("reader"))
	err := TableAccess(ctx, p, "events", ActionRead)
	if err == nil {
		t.Fatal("an unbound policy set enforced nothing and allowed the read")
	}
	if got := sqlerr.StateOf(err); got != "42501" {
		t.Fatalf("SQLSTATE = %q, want 42501", got)
	}
}

// permissionForAction maps onto the three permissions that exist and nothing
// else, and an action it does not know is answered by the fewest identities.
func TestPermissionForActionUsesOnlyTheThreePermissions(t *testing.T) {
	want := map[Action]string{
		ActionRead:           "read",
		ActionDescribe:       "read",
		ActionWrite:          "write",
		ActionCreate:         "write",
		ActionDrop:           "write",
		ActionAdmin:          "admin",
		Action("frobnicate"): "admin",
	}
	for action, perm := range want {
		if got := permissionForAction(action); got != perm {
			t.Errorf("permissionForAction(%q) = %q, want %q", action, got, perm)
		}
	}
}

// TestTableAccessStampsTimeAtDecisionTime.
//
// A pgwire connection lives for hours, so an `env.hour` condition that read
// the ATTACH time would answer for the hour the socket opened. The decision
// stamps `Time` itself, and only when the boundary did not.
func TestTableAccessStampsTimeAtDecisionTime(t *testing.T) {
	// A deny that matches at every real hour: it can only match if the
	// decision put a clock on the environment.
	p := taProvider(t, taRoles(), []AccessControlPolicy{{
		Name: "hourly", Version: 1, Enabled: true,
		Rules: []PolicyRule{
			{ID: "allow", EffectStr: "allow",
				Subjects: []Condition{{Attribute: "subject.role", Op: "eq", Value: "reader"}},
				Actions:  []Action{ActionRead}},
			{ID: "deny-at-any-real-hour", EffectStr: "deny",
				Subjects:    []Condition{{Attribute: "subject.role", Op: "eq", Value: "reader"}},
				Actions:     []Action{ActionRead},
				Environment: []Condition{{Attribute: "env.hour", Op: "gte", Value: 0}}},
		},
	}})
	ctx := ContextWithIdentity(context.Background(), taIdentity("reader"))
	if err := TableAccess(ctx, p, "events", ActionRead); err == nil {
		t.Fatal("an env.hour deny did not match: the decision did not stamp Time")
	}
}

// An environment the boundary DID attach is used as attached — the source
// address is the boundary's observation, not the decision's guess.
func TestTableAccessUsesTheAttachedEnvironment(t *testing.T) {
	p := taProvider(t, taRoles(), []AccessControlPolicy{{
		Name: "ip", Version: 1, Enabled: true,
		Rules: []PolicyRule{
			{ID: "allow", EffectStr: "allow",
				Subjects: []Condition{{Attribute: "subject.role", Op: "eq", Value: "reader"}},
				Actions:  []Action{ActionRead}},
			{ID: "deny-from-10-net", EffectStr: "deny",
				Subjects:    []Condition{{Attribute: "subject.role", Op: "eq", Value: "reader"}},
				Actions:     []Action{ActionRead},
				Environment: []Condition{{Attribute: "env.source_ip", Op: "eq", Value: "10.0.0.9"}}},
		},
	}})
	base := ContextWithIdentity(context.Background(), taIdentity("reader"))

	allowed := ContextWithEnvironment(base, Environment{SourceIP: "192.168.1.4", Protocol: "pgwire"})
	if err := TableAccess(allowed, p, "events", ActionRead); err != nil {
		t.Fatalf("read from an un-denied address was refused: %v", err)
	}
	denied := ContextWithEnvironment(base, Environment{SourceIP: "10.0.0.9", Protocol: "pgwire"})
	if err := TableAccess(denied, p, "events", ActionRead); err == nil {
		t.Fatal("read from the denied address was allowed: the attached environment was ignored")
	}
}

// A boundary that pinned an explicit Time keeps it: the decision stamps only
// when the environment carries none.
func TestEnvironmentRoundTripsThroughTheContext(t *testing.T) {
	if got := EnvironmentFromContext(context.Background()); got.Protocol != "" || !got.Time.IsZero() {
		t.Fatalf("no environment attached: got %+v, want the zero Environment", got)
	}
	pinned := time.Date(2026, 3, 17, 2, 0, 0, 0, time.UTC)
	ctx := ContextWithEnvironment(context.Background(),
		Environment{Time: pinned, SourceIP: "10.0.0.9", Protocol: "grpc"})
	got := EnvironmentFromContext(ctx)
	if !got.Time.Equal(pinned) || got.SourceIP != "10.0.0.9" || got.Protocol != "grpc" {
		t.Fatalf("environment round-trip = %+v, want the attached one", got)
	}
}

// TestVisibleTablesFiltersToTheReadDecision.
//
// A listing publishes NAMES, and a name an identity may not read is not
// published by the listing either (ADR-0034). Order is the catalog's.
func TestVisibleTablesFiltersToTheReadDecision(t *testing.T) {
	p := taProvider(t, taRoles(), taABAC())
	all := []string{"events", "scratch", "secrets"}

	ctx := ContextWithIdentity(context.Background(), taIdentity("reader"))
	got := VisibleTables(ctx, p, all)
	if len(got) != 1 || got[0] != "events" {
		t.Fatalf("reader sees %v, want [events] — `scratch` is write-only and `secrets` matches no rule", got)
	}

	// The identity with an explicit deny on `events` sees the rest.
	ctx = ContextWithIdentity(context.Background(), taIdentity("contractor"))
	got = VisibleTables(ctx, p, all)
	if len(got) != 2 || got[0] != "scratch" || got[1] != "secrets" {
		t.Fatalf("contractor sees %v, want [scratch secrets] in catalog order", got)
	}

	// Auth enabled, nobody authenticated: nothing is listed.
	if got := VisibleTables(context.Background(), p, all); len(got) != 0 {
		t.Fatalf("unauthenticated listing = %v, want nothing", got)
	}
}

// With no provider — or auth disabled — a listing is returned untouched: a
// no-auth deployment sees no change at all.
func TestVisibleTablesIsATransparentPassThroughWithoutAuth(t *testing.T) {
	all := []string{"events", "scratch", "secrets"}
	if got := VisibleTables(context.Background(), nil, all); len(got) != 3 {
		t.Fatalf("nil provider listing = %v, want all three", got)
	}
	disabledAuthn, disabledAuthz := New(Config{Roles: taRoles()})
	disabled := NewProvider(disabledAuthn, disabledAuthz, nil, nil)
	if got := VisibleTables(context.Background(), disabled, all); len(got) != 3 {
		t.Fatalf("disabled-auth listing = %v, want all three", got)
	}
	if got := VisibleTables(context.Background(), nil, nil); got != nil {
		t.Fatalf("nil listing became %v, want nil", got)
	}
}

// The legacy arm's listing is the legacy rule's own answer: the role's table
// list, with `*` meaning every name.
func TestVisibleTablesOnTheLegacyArm(t *testing.T) {
	p := taProvider(t, taRoles(), nil)
	all := []string{"events", "scratch", "secrets"}

	ctx := ContextWithIdentity(context.Background(), legacyIdentity("reader"))
	got := VisibleTables(ctx, p, all)
	if len(got) != 2 || got[0] != "events" || got[1] != "scratch" {
		t.Fatalf("legacy reader sees %v, want [events scratch]", got)
	}

	wildcard := &Identity{Name: "admin", Role: "admin", Method: "apikey",
		Tables: []string{"*"}, Perms: []string{"admin"}}
	ctx = ContextWithIdentity(context.Background(), wildcard)
	if got := VisibleTables(ctx, p, all); len(got) != 3 {
		t.Fatalf("wildcard role sees %v, want all three", got)
	}
}
