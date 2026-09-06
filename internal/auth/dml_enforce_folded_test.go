package auth

import (
	"context"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// dfCatalog holds one policed table registered under a MIXED-CASE name, the
// way a parquet dataset or an Iceberg import brings one in. Every policy
// fixture in this package names a lower-case table, which is exactly why none
// of them could see the defect this gate exists for: with a lower-case name
// the statement's folded spelling and the catalog's spelling are the SAME
// STRING, so a door deciding on the wrong one of the two is indistinguishable
// from a correct one.
func dfCatalog(t *testing.T, ctx context.Context) *catalog.Catalog {
	t.Helper()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	sch := parquet.Schema{Columns: []parquet.Column{
		{Name: "WatchID", Type: parquet.TypeInt64},
		{Name: "Secret", Type: parquet.TypeString},
	}}
	if err := cat.CreateTable(ctx, "Hits", sch, nil); err != nil {
		t.Fatal(err)
	}
	return cat
}

// dfProvider grants the identity a BROAD allow and hangs the obligations off a
// rule scoped to the relation — the shape every `roles:` block plus a cell
// policy produces, and the shape in which a policy that fails to bind is a
// silent OPEN DOOR rather than a refusal.
func dfProvider(t *testing.T, resourceName string) *Provider {
	t.Helper()
	evaluator := NewPolicyEvaluator([]AccessControlPolicy{{
		Name: "df", Version: 1, Enabled: true,
		Rules: []PolicyRule{
			{
				ID: "df-scoped", EffectStr: "allow", Priority: 10,
				Subjects:  []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []Condition{{Attribute: "resource.name", Op: "eq", Value: resourceName}},
				Actions:   []Action{ActionRead, ActionWrite},
				Obligations: []Obligation{
					{Type: "deny_column", Target: "Secret"},
				},
			},
			{
				ID: "df-broad", EffectStr: "allow", Priority: 20,
				Subjects: []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Actions:  []Action{ActionRead, ActionWrite},
			},
		},
	}})
	authn, authz := New(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
		Roles:   []RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read", "write"}}},
	})
	p := NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, evaluator)
	return p
}

func dfIdentityCtx(ctx context.Context) context.Context {
	return ContextWithIdentity(ctx, &Identity{Name: "analyst", Role: "analyst",
		Tables: []string{"*"}, Perms: []string{"read", "write"}})
}

// TestDMLPoliciesDecideOnTheCatalogsTableName is the regression gate for a
// DML door that polices a different relation than the read door does.
//
// The write executors resolve the statement's folded table name against the
// catalog before they touch a manifest — that concession is what makes a
// mixed-case table reachable unquoted. `EnforceDMLPolicies` ran BEFORE that
// resolution, so it evaluated `hits` while `EnforcePlanPolicies` evaluated
// `Hits`, and a rule scoped to the relation bound to one door and not the
// other. Beside a broader allow — which is what a `roles:` migration emits —
// the unbound door does not refuse: it permits the write with NO obligations,
// so a denied column is writable and a masked one is unmasked.
func TestDMLPoliciesDecideOnTheCatalogsTableName(t *testing.T) {
	ctx := dfIdentityCtx(context.Background())
	cat := dfCatalog(t, ctx)

	for _, tt := range []struct {
		name         string
		resourceName string
		sql          string
	}{
		{"UPDATE naming the table unquoted", "Hits",
			`UPDATE hits SET Secret = 'x' WHERE WatchID = 1`},
		{"DELETE predicated on a denied column", "Hits",
			`DELETE FROM hits WHERE Secret = 'x'`},
		{"INSERT into a denied column", "Hits",
			`INSERT INTO hits (WatchID, Secret) VALUES (1, 'x')`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := plansql.Parse(tt.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			err = EnforceDMLPolicies(ctx, dfProvider(t, tt.resourceName), cat, parsed, "test")
			if err == nil {
				t.Fatalf("the policy did not bind: %q was permitted with no obligations, "+
					"so the denied column is writable. The door decided on the "+
					"statement's folded spelling instead of the catalog's.", tt.sql)
			}
			if !strings.Contains(err.Error(), "Secret") && !strings.Contains(err.Error(), "secret") {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
		})
	}

	t.Run("a policy naming a relation that does not exist still does not bind", func(t *testing.T) {
		// The concession is case, not spelling: a rule scoped to some OTHER
		// table must stay unbound, or the fix would make every policy global.
		parsed, err := plansql.Parse(`UPDATE hits SET Secret = 'x' WHERE WatchID = 1`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if err := EnforceDMLPolicies(ctx, dfProvider(t, "SomeOtherTable"), cat, parsed, "test"); err != nil {
			t.Fatalf("a policy scoped to another relation bound to this one: %v", err)
		}
	})
}
