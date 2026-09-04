package auth

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// peCatalog is a catalog holding one policed table.
func peCatalog(t *testing.T, ctx context.Context) *catalog.Catalog {
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
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "ssn", Type: parquet.TypeString},
		{Name: "acct", Type: parquet.TypeInt64},
		{Name: "salary", Type: parquet.TypeInt64},
	}}
	if err := cat.CreateTable(ctx, "pe_emp", sch, nil); err != nil {
		t.Fatal(err)
	}
	return cat
}

func peProvider(t *testing.T, obligations []Obligation) *Provider {
	t.Helper()
	evaluator := NewPolicyEvaluator([]AccessControlPolicy{{
		Name: "pe", Version: 1, Enabled: true,
		Rules: []PolicyRule{{
			ID: "pe-rule", EffectStr: "allow", Priority: 10,
			Subjects:    []Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
			Actions:     []Action{ActionRead},
			Obligations: obligations,
		}},
	}})
	authn, authz := New(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
		Roles:   []RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	p := NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, evaluator)
	return p
}

func peEnforce(t *testing.T, ctx context.Context, provider *Provider, cat *catalog.Catalog, sql string) (*logical.Node, error) {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract %q: %v", sql, err)
	}
	plan, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("build %q: %v", sql, err)
	}
	_, out, err := EnforcePlanPolicies(ctx, provider, cat, info, plan, "test")
	return out, err
}

// TestEnforcePlanPoliciesRefusesADeniedColumnOnItsOwn.
//
// The query entry points call ValidateStatementColumns before they build a
// plan, so in a running server a denied column is already 42703 by the time
// enforcement runs. This test drives EnforcePlanPolicies WITHOUT that pass —
// the way any other caller of the exported function would — and asserts the
// refusal is its own, not something it inherits from a call site that
// remembered. A denied column that reaches the plan comes back as a phantom
// all-NULL column, which is #859's own defect.
func TestEnforcePlanPoliciesRefusesADeniedColumnOnItsOwn(t *testing.T) {
	ctx := ContextWithIdentity(context.Background(),
		&Identity{Name: "analyst", Role: "analyst", Method: "apikey"})
	cat := peCatalog(t, ctx)
	provider := peProvider(t, []Obligation{{Type: "deny_column", Target: "salary"}})

	for _, sql := range []string{
		`SELECT salary FROM pe_emp`,
		`SELECT SUM(salary) AS s FROM pe_emp`,
		`SELECT COUNT(*) AS c FROM pe_emp WHERE salary > 0`,
	} {
		if _, err := peEnforce(t, ctx, provider, cat, sql); err == nil {
			t.Errorf("%q was enforced without refusing a denied column", sql)
		} else if !strings.Contains(err.Error(), "salary") {
			t.Errorf("%q: error %v does not name the denied column", sql, err)
		}
	}
	// The control: a column the policy leaves alone still resolves.
	if _, err := peEnforce(t, ctx, provider, cat, `SELECT ssn FROM pe_emp`); err != nil {
		t.Errorf("an unpoliced column must still resolve: %v", err)
	}
}

// TestEnforcePlanPoliciesPicksTheMaskFromTheColumnType: a plan-time mask has
// to type-check, so the default placeholder for a `mask` with no `value:`
// comes from the column's DECLARED type. A bare `'***'` over a BIGINT column
// would make SUM(col) an error.
func TestEnforcePlanPoliciesPicksTheMaskFromTheColumnType(t *testing.T) {
	ctx := ContextWithIdentity(context.Background(),
		&Identity{Name: "analyst", Role: "analyst", Method: "apikey"})
	cat := peCatalog(t, ctx)
	provider := peProvider(t, []Obligation{
		{Type: "mask_column", Target: "ssn", MaskFunc: "redact"},
		{Type: "mask_column", Target: "acct", MaskFunc: "redact"},
	})

	plan, err := peEnforce(t, ctx, provider, cat, `SELECT ssn, acct FROM pe_emp`)
	if err != nil {
		t.Fatalf("enforce: %v", err)
	}
	want := map[string]string{"ssn": "'***'", "acct": "0"}
	var found int
	var walk func(*logical.Node)
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeProject && n.SecurityBarrier {
			for _, p := range n.Projections {
				if w, ok := want[p.Alias]; ok {
					found++
					if p.Expr != w {
						t.Errorf("mask for %q = %q, want %q", p.Alias, p.Expr, w)
					}
				}
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(plan)
	if found != len(want) {
		t.Fatalf("found %d masked projections, want %d — no security barrier in the plan?", found, len(want))
	}
}

// TestEnforcePlanPoliciesRefusesWhenTheProjectionCannotBeBuilt: a policy that
// leaves nothing visible is not "no policy". The query is refused rather than
// answered from the bare scan.
func TestEnforcePlanPoliciesRefusesWhenTheProjectionCannotBeBuilt(t *testing.T) {
	ctx := ContextWithIdentity(context.Background(),
		&Identity{Name: "analyst", Role: "analyst", Method: "apikey"})
	cat := peCatalog(t, ctx)
	provider := peProvider(t, []Obligation{
		{Type: "deny_column", Target: "id"},
		{Type: "deny_column", Target: "ssn"},
		{Type: "deny_column", Target: "acct"},
		{Type: "deny_column", Target: "salary"},
	})
	_, err := peEnforce(t, ctx, provider, cat, `SELECT COUNT(*) AS c FROM pe_emp`)
	if err == nil {
		t.Fatal("a table with every column denied was answered from the bare scan")
	}
	if !strings.Contains(err.Error(), "refusing to answer unmasked") {
		t.Fatalf("error %v, want the unenforceable-policy refusal", err)
	}
}

// TestPolicedRelationsComeFromThePlanNotTheStatement: a derived table's
// subquery TEXT and a CTE's name are not tables, and the arms of a UNION are
// not in the statement's FROM list at all.
func TestPolicedRelationsComeFromThePlanNotTheStatement(t *testing.T) {
	ctx := ContextWithIdentity(context.Background(),
		&Identity{Name: "analyst", Role: "analyst", Method: "apikey"})
	cat := peCatalog(t, ctx)

	for _, sql := range []string{
		`SELECT d.ssn AS s FROM (SELECT ssn FROM pe_emp) d`,
		`WITH u AS (SELECT ssn FROM pe_emp) SELECT ssn FROM u`,
		`SELECT ssn FROM pe_emp UNION ALL SELECT ssn FROM pe_emp`,
	} {
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
		got := policedRelations(ctx, cat, info, plan)
		if len(got) != 1 || !strings.EqualFold(got[0], "pe_emp") {
			t.Errorf("%q polices %v, want [pe_emp]", sql, got)
		}
	}
}
