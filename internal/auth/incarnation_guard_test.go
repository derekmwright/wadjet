// SPDX-License-Identifier: MIT

package auth

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ADR-0050 C1/C2 at every authorization decision site in this package: the
// table is pinned before the decision reads anything, so a table dropped and
// re-created between the statement's pin and the decision is caught by the
// verify that follows. Each site runs AFTER a clean verify, so the only
// decision the verify can be about is the site's own.

func guardSelect(t *testing.T, sql string) (*plansql.SelectInfo, *logical.Node) {
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
	return info, plan
}

func guardRecreateEmp(t *testing.T, cat *catalog.Catalog, cols ...string) {
	t.Helper()
	ctx := context.Background()
	if err := cat.DropTable(ctx, "pe_emp"); err != nil {
		t.Fatal(err)
	}
	var sch parquet.Schema
	for _, c := range cols {
		sch.Columns = append(sch.Columns, parquet.Column{Name: c, Type: parquet.TypeInt64})
	}
	if err := cat.CreateTable(ctx, "pe_emp", sch, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEveryDecisionSiteIsBracketed(t *testing.T) {
	base := ContextWithIdentity(context.Background(),
		&Identity{Name: "analyst", Role: "analyst", Method: "apikey", Tables: []string{"*"}, Perms: []string{"read"}})
	masked := []Obligation{{Type: "mask_column", Target: "ssn", MaskFunc: "redact"},
		{Type: "row_filter", Value: "id > 0"}}

	// enforced returns the context EnforcePlanPolicies hands on (the late
	// lookup is installed on it) for a statement over pe_emp.
	enforced := func(t *testing.T, ctx context.Context, p *Provider, cat *catalog.Catalog) context.Context {
		info, plan := guardSelect(t, "SELECT id, ssn FROM pe_emp")
		out, _, err := EnforcePlanPolicies(ctx, p, cat, info, plan, "test")
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	sites := []struct {
		name     string
		legacy   bool
		denyOnly bool // the lookup reads no record: its own pin is the only one
		prepare  func(t *testing.T, ctx context.Context, p *Provider, cat *catalog.Catalog) context.Context
		decision func(t *testing.T, ctx context.Context, p *Provider, cat *catalog.Catalog)
	}{
		{name: "binder", decision: func(t *testing.T, ctx context.Context, p *Provider, cat *catalog.Catalog) {
			info, _ := guardSelect(t, "SELECT id FROM pe_emp")
			_ = ValidateStatementColumns(ctx, p, cat, info, "test")
		}},
		{name: "enforcement", decision: func(t *testing.T, ctx context.Context, p *Provider, cat *catalog.Catalog) {
			info, plan := guardSelect(t, "SELECT id FROM pe_emp")
			_, _, _ = EnforcePlanPolicies(ctx, p, cat, info, plan, "test")
		}},
		{name: "late_lookup", prepare: enforced, decision: func(t *testing.T, ctx context.Context, _ *Provider, _ *catalog.Catalog) {
			_, _, _ = logical.PolicyLookupFromContext(ctx)("pe_emp")
		}},
		{name: "late_lookup_deny_only", denyOnly: true, prepare: enforced,
			decision: func(t *testing.T, ctx context.Context, _ *Provider, _ *catalog.Catalog) {
				_, _, _ = logical.PolicyLookupFromContext(ctx)("pe_emp")
			}},
		{name: "late_lookup_roles_only", legacy: true, prepare: enforced,
			decision: func(t *testing.T, ctx context.Context, _ *Provider, _ *catalog.Catalog) {
				_, _, _ = logical.PolicyLookupFromContext(ctx)("pe_emp")
			}},
		{name: "optimized_plan_new_scan", prepare: enforced,
			decision: func(t *testing.T, ctx context.Context, _ *Provider, cat *catalog.Catalog) {
				_, _ = EnforceOptimizedPlan(ctx, cat, &logical.Node{Type: logical.NodeScan, TableName: "pe_emp"})
			}},
	}
	for _, s := range sites {
		t.Run(s.name, func(t *testing.T) {
			cat := peCatalog(t, context.Background())
			obl := masked
			if s.denyOnly {
				obl = []Obligation{{Type: "deny_column", Target: "salary"}}
			}
			p := peProvider(t, obl)
			if s.legacy {
				p.Update(p.Authenticator(), p.Authorizer(), nil)
			}
			ctx := physical.PlanContext{}.WithManifestSnapshot(base, physical.NewManifestSnapshot())
			ctx = physical.WithIncarnationGuard(ctx, cat)
			if s.prepare != nil {
				ctx = s.prepare(t, ctx, p, cat)
			} else {
				physical.PinForDecision(ctx, "pe_emp")
			}
			if err := physical.VerifyIncarnations(ctx); err != nil {
				t.Fatalf("nothing changed, verify refused: %v", err)
			}
			guardRecreateEmp(t, cat, "id", "ssn")
			s.decision(t, ctx, p, cat)
			if err := physical.VerifyIncarnations(ctx); !physical.IsIncarnationChanged(err) {
				t.Fatalf("the %s decision was not bracketed: verify = %v", s.name, err)
			}
		})
	}
}

// TestNoGuardNoChange: a door that supplies no pin (the embedded doors, the
// HTTP door's own path, SubmitSQL) decides as before — the decision sites
// pin and verify nothing — even across a drop and re-create.
func TestNoGuardNoChange(t *testing.T) {
	ctx := ContextWithIdentity(context.Background(),
		&Identity{Name: "analyst", Role: "analyst", Method: "apikey", Tables: []string{"*"}, Perms: []string{"read"}})
	cat := peCatalog(t, context.Background())
	p := peProvider(t, []Obligation{{Type: "mask_column", Target: "ssn", MaskFunc: "redact"}})
	info, plan := guardSelect(t, "SELECT id, ssn FROM pe_emp")
	if _, _, err := EnforcePlanPolicies(ctx, p, cat, info, plan, "test"); err != nil {
		t.Fatal(err)
	}
	guardRecreateEmp(t, cat, "id", "ssn")
	info, plan = guardSelect(t, "SELECT id, ssn FROM pe_emp")
	out, _, err := EnforcePlanPolicies(ctx, p, cat, info, plan, "test")
	if err != nil {
		t.Fatalf("a door with no pin was refused: %v", err)
	}
	if err := physical.VerifyIncarnations(out); err != nil {
		t.Fatalf("a door with no pin verified: %v", err)
	}
}
