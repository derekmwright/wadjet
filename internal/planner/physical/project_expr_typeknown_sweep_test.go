package physical

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestProjectExprSpecsDeclareTypeKnownForBooleanOutputs is the
// TypeKnown-forgetter sweep: a corpus of one plan per ProjectExprSpec
// producer site, each shaped so that site materializes a genuinely BOOLEAN
// computed column, asserting TypeKnown rides along.
//
// The bug class (#445, #472, #473) is a specific zero-value collision:
// parquet.TypeBool IS TypeID's zero value, so a computed BOOL column whose
// producer forgot to set TypeKnown is bit-for-bit indistinguishable from a
// passthrough spec that never declared a type at all. projectOpFromSpecs
// then drops the (apparently unset) type off the wire and the worker
// defaults to STRING — silently, because the row VALUES still round-trip as
// "true"/"false" text. hidden_sort_key.go was exactly this: every existing
// gate passed while its BOOL sort key was silently mistyped.
//
// This test does not re-derive each site's specific expression/rename
// mechanics (those are covered in detail by TestScanProjectionAttachArithmeticAndBoolean,
// TestJoinInputComputedProjection, TestSetOpArmTypesReconciled and
// TestHiddenSortKeyDeclaresComputedType) — it is a single generic walker
// applied to one query per site, so a SIXTH producer site added later has
// an obvious place to add a seventh case rather than reinventing the check.
func TestProjectExprSpecsDeclareTypeKnownForBooleanOutputs(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	for _, tc := range []struct {
		name string
		sql  string
		// wantCol is the ProjectExprSpec name the site under test
		// materializes for its computed BOOL expression. Asserting it is
		// found (rather than just scanning every spec in the plan) makes a
		// shape drift — the query stops exercising the intended pass — a
		// loud failure instead of a silently vacuous pass.
		wantCol string
	}{
		{
			// ORDER BY the alias forces attachScanSelectProjections down its
			// "aliased" branch (a sort feeds the gather), which names the
			// materialized spec by the user's ALIAS rather than the
			// lowercased expression text — a bare `SELECT ... AS flag` with
			// nothing else consuming it takes the direct-scan branch
			// instead, which keeps the #169 expression-text naming
			// convention and leaves the alias to the gather's rename.
			name:    "attachScanSelectProjections",
			sql:     `SELECT s_suppkey, s_acctbal > 100 AS flag FROM supplier ORDER BY flag`,
			wantCol: "flag",
		},
		{
			name: "join_input_projection.go (absorbComputedSubqueryProjection)",
			sql: `SELECT n.n_name, r.flag FROM nation n
				JOIN (SELECT r_regionkey, r_regionkey > 2 AS flag FROM region) r
				ON n.n_regionkey = r.r_regionkey`,
			wantCol: "flag",
		},
		{
			name:    "set_op_stages.go (setOpArmProjection)",
			sql:     `SELECT r_regionkey > 2 AS flag FROM region UNION ALL SELECT n_nationkey > 2 AS flag FROM nation`,
			wantCol: "flag",
		},
		{
			name:    "hidden_sort_key.go (materializeSortKey)",
			sql:     `SELECT COUNT(*) AS c FROM (SELECT s_suppkey FROM supplier ORDER BY s_acctbal > 100 DESC LIMIT 7) t`,
			wantCol: "__sortkey_0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, tc.sql, 3)
			assertBoolSpecTypeKnown(t, stages, tc.wantCol)
		})
	}

	// absorbSecurityBarrier needs a policy-injected plan rather than plain
	// SQL, so it is not shaped through the table above: InjectColumnPolicies
	// is what real callers (auth.EnforcePlanPolicies) use to attach the
	// SecurityBarrier Project this pass absorbs, replacing a masked column
	// with a computed BOOL "over threshold" flag under its original name.
	t.Run("absorbSecurityBarrier", func(t *testing.T) {
		policies := []logical.ColumnPolicy{{Column: "s_acctbal", MaskExpr: "s_acctbal > 1000"}}
		stages := sqlToStagesWithColumnPolicies(t, cat, ctx,
			`SELECT s_suppkey, s_acctbal FROM supplier`, "supplier", policies, 3)
		assertBoolSpecTypeKnown(t, stages, "s_acctbal")
	})
}

// assertBoolSpecTypeKnown finds the ProjectExprSpec named col across every
// stage's ProjectExprs, SecurityProjectExprs and UnionArm projections, and
// requires it to be declared BOOL with TypeKnown set.
func assertBoolSpecTypeKnown(t *testing.T, stages []Stage, col string) {
	t.Helper()
	var found *ProjectExprSpec
	for _, spec := range allProjectExprSpecs(stages) {
		spec := spec
		if strings.EqualFold(spec.Name, col) {
			found = &spec
			break
		}
	}
	if found == nil {
		t.Fatalf("no ProjectExprSpec named %q across any stage — this shape no longer exercises "+
			"the producer site under test: %v", col, stageTypeIDs(stages))
	}
	if found.Type != parquet.TypeBool {
		t.Fatalf("spec %q declared %v, want BOOL — the test SQL must itself produce a boolean "+
			"expression here for this check to mean anything", col, found.Type)
	}
	if !found.TypeKnown {
		t.Errorf("spec %q is BOOL-typed with TypeKnown=false — indistinguishable from \"no type "+
			"declared\" since parquet.TypeBool is TypeID's zero value, so projectOpFromSpecs drops "+
			"it off the wire and the worker guesses STRING for a column that is a bool (#445/#472/#473 class)",
			col)
	}
}

// allProjectExprSpecs collects every ProjectExprSpec a stage list carries,
// across the three shapes that hold them: a stage's own ProjectExprs, its
// SecurityProjectExprs (the absorbed ABAC barrier), and a union stage's
// per-arm Projections.
func allProjectExprSpecs(stages []Stage) []ProjectExprSpec {
	var specs []ProjectExprSpec
	for _, s := range stages {
		specs = append(specs, s.ProjectExprs...)
		specs = append(specs, s.SecurityProjectExprs...)
		for _, arm := range s.UnionArms {
			specs = append(specs, arm.Projections...)
		}
	}
	return specs
}

// sqlToStagesWithColumnPolicies mirrors sqlToStages but threads an
// ABAC column policy through logical.InjectColumnPolicies, in the same
// position the real enforcement path (auth.EnforcePlanPolicies, called from
// internal/coordinator/coordinator.go) puts it: after scan-column
// annotation (InjectColumnPolicies needs Node.ScanColumns/RequiredColumns
// populated to know what to wrap) and before logical.Optimize.
func sqlToStagesWithColumnPolicies(t *testing.T, cat *catalog.Catalog, ctx context.Context,
	sql, policyTable string, policies []logical.ColumnPolicy, workerCount int) []Stage {
	t.Helper()

	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		t.Fatalf("logical plan: %v", err)
	}

	scanAnnotator := func(plan *logical.Node) {
		NewPlanner(cat).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)
	var unprotected int
	logicalPlan, unprotected = logical.InjectColumnPolicies(logicalPlan, policyTable, policies, nil)
	if unprotected != 0 {
		t.Fatalf("%d scans of %q left unprotected by the security projection", unprotected, policyTable)
	}
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	planner := NewPlanner(cat)
	planner.WorkerCount = workerCount
	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Fatalf("plan distributed: %v", err)
	}
	return stages
}
