// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"
	"testing"
)

// #1613: a SELECT list over a bare UNION (a derived table whose set operation
// deduplicates) was evaluated by no stage. The dedup half of the union is a
// keys-only GroupByAll aggregate that lists no columns of its own, so the root
// SELECT-list pass read it as emitting nothing, declined to give the list a
// StageProject, and the gather-reachability check called the plan unmodelled:
// the client got the union's raw columns. The plan-shape invariant here: the
// list is computed by a StageProject reading the dedup, and the gather reads
// what that stage emits.
func TestUnionDedupSelectListIsEvaluatedAboveTheDedup(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	const body = "(SELECT n_nationkey AS k, n_regionkey AS r FROM nation UNION SELECT r_regionkey, r_regionkey FROM region) s"
	for _, tc := range []struct {
		name, sql, expr string
	}{
		{"arith", "SELECT k + 100 AS z FROM " + body, "k + 100"},
		{"cast", "SELECT CAST(k AS VARCHAR) AS t FROM " + body, "cast"},
		{"unknown_function", "SELECT nosuchfn(k) AS z FROM " + body, "nosuchfn"},
		{"filter_above", "SELECT r * 2 AS d FROM " + body + " WHERE s.k > 3", "r * 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, tc.sql, 3)
			byID := map[string]*Stage{}
			for i := range stages {
				byID[stages[i].ID] = &stages[i]
			}
			var dedup *Stage
			for i := range stages {
				if isNameTransparentDedup(&stages[i]) {
					dedup = &stages[i]
				}
			}
			if dedup == nil {
				t.Fatalf("no GroupByAll dedup stage: the shape this test guards is gone\n%s", stageSummary(stages))
			}
			var proj *Stage
			for i := range stages {
				s := &stages[i]
				if s.Type == StageProject && len(s.Dependencies) == 1 && s.Dependencies[0] == dedup.ID {
					proj = s
				}
			}
			if proj == nil {
				t.Fatalf("no StageProject reads the dedup %s: the SELECT list is evaluated nowhere\n%s", dedup.ID, stageSummary(stages))
			}
			found := false
			for _, sp := range proj.ProjectExprs {
				if strings.Contains(strings.ToLower(sp.Expr), tc.expr) {
					found = true
				}
			}
			if !found {
				t.Fatalf("the StageProject above the dedup does not compute %q: %+v", tc.expr, proj.ProjectExprs)
			}
			if err := ValidateNativeDAGShape(stages); err != nil {
				t.Fatalf("validate: %v", err)
			}
		})
	}
}

// The ORDER BY form, whose sort the planner fuses onto the dedup, keeps
// planning (the SELECT-list pass declines it as before and the dispatch-time
// check owns it): PlanDistributed must not start refusing it, because the
// asynchronous door turns any PlanDistributed error into a refusal of a
// statement it answers as one pipeline task (TestArcUNSetOperationMatrix's
// async arms). sqlToStages fails the test on a planning error.
func TestUnionDedupOrderByFormStillPlans(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	stages := sqlToStages(t, cat, ctx,
		"SELECT CAST(k AS VARCHAR) FROM (SELECT n_nationkey AS k FROM nation UNION SELECT r_regionkey FROM region) s ORDER BY 1", 3)
	for i := range stages {
		if isNameTransparentDedup(&stages[i]) {
			return
		}
	}
	t.Fatalf("no GroupByAll dedup stage\n%s", stageSummary(stages))
}

// #1613 round 2: the bare UNION's OWN ORDER BY inside the derived table. The
// sort fuses onto the dedup (or, under a WHERE written above it, sits as a
// standalone sort over the dedup), its keys are columns the SELECT list drops,
// and the pass declined so as not to hide that ordering, though no consumer
// reads it: nothing sorts above the SELECT list. The invariant: the gather's
// input computes the list, and when that input is an inserted StageProject it
// declares no ordering on keys it no longer emits.
func TestUnionDedupInnerOrderBySelectListIsEvaluated(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	const arms = "SELECT n_nationkey AS k, n_regionkey AS r FROM nation UNION SELECT r_regionkey, r_regionkey FROM region"
	for _, tc := range []struct {
		name, sql, expr string
	}{
		{"order_limit_fused", "SELECT k + 100 AS z FROM (" + arms + " ORDER BY r, k LIMIT 3) s", "k + 100"},
		{"order_no_limit_fused", "SELECT k + 100 AS z FROM (" + arms + " ORDER BY r) s", "k + 100"},
		{"unknown_function", "SELECT nosuchfn(k) AS z FROM (" + arms + " ORDER BY r, k LIMIT 3) s", "nosuchfn"},
		{"filter_above_standalone_sort", "SELECT k + 100 AS z FROM (" + arms + " ORDER BY r, k LIMIT 3) s WHERE s.k > 1", "k + 100"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, tc.sql, 3)
			var gather *Stage
			for i := range stages {
				if stages[i].Type == StageExchangeGather {
					gather = &stages[i]
				}
			}
			if gather == nil || len(gather.Dependencies) != 1 {
				t.Fatalf("no single-input gather\n%s", stageSummary(stages))
			}
			var carrier *Stage
			for i := range stages {
				if stages[i].ID == gather.Dependencies[0] {
					carrier = &stages[i]
				}
			}
			found := false
			for _, sp := range carrier.ProjectExprs {
				if strings.Contains(strings.ToLower(sp.Expr), tc.expr) {
					found = true
				}
			}
			if !found {
				t.Fatalf("the gather's input %s does not compute %q: the SELECT list is evaluated nowhere\n%s",
					carrier.ID, tc.expr, stageSummary(stages))
			}
			if carrier.Type == StageProject && len(carrier.SortKeys) != 0 {
				t.Fatalf("%s declares an ordering on %v, which its projection drops", carrier.ID, carrier.SortKeys)
			}
			if err := ValidateNativeDAGShape(stages); err != nil {
				t.Fatalf("validate: %v", err)
			}
		})
	}
}
