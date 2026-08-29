package physical

import (
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// The STRUCTURAL gate for the producer-emits-no-stage class (#656).
//
// The seven shapes the umbrella lists are symptoms; the mechanism is that
// walkStages attaches a predicate to `len(*stages)-1` and a projection to a
// narrow list of producers, and neither placement is checked against what the
// coordinator's fragment builders actually run. A predicate on a stage that
// ignores the field, or on a stage a later pass deletes, is the query
// answered WITHOUT it — silently, because no operator ever sees a name it
// cannot resolve.
//
// This test asserts the two halves that make the class impossible rather than
// the seven shapes fixed:
//
//   - CONSERVATION. Every predicate walkStages could attach survives into the
//     final plan as a slot some stage carries. A pass that deletes the
//     carrying stage (collapseRedundantFinalMergeSort, flattenCTEAliases,
//     fuseSortIntoPredecessor) or forgets to migrate the field drops the
//     count, which is exactly shapes a–e.
//
//   - PLACEMENT. Every stage carrying FilterExprs or ProjectExprs is one
//     whose fragment evaluates them. ValidateNativeDAGShape enforces this on
//     every distributed query at run time; the test runs it over the corpus
//     so a regression is caught at build time too.
//
// The corpus is TPC-H's 22 queries plus the umbrella's shape table plus its
// loud siblings, because the class is not visible in TPC-H alone: no TPC-H
// query puts a WHERE above a CTE's ORDER BY or a SELECT list above a window.

// fcpCorpus is the shape table, spelled over the TPC-H fixture so it can be
// planned without a cluster. Names match the umbrella's rows.
func fcpCorpus() []struct{ name, sql string } {
	return []struct{ name, sql string }{
		{"a/WhereAboveCTEOrderByLimit",
			`WITH c AS (SELECT n_nationkey AS id, n_regionkey AS v FROM nation ORDER BY n_nationkey LIMIT 10) ` +
				`SELECT COUNT(*) AS n FROM c WHERE v > 0`},
		{"b/WhereAboveDerivedOrderByLimit",
			`SELECT COUNT(*) AS n FROM (SELECT n_nationkey AS id, n_regionkey AS v FROM nation ORDER BY n_nationkey LIMIT 10) s WHERE s.v > 0`},
		{"c/WhereAboveCTEOrderBy",
			`WITH c AS (SELECT n_nationkey AS id, n_regionkey AS v FROM nation ORDER BY n_nationkey) ` +
				`SELECT COUNT(*) AS n FROM c WHERE v > 0`},
		{"d/WhereAboveTopN",
			`WITH c AS (SELECT n_nationkey AS id, n_regionkey AS v FROM nation WHERE n_nationkey < 100 ORDER BY n_regionkey DESC LIMIT 5) ` +
				`SELECT id, v FROM c WHERE v > 2`},
		{"e/WhereAboveTheInnerOfTwoCTERefs",
			`WITH c AS (SELECT n_nationkey AS id, n_regionkey AS v FROM nation) ` +
				`SELECT COUNT(*) AS n FROM c JOIN (SELECT id AS j FROM c WHERE v > 0) x ON c.id = x.j`},
		{"f/WhereOnAnAggregateOutputAlias",
			`WITH c AS (SELECT n_regionkey + 1 AS gk, COUNT(*) AS n FROM nation GROUP BY n_regionkey + 1) ` +
				`SELECT gk, n FROM c WHERE gk > 3`},
		{"g/ProjectionAboveAWindow",
			`SELECT n_nationkey, UPPER(n_name) AS v FROM ` +
				`(SELECT n_nationkey, n_name, ROW_NUMBER() OVER (ORDER BY n_nationkey) AS w FROM nation) x`},
		{"658/WindowPartitionByAnAlias",
			`WITH c AS (SELECT n_nationkey AS id, n_regionkey AS gk FROM nation WHERE n_nationkey < 200) ` +
				`SELECT id, ROW_NUMBER() OVER (PARTITION BY gk ORDER BY id) AS rn FROM c ORDER BY id LIMIT 5`},
		{"660/UnionAllOverATwiceReferencedCTE",
			`WITH c AS (SELECT n_nationkey AS id, n_regionkey AS v FROM nation) ` +
				`SELECT COUNT(*) AS n FROM (SELECT id FROM c UNION ALL SELECT id FROM c) u`},
		{"558/HiddenOrderByAboveAWindow",
			`SELECT n_name, ROW_NUMBER() OVER (ORDER BY n_nationkey) AS rn FROM nation ORDER BY n_comment`},
		{"681/ComputedAggregateAliasAsAJoinKey",
			`SELECT COUNT(*) AS n FROM nation a JOIN ` +
				`(SELECT n_regionkey AS g, COUNT(*) + 1 AS k FROM nation GROUP BY n_regionkey) b ON a.n_nationkey = b.k`},
		// Controls: shapes that were already right, so a fix that broke them
		// would show up here rather than only in the two-path gates.
		{"ctl/HavingBelowTheSelectList",
			`SELECT n_regionkey, COUNT(*) AS c FROM nation GROUP BY n_regionkey HAVING COUNT(*) > 1`},
		{"ctl/WhereBelowTheCTE",
			`WITH c AS (SELECT n_nationkey AS id FROM nation WHERE n_nationkey > 3) SELECT COUNT(*) AS n FROM c`},
		{"ctl/WhereAboveAUnion",
			`SELECT id FROM (SELECT n_nationkey AS id FROM nation UNION ALL SELECT r_regionkey AS id FROM region) u WHERE id > 3`},
		{"ctl/WhereAboveAWindow",
			`SELECT n_name, rn FROM (SELECT n_name, ROW_NUMBER() OVER (ORDER BY n_nationkey) AS rn FROM nation) x WHERE rn <= 3`},
		{"ctl/WhereAboveABareLimit",
			`SELECT id FROM (SELECT n_nationkey AS id FROM nation LIMIT 10) s WHERE id > 3`},
	}
}

func TestStageDAGCarriesEveryFilterAndProjection(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	plan := func(t *testing.T, sql string) ([]string, []Stage) {
		t.Helper()
		parsed, err := plansql.Parse(sql)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		info, err := plansql.ExtractSelect(parsed)
		if err != nil {
			t.Fatalf("extract: %v", err)
		}
		node, err := logical.BuildFromSelect(info)
		if err != nil {
			t.Fatalf("logical: %v", err)
		}
		annotate := func(n *logical.Node) { NewPlanner(cat).AnnotateScanColumns(ctx, n) }
		annotate(node)
		node = logical.Optimize(node, annotate)
		p := NewPlanner(cat)
		p.WorkerCount = 3
		stages, err := p.PlanDistributed(ctx, node)
		if err != nil {
			t.Fatalf("PlanDistributed: %v", err)
		}
		return p.AttachedFilterExprs(), stages
	}

	check := func(t *testing.T, name, sql string) {
		t.Helper()
		attached, stages := plan(t, sql)
		// PLACEMENT. The same check the coordinator runs before dispatch.
		if err := ValidateNativeDAGShape(stages); err != nil {
			t.Errorf("%s: %v\n  SQL: %s\n%s", name, err, sql, renderStagesForPlacement(stages))
		}
		// CONSERVATION. Measured against what stage emission ATTACHED rather
		// than against the logical Filter nodes, because the optimizer
		// legitimately turns a predicate into something that is not a
		// predicate any more — a join condition (Q17's correlated
		// `l_partkey = p_partkey`), an ON residual (Q21), a partition
		// filter, a semijoin. And measured as a SET, because the
		// deduplicating passes legitimately collapse two identical subtrees'
		// identical predicates into one (Q11's scalar-subquery leg, Q17's
		// semi sibling, Q21's subsumed scan exchange). What must never
		// happen is a predicate stage emission DID attach being readable
		// nowhere in the final plan.
		present := stagePredicateTexts(stages)
		for _, want := range attached {
			if predicateIsPresent(present, want) {
				continue
			}
			t.Errorf("%s: stage emission attached the predicate %q and the final plan carries it "+
				"nowhere — it reached a stage that was deleted or that does not evaluate it, so "+
				"the query answers WITHOUT it (#656)\n  SQL: %s\n%s",
				name, strings.ReplaceAll(want, "\x00", "  |  "), sql, renderStagesForPlacement(stages))
		}
	}

	for _, c := range fcpCorpus() {
		c := c
		t.Run(c.name, func(t *testing.T) { check(t, c.name, c.sql) })
	}
	// And the whole TPC-H corpus, which is where a regression in the
	// existing placements would show.
	for qNum := 1; qNum <= 22; qNum++ {
		sql, ok := tpchPlanQueryMap[qNum]
		if !ok {
			continue
		}
		name := fmt.Sprintf("tpch/Q%02d", qNum)
		t.Run(name, func(t *testing.T) { check(t, name, sql) })
	}
}

// TestStageDAGPlacementRejectsAnUnrunnablePredicate is the fail-half of the
// placement check: the validator must REFUSE a plan that attaches a predicate
// to a stage whose fragment ignores the field, rather than let it dispatch.
func TestStageDAGPlacementRejectsAnUnrunnablePredicate(t *testing.T) {
	base := []Stage{
		{ID: "scan-0", Type: StageScan, Tasks: 1, TableName: "nation", Columns: []string{"n_nationkey"}},
		{ID: "exchange-gather-scan-0", Type: StageExchangeGather, Tasks: 1, Dependencies: []string{"scan-0"}},
	}
	if err := ValidateNativeDAGShape(base); err != nil {
		t.Fatalf("the control plan must validate: %v", err)
	}
	withFilter := append([]Stage(nil), base...)
	withFilter[1].FilterExprs = []string{"n_nationkey > 3"}
	if err := ValidateNativeDAGShape(withFilter); err == nil {
		t.Error("a predicate on an exchange-gather — whose fragment never reads FilterExprs — " +
			"must be refused, not silently dropped")
	}
	withProj := append([]Stage(nil), base...)
	withProj[1].ProjectExprs = []ProjectExprSpec{{Expr: "n_nationkey", Name: "k"}}
	if err := ValidateNativeDAGShape(withProj); err == nil {
		t.Error("a projection on an exchange-gather — whose fragment never reads ProjectExprs — " +
			"must be refused, not silently dropped")
	}
	// And the same for a fused scan-aggregate, whose builder has an OpFilter
	// slot but no OpProject one.
	fused := []Stage{
		{ID: "scan-0", Type: StageScan, Tasks: 1, TableName: "nation",
			FusedAggGroupBy: []string{"n_regionkey"},
			FusedAggSpecs:   []AggSpec{{Func: "count", OutputCol: "c"}},
			ProjectExprs:    []ProjectExprSpec{{Expr: "n_regionkey", Name: "g"}}},
	}
	if err := ValidateNativeDAGShape(fused); err == nil {
		t.Error("a projection on a fused scan-aggregate — buildScanAggregateFragment has no " +
			"OpProject slot — must be refused")
	}
}

// stagePredicateTexts is the set of predicate texts the final plan carries,
// gathered from every place a rewriting pass may have moved one to: a stage's
// own filter, a fused or chained join's residual, a build-side filter, and
// the computed flag column dedupeSubsumedScanExchanges ships in place of a
// dropped sibling scan's filter.
func stagePredicateTexts(stages []Stage) map[string]bool {
	out := map[string]bool{}
	add := func(exprs ...string) {
		for _, e := range exprs {
			if e = strings.ToLower(strings.TrimSpace(e)); e != "" {
				out[e] = true
			}
		}
	}
	for _, s := range stages {
		add(s.FilterExprs...)
		add(s.BuildFilterExprs...)
		add(s.JoinFilter)
		for _, fj := range s.FusedJoins {
			add(fj.FilterExprs...)
			add(fj.JoinFilter)
		}
		for _, cj := range s.ChainedJoins {
			add(cj.FilterExprs...)
			add(cj.BuildFilterExprs...)
			add(cj.JoinFilter)
		}
		if s.Exchange != nil {
			for _, cc := range s.Exchange.ComputedCols {
				add(cc.Expr)
			}
		}
	}
	return out
}

// predicateIsPresent reports whether either spelling of an attached predicate
// is readable somewhere in the final plan.
func predicateIsPresent(present map[string]bool, attached string) bool {
	for _, spelling := range strings.Split(attached, "\x00") {
		if present[strings.ToLower(strings.TrimSpace(spelling))] {
			return true
		}
	}
	return false
}

// renderStagesForPlacement is the failure message's plan dump.
func renderStagesForPlacement(stages []Stage) string {
	var b strings.Builder
	for _, s := range stages {
		fmt.Fprintf(&b, "  %s\t%s\tdeps=%v", s.ID, s.Type, s.Dependencies)
		if len(s.FilterExprs) > 0 {
			fmt.Fprintf(&b, "\tfilter=%v", s.FilterExprs)
		}
		if len(s.ProjectExprs) > 0 {
			fmt.Fprintf(&b, "\tproject=%v", stageProjectionOutputs(&s))
		}
		b.WriteByte('\n')
	}
	return b.String()
}
