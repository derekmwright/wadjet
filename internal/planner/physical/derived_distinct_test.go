package physical

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// Regression for #466: the stage DAG dropped DISTINCT when a derived table
// fed an aggregate.
//
// walkStages has no execution for NodeDistinct — it falls into the
// passthrough branch and emits no stage. Two things compensated, and neither
// covered this shape: rewriteDistinctAsGroupBy walked only the ROOT path, and
// the coordinator's post-gather dedup keys off ExtractMergeInfo, which
// returns at the first NodeAggregate it meets and never looks below it. So
// `SELECT COUNT(*) FROM (SELECT DISTINCT c FROM t) u` planned an aggregate
// over the raw scan and answered with the raw count.
//
// The invariant asserted here: a DISTINCT anywhere in the plan must reach the
// worker as a real dedup — an aggregate stage grouping on the distinct
// columns — and never as nothing at all.

// distinctGroupStages returns every aggregate-bearing stage whose GroupBy is
// exactly the given key set and which computes no aggregate functions. That
// is the shape a DISTINCT lowers to: an aggregate-free GROUP BY.
func distinctGroupStages(stages []Stage, keys ...string) []*Stage {
	want := make(map[string]bool, len(keys))
	for _, k := range keys {
		want[strings.ToLower(k)] = true
	}
	var out []*Stage
	for i := range stages {
		s := &stages[i]
		if len(s.GroupByCols) != len(want) || len(s.AggSpecs) != 0 {
			continue
		}
		match := true
		for _, g := range s.GroupByCols {
			if !want[strings.ToLower(g)] {
				match = false
				break
			}
		}
		if match {
			out = append(out, s)
		}
	}
	return out
}

func TestDerivedDistinctEmitsADedupStage(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	tests := []struct {
		name string
		sql  string
		keys []string
	}{
		{
			// The reported shape. Before the fix the only aggregate stage was
			// the COUNT, grouping on nothing, reading the raw scan.
			name: "DISTINCT under COUNT",
			sql:  "SELECT COUNT(*) AS c FROM (SELECT DISTINCT o_orderstatus FROM orders) u",
			keys: []string{"o_orderstatus"},
		},
		{
			name: "DISTINCT under SUM, renamed",
			sql:  "SELECT SUM(k) AS s FROM (SELECT DISTINCT o_custkey AS k FROM orders) u",
			keys: []string{"o_custkey"},
		},
		{
			name: "DISTINCT on several columns",
			sql:  "SELECT COUNT(*) AS c FROM (SELECT DISTINCT o_orderstatus, o_orderpriority FROM orders) u",
			keys: []string{"o_orderstatus", "o_orderpriority"},
		},
		{
			name: "DISTINCT under an aggregate under a join",
			sql: `SELECT COUNT(*) AS c FROM (SELECT DISTINCT s_nationkey FROM supplier) u
			      JOIN nation ON u.s_nationkey = nation.n_nationkey`,
			keys: []string{"s_nationkey"},
		},
		{
			name: "GROUP BY over a derived DISTINCT",
			sql: `SELECT k, COUNT(*) AS c FROM (SELECT DISTINCT o_custkey AS k, o_orderstatus FROM orders) u
			      GROUP BY k`,
			keys: []string{"o_custkey", "o_orderstatus"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, tt.sql, 3)
			if got := distinctGroupStages(stages, tt.keys...); len(got) == 0 {
				t.Fatalf("no stage deduplicates on %v — the DISTINCT reached no worker at all;"+
					" the query would answer with every pre-dedup row\nstages:\n%s",
					tt.keys, describeStages(stages))
			}
			// The DISTINCT must not survive as a bare logical passthrough:
			// nothing downstream would apply it.
			assertNoUnstageableDistinct(t, cat, ctx, tt.sql)
		})
	}
}

// The root SELECT DISTINCT and a derived DISTINCT feeding a plain projection
// were already correct. They must stay correct — the rewrite widened from the
// root path to the whole tree, which is exactly the code path they use.
func TestRootAndProjectedDistinctStillDedup(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	for _, tt := range []struct {
		name string
		sql  string
		keys []string
	}{
		{"root SELECT DISTINCT", "SELECT DISTINCT o_orderstatus FROM orders", []string{"o_orderstatus"}},
		{"derived DISTINCT under a projection",
			"SELECT o_orderstatus FROM (SELECT DISTINCT o_orderstatus FROM orders) u",
			[]string{"o_orderstatus"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, tt.sql, 3)
			if got := distinctGroupStages(stages, tt.keys...); len(got) == 0 {
				t.Fatalf("no stage deduplicates on %v\nstages:\n%s", tt.keys, describeStages(stages))
			}
		})
	}
}

// A DISTINCT the rewrite cannot lower — an aggregate projection has no group
// key — must be REFUSED off the root path rather than passed through. Passing
// it through is the silent wrong answer #466 was; on the root path the
// coordinator's post-gather dedup still applies it, so that shape must NOT
// refuse.
func TestUnstageableDistinctIsRefusedNotDropped(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	t.Run("refused below an aggregate", func(t *testing.T) {
		err := planDistributedErr(t, cat, ctx,
			`SELECT COUNT(*) AS c FROM
			   (SELECT DISTINCT o_orderstatus, SUM(o_totalprice) AS s FROM orders GROUP BY o_orderstatus) u`)
		if !errors.Is(err, ErrDistinctDistributed) {
			t.Fatalf("planning succeeded or failed with the wrong error: %v\n"+
				"a DISTINCT nothing executes must be refused, not dropped", err)
		}
	})

	t.Run("root path is not refused", func(t *testing.T) {
		// The coordinator dedups this one after the gather.
		if err := planDistributedErr(t, cat, ctx,
			"SELECT DISTINCT o_orderstatus, COUNT(*) AS c FROM orders GROUP BY o_orderstatus"); err != nil {
			t.Fatalf("a root-path DISTINCT the coordinator dedups was refused: %v", err)
		}
	})

	t.Run("semi-join build dedup is not refused", func(t *testing.T) {
		// dedupSemiAntiBuildSide inserts a Distinct under the join. It is
		// planner-inserted, carries no user-visible semantics, and must
		// neither be rewritten nor refused.
		if err := planDistributedErr(t, cat, ctx,
			`SELECT o_orderkey FROM orders WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem)`); err != nil {
			t.Fatalf("a planner-inserted build-side dedup was refused: %v", err)
		}
	})
}

// assertNoUnstageableDistinct fails when the optimized plan still carries a
// DISTINCT off the root path — the state in which the DAG drops it.
func assertNoUnstageableDistinct(t *testing.T, cat *catalog.Catalog, ctx context.Context, sql string) {
	t.Helper()
	if err := planDistributedErr(t, cat, ctx, sql); err != nil {
		t.Fatalf("plan refused: %v", err)
	}
}

func planDistributedErr(t *testing.T, cat *catalog.Catalog, ctx context.Context, sql string) error {
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
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)
	planner := NewPlanner(cat)
	planner.WorkerCount = 3
	_, err = planner.PlanDistributed(ctx, logicalPlan)
	return err
}

// describeStages renders the stage list for a failure message.
func describeStages(stages []Stage) string {
	var b strings.Builder
	for _, s := range stages {
		b.WriteString("  ")
		b.WriteString(s.ID)
		b.WriteString(" type=")
		b.WriteString(s.Type)
		if len(s.GroupByCols) > 0 {
			b.WriteString(" group_by=")
			b.WriteString(strings.Join(s.GroupByCols, ","))
		}
		if len(s.AggSpecs) > 0 {
			names := make([]string, 0, len(s.AggSpecs))
			for _, a := range s.AggSpecs {
				names = append(names, a.Func+"("+a.InputCol+")")
			}
			b.WriteString(" aggs=")
			b.WriteString(strings.Join(names, ","))
		}
		b.WriteString("\n")
	}
	return b.String()
}
