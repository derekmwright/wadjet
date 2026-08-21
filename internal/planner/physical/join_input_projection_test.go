package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// Regression for #383: a join input that is a subquery with a COMPUTED
// projection must materialize the computed column into its producing scan
// fragment.
//
// walkStages treats an ordinary Project as a passthrough, so the computed
// column existed nowhere on the DAG: the scan stage carried it as a phantom
// read (which the parquet projection guard answered by reverting to full
// width), the build/probe files never held it, and everything downstream
// that read it — an outer join's ON residual (#358), a projected output, a
// sort key — saw NULL or a missing column, silently. Renames were already
// resolved through per consumer (#355 aggregates, #313 sort keys, join keys
// via resolveShuffleKey); a computed value has no source column to resolve
// TO, so absorbComputedSubqueryProjection materializes it at the source
// instead (Stage.ProjectExprs → OpProject, the #169 machinery).
func TestJoinInputComputedProjection(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	const buildComputed = `SELECT n.n_name, r.rk2 FROM nation n
		LEFT JOIN (SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region) r
		ON n.n_regionkey = r.r_regionkey AND n.n_nationkey > r.rk2
		ORDER BY n.n_name`

	// scanStageOf returns the single scan stage for table, failing the test
	// when it is absent or duplicated.
	scanStageOf := func(t *testing.T, stages []Stage, table string) Stage {
		t.Helper()
		var found []Stage
		for _, s := range stages {
			if s.Type == StageScan && s.TableName == table {
				found = append(found, s)
			}
		}
		if len(found) != 1 {
			t.Fatalf("want exactly 1 scan of %s, got %d in %v", table, len(found), stageTypeIDs(stages))
		}
		return found[0]
	}

	// assertComputed requires the stage to carry a projection computing
	// name, alongside a passthrough for every remaining read column, and
	// the read set to no longer list the phantom alias.
	assertComputed := func(t *testing.T, s Stage, name, wantExpr string) {
		t.Helper()
		var got *ProjectExprSpec
		for i := range s.ProjectExprs {
			if strings.EqualFold(s.ProjectExprs[i].Name, name) {
				got = &s.ProjectExprs[i]
			}
		}
		if got == nil {
			t.Fatalf("scan %s carries no projection for %q: %+v", s.ID, name, s.ProjectExprs)
		}
		if !strings.EqualFold(got.Expr, wantExpr) {
			t.Errorf("%s projected as %q, want %q", name, got.Expr, wantExpr)
		}
		if got.Type == 0 {
			t.Errorf("%s carries no declared type — the worker builds the output vector from it (#333)", name)
		}
		for _, c := range s.Columns {
			if strings.EqualFold(c, name) {
				t.Errorf("read set still lists the phantom alias %q: %v — the parquet projection "+
					"guard reverts to full width on any unknown name", name, s.Columns)
			}
		}
		for _, c := range s.Columns {
			covered := false
			for _, sp := range s.ProjectExprs {
				if strings.EqualFold(sp.Name, c) {
					covered = true
					break
				}
			}
			if !covered {
				t.Errorf("read column %q is not passed through the projection — OpProject narrows "+
					"to exactly its projections, so a consumer resolving it by source name loses it", c)
			}
		}
	}

	t.Run("broadcast build side", func(t *testing.T) {
		stages := sqlToStages(t, cat, ctx, buildComputed, 3)
		scan := scanStageOf(t, stages, "region")
		assertComputed(t, scan, "rk2", "nullif(r_regionkey, 2)")
		// The declared build schema is the empty-side answer (#348): the
		// computed column must be present there too, or a LEFT join over an
		// empty build shapes rows without it.
		join := onlyStageOfType(t, stages, "broadcast_join")
		found := false
		for _, c := range join.JoinBuildSchema {
			if strings.EqualFold(c.Name, "rk2") {
				found = true
			}
		}
		if !found {
			t.Errorf("JoinBuildSchema misses the computed rk2: %+v", join.JoinBuildSchema)
		}
	})

	t.Run("shuffle build side", func(t *testing.T) {
		// Broadcast disabled: the same query in the hash-shuffle regime.
		// The exchange reads the scan's materialized output, so the scan
		// must still carry the projection.
		parsed, err := plansql.Parse(buildComputed)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		selectInfo, err := plansql.ExtractSelect(parsed)
		if err != nil {
			t.Fatalf("extract: %v", err)
		}
		plan, err := logical.BuildFromSelect(selectInfo)
		if err != nil {
			t.Fatalf("logical plan: %v", err)
		}
		annotate := func(n *logical.Node) { NewPlanner(cat).AnnotateScanColumns(ctx, n) }
		annotate(plan)
		plan = logical.Optimize(plan, annotate)
		p := NewPlanner(cat)
		p.WorkerCount = 3
		p.BroadcastBytesThreshold = -1
		stages, err := p.PlanDistributed(ctx, plan)
		if err != nil {
			t.Fatalf("PlanDistributed: %v", err)
		}
		if n := len(stagesOfType(stages, StageExchangeRepartition)); n == 0 {
			t.Fatalf("broadcast disabled but no exchange-repartition emitted: %v", stageTypeIDs(stages))
		}
		scan := scanStageOf(t, stages, "region")
		assertComputed(t, scan, "rk2", "nullif(r_regionkey, 2)")
	})

	t.Run("probe side", func(t *testing.T) {
		stages := sqlToStages(t, cat, ctx, `SELECT nx.n_name, nx.nk2, r.r_name
			FROM (SELECT n_name, n_regionkey, NULLIF(n_nationkey, 3) AS nk2 FROM nation) nx
			JOIN region r ON nx.n_regionkey = r.r_regionkey ORDER BY nx.n_name`, 3)
		scan := scanStageOf(t, stages, "nation")
		assertComputed(t, scan, "nk2", "nullif(n_nationkey, 3)")
	})

	t.Run("sort over a computed subquery column", func(t *testing.T) {
		stages := sqlToStages(t, cat, ctx, `SELECT rk2
			FROM (SELECT NULLIF(r_regionkey, 2) AS rk2 FROM region) t ORDER BY rk2`, 3)
		scan := scanStageOf(t, stages, "region")
		assertComputed(t, scan, "rk2", "nullif(r_regionkey, 2)")
	})

	// Decline shapes: the pass is additive-only and scoped — everything it
	// does not handle keeps its exact pre-#383 plan.
	t.Run("rename-only subquery is left alone", func(t *testing.T) {
		stages := sqlToStages(t, cat, ctx, `SELECT n.n_name, r.rn FROM nation n
			JOIN (SELECT r_regionkey, r_name AS rn FROM region) r
			ON n.n_regionkey = r.r_regionkey ORDER BY n.n_name`, 3)
		scan := scanStageOf(t, stages, "region")
		if len(scan.ProjectExprs) != 0 {
			t.Errorf("rename-only subquery grew a projection %+v — renames resolve through, "+
				"and materializing them would change the DAG's source-name convention", scan.ProjectExprs)
		}
	})

	t.Run("aggregate over a computed subquery keeps the #355 route", func(t *testing.T) {
		stages := sqlToStages(t, cat, ctx,
			`SELECT MAX(n) AS m FROM (SELECT o_custkey * 2 AS n FROM orders) t`, 3)
		for _, s := range stages {
			if s.Type == StageScan && len(s.ProjectExprs) != 0 {
				t.Errorf("aggregate-input subquery grew a scan projection %+v — that face rides "+
					"resolveAggInputName's InputExpr, and a second materialization would perturb "+
					"every #355-shaped plan", s.ProjectExprs)
			}
		}
	})

	t.Run("top-level computed SELECT under a sort is attachScanSelectProjections territory", func(t *testing.T) {
		stages := sqlToStages(t, cat, ctx,
			`SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation ORDER BY k, n_name`, 3)
		scan := scanStageOf(t, stages, "nation")
		// That pass projects exactly the SELECT list under final names; the
		// absorb's signature — a passthrough of the whole read set — must
		// not appear.
		if len(scan.ProjectExprs) != 2 {
			t.Errorf("output projection = %+v, want exactly the 2-item SELECT list "+
				"(attachScanSelectProjections)", scan.ProjectExprs)
		}
	})
}

func stagesOfType(stages []Stage, typ string) []Stage {
	var out []Stage
	for _, s := range stages {
		if s.Type == typ {
			out = append(out, s)
		}
	}
	return out
}
