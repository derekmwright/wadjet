package physical

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// The two names a GROUP BY key travels under have to stay a PAIR: index-
// aligned, carried by exactly the stages whose fragment computes the key, and
// carried by no other.
//
// Both halves are asserted because either alone is satisfiable by a plan that
// cannot run. A stage with a published list and no resolution sends the worker
// back to re-deriving the key by parsing its published name, which is the
// defect the second field exists to remove; a stage with a resolution list it
// does not compute (a merge) would resolve a key against a partial's output by
// a spelling that names nothing there (ADR-0026 §2, #794).

// tpchPlansForCarrier plans every TPC-H query and returns each plan's stages.
func tpchPlansForCarrier(t *testing.T) map[int][]Stage {
	t.Helper()
	cat, ctx := setupTPCHCatalog(t)
	out := map[int][]Stage{}
	for qNum := 1; qNum <= 22; qNum++ {
		sql, ok := tpchPlanQueryMap[qNum]
		if !ok {
			continue
		}
		stages, err := planCarrierStages(t, cat, ctx, sql)
		if err != nil {
			continue // a refusal is a disposition, not a carrier fact
		}
		out[qNum] = stages
	}
	return out
}

func planCarrierStages(t *testing.T, cat *catalog.Catalog, ctx context.Context, sql string) ([]Stage, error) {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	lp, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("logical: %v", err)
	}
	ann := func(p *logical.Node) { NewPlanner(cat).AnnotateScanColumns(ctx, p) }
	ann(lp)
	node := logical.Optimize(lp, ann)
	planner := NewPlanner(cat)
	planner.WorkerCount = 4
	return planner.PlanDistributed(ctx, node)
}

// TestStageCarriesOneGroupKeyList is the invariant Stage.GroupByResolve's own
// doc comment states: a stage carries exactly one group-key list, the
// resolution list is index-aligned with it, and only a fragment that COMPUTES
// its keys carries one.
func TestStageCarriesOneGroupKeyList(t *testing.T) {
	plans := tpchPlansForCarrier(t)
	if len(plans) == 0 {
		t.Fatal("no TPC-H plan was produced — the carrier assertion saw nothing")
	}
	for qNum, stages := range plans {
		for i := range stages {
			s := &stages[i]
			lists := 0
			for _, l := range [][]string{s.GroupByCols, s.FusedAggGroupBy, s.ChainedAggGroupBy} {
				if len(l) > 0 {
					lists++
				}
			}
			if lists > 1 {
				t.Errorf("Q%02d stage %s carries %d group-key lists — GroupByResolve is index-"+
					"aligned with ONE of them and cannot say which", qNum, s.ID, lists)
			}
			keys := stageGroupKeyList(s)
			switch {
			case len(s.GroupByResolve) == 0:
				if stageComputesGroupKeys(s) && len(keys) > 0 {
					t.Errorf("Q%02d stage %s (%s) COMPUTES its %d group keys and carries no "+
						"resolution list — the worker is back to deriving the second name by "+
						"parsing the first (ADR-0026 §2)", qNum, s.ID, s.Type, len(keys))
				}
			case len(s.GroupByResolve) != len(keys):
				t.Errorf("Q%02d stage %s: %d resolutions against %d keys — the two lists are "+
					"index-aligned or they are nothing", qNum, s.ID, len(s.GroupByResolve), len(keys))
			case !stageComputesGroupKeys(s):
				t.Errorf("Q%02d stage %s (%s) carries a resolution list but does not compute its "+
					"keys — a merge reads a partial's output, where the two names are one",
					qNum, s.ID, s.Type)
			}
			for k, r := range s.GroupByResolve {
				if r.deferred() {
					t.Errorf("Q%02d stage %s key %d is still DEFERRED (%q/%q) — "+
						"resolveStageGroupKeys settles every one before planning returns",
						qNum, s.ID, k, r.Alias, r.Def)
				}
			}
		}
	}
}

// TestEveryComputedKeyResolvesAgainstItsProducer is the pass's own claim, and
// the gate that fails when it is reverted: the spelling a fragment resolves a
// key by NAMES SOMETHING that fragment's input carries.
//
// It is asserted over shapes whose key is a derived table's computed alias,
// because that is the class where the two names differ. Before
// resolveStageGroupKeys existed, every one of these carried the alias's
// DEFINING EXPRESSION — spelled over a column the join does not carry — and
// answered one NULL group over the whole table (#781, #794).
func TestEveryComputedKeyResolvesAgainstItsProducer(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	for _, sql := range []string{
		// The alias through a JOIN: the arm's projection materializes it, so
		// the stream carries `w` and the join does not carry `l_extendedprice`.
		`SELECT x.w AS w, COUNT(*) AS n FROM (SELECT l_orderkey, l_extendedprice * 3 AS w
		   FROM lineitem) x JOIN orders z ON x.l_orderkey = z.o_orderkey GROUP BY x.w`,
		// Two arms publishing ONE alias from different expressions, in both key
		// directions: only a per-arm answer can tell them apart.
		`SELECT y.w AS w, COUNT(*) AS n FROM (SELECT l_orderkey, l_extendedprice * 3 AS w
		   FROM lineitem) x JOIN (SELECT l_orderkey, l_extendedprice * 100 AS w FROM lineitem) y
		   ON x.l_orderkey = y.l_orderkey GROUP BY y.w`,
		`SELECT x.w AS w, COUNT(*) AS n FROM (SELECT l_orderkey, l_extendedprice * 3 AS w
		   FROM lineitem) x JOIN (SELECT l_orderkey, l_extendedprice * 100 AS w FROM lineitem) y
		   ON x.l_orderkey = y.l_orderkey GROUP BY x.w`,
		// No join: nothing materializes the alias, so the DEFINITION is the
		// resolution and the scan carries its columns.
		`SELECT x.w AS w, COUNT(*) AS n FROM (SELECT l_extendedprice * 3 AS w FROM lineitem) x
		   GROUP BY x.w`,
		// The key an aggregate DIRECTLY BELOW already publishes.
		`SELECT DISTINCT l_partkey + 1 AS k FROM lineitem GROUP BY l_partkey + 1`,
		// An ordinary computed key, which needs no second name at all.
		`SELECT l_partkey + 1 AS k, COUNT(*) AS n FROM lineitem GROUP BY l_partkey + 1`,
	} {
		stages, err := planCarrierStages(t, cat, ctx, sql)
		if err != nil {
			t.Errorf("planning refused a shape the model can state: %v\n  SQL: %s", err, sql)
			continue
		}
		idx := make(map[string]int, len(stages))
		for i := range stages {
			idx[stages[i].ID] = i
		}
		checked := 0
		for i := range stages {
			s := &stages[i]
			if len(s.GroupByResolve) == 0 || !stageComputesGroupKeys(s) {
				continue
			}
			in, _ := aggregateInputStreamColumns(stages, idx, s)
			names := make([]string, 0, len(in))
			for _, c := range in {
				if !c.Dropped {
					names = append(names, c.Name)
				}
			}
			for k, r := range s.GroupByResolve {
				checked++
				// §2c in the assertion too: a resolution that is a NAME is
				// looked up as one, and only a COMPUTED resolution is read as
				// structure. Parsing `l_partkey + 1` as arithmetic where the
				// aggregate below already publishes a COLUMN of that text is
				// the very confusion the Computed flag exists to end.
				if r.Computed {
					if !defResolvesOverStream(r.Expr, in) {
						t.Errorf("stage %s materializes key %d from %q, and its input carries %v "+
							"— the fragment evaluates that against a schema without those "+
							"columns and answers ONE NULL group\n  SQL: %s",
							s.ID, k, r.Expr, names, sql)
					}
					continue
				}
				if !streamCarriesName(in, r.Expr) {
					t.Errorf("stage %s resolves key %d by the NAME %q, and its input carries %v "+
						"— HashAggregate cannot find it and every row lands in one NULL group"+
						"\n  SQL: %s", s.ID, k, r.Expr, names, sql)
				}
			}
		}
		if checked == 0 {
			t.Errorf("no stage in this plan computes a group key — the assertion saw nothing"+
				"\n  SQL: %s", sql)
		}
	}
}

// streamCarriesName reports whether the modelled stream answers to name, with
// the runtime lookup's qualified↔bare tolerance.
func streamCarriesName(in []streamCol, name string) bool {
	emitted := make(map[string]string, len(in))
	for _, c := range in {
		if !c.Dropped {
			emitted[strings.ToLower(c.Name)] = c.Name
		}
	}
	return columnResolves(&plansql.ColRef{Column: name}, emitted)
}

// TestGroupKeyPublishedNameIsTheQuerysOwn is §2b's half: what the aggregate
// EMITS for a key is what the single-process operator emits for the same
// query, which is why one sort key, one HAVING term and one projection resolve
// on both engines.
func TestGroupKeyPublishedNameIsTheQuerysOwn(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	for _, tc := range []struct {
		sql  string
		want []string
	}{
		// A qualified bare key keeps its qualifier on the stage and is EMITTED
		// stripped, on both engines — the rule lives in exec.
		{`SELECT n1.n_name, COUNT(*) AS n FROM nation n1 GROUP BY n1.n_name`, []string{"n_name"}},
		// Two qualified keys that strip to one name keep their qualifiers.
		{`SELECT n1.n_name, n2.n_name, COUNT(*) AS c FROM nation n1 JOIN nation n2
		    ON n1.n_regionkey = n2.n_regionkey GROUP BY n1.n_name, n2.n_name`,
			[]string{"n1.n_name", "n2.n_name"}},
		// A derived key is emitted under its canonical text.
		{`SELECT l_partkey + 1 AS k, COUNT(*) AS n FROM lineitem GROUP BY l_partkey + 1`,
			[]string{"l_partkey + 1"}},
		// A key naming a derived table's plain rename is emitted under the
		// alias, not under the source column it is RESOLVED by.
		{`SELECT u.k, COUNT(*) AS n FROM (SELECT o_orderstatus AS k FROM orders) u GROUP BY u.k`,
			[]string{"k"}},
	} {
		stages, err := planCarrierStages(t, cat, ctx, tc.sql)
		if err != nil {
			t.Errorf("planning refused: %v\n  SQL: %s", err, tc.sql)
			continue
		}
		found := false
		for i := range stages {
			s := &stages[i]
			if !stageComputesGroupKeys(s) || len(s.GroupByResolve) == 0 {
				continue
			}
			found = true
			got := aggregateEmittedKeyNames(s)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("stage %s emits %v for its keys, want %v\n  SQL: %s",
					s.ID, got, tc.want, tc.sql)
			}
		}
		if !found {
			t.Errorf("no computing aggregate stage in the plan\n  SQL: %s", tc.sql)
		}
	}
}
