package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// A derived table's SELECT-list alias, at the consumers that resolve a name
// on the DAG.
//
// A Project emits no stage, so the rename never happens on the DAG: streams
// carry SOURCE column names and each consumer resolves the alias back through
// the plan. The invariant every test below asserts is the same one
// TestAliasedSortKeyResolvesToGroupedColumn asserts for the aggregate case:
// every key a stage dispatches must name a column the stage it reads really
// produces. The answers themselves are gated end-to-end by the two-path suite
// (benchmarks/tpch) and against PostgreSQL by the differential oracle.

// stageEmits reports whether any stage in the plan produces col — through a
// projection it computes, its pruned output list, or its read set.
func stageEmits(stages []Stage, id, col string) bool {
	for i := range stages {
		if stages[i].ID != id {
			continue
		}
		_, ok := lookupEmittedColumn(stageEmittedColumns(&stages[i]), col)
		return ok
	}
	return false
}

// sortStageOf returns the first standalone sort stage, which is where a
// derived table's ORDER BY lands.
func sortStageOf(t *testing.T, stages []Stage) *Stage {
	t.Helper()
	for i := range stages {
		if stages[i].Type == StageSort {
			return &stages[i]
		}
	}
	t.Fatalf("no sort stage in the plan")
	return nil
}

// TestDerivedTableAliasSortKeyNamesAnEmittedColumn: the sort key a derived
// table's ORDER BY produces has to name a column its producing stage emits,
// and for a SHADOWING alias it has to name the ALIASED column rather than the
// base one that happens to share the spelling (#468, and #467's loud half).
func TestDerivedTableAliasSortKeyNamesAnEmittedColumn(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		// wantKeys is the exact sort-key column list the sort stage must
		// dispatch. Every entry is also checked against what the stage the
		// sort reads actually emits.
		wantKeys []string
	}{
		{
			// #467 repro 1: ORDER BY resolves its second term to the SELECT
			// alias `k`, which no stage emits — the task failed with
			// `sort: key column "k" does not exist in the input schema`.
			// The key lands on the SCAN's spelling: the alias resolves to
			// `s1.s_suppkey` in the plan and lookupEmittedColumn maps it to
			// the bare name the fragment ships.
			name: "alias in the second sort term",
			sql: `SELECT k FROM (SELECT s1.s_suppkey AS k, s1.s_name FROM supplier s1
				ORDER BY s1.s_name, s1.s_suppkey DESC) x`,
			wantKeys: []string{"s1.s_name", "s_suppkey"},
		},
		{
			// #467 repro 2: both terms aliased.
			name: "alias in both sort terms",
			sql: `SELECT k, nm FROM (SELECT s_suppkey AS k, s_name AS nm FROM supplier s1
				ORDER BY nm, s1.s_suppkey DESC) x`,
			wantKeys: []string{"s_name", "s_suppkey"},
		},
		{
			// The consumer is an aggregate rather than the gather, so
			// attachScanSelectProjections never looks at this sort at all.
			name: "derived sort under an aggregate",
			sql: `SELECT SUM(k) AS c FROM (SELECT s_suppkey AS k FROM supplier s1
				ORDER BY k DESC LIMIT 7) t`,
			wantKeys: []string{"s_suppkey"},
		},
		{
			// #468: the alias SHADOWS a base column of the same relation.
			// The name exists on both sides, so nothing errored — the DAG
			// simply keyed on the base s_suppkey where PostgreSQL orders by
			// the aliased s_acctbal.
			name: "alias shadowing a base column",
			sql: `SELECT real_key FROM (SELECT s_acctbal AS s_suppkey, s_suppkey AS real_key
				FROM supplier ORDER BY s_suppkey DESC) x`,
			wantKeys: []string{"s_acctbal"},
		},
		{
			// Two-level nesting under an AGGREGATE, so nothing can
			// materialize the alias and the chain j → k → s_nationkey has
			// to be walked all the way down.
			name: "chained rename under an aggregate",
			sql: `SELECT SUM(j) AS c FROM
				(SELECT k AS j FROM (SELECT s_nationkey AS k FROM supplier) x
				 ORDER BY k DESC LIMIT 7) y`,
			wantKeys: []string{"s_nationkey"},
		},
		{
			// The same nesting feeding the GATHER instead: here
			// attachScanSelectProjections DOES materialize the outer name on
			// the scan (the #316 path), so the key correctly stays `j` and
			// this pass must leave it alone. The emitted-column check below
			// is what proves the two decisions are consistent.
			name: "chained rename materialized at the producer",
			sql: `SELECT j FROM (SELECT k AS j FROM (SELECT s_suppkey AS k, s_name FROM supplier) x
				ORDER BY k DESC) y`,
			wantKeys: []string{"j"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stages := planStagesForRenameTest(t, tc.sql)
			sort := sortStageOf(t, stages)
			var got []string
			for _, k := range sort.SortKeys {
				got = append(got, k.Column)
			}
			if len(got) != len(tc.wantKeys) {
				t.Fatalf("sort keys %v, want %v", got, tc.wantKeys)
			}
			for i, want := range tc.wantKeys {
				if !strings.EqualFold(got[i], want) {
					t.Errorf("sort key %d = %q, want %q (keys: %v)", i, got[i], want, got)
				}
			}
			if len(sort.Dependencies) != 1 {
				t.Fatalf("sort stage has %d dependencies, want 1", len(sort.Dependencies))
			}
			for _, k := range sort.SortKeys {
				if !stageEmits(stages, sort.Dependencies[0], k.Column) {
					t.Errorf("sort key %q names no column stage %s emits — the task fails with "+
						"`sort: key column %q does not exist in the input schema`",
						k.Column, sort.Dependencies[0], k.Column)
				}
			}
		})
	}
}

// TestRootSelectAliasStillMaterializesOnTheProducer is the other side of the
// same decision: where attachScanSelectProjections DOES put the alias on the
// producing fragment (the outermost SELECT list, #316), the sort must keep
// keying on the alias. resolveDerivedAliasSortKeys must not "repair" a key
// that is already right — doing so would point it at a source column the
// narrowing OpProject no longer emits.
func TestRootSelectAliasStillMaterializesOnTheProducer(t *testing.T) {
	stages := planStagesForRenameTest(t,
		`SELECT s_suppkey AS k FROM supplier ORDER BY k DESC`)
	sort := sortStageOf(t, stages)
	if len(sort.SortKeys) != 1 || !strings.EqualFold(sort.SortKeys[0].Column, "k") {
		t.Fatalf("sort keys %v, want the alias [k] — the producing scan projects it", sort.SortKeys)
	}
	if !stageEmits(stages, sort.Dependencies[0], "k") {
		t.Errorf("stage %s does not emit the alias the sort keys on", sort.Dependencies[0])
	}
}

// TestDerivedScopeBareNameOnlyStripsInsideItsOwnScope is the safety rule the
// whole family rests on. Dropping a qualifier unconditionally would resolve
// `SUM(t.c)` over `t JOIN (SELECT d AS c FROM u) v` to `d` — a silently
// different answer — so the qualifier may only be dropped in a subtree that
// contains the relation it names, which is exactly the derived table's own
// scope (BuildFromTable's setSubtreeAlias stamps the alias onto every scan
// below it).
func TestDerivedScopeBareNameOnlyStripsInsideItsOwnScope(t *testing.T) {
	derived := func(alias string) *logical.Node {
		return &logical.Node{Type: logical.NodeProject,
			Projections: []logical.Projection{{Column: "d", Expr: "d", Alias: "c"}},
			Children: []*logical.Node{
				{Type: logical.NodeScan, TableName: "u", TableAlias: alias},
			}}
	}
	for _, tc := range []struct {
		name    string
		ref     string
		subtree *logical.Node
		want    string
	}{
		{"qualifier names the derived table", "v.c", derived("v"), "c"},
		{"qualifier names another relation", "t.c", derived("v"), ""},
		{"qualifier is the bare table name", "u.c",
			&logical.Node{Type: logical.NodeScan, TableName: "u"}, "c"},
		{"unqualified reference", "c", derived("v"), ""},
		{"trailing dot", "v.", derived("v"), ""},
		{"leading dot", ".c", derived("v"), ""},
		{"nil subtree", "v.c", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := derivedScopeBareName(tc.ref, tc.subtree); got != tc.want {
				t.Errorf("derivedScopeBareName(%q) = %q, want %q", tc.ref, got, tc.want)
			}
		})
	}

}
