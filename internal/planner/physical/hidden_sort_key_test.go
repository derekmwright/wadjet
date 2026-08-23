package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// #424 with #390, which is the pair the plan actually produces.
//
// #390's guard keeps a sort with a dependent as its own stage instead of
// folding it into a predecessor that dispatch may re-fan-out. That stage is
// then fed by a fragment nobody told to emit the sort's key: a derived
// table's ORDER BY over a column its SELECT list drops is materialized as
// __sortkey_N by the logical builder, and on the DAG a Project emits no
// stage. So every one of these plans must satisfy BOTH: the sort stage
// survives, and its key names a column its producer really ships.
func TestDerivedTableSortKeyResolvesToAnEmittedColumn(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	for _, tc := range []struct {
		name string
		sql  string
		// wantMaterialized: the key has no source column (a computed term),
		// so the producer must gain a projection that computes it. Otherwise
		// the key is expected to be renamed to its source column.
		wantMaterialized bool
	}{
		{
			name: "no limit",
			sql:  `SELECT COUNT(*) AS c FROM (SELECT s_suppkey FROM supplier ORDER BY s_acctbal DESC) t`,
		},
		{
			name: "with limit",
			sql: `SELECT COUNT(*) AS c FROM (
				SELECT s_suppkey FROM supplier ORDER BY s_acctbal DESC LIMIT 7) t`,
		},
		{
			name: "join",
			sql: `SELECT COUNT(*) AS c FROM (
				SELECT s_suppkey FROM supplier JOIN nation ON s_nationkey = n_nationkey
				ORDER BY s_acctbal DESC LIMIT 7) t`,
		},
		{
			name: "join consumer",
			sql: `SELECT COUNT(*) AS c FROM (
				SELECT s_suppkey FROM supplier ORDER BY s_acctbal DESC LIMIT 7) t
				JOIN supplier s2 ON t.s_suppkey = s2.s_suppkey`,
		},
		{
			name:             "computed key",
			sql:              `SELECT COUNT(*) AS c FROM (SELECT s_suppkey FROM supplier ORDER BY s_acctbal * 2 DESC LIMIT 7) t`,
			wantMaterialized: true,
		},
		{
			name:             "computed key over a join",
			sql:              `SELECT COUNT(*) AS c FROM (SELECT s_suppkey FROM supplier JOIN nation ON s_nationkey = n_nationkey ORDER BY LENGTH(s_name) DESC LIMIT 7) t`,
			wantMaterialized: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, tc.sql, 3)
			byID := map[string]*Stage{}
			for i := range stages {
				byID[stages[i].ID] = &stages[i]
			}

			// #390: the sort feeds a stage, not the terminal gather, so it
			// must not have been folded away.
			sortID := findStageOfType(stages, "sort")
			if sortID == "" {
				t.Fatal("the sort stage was folded away even though a stage reads its output (#390)")
			}
			sort := byID[sortID]
			if len(sort.SortKeys) == 0 {
				t.Fatal("the sort stage carries no keys")
			}
			if len(sort.Dependencies) != 1 {
				t.Fatalf("sort %s has %d dependencies, want 1", sortID, len(sort.Dependencies))
			}
			producer := byID[sort.Dependencies[0]]
			if producer == nil {
				t.Fatalf("sort %s depends on unknown stage %q", sortID, sort.Dependencies[0])
			}

			// #424: every key names something the producer emits.
			emitted := stageEmittedColumns(producer)
			for _, k := range sort.SortKeys {
				if _, ok := emitted[strings.ToLower(k.Column)]; !ok {
					t.Fatalf("sort %s keys on %q, which stage %s (%s) does not emit "+
						"(emits %v) — the task fails with `key column %q does not exist "+
						"in the input schema` (#424)",
						sortID, k.Column, producer.ID, producer.Type, emitted, k.Column)
				}
				if tc.wantMaterialized {
					if !logical.IsHiddenSortColumn(k.Column) {
						t.Errorf("key %q was renamed to a source column, but a computed "+
							"term has none — it must be projected under its hidden name", k.Column)
					}
					if len(producer.ProjectExprs) == 0 {
						t.Errorf("stage %s carries no projection, so nothing computes %q",
							producer.ID, k.Column)
					}
					continue
				}
				if logical.IsHiddenSortColumn(k.Column) {
					// Legal only when some pass really did materialize it.
					if len(producer.ProjectExprs) == 0 {
						t.Errorf("key %q is still synthetic and stage %s projects nothing",
							k.Column, producer.ID)
					}
				}
			}
		})
	}
}

// The controls: shapes whose sort key already resolved must keep the plan
// they had. resolveHiddenSortKeys runs after attachScanSelectProjections
// precisely so it repairs only what that pass left unresolved — a pass that
// also rewrote the outermost SELECT's plans would take the computed columns
// attachScanSelectProjections materializes with it.
func TestHiddenSortKeyPassLeavesResolvedPlansAlone(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	for _, tc := range []struct {
		name    string
		sql     string
		wantKey string
		// wantProjected: names attachScanSelectProjections must still emit
		// on the producing scan.
		wantProjected []string
	}{
		{
			// The root sort: attachScanSelectProjections materializes the
			// hidden key, so the repair pass must find nothing to do.
			name:          "root sort over a dropped column",
			sql:           `SELECT s_suppkey FROM supplier ORDER BY s_acctbal DESC LIMIT 7`,
			wantKey:       "__sortkey_0",
			wantProjected: []string{"s_suppkey", "__sortkey_0"},
		},
		{
			// The shape that would break if the key were renamed early: the
			// SELECT list also carries a computed column, and the projection
			// that computes it is the same one carrying the hidden key.
			name:          "root sort beside a computed select item",
			sql:           `SELECT s_suppkey, LENGTH(s_name) AS l FROM supplier ORDER BY s_acctbal DESC LIMIT 7`,
			wantKey:       "__sortkey_0",
			wantProjected: []string{"s_suppkey", "l", "__sortkey_0"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, tc.sql, 3)
			sortID := findStageOfType(stages, "sort")
			if sortID == "" {
				t.Fatal("no sort stage")
			}
			var sort, scan *Stage
			for i := range stages {
				switch {
				case stages[i].ID == sortID:
					sort = &stages[i]
				case stages[i].Type == StageScan:
					scan = &stages[i]
				}
			}
			if got := sort.SortKeys[0].Column; got != tc.wantKey {
				t.Errorf("sort key = %q, want %q", got, tc.wantKey)
			}
			var got []string
			for _, p := range scan.ProjectExprs {
				got = append(got, p.Name)
			}
			if strings.Join(got, ",") != strings.Join(tc.wantProjected, ",") {
				t.Errorf("scan projects %v, want %v", got, tc.wantProjected)
			}
		})
	}
}
