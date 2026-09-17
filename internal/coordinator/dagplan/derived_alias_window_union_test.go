// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"
	"testing"
)

// Two more consumers of a derived table's SELECT-list alias, both of which
// used to fail LOUD on the DAG for a query the single-process pipeline
// answered (#490).

// TestDerivedAliasSortKeyOverAWindowProducer: a WINDOW stage forwards its
// input's columns and appends its own outputs, so a sort key naming a derived
// alias above one is settleable — the source column really is on the stream.
// The producer test used for the alias rewrite was the one written for
// MATERIALIZING a computed key, which needs a fragment that can carry an
// OpProject, and a window stage is not one; refusing both questions with one
// answer left the key spelled `k` and the task failed with `sort: key column
// "k" does not exist in the input schema`.
func TestDerivedAliasSortKeyOverAWindowProducer(t *testing.T) {
	stages := planStagesForRenameTest(t,
		`SELECT k, rn FROM (SELECT s_suppkey AS k, ROW_NUMBER() OVER (ORDER BY s_name) AS rn
		 FROM supplier) x ORDER BY k`)
	sort := sortStageOf(t, stages)
	if len(sort.SortKeys) != 1 {
		t.Fatalf("sort has %d keys, want 1", len(sort.SortKeys))
	}
	// Two repairs can settle this, and the invariant is that ONE of them
	// did: either the key is pointed at the source column
	// (resolveDerivedAliasSortKeys) or the window fragment now MATERIALIZES
	// the alias, because attachScanSelectProjections can attach a SELECT
	// list to a window producer since #656. The emitted-column check below
	// is the assertion either way; this one only says which happened.
	key := sort.SortKeys[0].Column
	if !strings.EqualFold(key, "s_suppkey") && !strings.EqualFold(key, "k") {
		t.Errorf("sort key %q, want the source column s_suppkey or the materialized alias k", key)
	}
	if len(sort.Dependencies) != 1 {
		t.Fatalf("sort has %d dependencies, want 1", len(sort.Dependencies))
	}
	idx := map[string]int{}
	for i := range stages {
		idx[stages[i].ID] = i
	}
	dep, ok := idx[sort.Dependencies[0]]
	if !ok {
		t.Fatalf("sort depends on %s, which is not a stage", sort.Dependencies[0])
	}
	emitted := emittedThroughPassThrough(stages, idx, &stages[dep])
	if _, ok := lookupEmittedColumn(emitted, key); !ok {
		t.Errorf("stage %s (%s) does not emit %q; it emits %v",
			stages[dep].ID, stages[dep].Type, key, emitted)
	}
}

// TestUnionArmProjectsThroughADerivedRename: a union arm's SELECT list is
// written against the arm's OUTPUT schema and the arm's stream carries SOURCE
// names, exactly like every other consumer. Projecting the alias verbatim
// failed with `column "k" does not exist in the input schema`.
func TestUnionArmProjectsThroughADerivedRename(t *testing.T) {
	stages := planStagesForRenameTest(t,
		`SELECT k FROM (SELECT s_suppkey AS k FROM supplier) x
		 UNION ALL SELECT n_nationkey FROM nation`)
	var union *Stage
	for i := range stages {
		if stages[i].Type == StageUnion {
			union = &stages[i]
			break
		}
	}
	if union == nil {
		t.Fatalf("no union stage in the plan")
	}
	if len(union.UnionArms) != 2 {
		t.Fatalf("union has %d arms, want 2", len(union.UnionArms))
	}
	specs := union.UnionArms[0].Projections
	if len(specs) != 1 {
		t.Fatalf("arm 0 projects %d columns, want 1", len(specs))
	}
	if strings.EqualFold(specs[0].Expr, "k") {
		t.Fatalf("arm 0 reads %q — the derived table's alias, which no stage emits", specs[0].Expr)
	}
	if !strings.EqualFold(specs[0].Expr, "s_suppkey") {
		t.Errorf("arm 0 reads %q, want the source column s_suppkey", specs[0].Expr)
	}
	// SQL takes the result names from the first arm, and that half is
	// unchanged: the arm still EMITS the alias.
	if !strings.EqualFold(specs[0].Name, "k") {
		t.Errorf("arm 0 emits %q, want the result column name k", specs[0].Name)
	}
}
