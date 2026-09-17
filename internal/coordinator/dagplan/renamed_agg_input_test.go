// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// TestAggregateOutputNameFollowsRename pins the other half: what a sort keyed
// on the alias must name.
//
// The answer is the key's PUBLISHED name, and it changed with ADR-0026 §2's
// two-name carrier. It used to be the source column `o_orderstatus`, because a
// stage published its keys under the same spelling the worker computed them
// from and a rename Project emits no stage of its own (#355). Now the
// resolution spelling rides beside the published one, so the aggregate finds
// the key by `o_orderstatus` and emits it as `k` — the same column name the
// single-process aggregate emits for the same query, which is what lets one
// sort key be resolved on both engines (§2b).
//
// Both halves are asserted here. Asserting the published name alone would pass
// just as happily if the stage had stopped carrying a resolution at all, and
// the key would then reach the worker spelled over a column the fragment's
// input does not have.
func TestAggregateOutputNameFollowsRename(t *testing.T) {
	scan := logical.NewScan("orders", "")
	scan.ScanColumns = []string{"o_orderstatus"}
	inner := &logical.Node{Type: logical.NodeProject,
		Projections: []logical.Projection{{Alias: "k", Column: "o_orderstatus", Expr: "o_orderstatus"}},
		Children:    []*logical.Node{scan}}
	agg := &logical.Node{Type: logical.NodeAggregate, GroupBy: []string{"k"},
		AggExprs: []logical.AggExpr{{Func: "count", OutputCol: "c"}},
		Children: []*logical.Node{inner}}

	got, ok := aggregateOutputName(agg, "k")
	if !ok {
		t.Fatal("group key \"k\" not recognized as an aggregate output")
	}
	if got != "k" {
		t.Errorf("aggregateOutputName = %q, want %q — the aggregate publishes the key under the "+
			"name the query wrote, so a sort keyed on the alias resolves to it", got, "k")
	}
	published, resolve := physical.GroupKeyNames(agg, inner)
	if len(published) != 1 || published[0] != "k" {
		t.Errorf("published names %v, want [k]", published)
	}
	if len(resolve) != 1 || resolve[0].Expr != "o_orderstatus" || resolve[0].Computed {
		t.Errorf("resolution %v, want [{o_orderstatus false}] — the fragment reads the source "+
			"column, because the rename Project emits no stage of its own (#355)", resolve)
	}
}
