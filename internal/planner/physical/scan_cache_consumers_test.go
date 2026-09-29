// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// A SCAN THE DUPLICATE-SCAN CACHE WAS NOT SIZED FOR READS STORAGE (#1382
// #1418). The cache entry is keyed by TABLE NAME and holds the union of the
// columns of the scans mergeDuplicateScans counted in the statement's tree.
// An uncorrelated subquery's own plan is parsed and built at run time, by a
// child planner that shares the map, after the entry was sized: at v0.25.2
// its scan of the same table found the entry by name and replayed batches
// holding only the outer scans' columns, so `12 IN (SELECT q.val FROM t q)`
// above `t a JOIN t b` read no val and answered 0 rows, and the EXISTS
// spelling failed with `filter column "q.val" does not exist`. Here the two
// counted scans read `id`; the third scan of the table, not in the tree,
// asks for `val` and must get it — every row, with its values — whether it
// runs after the cache is filled or before anyone claimed it.
func TestScanCacheServesOnlyTheScansItCounted(t *testing.T) {
	ctx := context.Background()
	for _, order := range []string{"afterFill", "beforeClaim"} {
		t.Run(order, func(t *testing.T) {
			cat, _ := setupManyFiles(t, "cpt", 2, 5)
			p := NewPlanner(cat)
			scan := func(cols ...string) *logical.Node {
				return &logical.Node{Type: logical.NodeScan, TableName: "cpt", TableAlias: "a", RequiredColumns: cols}
			}
			a, b := scan("id"), scan("id")
			p.mergeDuplicateScans(&logical.Node{Type: logical.NodeJoin, Children: []*logical.Node{a, b}})
			defer p.releaseScanCache()
			drain := func(n *logical.Node) (int, map[string]bool) {
				t.Helper()
				src, ops, _, err := p.buildScan(ctx, n)
				if err != nil {
					t.Fatalf("buildScan: %v", err)
				}
				sink := &exec.CollectSink{}
				if err := (&exec.Pipeline{Source: src, Ops: ops, Sink: sink}).Run(ctx); err != nil {
					t.Fatalf("run %v: %v", n.RequiredColumns, err)
				}
				cols := map[string]bool{}
				for _, c := range sink.Schema() {
					cols[c.Name] = true
				}
				return len(sink.ToRows()), cols
			}
			if order == "afterFill" {
				drain(a)
			}
			rows, cols := drain(scan("val"))
			if rows != 10 || !cols["val"] {
				t.Fatalf("uncounted scan of cpt(val): %d rows, columns %v; want 10 rows carrying val", rows, cols)
			}
			if order == "beforeClaim" {
				// The counted scans still share one entry and answer.
				for _, n := range []*logical.Node{a, b} {
					if rows, cols := drain(n); rows != 10 || !cols["id"] {
						t.Fatalf("counted scan: %d rows, columns %v; want 10 rows carrying id", rows, cols)
					}
				}
			}
		})
	}
}
