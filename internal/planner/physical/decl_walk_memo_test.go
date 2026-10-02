// SPDX-License-Identifier: MIT

package physical

import (
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// nestedDerivedPlan is `SELECT v FROM (… (SELECT BITWISE_AND(id, 3) AS v
// FROM users) …)` nested depth deep, as the logical builder shapes it: one
// Project per derived table over the base Project and the Scan.
func nestedDerivedPlan(t *testing.T, depth int) (root, base *logical.Node, nodes int) {
	t.Helper()
	parse := func(s string) plansql.Node {
		n, err := plansql.ParseExpression(s)
		if err != nil {
			t.Fatalf("parse %q: %v", s, err)
		}
		return n
	}
	scan := &logical.Node{Type: logical.NodeScan, TableName: "users",
		ScanColTypes: map[string]parquet.TypeID{"id": parquet.TypeInt32}}
	base = &logical.Node{Type: logical.NodeProject, Children: []*logical.Node{scan},
		Projections: []logical.Projection{{Expr: "BITWISE_AND(id, 3)", Alias: "v", ASTExpr: parse("BITWISE_AND(id, 3)")}}}
	n := base
	for i := 0; i < depth; i++ {
		n = &logical.Node{Type: logical.NodeProject, Children: []*logical.Node{n},
			Projections: []logical.Projection{{Column: "v", Expr: "v", Alias: "v", ASTExpr: parse("v")}}}
	}
	return n, base, depth + 2
}

// TestDeclWalkComputesEachNodeOnce pins the declaration walks' memo: one walk
// computes each node's carrier, (p,s), shapes and integer width ONCE. Before
// it a Project's childDecls re-ran all three walks on the child, and each of
// those ran childDecls on ITS child, so `SELECT v FROM (…)` nested sixteen
// deep planned in minutes (b69c2412; TestTCPFlagPlanningDepth's bound).
func TestDeclWalkComputesEachNodeOnce(t *testing.T) {
	for _, depth := range []int{1, 8, 16, 24} {
		t.Run(fmt.Sprintf("depth%d", depth), func(t *testing.T) {
			root, base, nodes := nestedDerivedPlan(t, depth)
			want := emittedColDecls(base)
			w := newDeclWalk()
			seen := map[string]int{}
			total := 0
			w.computed = func(kind string, n *logical.Node) {
				seen[fmt.Sprintf("%s/%p", kind, n)]++
				total++
			}
			d := w.emittedColDecls(root)
			for k, c := range seen {
				if c != 1 {
					t.Errorf("%s computed %d times in one walk, want 1", k, c)
				}
			}
			// Four memoized walks, each at most once per node.
			if total > 4*nodes {
				t.Errorf("depth %d: %d computations over %d nodes, want <= %d", depth, total, nodes, 4*nodes)
			}
			// Every level forwards v, so the root declares it as the base
			// Project does.
			if d.Types["v"] != want.Types["v"] || d.intWidth["v"] != want.intWidth["v"] ||
				d.Dec["v"] != want.Dec["v"] || want.intWidth["v"] != intWidth4 {
				t.Errorf("root declares v as %v/%v/%v, base as %v/%v/%v (want int4)",
					d.Types["v"], d.intWidth["v"], d.Dec["v"], want.Types["v"], want.intWidth["v"], want.Dec["v"])
			}
		})
	}
}

// TestDeclWalkLinearInDepth is the same pin on the package-level spellings a
// caller without a walk uses: each starts a walk of its own, so a 24-deep
// derived table is declared in a blink (2^24 node visits without the memo).
func TestDeclWalkLinearInDepth(t *testing.T) {
	root, base, _ := nestedDerivedPlan(t, 24)
	done := make(chan ColDecls, 1)
	start := time.Now()
	go func() {
		d := emittedColDecls(root)
		_ = declaredOutputSchema(root, nil)
		done <- d
	}()
	select {
	case d := <-done:
		t.Logf("depth 24 declared in %s", time.Since(start))
		if want := emittedColTypes(base)["v"]; d.Types["v"] != want {
			t.Fatalf("root declares v %v, base %v", d.Types["v"], want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("declaring a 24-deep derived table took more than 2s: the walks are not linear in depth")
	}
}
