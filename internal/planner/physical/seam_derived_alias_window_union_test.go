// SPDX-License-Identifier: MIT

package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// TestOwnsKeyRecognizesADerivedTablesOutputColumn: join-key SIDE assignment
// decides by column OWNERSHIP, and a derived table's output column belongs to
// no scan's column set. With ownership unanswerable for both sides the pair
// kept its positional order, and each key was then resolved against the arm
// that does not own it — invisible in a two-way join (the arms are
// symmetric), a loud `partitioned shuffle: key "y.b" not in schema` in a
// three-way one (#490).
func TestOwnsKeyRecognizesADerivedTablesOutputColumn(t *testing.T) {
	derived := func(alias, col, out string) *logical.Node {
		return &logical.Node{Type: logical.NodeProject,
			Projections: []logical.Projection{{Column: col, Expr: col, Alias: out}},
			Children: []*logical.Node{{
				Type: logical.NodeScan, TableName: "t" + alias, TableAlias: alias,
				ScanColumns: []string{col}, DerivedAliases: []string{alias},
			}},
		}
	}
	y := subtreeNamingOf(derived("y", "n_nationkey", "b"))
	z := subtreeNamingOf(derived("z", "r_regionkey", "c"))

	if !y.ownsKey("y.b") {
		t.Errorf("the y arm does not own y.b — its own output column")
	}
	if y.ownsKey("z.c") {
		t.Errorf("the y arm claims z.c — a qualifier naming nothing in it")
	}
	if !z.ownsKey("z.c") {
		t.Errorf("the z arm does not own z.c — its own output column")
	}
	if z.ownsKey("y.b") {
		t.Errorf("the z arm claims y.b")
	}

	// End to end: a pair written the other way round has to be swapped so
	// leftKeys name the probe child.
	left, right := []string{"z.c"}, []string{"y.b"}
	assignJoinKeySides(left, right, y, z)
	if left[0] != "y.b" || right[0] != "z.c" {
		t.Errorf("AssignJoinKeySides left=%v right=%v, want left=[y.b] right=[z.c]", left, right)
	}
}

// TestOwnsKeyStillRefusesTheOtherSelfJoinCopy is the guard the derived-scope
// fallback must not weaken: `n2.x` names n2's copy and no other, so a subtree
// holding only n1 does not own it.
func TestOwnsKeyStillRefusesTheOtherSelfJoinCopy(t *testing.T) {
	n1 := subtreeNamingOf(&logical.Node{Type: logical.NodeScan, TableName: "n", TableAlias: "n1",
		ScanColumns: []string{"id", "nm"}})
	if !n1.ownsKey("n1.nm") {
		t.Errorf("the n1 subtree does not own n1.nm")
	}
	if n1.ownsKey("n2.nm") {
		t.Errorf("the n1 subtree claims n2.nm — the other copy of a self-joined table")
	}
}
