package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// renameCollidingSlots renumbers the planner's own hidden slots past any
// STORED column of the same name.
//
// A table written before the namespace was reserved — or by any binary that
// did not enforce it — may carry a column called `__win_0`. Such a table stays
// READABLE: refusing it at read time made every query against it fail,
// `SELECT *` included, which is a trap rather than a guard rail (the DDL and
// ingest doors refuse the name being CREATED, which is where the reservation
// belongs). Readable means the planner and the stored column can meet in one
// query, and then the planner's slot has to move:
//
//	SELECT __win_0, SUM(id) OVER () AS w FROM oldtab
//
// Here the window mints `__win_0`, the scan emits a column of that name, and
// exec.Window appends its output beside it — #694's collision exactly, with
// the planner on the other side of it. Renumbering the SLOT is the repair,
// because the stored column is the one the user can see and name.
//
// It runs after AnnotateScanColumns, which is what puts a table's real column
// list on the Scan node; before that pass there is no schema to collide with.
func renameCollidingSlots(root *logical.Node) {
	stored := map[string]bool{}
	collectStoredNames(root, stored)
	if len(stored) == 0 {
		return
	}
	// Only a stored name INSIDE a reserved family can collide with a slot.
	hot := false
	for name := range stored {
		if plansql.ReservedSlotFamily(name) != "" {
			hot = true
			break
		}
	}
	if !hot {
		return // the overwhelmingly common case, one map walk
	}
	// taken is every window-slot name this QUERY already uses — the ones the
	// builder minted, renamed or not — as well as the stored ones. Searching
	// only the stored set renumbered one window onto ANOTHER window's slot:
	// with `__win_0` stored, window #1 moved to the first non-stored name,
	// `__win_1`, which window #2 already held; #2 was not renamed because
	// `__win_1` is not stored, both wrote `__win_1`, and the by-name projection
	// handed the second window the first one's value. Silently, and on the
	// single-process path only, so it was a two-path divergence as well as a
	// wrong number.
	alloc := plansql.NewSlotAllocator()
	for k := range stored {
		alloc.Seed(k)
	}
	seedWindowSlotNames(root, alloc)

	// The rename is SCOPED to the subtree that minted the slot.
	//
	// One global map keyed by the old name cannot express two siblings, and
	// two siblings are ordinary SQL: `(SELECT SUM(plain) OVER () AS w FROM t) p
	// JOIN (SELECT SUM(id) OVER () AS w FROM t) q` mints `__win_0` in BOTH
	// blocks. A map holding `__win_0 -> …` has room for one of them, and
	// applying it across the whole tree rewrote the OTHER block's projection to
	// a slot its own window never wrote: the single-process path failed with
	// `column "__win_2" does not exist in the input schema` and the DAG handed
	// both outputs one window's value.
	//
	// SlotAllocator fixed the collision WITHIN one scope; this is the same
	// defect one level out, and the fix is the same idea applied to the map.
	// walk returns the renames minted at or below a node that no ancestor has
	// consumed yet. Each is applied to the node's own fields on the way up, so
	// the Project above a Window (however many pass-throughs are between them)
	// sees it — and a node with TWO OR MORE children is the BOUNDARY: it
	// applies each child's map to that child alone and returns nothing, so a
	// sibling's map can never reach across.
	var walk func(n *logical.Node) map[string]string
	walk = func(n *logical.Node) map[string]string {
		if n == nil {
			return nil
		}
		pending := map[string]string{}
		for _, c := range n.Children {
			m := walk(c)
			if len(m) == 0 {
				continue
			}
			applySlotRename(c, m)
			if len(n.Children) > 1 {
				// A join, a set operation: consume the arm's map here rather
				// than letting it escape into a sibling.
				applySlotRenameNodeOnly(n, m)
				continue
			}
			for k, v := range m {
				pending[k] = v
			}
		}
		if n.Type == logical.NodeWindow {
			for i := range n.WindowExprs {
				out := strings.ToLower(n.WindowExprs[i].OutputCol)
				if out == "" || !stored[out] {
					continue
				}
				fresh, ok := alloc.Next(plansql.SlotWindowOutput)
				if !ok {
					continue // the family is exhausted; leave the plan as it was
				}
				pending[out] = fresh
				n.WindowExprs[i].OutputCol = fresh
			}
		}
		if len(pending) > 0 {
			applySlotRenameNodeOnly(n, pending)
		}
		return pending
	}
	if m := walk(root); len(m) > 0 {
		applySlotRename(root, m)
	}
}

// collectStoredNames gathers every column name a base table in the plan really
// stores, lowercased. ScanColumns is AnnotateScanColumns' record of the
// catalog schema, so this is the STORED set and not the projected one.
func collectStoredNames(n *logical.Node, out map[string]bool) {
	if n == nil {
		return
	}
	for _, c := range n.ScanColumns {
		out[strings.ToLower(c)] = true
	}
	for _, c := range n.Children {
		collectStoredNames(c, out)
	}
}

// applySlotRename repoints every reference to a renamed slot: the SELECT-list
// projection that publishes it, and the nested-window rewrite's ColRef inside
// a larger expression.
func applySlotRename(n *logical.Node, rename map[string]string) {
	if n == nil {
		return
	}
	applySlotRenameNodeOnly(n, rename)
	for _, c := range n.Children {
		applySlotRename(c, rename)
	}
}

// applySlotRenameNodeOnly repoints this node's own references and does not
// descend. It is what lets a rename be applied along the ancestor chain up to
// its consumer without crossing into a sibling subtree.
func applySlotRenameNodeOnly(n *logical.Node, rename map[string]string) {
	if n == nil {
		return
	}
	for i := range n.Projections {
		p := &n.Projections[i]
		// SlotSource, not Column: a table that already stores a `__win_0`
		// column gives this Project TWO projections spelled `__win_0` — the
		// user's, reading the scan, and the window's, reading the slot — and
		// only the second one moves. Renaming by name moved both, which
		// handed the user's column the window's value and left the window's
		// output reading a name nothing emitted.
		if p.SlotSource == "" {
			continue
		}
		to, ok := rename[strings.ToLower(p.SlotSource)]
		if !ok {
			continue
		}
		p.SlotSource = to
		if strings.EqualFold(p.Column, p.SlotSource) || rename[strings.ToLower(p.Column)] == to {
			p.Column = to
		}
		if rename[strings.ToLower(p.Expr)] == to {
			p.Expr = to
		}
		if p.ASTExpr != nil {
			if rewritten, changed := renameColRefs(p.ASTExpr, rename); changed {
				p.ASTExpr = rewritten
			}
		}
	}
	// Deliberately NOT the ORDER BY terms. A sort above the Project keys on
	// the PUBLISHED name (`w`, `sum`), never on the slot, and a term that
	// really does spell `__win_0` is the user ordering by their own stored
	// column — renaming it would sort by the window instead.
}

// renameColRefs is rewriteColRefs with a name map, for the nested-window
// rewrite's `__win_0 + 1`.
func renameColRefs(node plansql.Node, rename map[string]string) (plansql.Node, bool) {
	out, changed, _ := rewriteColRefs(node, func(ref *plansql.ColRef) (plansql.Node, bool) {
		to, ok := rename[strings.ToLower(ref.Column)]
		if !ok || ref.Table != "" {
			return nil, false
		}
		return &plansql.ColRef{Column: to}, true
	})
	return out, changed
}

// seedWindowSlotNames tells the allocator about every window OUTPUT slot the
// plan already uses. A renumbering that ignores them moves one window onto
// another's name — which is exactly what happened before the allocator
// existed, and what its issued-set now prevents for the slots it hands out.
func seedWindowSlotNames(n *logical.Node, alloc *plansql.SlotAllocator) {
	if n == nil {
		return
	}
	for _, w := range n.WindowExprs {
		if w.OutputCol != "" {
			alloc.Seed(w.OutputCol)
		}
	}
	for _, c := range n.Children {
		seedWindowSlotNames(c, alloc)
	}
}
