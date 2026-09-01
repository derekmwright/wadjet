package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// renameCollidingSlots renumbers the planner's own hidden slots past any
// STORED column of the same name, and past a slot ANOTHER BLOCK of the same
// query already minted.
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
// The SECOND collision is between two blocks of one query (#747). The window
// slot counter lives in `logical.BuildFromSelectWithCTEs`, which recurses per
// SELECT BLOCK, so every block starts at zero and two sibling subqueries mint
// the SAME `__win_0`:
//
//	SELECT p.w AS pw, q.w AS qw
//	  FROM (SELECT id, SUM(b) OVER () AS w FROM t) p
//	  JOIN (SELECT id, SUM(a) OVER () AS w FROM t) q ON p.id = q.id
//	-- PostgreSQL pw=49.2400 qw=52.9900; the DAG answered p's window TWICE
//
// Both arms carry a column called `__win_0` into the join, the projection
// above it resolves each reference to that one name, and one window's value
// is published under both output columns. Three siblings collapsed on EVERY
// path, single-process included, because the third arm's slot won.
//
// ADR-0025 recorded the opposite — "the blocks' slots are already distinct,
// the allocator is per query" — and no fixture attempted it, which is method
// 10 of the correctness protocol exactly. The allocator is per BLOCK; this is
// the pass that makes the claim true, at the first point where the whole
// query's slots are visible in one tree.
//
// It runs after AnnotateScanColumns, which is what puts a table's real column
// list on the Scan node; before that pass there is no schema to collide with.
// It is idempotent: a second run sees slots that are already distinct and
// renames nothing.
func renameCollidingSlots(root *logical.Node) {
	stored := map[string]bool{}
	collectStoredNames(root, stored)
	// Two windows minting one name is the sibling collision; a stored name
	// inside a reserved family is the #694 one. Either makes the pass hot.
	uses := map[string]int{}
	countWindowSlotNames(root, uses)
	hot := false
	for name := range stored {
		if plansql.ReservedSlotFamily(name) != "" {
			hot = true
			break
		}
	}
	if !hot {
		for _, n := range uses {
			if n > 1 {
				hot = true
				break
			}
		}
	}
	if !hot {
		return // the overwhelmingly common case, two map walks
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
	//
	// claimed is the FIRST block to mint each slot, in walk order. It keeps
	// one occurrence where it is and moves every later one, so a query with
	// no collision is untouched and a query with two is renumbered by the
	// minimum: the first arm's plan, its stage names and its snapshots do not
	// move because a sibling appeared.
	claimed := map[string]bool{}
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
			// c has already applied m to ITSELF, on the way out of its own
			// walk. Applying it again to c's SUBTREE is what made a rename
			// travel DOWNWARD into the block that did not mint the slot: a
			// window in the SELECT list above a join renumbers to `__win_1`,
			// and the descent rewrote the ARM's projection to read `__win_1`
			// too, while the arm's own window still wrote `__win_0`. The
			// single-process path then failed with `column "__win_1" does not
			// exist in the input schema`, and on the DAG — where the outer
			// window really does emit `__win_1` — the arm's column silently
			// answered the OUTER window's value (#772).
			//
			// A slot travels UP from the node that mints it to the consumer
			// that reads it, and nowhere else.
			if len(n.Children) > 1 {
				// A join, a set operation: consume the arm's map here rather
				// than letting it escape into a sibling.
				applySlotRenameNodeOnly(n, m, stored)
				continue
			}
			for k, v := range m {
				pending[k] = v
			}
		}
		if n.Type == logical.NodeWindow {
			for i := range n.WindowExprs {
				out := strings.ToLower(n.WindowExprs[i].OutputCol)
				if out == "" {
					continue
				}
				if !stored[out] && !claimed[out] {
					claimed[out] = true
					continue
				}
				fresh, ok := alloc.Next(plansql.SlotWindowOutput)
				if !ok {
					continue // the family is exhausted; leave the plan as it was
				}
				claimed[strings.ToLower(fresh)] = true
				pending[out] = fresh
				n.WindowExprs[i].OutputCol = fresh
			}
		}
		if len(pending) > 0 {
			applySlotRenameNodeOnly(n, pending, stored)
		}
		return pending
	}
	// root applies its own map on the way out of walk, so there is nothing
	// left to do here: a rename that reached the root with no consumer above
	// it has none.
	walk(root)
}

// countWindowSlotNames tallies how many window nodes mint each output slot.
// A name minted twice is the sibling collision; the count is what tells it
// from the ordinary case without renaming anything.
func countWindowSlotNames(n *logical.Node, out map[string]int) {
	if n == nil {
		return
	}
	for _, w := range n.WindowExprs {
		if w.OutputCol != "" {
			out[strings.ToLower(w.OutputCol)]++
		}
	}
	for _, c := range n.Children {
		countWindowSlotNames(c, out)
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

// applySlotRenameNodeOnly repoints this node's own references and does not
// descend. It is what lets a rename be applied along the ancestor chain up to
// its consumer without crossing into a sibling subtree.
func applySlotRenameNodeOnly(n *logical.Node, rename map[string]string, stored map[string]bool) {
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
			// A window WRAPPED in a larger expression (`SUM(a) OVER () + 0`)
			// has no SlotSource — the builder's nested-window rewrite leaves
			// only a ColRef to the slot inside ASTExpr — so the loop above
			// cannot see it, and a sibling collision left that expression
			// reading the OTHER block's window (#610's spelling of #747).
			//
			// The reference the rewrite plants carries its PROVENANCE
			// (plansql.ColRef.Slot), so it is moved wherever the slot moves,
			// a stored column of that name included. It has to be: over a
			// table that really stores `__win_0` the window's slot steps to
			// `__win_1` and the expression stayed on `__win_0`, so
			// `SUM(plain) OVER () + 0` answered the STORED column's values on
			// every arm (#750). Declining there was the old rule, and it read
			// the planner's own reference as if it might be the user's.
			//
			// An UNMARKED reference is still moved only when the name is
			// stored NOWHERE in the query — that is a reference whose
			// provenance is not recorded (an expression re-parsed from text),
			// and moving the user's own column is the #694 defect this pass
			// exists to avoid.
			if p.ASTExpr == nil {
				continue
			}
			if rewritten, changed := renameSlotColRefs(p.ASTExpr, rename, stored); changed {
				p.ASTExpr = rewritten
				if to, ok := rename[strings.ToLower(p.Expr)]; ok && !stored[strings.ToLower(p.Expr)] {
					p.Expr = to
				}
			}
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

// renameColRefs is rewriteColRefs with a name map, for a projection whose
// slot the SlotSource branch has already identified.
func renameColRefs(node plansql.Node, rename map[string]string) (plansql.Node, bool) {
	out, changed, _ := rewriteColRefs(node, func(ref *plansql.ColRef) (plansql.Node, bool) {
		to, ok := rename[strings.ToLower(ref.Column)]
		if !ok || ref.Table != "" {
			return nil, false
		}
		return &plansql.ColRef{Column: to, Slot: ref.Slot}, true
	})
	return out, changed
}

// renameSlotColRefs is renameColRefs for the nested-window rewrite's
// `__win_0 + 1`, where the expression may hold BOTH the planner's reference to
// a slot and the user's reference to a stored column of that very name.
//
// A reference the planner planted (ColRef.Slot) moves with the slot; one whose
// provenance is not recorded moves only if the name is stored nowhere, which is
// the rule this branch had for every reference before provenance existed.
func renameSlotColRefs(node plansql.Node, rename map[string]string, stored map[string]bool) (plansql.Node, bool) {
	out, changed, _ := rewriteColRefs(node, func(ref *plansql.ColRef) (plansql.Node, bool) {
		if ref.Table != "" {
			return nil, false
		}
		name := strings.ToLower(ref.Column)
		to, ok := rename[name]
		if !ok {
			return nil, false
		}
		if !ref.Slot && stored[name] {
			return nil, false
		}
		return &plansql.ColRef{Column: to, Slot: ref.Slot}, true
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
