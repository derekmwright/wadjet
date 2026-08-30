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
	rename := map[string]string{}
	next := 0
	var walk func(n *logical.Node)
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeWindow {
			for i := range n.WindowExprs {
				out := strings.ToLower(n.WindowExprs[i].OutputCol)
				if out == "" || !stored[out] {
					continue
				}
				fresh := ""
				for {
					fresh = plansql.SlotName(plansql.SlotWindowOutput, next)
					next++
					if !stored[strings.ToLower(fresh)] {
						break
					}
				}
				rename[out] = fresh
				n.WindowExprs[i].OutputCol = fresh
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(root)
	if len(rename) == 0 {
		return
	}
	applySlotRename(root, rename)
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
	for _, c := range n.Children {
		applySlotRename(c, rename)
	}
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
