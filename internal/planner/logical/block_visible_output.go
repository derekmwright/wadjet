package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A DERIVED BLOCK PUBLISHES ITS VISIBLE LIST, AND ITS OWN SORT KEY DIES WITH
// ITS SORT (#991, #1020, ADR-0026 §3c and §7).
//
// An ORDER BY term the SELECT list does not carry is MATERIALIZED as a hidden
// projection and sorted on (resolveOrderBy). At the top of a statement the
// hidden tail is trimmed where the rows leave the engine —
// `physical.hiddenSortTrimOp` on the single-process pipeline, the gather's
// output renames on the DAG — so no client ever sees it.
//
// A derived block has neither. Its projection sits BELOW its own Sort, because
// the sort reads the key, and nothing above re-projects: a consumer that reads
// the block's STREAM therefore reads the key too. With the block as the only
// relation the statement's own output projection IS the block's and the trim
// still fires, which is why this was invisible; put a JOIN above it and
//
//	SELECT * FROM lat_ord o JOIN (SELECT order_id, product FROM lat_item
//	                              ORDER BY amount LIMIT 3) s ON s.order_id = o.id
//
// published a sixth column, `__sortkey_0`, on every arm and in `RowDescription`
// — a name no query can spell, reaching a client.
//
// The repair is the one ADR-0026 §3c states for every minted slot: it is
// dropped by the operator that made it, where that operator ends. The block is
// re-projected to its VISIBLE list above its own Sort and LIMIT, so the sort
// still reads the key below and nothing above can see it. It is the block's
// own projection re-applied, not a new one: same names, same order, one
// reference per item.
//
// The re-projection is added ONLY where the block materialized a key, so an
// ordinary block's plan is byte-identical to what it was.

// dropBlockHiddenSlots re-projects a derived block to the list it PUBLISHES,
// where the block materialized an ORDER BY term of its own below the operator
// that publishes it. It returns plan unchanged for every other block.
func dropBlockHiddenSlots(plan *Node) *Node {
	proj := blockOutputProjection(plan)
	if proj == nil || proj == plan || !HasHiddenProjection(proj.Projections) {
		return plan
	}
	visible := VisibleProjections(proj.Projections)
	if len(visible) == 0 {
		return plan
	}
	items := make([]Projection, 0, len(visible))
	for _, pr := range visible {
		name := blockProjectionName(pr)
		if name == "" {
			// A projection this pass cannot name is one it cannot re-apply,
			// and re-applying a guess would change which column a consumer
			// above binds. Leave the block exactly as it was.
			return plan
		}
		item := Projection{
			Column:  name,
			Alias:   name,
			Expr:    name,
			ASTExpr: &plansql.ColRef{Column: name},
		}
		if pr.PublishedName != "" && !strings.EqualFold(pr.PublishedName, name) {
			item.PublishedName = pr.PublishedName
		}
		items = append(items, item)
	}
	return NewProject(plan, items)
}

// blockOutputProjection is the Project a derived block PUBLISHES from: the
// block's root, or the Project under the operators that reorder and bound its
// rows without changing its columns.
//
// The descent stops at anything else. A node this pass does not recognise may
// change the column set, and re-projecting a list that is not the block's own
// would drop columns the block publishes.
func blockOutputProjection(n *Node) *Node {
	for cur := n; cur != nil; {
		switch cur.Type {
		case NodeProject:
			return cur
		case NodeSort, NodeLimit, NodeDistinct:
			if len(cur.Children) != 1 {
				return nil
			}
			cur = cur.Children[0]
		default:
			return nil
		}
	}
	return nil
}

// blockProjectionName is the column name a block's projection EMITS — the
// resolution spelling of ADR-0026 §2, the same rule
// `physical.projectionOutputName` applies when it builds the operator.
func blockProjectionName(pr Projection) string {
	if pr.Alias != "" {
		return pr.Alias
	}
	if pr.Column != "" {
		return pr.Column
	}
	return cleanExpr(pr.Expr)
}

// setCombinedChild wires side i of a RELATION-COMBINING node — a join or a set
// operation — to plan, and is the ONE door every such wiring goes through.
//
// The rule is §9's: a relation-combining operator builds its output from its
// sides' STREAMS, so each side publishes its VISIBLE list there. Round 2 put
// the call in the four constructors and said they were the whole class,
// because there is no fifth binary node TYPE. That was true of the type and
// false of the wiring: the decorrelation of `IN`, `NOT IN`, `EXISTS` and a
// correlated scalar subquery each builds a `NodeJoin` LITERALLY, with a nil
// left child the caller fills in afterwards — so the constructor never saw the
// side, and a sorted derived block under any of them still published
// `__sortkey_0` on five arms and in `RowDescription` (#1080).
//
// A door is a door only if everything uses it, which is why this is a function
// rather than a line repeated at each site, and why
// `TestEveryRelationCombiningSideIsWiredThroughOneDoor` fails on a new
// assignment that does not come through here.
func setCombinedChild(n *Node, i int, child *Node) {
	if n == nil || i < 0 || i >= len(n.Children) {
		return
	}
	n.Children[i] = dropBlockHiddenSlots(child)
}
