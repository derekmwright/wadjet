package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A BARE `*` OVER A JOIN IS THE FROM CLAUSE'S ARMS, IN WRITTEN ORDER.
//
// PostgreSQL expands it to every arm's own output list, left arm first,
// duplicate names kept BY POSITION and never qualified. This engine published
// the JOIN OPERATOR's stream instead — probe columns then build columns, the
// build's duplicates qualified by its alias — and which side probes is a COST
// decision, so one statement published a different relation as the data moved
// (#997, #1012, #993).
//
// The expansion here runs at Optimize step 1, before any pass that reorders a
// join, so the list is read off the FROM clause as written; each item is a
// QUALIFIED reference published under the column's own name, and the
// projection carrying them permutes the executed stream back into the query's
// order. The join's own qualification survives underneath as a RESOLUTION
// spelling (ADR-0026 §2's pair of names).
//
// Each item carries ADR-0026 §2's PAIR — the spelling that RESOLVES in the
// producer's stream and the name the client is PUBLISHED (`StarColumn`) — so
// an arm's unaliased `COUNT(*)` is referenced as `count(*)` and published as
// `count`, which is what PostgreSQL publishes.
//
// A BUILD-SIDE MARK on the join node cannot state this, the mint is a
// HYPOTHESIS the expansion confirms, the projection sits ABOVE the ORDER BY,
// and six shapes decline — each with its measurement in
// docs/internals/bare-star-over-a-join-arms.md, which is the design.

// joinStarItem is one column a bare `*` over a join publishes: the relation
// the FROM clause names it through, and the column's own name — which is the
// name the client is told, exactly as PostgreSQL publishes it.
type joinStarItem struct {
	qualifier string
	column    StarColumn
}

// joinStarColumns is the list a bare `*` over a JOIN publishes, or nil when
// this shape is not knowable here — in which case the star is left unexpanded
// and the behaviour is the one it had.
//
// The descent through Filter, Sort, Limit and Distinct is
// physical.starOnlySourceScan's, and for its reason: each passes its input
// through unchanged, so `SELECT * FROM a JOIN b WHERE p ORDER BY c LIMIT 10`
// publishes the join's arms. Reaching an Aggregate, a Window, a set operation
// or a named block means the emitted columns are not the join's, so this
// declines and says nothing.
func joinStarColumns(input *Node) []joinStarItem {
	join := starJoinSource(input)
	if join == nil {
		return nil
	}
	return joinArmColumns(join)
}

// starJoinSource is the JOIN a bare `*` over input publishes the arms of, or
// nil when input is not that shape. It is the SHAPE question alone — the
// builder asks it before any scan is annotated, to decide whether the star
// needs a projection at all — and joinStarColumns then asks what the arms
// publish.
func starJoinSource(input *Node) *Node {
	n := input
	for n != nil {
		switch n.Type {
		case NodeFilter, NodeSort, NodeLimit, NodeDistinct:
			if len(n.Children) != 1 {
				return nil
			}
			n = n.Children[0]
		case NodeJoin:
			if len(n.Children) != 2 || !joinPublishesBothArms(n) {
				return nil
			}
			return n
		default:
			return nil
		}
	}
	return nil
}

// ResolveStarJoinOrdinalSortKeys answers a POSITIONAL ORDER BY term under a
// minted star projection from the star's own expanded list.
//
// `SELECT * FROM lat_ord o JOIN lat_item i ON … ORDER BY 4` names the FOURTH
// OUTPUT column, and until the star expanded there was no list to count: the
// key was refused with "a star over a join … is left unexpanded" on every arm,
// for a statement PostgreSQL answers. The list exists now.
//
// The key is rewritten to the item's SOURCE EXPRESSION — `i.id`, not the
// published `id` — because the Sort reads the JOIN's stream, where the
// qualified spelling names exactly one column and the published one may name
// two (ADR-0026 §2's pair of names, chosen by who is asking). That is also
// why no SlotPos is set: a position addresses the OUTPUT list, and this sort's
// input is not that list.
//
// ResolveOrdinalSortKeys owns every other spelling; it declines this one
// because the projection sits ABOVE the Sort rather than below it.
func ResolveStarJoinOrdinalSortKeys(n *Node) {
	if n == nil {
		return
	}
	for _, child := range n.Children {
		ResolveStarJoinOrdinalSortKeys(child)
	}
	if !n.StarJoinArms || len(n.Children) != 1 || HasStarProjection(n) {
		return
	}
	items := VisibleProjections(n.Projections)
	for cur := n.Children[0]; cur != nil && len(cur.Children) == 1; cur = cur.Children[0] {
		switch cur.Type {
		case NodeSort:
			for i, k := range cur.OrderBy {
				if k.Position <= 0 || k.Position > len(items) {
					// Out of range is a real error and not this pass's to
					// raise: RefuseUnresolvedOrdinalSortKeys reports it with
					// the position in the message, which is what leaving the
					// key alone keeps reachable.
					continue
				}
				src := items[k.Position-1]
				cur.OrderBy[i].Column = src.Expr
				if cur.OrderBy[i].Column == "" {
					cur.OrderBy[i].Column = src.Column
				}
				cur.OrderBy[i].Position = 0
			}
			return
		case NodeLimit, NodeDistinct, NodeFilter:
			// Pass-throughs between the projection and the sort.
		default:
			return
		}
	}
}

// ElideUnstatedJoinStar takes back every minted star projection whose arms
// ExpandStarProjections could not state, leaving the tree the builder would
// have built without the hypothesis (Node.StarJoinArms).
//
// It runs immediately after the expansion, wherever that runs, and it returns
// the node the way ApplyDeferredColumnAliases does — a pass that removes a
// node has to answer to the parent that holds it, and the root is a node too.
func ElideUnstatedJoinStar(n *Node) *Node {
	if n == nil {
		return nil
	}
	for i, child := range n.Children {
		next := ElideUnstatedJoinStar(child)
		switch n.Type {
		case NodeJoin, NodeUnion, NodeIntersect, NodeExcept:
			// A RELATION-COMBINING SIDE goes through the ONE door, even when
			// this pass is only putting back the child that was already
			// there: taking the minted projection off a side re-exposes
			// whatever the block materialized under it, and the door is what
			// keeps a hidden slot off a star above (setCombinedChild, #1080).
			setCombinedChild(n, i, next)
		default:
			n.Children[i] = next
		}
	}
	if !n.StarJoinArms || len(n.Children) != 1 || !HasStarProjection(n) {
		return n
	}
	// The naming the ENCLOSING query stamped on this node belongs to the
	// relation, not to the projection: a derived table's alias, a CTE's name
	// and the reference's rename are all recorded on a block's subtree ROOT
	// (plan.go), and the root is about to become the child again. The WITH
	// list travels for the same reason — BuildFromSelect records it on
	// whatever node it returns.
	child := n.Children[0]
	if child.DerivedAlias == "" {
		child.DerivedAlias = n.DerivedAlias
	}
	if child.CTEName == "" {
		child.CTEName = n.CTEName
	}
	if child.CTERefAlias == "" {
		child.CTERefAlias = n.CTERefAlias
	}
	if len(child.CTEs) == 0 {
		child.CTEs = n.CTEs
	}
	return child
}

// joinArmColumns appends each arm of the join chain's own output list, in the
// FROM clause's written order. nil means one arm could not be stated, and a
// partial answer is never returned: a star that publishes SOME relation's
// columns is a wrong answer, where leaving it unexpanded is the answer this
// shape already had.
func joinArmColumns(join *Node) []joinStarItem {
	var items []joinStarItem
	seen := make(map[string]bool, 4)
	ok := true
	var walk func(n *Node)
	walk = func(n *Node) {
		if !ok || n == nil {
			return
		}
		if n.Type == NodeJoin {
			if len(n.Children) != 2 || !joinPublishesBothArms(n) {
				// A chain whose inner join publishes something other than its
				// two arms side by side is not stated here at all: the star
				// covers EVERY arm or none of them.
				ok = false
				return
			}
			walk(n.Children[0])
			walk(n.Children[1])
			return
		}
		name, cols := armRelationColumns(n)
		if name == "" || len(cols) == 0 || repeatsAName(cols) {
			// AN ARM THAT PUBLISHES ONE NAME TWICE cannot be expanded by
			// name: every item here is a QUALIFIED REFERENCE, and `s.id`
			// over a block publishing two `id`s binds the first, so the
			// SECOND one would carry the first's values — a wrong VALUE
			// where leaving the star alone is only a wrong NAME. Addressing
			// a block's column by POSITION is what closing it needs.
			ok = false
			return
		}
		// TWO ARMS OF ONE NAME are not expandable, whatever the parser let
		// through: both would expand to the same qualified reference, which
		// resolves to one relation's column twice. PostgreSQL refuses the
		// spelling outright ("table name specified more than once"); leaving
		// the star alone keeps this engine's existing answer instead of
		// inventing a wrong one.
		if seen[strings.ToLower(name)] {
			ok = false
			return
		}
		seen[strings.ToLower(name)] = true
		for _, c := range cols {
			items = append(items, joinStarItem{qualifier: name, column: c})
		}
	}
	walk(join)
	if !ok {
		return nil
	}
	return items
}

// joinPublishesBothArms reports whether this join's output is its two arms'
// columns side by side — the only shape whose star is the concatenation of
// them.
//
// A SEMI or ANTI join publishes its probe ALONE, and no star spells one: the
// lowering makes them from IN and EXISTS. A join carrying a MANUFACTURED
// LATERAL's lowering publishes minted slots and empty-input defaults that
// belong to the operator above it (ADR-0026 §3c), and the arms it is built
// from are not the relations the query wrote, so those decline here and keep
// the answer they have.
func joinPublishesBothArms(n *Node) bool {
	switch strings.ToLower(strings.TrimSpace(n.JoinType)) {
	case "semi", "anti":
		return false
	}
	if isDependentJoin(n) {
		return false
	}
	for _, c := range n.Children {
		if c != nil && c.LateralSubtree {
			return false
		}
	}
	return true
}

// armRelationColumns is one FROM arm's NAME and PUBLISHED column list.
//
// A named block answers from its OWN projection — `relationOutputColumns`'s
// rule that a block naming itself answers for that name and hides what is
// under it — and a base relation answers through `relationOutputColumns`
// itself, so an ABAC security projection over the scan is what publishes
// (StarSourceColumns, ADR-0033 decision 1). There is no third list: these are
// the two `StarSourceColumns` already asks for a qualified star.
func armRelationColumns(arm *Node) (string, []StarColumn) {
	if name := blockRelationName(arm); name != "" {
		if sel := blockOwnProjection(arm); sel != nil {
			return name, projectionOutputNames(sel)
		}
		// A block with no projection of its own is `SELECT *` over ONE
		// relation, which is the case isStarOnly builds no Project for: its
		// output IS that relation's, and StarSourceColumns answers for it
		// under the bare-star rule. The pass-through descent is what tells
		// that apart from every other elided shape — an aggregate whose own
		// list was elided, a table function — whose output is its own and not
		// the scan's.
		if armRelationName(arm) == "" {
			return "", nil
		}
		return name, StarSourceColumns(arm, "")
	}
	name := armRelationName(arm)
	if name == "" {
		return "", nil
	}
	return name, relationOutputColumns(arm, name)
}

// blockOwnProjection is the SELECT list a named block publishes, reached
// through the nodes that publish their input's columns UNCHANGED, or nil when
// the block has none this pass can read.
//
// A block's root is not always its projection. `(SELECT id, customer FROM t
// ORDER BY id LIMIT 2) a` is a Limit over a Sort over the Project, and
// `(SELECT DISTINCT order_id FROM t) a` a Distinct over it — none of those
// renames a column or changes the width, so the block publishes the Project's
// list and the star can state it. Stopping at the root instead made the star
// read the JOIN's stream for every such block, which is the PLAN's order and
// the PLAN's qualified side: the very divergence this file exists to close,
// surviving one node above where it was looked for (round-2 review, P1).
//
// A SET OPERATION is NOT descended into, and the reason is measured: its
// arms' columns reach the join under the SCAN's qualifier rather than the
// block's — `(SELECT id FROM lat_ord UNION ALL SELECT id FROM lat_ord) a`
// joined against `lat_item` publishes `lat_ord.id` on the three DAG arms — so
// `a.id` binds the OTHER arm's `id` through the bare fallback and the column
// carries the wrong VALUE. It publishes its leftmost arm's NAMES (PostgreSQL's
// rule, which `plansql.BlockOutputColumns` and `projectOutputNamesBelow`
// read), but a name the stream does not spell through this block is not an
// address — the same rule the PAIR states per item, one level up: a reference
// is only a handle where the producer answers to it.
//
// Everything else answers nil too: an Aggregate, a Window or a table function
// publishes something this walk cannot state, and a block still carrying an
// unexpanded star has no list at all.
func blockOwnProjection(block *Node) *Node {
	for n, hops := block, 0; n != nil && hops < 8; hops++ {
		switch n.Type {
		case NodeProject:
			if HasStarProjection(n) || len(n.Projections) == 0 {
				return nil
			}
			return n
		case NodeSort, NodeLimit, NodeDistinct, NodeFilter:
			if len(n.Children) != 1 {
				return nil
			}
			n = n.Children[0]
		default:
			return nil
		}
	}
	return nil
}

// blockRelationName is the name a DERIVED TABLE or CTE reference is known by
// in the enclosing FROM clause, or "" when this node is not one. A reference's
// own rename wins: `FROM c AS x` makes `x` the only spelling PostgreSQL allows.
//
// A BLOCK RE-PROJECTED TO ITS VISIBLE LIST IS STILL THAT BLOCK. A block that
// materialized an ORDER BY term of its own is wrapped in a Project of its
// visible list above its own Sort and LIMIT, so the minted key dies with the
// sort (`dropBlockHiddenSlots`, #991) — and that wrapper carries no alias,
// because the NAME is the block's, one node down. Reading only the root made
// every such arm unstatable and put the PLAN's order back on exactly the
// shapes #991 repaired (`SELECT * FROM lat_ord o JOIN (SELECT order_id,
// amount, amount AS a2 FROM lat_item ORDER BY id LIMIT 4) s ON …`, measured on
// five arms). The list comes from the wrapper — it is what the block
// publishes — and the name from the block under it.
func blockRelationName(n *Node) string {
	for hops := 0; n != nil && hops < 4; hops++ {
		switch {
		case n.CTERefAlias != "":
			return n.CTERefAlias
		case n.DerivedAlias != "":
			return n.DerivedAlias
		case n.CTEName != "":
			return n.CTEName
		case n.Type == NodeProject && len(n.Children) == 1 && !HasStarProjection(n):
			n = n.Children[0]
		default:
			return ""
		}
	}
	return ""
}

// armRelationName is the name a base-relation arm is known by — its alias
// where the query gave one, its table name otherwise. A Filter or an ABAC
// security projection above the scan is passed through, because neither
// renames the relation.
func armRelationName(n *Node) string {
	for n != nil {
		switch {
		case n.Type == NodeScan:
			if n.IsTableFunc {
				// A table function carries no catalog annotation to publish
				// a list from (StarSourceColumns's own boundary).
				return ""
			}
			if n.TableAlias != "" {
				return n.TableAlias
			}
			return n.TableName
		case n.Type == NodeFilter && len(n.Children) == 1,
			n.Type == NodeProject && n.SecurityBarrier && len(n.Children) == 1:
			n = n.Children[0]
		default:
			return ""
		}
	}
	return ""
}

// repeatsAName reports whether one relation emits two columns under the same
// RESOLUTION spelling, FOLDED — the identity every resolver above compares by
// (#731).
//
// The resolution spelling and not the published one: an expanded item
// references `<arm>.<Resolve>`, so two items that RESOLVE to one name cannot be
// told apart and the second would carry the first's values. Two items that
// merely PUBLISH one name are fine — PostgreSQL publishes duplicates too, and
// each reference still names its own column (ADR-0026 §9's pair).
func repeatsAName(cols []StarColumn) bool {
	seen := make(map[string]bool, len(cols))
	for _, c := range cols {
		lc := strings.ToLower(strings.TrimSpace(c.Resolve))
		if seen[lc] {
			return true
		}
		seen[lc] = true
	}
	return false
}

// starItemProjection is the projection one star column expands to: a
// QUALIFIED reference, so it binds its own relation's column whichever side
// the plan built, published under the column's own name, which is what
// PostgreSQL publishes.
func starItemProjection(qualifier string, col StarColumn) Projection {
	ref := &plansql.ColRef{Column: col.Resolve}
	expr := col.Resolve
	if qualifier != "" {
		ref.Table = qualifier
		expr = qualifier + "." + col.Resolve
	}
	item := Projection{Column: expr, Alias: col.Resolve, Expr: expr, ASTExpr: ref}
	if !strings.EqualFold(col.Publish, col.Resolve) {
		item.PublishedName = col.Publish
	}
	return item
}
