# Scoped slot rename propagation

Source: internal/planner/physical/slot_collision.go — renameCollidingSlots (scoped walk), moved 2026-09-11 (#1026)

```go
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
```
