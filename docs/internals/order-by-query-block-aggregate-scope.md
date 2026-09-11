# Order by query block aggregate scope

Source: internal/planner/logical/order_by_keys.go — aggregateBelow, moved 2026-09-11 (#1026)

aggregateBelow finds the Aggregate THIS QUERY BLOCK's SELECT-list
projection reads from, descending only through nodes that pass its output
along unchanged. Returns nil when the projection reads rows rather than
groups.

A NESTED SCOPE's root ends the walk. A derived table or a CTE is another
query block: its aggregate is not this block's, and its output is ROWS to
the block above however it was computed. Descending into one made an ORDER
BY over a derived table answer the question "is this term spellable over MY
grouping" about SOMEBODY ELSE's grouping, and refused shapes PostgreSQL
answers on every arm:

	SELECT d.g, d.s FROM (SELECT g, SUM(h) AS s FROM collslot GROUP BY g) d
	ORDER BY d.s * 2
	-- PostgreSQL 3 rows; wadjet 0A000, loudly, single and DAG (#787)

The marker is the one the rest of the planner already uses for a nested
scope: `Node.DerivedAlias` on a derived table's root and `Node.CTEName` on
a CTE's (physical.subtreeNamesRelation reads the same pair). It is asked of
every node the walk touches, the Aggregate included — a derived table whose
own root IS an Aggregate is still another block.

This is the Project rule ADR-0026 §4's shared list deliberately leaves to
each walk (`AggScopePreservingWrapper` omits NodeProject, because what a
Project does to the schema is the caller's own question). The four wrapper
kinds keep the shared answer; the boundary is this walk's own.
