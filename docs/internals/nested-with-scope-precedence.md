# Nested with scope precedence

Source: internal/planner/logical/builder.go — scopeCTEs, moved 2026-09-11 (#1026)

scopeCTEs is the CTE list in scope INSIDE a nested query block: the items the
enclosing scope offers, then the block's OWN WITH items.

A block's own WITH used to be DROPPED at every door that re-parses its SQL —
a derived table, a CTE body, a LATERAL subquery — because the builder was
handed the ENCLOSING list and the parsed SelectInfo's `CTEs` field was never
read. `SELECT v FROM (WITH c AS (SELECT dx FROM setopdecjb) SELECT dx AS v
FROM c) t` therefore planned a SCAN of a table called `c`, and since no such
table exists the single-process path answered NO ROWS where PostgreSQL
answers four, and the stage DAG failed with "stage scan-0 has no
dependencies and no ScanFiles" (#684). It is a wrong answer on one path and
a plan the other cannot run.

The block's own items come LAST, so `resolveTableOrCTE`'s first-match walk
keeps the ENCLOSING scope's precedence and — the reason that walk passes
`ctes[:i]` down — an item can still only see the items DEFINED BEFORE IT
(#771: handing a CTE the whole list let one that shadows a base table read
ITSELF, without bound, and took the process down with a stack overflow).

That precedence is BACKWARDS for the one shape where the two scopes collide
— PostgreSQL reads a block's own item where this reads the enclosing one —
and it is deliberately left that way here, because reversing the search
fixes it on the stage DAG and NOT on the single-process path, which would
answer one query two ways. The single-process planner materializes CTEs into
`Planner.cteCache` keyed by NAME over the STATEMENT's top-level list, so a
shadowing item's subtree, tagged with the same CTEName, reads the enclosing
item's materialization whatever the logical builder resolved. Correct
shadowing is that cache becoming scope-aware as well as this search being
reversed, which is a change to the CTE identity itself and not to this list.
TestAWithInsideASubqueryBlockIsInScopeThere pins the divergence.
