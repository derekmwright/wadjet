# Nested with scope precedence

Source: internal/planner/logical/builder.go — scopeCTEs, resolveTableOrCTESource;
internal/planner/physical/planner_config.go — cteCacheFor. Rewritten 2026-10-09
(ADR-0047 stage 3, #1606).

scopeCTEs is the CTE list in scope INSIDE a nested query block: the items the
enclosing scope offers, then the block's OWN WITH items
(`plansql.ScopeChain`).

A block's own WITH used to be DROPPED at every door that re-parses its SQL —
a derived table, a CTE body, a LATERAL subquery — because the builder was
handed the ENCLOSING list and the parsed SelectInfo's `CTEs` field was never
read. `SELECT v FROM (WITH c AS (SELECT dx FROM setopdecjb) SELECT dx AS v
FROM c) t` therefore planned a SCAN of a table called `c`, and since no such
table exists the single-process path answered NO ROWS where PostgreSQL
answers four, and the stage DAG failed with "stage scan-0 has no
dependencies and no ScanFiles" (#684).

A reference binds the LAST item of its name in that list — the innermost
scope's — so a nested WITH that reuses an enclosing item's name shadows it
inside its block, as PostgreSQL scopes it (except inside a recursive CTE's own
recursive term, where a nested item of the CTE's name is refused 42P19 where
PostgreSQL answers — a recorded residual, not a shadowing). The item's own body is planned with
the list BEFORE it (`ctes[:i]`): a non-recursive item is not in scope inside
its own body, so one that shadows a base table or an enclosing item reads that
table or item (#771: handing a CTE the whole list let one that shadows a base
table read ITSELF, without bound, and took the process down with a stack
overflow).

The search was first-match until 2026-10-09, deliberately, because reversing
it alone would have fixed the stage DAG and not the single-process path: the
planner materializes CTEs into `Planner.cteCache` keyed by NAME, so a
shadowing item's subtree, tagged with the same CTEName, read the enclosing
item's materialization whatever the builder resolved. The cache now answers a
reference by the item's IDENTITY (`logical.Node.CTEIdent`, `cteMaterialized.
ident`, `Planner.cteCacheFor`): an entry of the name answers only a reference
to that item. The binder's WITH registry is lexical in the same way
(`binder.registerCTE`), the correlation classifier's resolver binds the
innermost item before the reader (`plansql.CTEColumns`), and an expression
subquery is planned in the chain the node records where it is written
(`plansql.StampSubqueryScopes`, ADR-0032's 2026-10-09 amendment).

Gated by `coordinator.TestAWithInsideASubqueryBlockIsInScopeThere`
(`#1606/an_inner_cte_shadows_an_outer_one_of_the_same_name`, pinned at 4
until then) and `coordinator.TestArcCI3SubqueryBodyInItsScopeEveryArm`
(`i1606/*`).
