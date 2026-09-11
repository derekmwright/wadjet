# Dependent lateral join reorder boundary

Source: internal/planner/logical/optimizer.go — isDependentJoin, moved 2026-09-11 (#1026)

isDependentJoin reports whether this join is one the PLANNER manufactured
for a decorrelated LATERAL — a dependent join, whose inner side is a plan OF
the outer side's rows.

Such a join is not a free inner join and reordering it is not a cost
decision (#1008):

  - `costBasedJoinReorder` REBUILDS the chain with `NewJoin`, which carries
    none of the rules the lowering attached to the node it built — the slot
    it minted and drops (`HiddenJoinCols`), the pad marker and the
    empty-input defaults (ADR-0026 §3c). Two LATERALs over one outer flatten
    to THREE relations, so `SELECT * FROM lat_ord o JOIN LATERAL (… GROUP BY
    i.product) s ON true JOIN LATERAL (…) s2 ON true` came back with
    `__key_0` and `__key_1` in the client's relation, and the re-hung
    conditions keyed a STRING against the integer correlation column: `join
    key "s.__key_0" is STRING on the probe side` on both DAG arms, and on
    the single-process arms a star over two joins that declares nothing —
    `cols=[] rows=0` where PostgreSQL 17 answers eight rows.
  - `flattenJoinChain` walks THROUGH the manufactured join, which makes the
    lateral's inner subtree and the relation it CORRELATES ON two
    independent relations the cost model may put in either order — the inner
    placed before the outer it depends on.
  - The two-way swap below exchanges the sides, and for a manufactured join
    the side order is the ANSWER: `SELECT *` publishes the outer relation's
    columns and then the lateral's, which is what PostgreSQL publishes.

The marker is the lowering's own: `Node.LateralSubtree` on the side it
BUILT (builder.go), plus the rules it hangs on the join, so a shape that
mints no slot is still recognised.
