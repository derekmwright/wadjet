# Set operation arm column needs

Source: internal/planner/logical/optimizer.go — pushColumnNeeds, moved 2026-09-11 (#1026)

A SET OPERATION'S ARMS SUPPLY THE OPERATION'S RESULT COLUMNS (#961).

`UNION`, `INTERSECT` and `EXCEPT` match their arms BY POSITION over the
operation's whole result row: the result column list is the first arm's,
every arm is projected onto it, and for every spelling but `UNION ALL`
that whole row is also the DEDUP KEY. Nothing above the operation can
therefore say that an arm may stop producing a column — narrowing an arm
changes the operation's own schema, and for a deduplicating spelling it
changes which rows survive.

This walk had no set-op arm at all, so an outer need fell through the
generic recursion straight into both arms.
`SELECT COUNT(*) FROM (SELECT * FROM t WHERE id < 2000 UNION ALL SELECT *
FROM t WHERE id >= 2000) u WHERE id < 10` pushed `{id}` into two star
arms whose scans then read `[id]` alone, while the union stage's
projection — built from the arms' declared output lists, which is what
the operation publishes — still asked for all 22. Both DAG arms failed
with `column "g" does not exist in the input schema` where PostgreSQL and
the single-process path answer 10; every set-op spelling over two star
arms inside a subquery had it. On the single-process path there is no
name-based arm projection to fail and the narrowing landed on the DEDUP
KEY instead: `INTERSECT`, `EXCEPT` and a distinct `UNION` over two star
arms answered 0 on all four arms.

nil is "all columns" for this walk, and it is what an arm's own SELECT
list narrows again on the way down: an arm with an explicit list is a
Project, which builds its own needs set from its own items, so only the
STAR arm — which has no Project at all — is widened by this.
