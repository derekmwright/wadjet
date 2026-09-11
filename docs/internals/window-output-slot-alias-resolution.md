# Window output slot alias resolution

Source: internal/planner/physical/window_alias_respell.go — respellWindowSlotAliasRefs, moved 2026-09-11 (#1026)

respellWindowSlotAliasRefs rewrites every column reference naming a derived
table's or CTE's SELECT-list alias for a WINDOW OUTPUT SLOT to the slot
itself.

It is the third answer to "what does the producer below call this value",
and the one the two answers above cannot give. A window publishes its result
under the hidden slot `__win_N`, never under the alias the SELECT list gives
it: `SELECT g.id AS id, SUM(g.a) OVER () AS w FROM …` is a Project over a
Window, and walkStages emits no stage for a Project (ADR-0025). On the
single-process pipeline that Project is a real operator and `w` is a real
column; on the DAG `w` is a name nothing publishes, so an aggregate above
reading `SUM(w * 2)` compiled that text against a batch with no `w`,
`expr.ColRef.Eval` answered nil on every row, and the SUM came back NULL —
953.82 single-process and on PostgreSQL, NULL on both DAG arms (#877, and
#878 one qualifier deeper, where the derived table is a CTE joined to a
second reference of itself).

The BOUNDARY is exact and needs no model of what a later pass will do: the
resolved name is in the window-output slot family, which is RESERVED
(plansql.RefuseReservedSlotName — a user cannot store or alias a column
there), so a reference that resolves to one names the planner's own slot and
nothing else. Every other resolution is left to the two rules above.

DAG-only, like its siblings: it rewrites the stage spec's TEXT, never the
logical node the local engine runs.
