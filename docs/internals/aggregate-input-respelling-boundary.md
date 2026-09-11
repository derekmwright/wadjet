# Aggregate input respelling boundary

Source: internal/planner/physical/window_alias_respell.go — aggInputRespellable, moved 2026-09-11 (#1026)

aggInputRespellable reports whether the derived names between the aggregate
and its producer are ones NO stage materializes — the only condition under
which respelling them to their sources is right.

The question is never "does this name exist below" but "is this name
MATERIALIZED HERE", and it has a different answer per producer:

  - A JOIN materializes it. attachScanSelectProjections puts an alias-naming
    OpProject on the arm's fragment, so `x.v` really IS a column of the
    join's output and the source spelling is the one that is not.
    Respelling took a CORRECT 25.50 to 0.00 on `SUM(CASE WHEN x.s = '1.50'
    THEN x.v ELSE 0 END)` over `(SELECT s, a * 2 AS v FROM t) x JOIN t y`,
    because a self-join qualifies both sides' `a`.
  - A DISTINCT materializes it. rewriteDistinctAsGroupBy lowers it to an
    aggregate whose OUTPUT is the projection's names, so `v` is emitted and
    `a` is gone: respelling turned a LOUD failure into a silent 0 on
    `SUM(CASE WHEN v > 0 THEN v ELSE 0 END)` over
    `(SELECT DISTINCT a * 2 AS v FROM t)`, where PostgreSQL answers 29.50.
  - An AGGREGATE, a SET OPERATION and a WINDOW each emit a new column set of
    their own, for the same reason.
  - A SORT and a LIMIT are pass-throughs, but ADR-0025 gave both an
    OpProject slot, so whether the alias is materialized on them is decided
    by a LATER pass and is not knowable here.

Enumerating the materializing kinds was the first attempt and it was wrong
twice — once per kind nobody had thought of. The rule is stated POSITIVELY
instead: respell only where the walk reaches a SCAN through Project and
Filter alone, which is exactly the shapes #702 names and exactly the ones
where walkStages provably emits no stage for the Project. Everything else
keeps today's behaviour, and assertAggregateInputsResolve is what makes a
residual there loud rather than silent.
