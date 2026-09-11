# Project reference scope

Source: internal/planner/logical/filter_project_pushdown.go — projRefs, moved 2026-09-11 (#1026)

projRefs is one Project's answer to "what does this column reference
mean?", and it is deliberately more than the output map.

The map alone matches on the BARE column name and ignores the qualifier,
which is right where a Filter sits directly on a Project (every reference
it can carry names that Project's output) and WRONG under a join, where the
walk applies each arm's map to the whole predicate in turn. A reference
qualified to the OTHER arm was rewritten with this arm's definition:
`… c JOIN typemx_dim d ON c.gg = d.k WHERE d.k > 3 OR c.gg > 100` over
`SELECT id AS k, g AS gg` became `id > 3 or g > 100` — 4612 rows where
PostgreSQL answers 1978, a silent wrong answer replacing the obviously
wrong 0 that came before. So the qualifier decides:

  - names is the set of relation names this Project's scope answers to
    (its CTE name, the derived alias stamped on the scans below it, each
    scan's own alias or table name). A reference qualified by one of them
    names this Project's OUTPUT column.
  - a qualifier this scope does not answer to belongs to a sibling arm or
    an outer scope, and is left exactly as written.
  - a qualifier that names one of this Project's OUTPUTS is a ROW FIELD
    PATH, not a table reference (ADR-0022): `rw.b` over `c_row AS rw` is
    field `b` of the renamed ROW column, so the QUALIFIER is substituted
    and the field kept — `c_row.b`. Looking `b` up as a column, which the
    bare-name map does, finds nothing and leaves a name no stage emits.
  - ambiguous, when set, reports a bare name the SIBLING join arm can also
    emit. Nothing in the predicate's text says which arm is meant, so the
    rewrite refuses rather than picking one.

projRefs is one Project's answer to "what does this column reference
mean?", and it is deliberately more than the output map.

The map alone matches on the BARE column name and ignores the qualifier,
which is right where a Filter sits directly on a Project (every reference
it can carry names that Project's output) and WRONG under a join, where the
walk applies each arm's map to the whole predicate in turn. A reference
qualified to the OTHER arm was rewritten with this arm's definition:
`… c JOIN typemx_dim d ON c.gg = d.k WHERE d.k > 3 OR c.gg > 100` over
`SELECT id AS k, g AS gg` became `id > 3 or g > 100` — 4612 rows where
PostgreSQL answers 1978, a silent wrong answer replacing the obviously
wrong 0 that came before. So the qualifier decides:

  - names is the set of relation names this Project's scope answers to
    (its CTE name, the derived alias stamped on the scans below it, each
    scan's own alias or table name). A reference qualified by one of them
    names this Project's OUTPUT column.
  - a qualifier that names one of this Project's OUTPUTS is a candidate ROW
    FIELD PATH — `rw.b` over `c_row AS rw`.
  - a qualifier this scope does not answer to and no output claims belongs
    to a sibling arm or an outer scope, and is left exactly as written.
