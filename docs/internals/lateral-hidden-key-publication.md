# Lateral hidden key publication

Source: internal/planner/logical/builder.go — buildLateralSubquery, moved 2026-09-11 (#1026)

THE KEY IS PUBLISHED UNDER A HIDDEN SLOT (ADR-0026 3a).

Under its SOURCE COLUMN's name -- what this injected until #956
-- the key is an ordinary output column of the lateral, and
every consumer above resolves by name off a batch that may
answer to that name twice. Two shapes did exactly that,
silently, and in opposite directions:

  SELECT MAX(t.id) AS g, COUNT(*) AS c ... WHERE t.g = d.k
    the aggregate's output is [g(key), g(max), c] and `s.g`
    read the KEY -- 0,1,2,... where PostgreSQL 17 answers
    4998,4999,4993,... (#956);
  SELECT amount AS order_id ... WHERE order_id = o.id
    an item ALREADY answers to the key's name while holding a
    different value, so the injection was skipped entirely and
    the join keyed on a column its build side does not carry --
    ZERO rows for PostgreSQL's four (#767's mirror).

`__key_N` is in the reserved namespace (plansql/reserved_slots),
so no query can spell it and no alias can shadow it: the
collision is impossible rather than unlikely. The promoted
equality is re-spelled to it below, and for an AGGREGATED
lateral the aggregate PUBLISHES the key under it while still
RESOLVING it by the source column (Node.GroupByPublish).
