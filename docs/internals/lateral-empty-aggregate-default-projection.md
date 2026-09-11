# Lateral empty aggregate default projection

Source: internal/engine/exec/lateral_empty_default.go — LateralEmptyDefault, moved 2026-09-11 (#1026)

LateralEmptyDefault carries an ungrouped aggregate's EMPTY-INPUT VALUES on
the lateral's own output columns, above the join that manufactured the row.

PostgreSQL evaluates a LATERAL once per outer row, and an UNGROUPED
aggregate over an empty input still yields a row: `COUNT(*)` is 0 there,
`SUM(x)` is NULL, and an item BUILT from them is that item over those
values. This engine decorrelates the subquery into a join, so an outer row
the lateral matches nothing for survives only as a LEFT pad — and a pad
writes NULL into every column of that side.

TWO RULES:

 1. WHICH ROWS. A pad is not "a row whose value is NULL": a matched row may
    hold a NULL of its own. `NULLIF(COUNT(*), 2)` is NULL for a matched row
    that counted 2, and stamping the column's own nulls turned two RIGHT
    rows into wrong ones. The pad is marked by the correlation key: the join
    keys on it, a NULL key matches nothing, so the KEY column is NULL
    exactly on the rows the pad manufactured.

 2. WHAT VALUE. The ITEM's own value over an empty input, not a literal 0 on
    a COUNT column: `COUNT(*)+1` is 1, `COUNT(*)=0` is true,
    `CAST(COUNT(*) AS VARCHAR)` is '0' and `ARRAY[COUNT(*)]` is `{0}`.

BOTH LIVE IN ONE COMPILED EXPRESSION, and that is the whole operator: the
first cut carried the value as TEXT and stamped it into the vector in place
through a hand-written type switch. That cannot be right for a varlen or a
container vector, and it was not — a STRING default emptied a MATCHED row
and concatenated two values into the padded one, and a star over it crashed
in `slice bounds out of range`. An expression takes the engine's own typed
kernel for all 22 types and writes a NEW vector, which is what every other
computed column in the engine does.

The marker is dropped here rather than by the join, because the join's drop
runs BELOW this operator and the marker is what the expression reads.
