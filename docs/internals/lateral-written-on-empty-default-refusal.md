# Lateral written on empty default refusal

Source: internal/planner/logical/lateral_empty_value.go — refuseUnorderedLateralOn, moved 2026-09-11 (#1026)

refuseUnorderedLateralOn refuses a written ON over a LATERAL join this pass
does NOT repair, unless the ON can be PROVEN to reject the padded row.

PostgreSQL evaluates the lateral once per outer row, so for an outer row
whose lateral input is empty there IS a lateral row — the item over an empty
input, `V` — and the ON then decides that pair. This lowering has the two the
other way round: the decorrelated join finds no build row, pads, and the ON
never sees `V` at all. So on the unrepaired path this engine answers NULL for
every such outer row, whatever the ON says, and PostgreSQL answers:

	ON true  on (R, V)  →  V      ON false / NULL on (R, V)  →  NULL

The two agree exactly where the ON REJECTS the pair. `ON s.n > 1` folds to
`0 > 1` with no row to read, which is a definite FALSE, so it keeps
answering as it does today. Everything else is refused:

  - `ON s.n = 0` folds to TRUE — PostgreSQL's `Carol, 0` against this
    engine's `Carol, NULL`, a wrong NUMBER on a row a pad manufactured.
  - `ON o.id > 1` cannot be folded AT ALL: it reads an OUTER column, so
    whether the padded row passes depends on the row. It was the loudest
    miss of the first cut, which only looked at ONs mentioning the lateral:
    PostgreSQL answers `Carol, 0` and this engine answered `Carol, NULL`
    with no complaint.

A join matches a pair only when its condition is TRUE, so UNKNOWN rejects
exactly as FALSE does and `ON s.n = NULL` keeps answering. What the fold has
to separate is "evaluated to UNKNOWN" from "could not be evaluated", and it
does that STRUCTURALLY: a substituted condition still holding a column
reference is not folded at all, because an evaluation that reads a column
with no row would report the same nil either way and the second one is the
wrong answer.

It is scoped to a WRITTEN ON. `ON true` and the no-ON spelling reach
lateralPadOnly and are repaired; an INNER join's written ON reaches
lateralPadThenFilter, which MOVES it into the enclosing WHERE where it is
evaluated ABOVE the default — PostgreSQL's order exactly. Only the outer
spelling keeps the ON inside the join, below the default.

The `laterNullExtends` decline is deliberately NOT refused here: it declines
for a different reason (a later RIGHT or FULL join manufactures NULLs this
repair cannot tell from the lateral's own) and its shapes are pinned in the
arc D5 census with PostgreSQL's answer beside them.
