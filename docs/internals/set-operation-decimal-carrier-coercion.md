# Set operation decimal carrier coercion

Source: internal/engine/exec/decimal_coerce.go — DecimalCoerce, moved 2026-09-11 (#1026)

DecimalCoerce puts named columns into ONE declared DECIMAL(p,s), rewriting
the unscaled carrier as it goes.

It exists for a set operation's arms. A DECIMAL value is an UNSCALED
integer plus the column's declared SCALE (ADR-0018 §4), and the two travel
apart on the stage DAG: each arm's task writes its own .wshf file carrying
its own scale in the header, and a downstream task that reads several such
files writes ONE file under the schema of the first batch it saw. Two arms
at different scales therefore hand the same unscaled integer to a reader
that believes a different scale — 12.7501 from a DECIMAL(18,4) arm came
back as 1275.01 under a DECIMAL(9,2) first arm, 100x too large, with
nothing anywhere reporting a problem (#533).

The fix is to make the arms AGREE before they meet, which means moving the
values: rescaling to the set operation's output scale is a multiplication
by a power of ten, not a reinterpretation. INT32/INT64 arms are coerced the
same way, because `numeric UNION ALL bigint` is `numeric` in PostgreSQL and
an integer box is a value at scale 0.

Only UPWARD moves are accepted. The output scale is the max over the arms,
so no arm is ever asked to drop digits; a request to scale DOWN is a
planner defect and is refused rather than silently truncating.

A value with no Int128 at the output scale is an ERROR naming the column,
not a wrapped one — the same rule, and the same reason, as SUM's overflow
(ADR-0012 item 9): a wrapped number is a different number wearing the right
type, and nobody downstream can tell.
