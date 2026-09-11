# Choice winning box conversion

Source: internal/engine/expr/choice_decimal.go — choiceBoxMode, moved 2026-09-11 (#1026)

choiceBoxMode is what a choice construct must do to the box its winning arm
produced so the value survives the vector the PLAN declared for it.

The two directions are the two halves of one rule, and each exists because a
box means something else to the other type's vector:

  - choiceBoxDecimal: the arms fold to a DECIMAL, so an INTEGER box becomes
    the value's TEXT. An integer written into a DECIMAL vector is the
    already-scaled carrier of ADR-0018 §4 — 100 would read back as 1.00 —
    and SetValueChecked refuses it outright (#695).
  - choiceBoxInt64/choiceBoxFloat32/choiceBoxFloat64: the arms fold to a
    non-DECIMAL number, so a STRING box becomes that number. Two arms can
    produce one: a DECIMAL column, whose value IS its rendered text, and a
    QUOTED literal, which arrives as the characters the query spelled.
    `COALESCE(numeric, float8)` is double precision in PostgreSQL and
    declares double precision here, and on the rows the DECIMAL wins the box
    was its text — which the #361 store guard refused, loudly, for as long as
    nothing converted it (#555's float half); `COALESCE(bigint, '16777217')`
    is bigint there and the literal's four characters had nowhere to go
    (#724). GREATEST/LEAST already answered both, through
    extremumArms.materialize, which is why the defect was invisible to every
    gate written over those two.
