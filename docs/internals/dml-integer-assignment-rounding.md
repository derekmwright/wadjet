# Dml integer assignment rounding

Source: wadjet/dml.go — func assignIntegerValue(v any, col parquet.Column, srcFloat bool) (any, error) {, moved 2026-09-11 (#1026)

assignIntegerValue rounds, ranges and narrows a value into an integer
column.

A fractional value ROUNDS the way PostgreSQL's assignment cast rounds, and
which way that is depends on the SOURCE's declared type: a float8 rounds
half to EVEN (C's rint) and a numeric half AWAY FROM ZERO. Only a value
outside the column's range (NaN and the infinities included) is 22003.

This engine boxes both families as float64, so the BOX cannot decide it:
`SET n = f` over a FLOAT64 column and `SET n = 0 - 2.5` arrive here as the
same Go type and want opposite answers — 2 and -3. One rule served both, and
it was the numeric one, so `UPDATE fl SET n = f` over 2.5, -2.5, 0.5, 3.5,
1.5 stored 3, -3, 1, 4, 2 where PostgreSQL stores 2, -2, 0, 4, 2 — three of
five rows wrong, silently (#699).

srcFloat is the DECLARATION, resolved once per SET clause through
physical.DeclaredTypeOfNode — the same declared-type layer the query path
reads, not a private approximation of it. An expression whose type the layer
declines to decide keeps the numeric rule, which is what it had.

The range check reaches PORT (uint16) and PROTOCOL (uint8) too, because
nothing below this line re-checks either — convertValue does, but only for
literals — so an out-of-range computed value would truncate into a port no
real port can be.
