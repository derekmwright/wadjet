# Window accumulator result declarations

Source: internal/planner/physical/window_declared_output.go — windowSpecOutputType SUM/AVG, moved 2026-09-11 (#1026)

SUM and AVG do NOT copy an input value, so their declaration is
not the input's: they accumulate, and over a DECIMAL they answer
what the GROUPED SUM/AVG answer — DECIMAL(38,s) and
DECIMAL(38,min(s+4,38)), exactly (#586, #475, ADR-0012 item 9,
ADR-0024 item 2). `SUM(d) GROUP BY g` and `SUM(d) OVER (PARTITION
BY g)` are the same question written twice; a client that reads
both in one result set was getting numeric for one and float8 for
the other, with the window's digits past a float64's ~16 already
gone.

An INTEGER input answers PostgreSQL's own result type — bigint for
sum(int4), numeric for sum(int8) and for avg of either — through
exec.IntegerAccOutputType, the SAME function the grouped
aggregate's declaration asks (aggIntegerOutputType) and the same
one the operator's runtime correction asks
(exec.windowAccOutputType). Until #987 this fell to float8 while
the grouped spelling was exact, so the two spellings of one
question disagreed about the type AND, past 2^53, about the digits
— an order-dependent total from a float64 accumulator (#813,
ADR-0012's divergence list, now deleted).

Every other input type keeps the float64 the name list answers.
