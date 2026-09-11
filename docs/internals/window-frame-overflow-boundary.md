# Window frame overflow boundary

Source: internal/engine/exec/window_decimal_agg.go — windowDecimalSumOverflow, moved 2026-09-11 (#1026)

windowDecimalSumOverflow reports a windowed DECIMAL SUM that left the
128-bit range. It is aggregate.go's decimalSumOverflow one operator over,
with the same SQLSTATE (22003, PostgreSQL's numeric_value_out_of_range) and
the same position: a wrapped total is a different number wearing the right
type, so the query fails instead of answering it (ADR-0012 item 9,
ADR-0024 item 4).

The refusal is scoped to ONE FRAME, and inside that frame it is item 9's
rule verbatim: the frame's own rows are added in order, and a running total
that leaves the range fails even if later rows would bring it back. That is
the same answer `SUM(d) ... GROUP BY` gives for the same set of rows, which
is the whole contract this file exists to keep.

It is NOT sticky across the SLIDE, and an earlier draft of this comment
claiming it was described a defect rather than a rule. A sliding
accumulator carries state between frames, and a transient it holds while
moving from one frame to the next belongs to NEITHER of them: adding the
arriving row before subtracting the departing one made
`SUM(d) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW)` over three
9x10^37 values hold 1.8x10^38 between two frames that each hold 9x10^37,
and refuse a query PostgreSQL and the grouped spelling both answer.
exactFrameAcc.slide retracts before it adds and resets outright between
disjoint frames; windowExactFrames RECOMPUTES any frame whose incremental
state flagged overflow, and refuses only if the frame's own rows overflow
on their own.
