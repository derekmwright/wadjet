# Exact decimal scalar math

Source: internal/engine/expr/decimal_scalar_fn.go — decimalScalarFn, moved 2026-09-11 (#1026)

The scalar math functions that answer in their argument's OWN domain —
abs/ceil/floor/round/trunc/sign/mod — computed exactly over a DECIMAL
(ADR-0024 items 2 and 3, #668).

PostgreSQL answers all seven in `numeric` over a numeric argument. Wadjet
declared every one of them RetFloat64 and computed through ToFloat64, whose
default arm parses a DECIMAL column's rendered TEXT with fmt.Sscanf — so the
value made a round trip through a double before any rounding happened, and
on the paths where that parse fails it produced 0 for every row. ROUND over a
DECIMAL was the visible one; the whole family shares the cause.

The transcendental functions — sqrt/exp/ln/log/power — stay float64. That is
a DELIBERATE, recorded divergence of the class ADR-0012 item 9 already
carries for STDDEV/VARIANCE/CORR/MEDIAN: PostgreSQL answers them in numeric,
and doing the same here means building an exact fixed-point tower, not
widening an accumulator.

Shape: a typed node that replaces the FuncCall and carries it as a
FALLBACK, the way ColShapeLen does for the length family. The argument's
type is not known until a batch arrives, so the node resolves per batch and
delegates every non-DECIMAL argument to the fallback — semantics for every
other type are unchanged, bit for bit.
