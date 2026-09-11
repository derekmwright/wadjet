# Batch decimal choice common type

Source: internal/engine/batch/decimal_result_type.go — func DecimalCommon(in []DecimalType) (DecimalType, bool) {, moved 2026-09-11 (#1026)
Superseded: The 38-digit declaration cap is narrower than the signed Int128 carrier, which can hold some 39-digit values; carrier fit alone does not establish declared precision.

DecimalCommon is the COMMON DECIMAL type of a set of operands (ADR-0024
item 2): the type every one of them can be moved into without dropping a
digit it holds.

	scale     = max over the operands
	precision = max over the operands of (precision - scale), plus that scale

The scale is the maximum because that is the only choice that moves no
value: a narrower one would DROP digits a wider operand holds, which is the
truncating half of #533. Precision is then reconstructed from the widest
INTEGER part rather than taken as max(precision), because max(precision) is
not a bound on the widened values — DECIMAL(18,2) alongside DECIMAL(9,4)
needs 16 integer digits at scale 4, i.e. 20, where max(precision) would
declare 18 and hand the parquet writer a leaf too small for the value
(ADR-0018 §4's encoding rule keys off precision).

This is the rule for every construct that CHOOSES BETWEEN its operands
rather than computing a new number from them: a set operation's arms,
CASE's branches, COALESCE/NULLIF/IFNULL/IF/GREATEST/LEAST's arguments.

Item 3's p>38 ADJUSTMENT is deliberately NOT applied here, and the reason is
item 7's: a choice's result IS one of its operands' stored values, so giving
up fraction digits would DROP digits a row actually holds — over
`GREATEST(numeric(38,0), numeric(11,10))` the adjustment reduces the scale
from 10 to 6 and the second column's 0.0000000001 becomes 0.000000, silently.
Arithmetic is where the adjustment belongs (DecimalResultType): a computed
scale is derived rather than carried, so there are no stored digits to lose.
The precision cap alone is therefore the whole rule here, and a value with no
carrier at the resulting type is a per-value 22003 at the store rather than a
plan-time refusal of the query — which is what lets
`GREATEST(numeric(38,30), bigint)` answer for every value that fits.

The result is capped at the carrier's full width — 38 digits is what an
Int128 holds. The cap reduces the PRECISION and leaves the scale, so it is
a RANGE reduction, which is why a value with no Int128 at the output type
is then an ERROR at the moment of coercion rather than a wrapped number
(ADR-0024 items 4 and 7; #552 records the cost).

ok=false means an operand contributed nothing this rule can use — a
computed DECIMAL whose (p,s) nobody resolved, or a non-numeric type. The
caller must then decline to declare a DECIMAL at all rather than guess one.
