# Quoted number declared width order

Source: internal/engine/expr/quoted_literal.go — quotedNumberOrder, moved 2026-09-11 (#1026)

quotedNumberOrder orders a NUMBER against a QUOTED literal under the rule
the NUMBER's own type selects — PostgreSQL coerces the unknown-typed literal
to that type and compares there, with no widening (#646).

The TYPE decides, never the box, and here that is load-bearing rather than
stylistic: ColRef.Eval WIDENS on the way out — a FLOAT32 column boxes as
float64 and an INT32 one as int64 — so a box-driven rule would compare
`r < '3.1'` at double width, which is a different predicate for every
literal a real cannot represent, and would skip int4's range check. The
widening is exact and order-preserving, so narrowing the box back inside the
real arm recovers the stored value bit for bit (the same argument
realLitSet.contains makes).

typ comes from numberKindType: the boxed layer's DECLARATION where it read
one, and the box otherwise (a numeric literal, a computed numeric
expression), which is exact for those because none of the four shares a Go
box with another once the KIND has separated DECIMAL and STRING out
(ADR-0012 item 8).

A literal the type refuses RAISES rather than falling through. Falling
through is what made `CASE WHEN int_col < 'NaN'` answer every row: compare()
finds no reading of an int64 against "NaN" and reports FALSE, which is a
value answer to a question that has none.
