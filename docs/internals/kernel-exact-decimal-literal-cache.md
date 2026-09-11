# Kernel exact decimal literal cache

Source: internal/engine/exec/kernel/decimal_literal.go — type DecimalLiteral struct {, moved 2026-09-11 (#1026)

DecimalLiteral is a numeric literal held as the EXACT text it was written
with, ready to be compared against a DECIMAL column in that column's own
domain.

It exists because a literal is not a float64. `compileLit` used to turn
every numeric literal that is not an int64 into one, and a float64 carries
~15-16 significant decimal digits where a DECIMAL(38,10) carries 38: the
literal a user typed was silently replaced by the nearest double before it
ever met the column, so `= 493827160549382.7160549350` matched nothing and
`>` gained a row (#452). Text is the only lossless carrier the whole way
from the parser to a kernel, which is why the filter kernels already take
their DECIMAL constant that way (compareFilterDecimal).

The resolution — text, at the column's scale, plus the residual of any
digits the scale cannot hold — is the SAME one compareFilterDecimal
performs, through the same decimalLiteralAt: one comparison rule for one
predicate, per #394. What this type adds is the cache, for the
row-at-a-time paths that would otherwise re-parse per row.

Safe for concurrent use: a resolved literal is published whole, through an
atomic pointer, and a losing racer merely re-resolves to the same value.
