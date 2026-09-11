# Parquet exact decimal text parts

Source: internal/storage/parquet/decimal_value.go — func DecimalTextParts(s string) (neg bool, digits string, exp int, ok bool) {, moved 2026-09-11 (#1026)

DecimalTextParts splits numeric TEXT — plain or exponent form — into its
sign, its digits with the decimal point removed, and the power of ten those
digits must be multiplied by, exactly and without ever going through a
float64: the value is `(-1)^neg * digits * 10^exp`.

The exponent is read as an INTEGER and folded into the power of ten, never
expanded through a float64. Expanding through strconv.ParseFloat is what
made `1e400` unreadable — ParseFloat reports ErrRange, the old expansion
gave up and handed the untouched "1e400" to a parser with no exponent
handling, and that returned the value ZERO, which matched every row holding
zero (#463). Here 1e400 is simply a number with a large exponent: it
resolves, saturates for a comparison (#462) and is 22003 for a value.

ok=false means the text names no number. It is deliberately NOT reported as
the value zero: a constant nobody can read used to compare EQUAL to every
stored zero (#463), and on the write path it used to be STORED as zero
(#647), which is the same failure one layer down.

The grammar is PostgreSQL's numeric input MINUS digit separators and radix
prefixes. PostgreSQL 16 added both to numeric_in, so 17.11 accepts `1_000`,
`1_0.5`, `0x10`, `0b101` and `0o17` (verified live) where this refuses all
five with 22P02. That gap is #634 and is deferred, not decided here; it is a
REFUSAL of input PostgreSQL takes, never a different value for input both
accept, so nothing silently disagrees while it is open.

The digit string is the only allocation in this file's parse, and it happens
only when a value HAS both an integer and a fraction part; the value builder
below never asks for it at all (decimalTextSplit).
