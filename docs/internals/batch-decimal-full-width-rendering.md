# Batch decimal full width rendering

Source: internal/engine/batch/decimal.go — func (d Int128) FormatDecimal(scale int) string {, moved 2026-09-11 (#1026)

FormatDecimal renders the unscaled value as a decimal string at the given
scale — the text form of a DECIMAL column, and so what GetValue hands the
row map, ToRows, the JSON encoder and the pgwire text protocol.

The fraction is EXACTLY scale digits, never fewer. It used to be trimmed of
trailing zeros, so a numeric(9,2) holding -24.50 reached a client as
"-24.5" and a numeric(38,10) zero as "0.0" (#453). PostgreSQL renders a
numeric(p,s) at its DECLARED scale always, and ADR-0012 makes PostgreSQL
the authority — but the deeper reason is that a DECIMAL column exists
BECAUSE its scale is part of the value's meaning. A currency column that
spells itself "-24.5" is one a BI tool displays wrong, and any client that
string-compares or formats from the text gets a different answer than it
gets from PostgreSQL. The trim also reached the wire's binary form, whose
dscale header pgNumericDigits counts off this very string.

scale <= 0 renders no point at all — "12345", not "12345." — which is
PostgreSQL's numeric(p,0) too.

It formats the whole 128 bits. It used to read only Lo, through
`v := int64(abs.Lo)` and an int64 divmod, which was wrong twice over: a
value past 64 bits rendered as its low half (Int128{Hi:5, Lo:0x112210f4-
7de98115} at scale 10 came out 123456789.0123456789 instead of
9346828825.8671214869), and any magnitude with Lo >= 2^63 made that int64
negative, so the sign leaked into both halves and produced text that is not
a number at all — "--922337203.-6854775808" for unscaled Int64Min (#434).

Splitting the digit STRING at the scale is also what makes the result exact
for every scale: math.Pow10 is a float64, and 10^23 has no exact one.
