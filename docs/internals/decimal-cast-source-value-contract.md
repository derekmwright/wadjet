# Decimal cast source value contract

Source: internal/engine/expr/cast_decimal.go — Cast.castToDecimal, moved 2026-09-11 (#1026)

castToDecimal converts one value to the destination's exact DECIMAL, boxed
as its rendered text — the same box a DECIMAL COLUMN produces, so a cast
result and a stored value reach every consumer in one shape.

The four source families and what PostgreSQL does with each, all verified
live on 17.11:

	numeric  12.7501::numeric(9,2)  = 12.75      (rounds, half away from zero)
	integer  1::numeric(10,2)       = 1.00
	float8   0.1::float8::numeric(10,4) = 0.1000 (the double's own decimal)
	text     '12.75'::numeric(9,2)  = 12.75      ('abc' is 22P02)

A float is spelled as its SHORTEST ROUND-TRIP decimal first — the unique
decimal that reads back as the same double, which is also what PostgreSQL
prints for it — and then resolved through the same exact text path as
everything else. Going through the binary value instead would carry the
double's 55-digit exact expansion, which rounds differently at the target
scale than the number the user can see.

Text is read from its DIGITS, not from the DECIMAL its spelling names: the
value is rounded once, half away from zero, by the first digit past the target
scale (batch.DecimalTextRoundedAt), however wide it was written.
`'14.' || 40 zeros` and a 42-digit `'14.000…0001'` are both 14.0000 as
numeric(18,4) on PostgreSQL 17.11 and here; reading through the spelling's own
DECIMAL first refused both 22003, since neither spelling fits 38 digits. A
value whose rounded digits do not fit the carrier at the target scale is still
22003 (`'1e40'::numeric(38,0)`).

NaN and the infinities are 22003 with a message naming ADR-0024 item 6: a
wadjet DECIMAL is an Int128 with no bit pattern for them, and PostgreSQL's
numeric does store NaN — a documented divergence, refused loudly rather than
stored as something else.
