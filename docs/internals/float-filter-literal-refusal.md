# Float filter literal refusal

Source: internal/engine/exec/filter.go — floatConstError, moved 2026-09-11 (#1026)

floatConstError is decimalConstError's counterpart for the FLOAT columns
whose kernel arm declines a constant, and it covers the two spellings
separately because PostgreSQL reads them as two different literals.

A QUOTED constant is unknown-typed and is coerced with the COLUMN's own
input function (#646): `real = 'abc'` is 22P02 "invalid input syntax for
type real", `real = '1e400'` is 22003 "\"1e400\" is out of range for type
real", and the message names the literal's TEXT VERBATIM — the cast that
fails is text->real, so there is nothing to expand. Both verified live on
postgres:17-alpine. Before this, kernel.toFloat64 had no string arm at all
and answered 0.0 for every such constant, so the predicate silently became a
comparison against zero.

An UNQUOTED numeric constant is `numeric`, and the only way it fails is
FLOAT32's range: the #549 fix narrows each multi-element IN literal to
float32, and a literal that does not FIT a real narrows onto a value that
does — one past FLT_MAX becomes +Inf and would MATCH a genuine +Inf row, one
below real's smallest denormal becomes 0.0 and would match every zero row
(`real IN (1e-46, 3.1)` answered with the zero row before the underflow arm
existed). PostgreSQL raises 22003 for the whole predicate rather than
dropping the element, and it names the DIGITS there, because the cast that
fails is numeric->real and a numeric's text is its digits. A literal that is
itself ±Inf is a legal real value, not an overflow, and does not reach here.
