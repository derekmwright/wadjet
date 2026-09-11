# Pgwire binary numeric input

Source: internal/server/pgwire/bindparams.go — func renderBinaryNumeric(raw []byte) (string, error) {, moved 2026-09-11 (#1026)

renderBinaryNumeric decodes PostgreSQL's binary `numeric` wire format —

	uint16 ndigits, int16 weight, uint16 sign, uint16 dscale, int16 digits[ndigits]

(numeric_recv in the backend reads ndigits with pq_getmsgint(..., uint16),
same as sign and dscale; weight is the one signed field — "we allow any
int16 for weight", per its own comment) — where the value is
sum(digits[i] * 10000^(weight-i)) under sign and dscale
is the number of fraction digits to DISPLAY — into the exact decimal TEXT
PostgreSQL itself would print for the same value, then hands that text to
renderTextParam so the bare-literal path (and its "confirm it's a number"
fallback) stays the one place a numeric literal gets written, text or
binary.

pgx v5 sends this for any parameter whose declared OID is 1700, which
paraminfer.go now infers for a placeholder compared against a DECIMAL
column — so this is the ordinary path for a Go client, not an exotic one
(#464). Before this, oidNumeric fell into the same arm as oidText and the
digit-group bytes were read back as if they were ASCII: garbage that
failed renderTextParam's number check and went out as a quoted string,
comparing a DECIMAL column to text and matching nothing (or, past `>`,
coercing in whatever direction that comparison's own fallback took).

This is the read side of appendBinaryNumeric/pgNumericDigits (server.go),
which encodes the same format for values wadjet SENDS. Neither calls the
other, so a header-arithmetic mistake here would not be caught by that
side agreeing with itself.
