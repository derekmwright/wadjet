# Bytes cast text versus like

Source: internal/engine/expr/expr_cast.go — Cast.Eval, moved 2026-09-11 (#1026)

A BYTES operand boxes as a raw []byte — both here and from
GetValue, since ColRef.Eval has no divergent fast path for
TypeBytes the way it does for the four types boxedTextOperand
resolves — and PostgreSQL's `bytea::text` is `\x` followed by
LOWERCASE hex, under the default bytea_output = hex. That is the
rendering, per ADR-0012 item 1: PostgreSQL gives BYTES a printed
form, so wadjet does not invent a second one.

Two earlier answers were both wrong. fmt.Sprint's default verb
printed Go's slice-of-decimal-bytes debug notation
("[98 121 116 ...]"), and the raw bytes as a Go string — which
agreed with kernel.likeTextRenderer but not with PostgreSQL —
produced, for 0xff 0xfe 0x00 0x41, a string that is invalid UTF-8
and holds an embedded NUL. No PostgreSQL server can put a NUL in
a text-format DataRow field, and libpq TRUNCATES at one, so the
same query answered four bytes to pgx and two to psql. The hex
form is pure ASCII and has neither problem (#570).

LIKE deliberately does NOT follow it here: PostgreSQL's `~~` over
bytea is BYTEWISE (verified live — `'\xfffe0041'::bytea LIKE
'%A%'` is true, matching the 0x41 byte, not the letter in a hex
spelling), so kernel.likeTextRenderer keeps matching the raw
bytes. The two disagree in PostgreSQL, so they disagree here.
