# Pgwire string column oid

Source: internal/server/pgwire/server.go — func pgColumnOID(m wadjet.ColumnMeta) int {, moved 2026-09-11 (#1026)

pgTypeOID maps Wadjet types to PostgreSQL type OIDs.
pgColumnOID is pgTypeOID with the whole column meta in hand, for the one
type whose OID depends on more than its name: a STRING cast to the VARCHAR
family is PostgreSQL's `character varying` (1043) rather than unconstrained
`text` (25), and a declared LENGTH rides in the type modifier beside it
(#838).

The OID follows the DESTINATION NAME, not the presence of a length.
`CAST(x AS VARCHAR)` describes as 1043 at typmod -1 on the server, exactly
as `CAST(x AS VARCHAR(4))` describes as 1043 at typmod 8 — the length is a
constraint on a varchar, not what makes it one, and sending 25 for the
unparameterized spelling made the same cast change type when its modifier
was dropped (round-1 review, P2). StringLength carries both answers:
positive is a length, -1 is `character varying` unconstrained, 0 is text.

`character(n)` (1042) is deliberately NOT sent for `CAST(x AS CHAR(n))`.
PostgreSQL's bpchar PADS a short value to n and then strips the blanks again
for length(), for `||` and for every comparison; this engine has one
TypeString and none of that, so declaring 1042 would name a type whose three
defining behaviours it does not implement. `character varying(n)` states
exactly what the value IS — at most n characters, compared by bytes — and
the padding residual is recorded in ADR-0012 item 5.
