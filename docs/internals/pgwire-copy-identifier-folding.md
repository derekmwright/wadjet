# Pgwire copy identifier folding

Source: internal/server/pgwire/server.go — func copyIdent(s string) string {, moved 2026-09-11 (#1026)
Superseded: This block precedes copyIdent, not parseCopySQL; identifier conversion is the immediate declaration.

parseCopySQL extracts the table name and optional column list from a
COPY table [(col1, col2, ...)] FROM STDIN statement.
copyIdent reads ONE identifier out of a COPY statement the way the lexer
reads every other identifier: an unquoted one FOLDS, a delimited one keeps
its bytes and loses only its quotes (#731).

COPY is hand-parsed rather than lexed, and this step used to be
`strings.Trim(col, "\"")` — which strips the quotes and folds nothing, so by
the time the name reached a resolver an unquoted `WatchID` and a delimited
`"WatchID"` were the same string and the rule that distinguishes them
("byte-exact, then a unique case-insensitive match FOR A REFERENCE THAT IS
ITSELF FOLDED") had nothing left to key on. Measured over a LOWER-case
schema `lhits(watchid, useragent)` — so this was never a CamelCase-only
problem:

	COPY lhits (watchid, useragent)  ACCEPTED
	COPY lhits (WatchID, UserAgent)  REFUSED, and PostgreSQL accepts it
	COPY lhits (WATCHID, USERAGENT)  REFUSED, and PostgreSQL accepts it

and over a CamelCase one it went the other way: `COPY hits ("useragent")`
was ACCEPTED against a column spelled `UserAgent`, where PostgreSQL raises
42703 for the delimited name.
