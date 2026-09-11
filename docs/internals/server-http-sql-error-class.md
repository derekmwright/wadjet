# Server http sql error class

Source: internal/server/server.go — func writeSQLError(w http.ResponseWriter, status int, msg string, err error) {, moved 2026-09-11 (#1026)
Superseded: Authorization errors can carry SQLSTATE42501 and do reach this function; 42501 is explicitly HTTP403, not the general class42 HTTP400 rule.

writeSQLError is writeError for a failure that may carry a SQLSTATE, and it
is the ONLY place this door decides an HTTP status from one.

Every refusal a STATEMENT earns comes through here — handleQuery's own
paths, handleDML's, handleExplain's, and the DDL sub-handlers the same POST
reaches by statement type. What does not, and correctly does not, is a
failure the statement did not cause: a malformed request body, a missing
`sql` field, and an authorization denial, none of which carry a SQLSTATE to
report. The round-1 review blocked on this claim being written wider than
the code: handleExplain's name resolution still called writeError, so
`EXPLAIN SELECT nosuchcol FROM t` dropped the 42703 the same statement
reports without the EXPLAIN.

A statement refused for what it CONTAINS is the client's error, not the
server's: a DECIMAL literal past the column's precision (22003) or text
naming no number (22P02) came back as 500 Internal Server Error with the
code nowhere in the body, so an HTTP client could neither see that its own
input was wrong nor branch on why (#647 re-review). #848 is the same defect
one class wider: `SELECT nosuchcol FROM t` and `SELECT * FROM nosuchtable`
answered 500 with no `sqlstate` at all, because the name-resolution, plan
and execution paths still called writeError. Every refusal on this door now
comes through here.

The class → status table, which is also docs/api-reference.md's:

	0A  feature not supported            400
	22  data exception                   400  (2201x, 22003, 22012, 22P02, …)
	23  integrity constraint violation   400
	42  syntax error / access rule       400  (42601, 42703, 42P01, 42883, …)
	anything else                        the caller's status — 500 for XX
	                                     (internal), 58 (storage/system) and
	                                     any class this engine has not placed.

The promotion applies to a SERVER status only: a caller that chose 5xx was
blaming the server for something the client's statement caused, which is
the whole defect. A caller that chose 404 or 409 made a considered
statement about the RESOURCE — `DESCRIBE nope` is 404 and a duplicate
CREATE TABLE is 409 — and that stands, with the class now beside it. The
first round-2 pass promoted those too and turned `DESCRIBE nope` from 404
into 400, which is a contract change no issue asked for.

The MESSAGE for a classified error is err.Error() verbatim: the same bytes
the pgwire door puts in the ErrorResponse 'M' field for the same statement,
so a client that reads one door's message can compare it with the other's.
The caller's contextual prefix ("execution error: ") survives only on the
unclassified path, where naming the stage is the only localization there is.

There is deliberately no second SQLSTATE table here: the code comes from
sqlerr.StateOf, which is the same call pgwire's sendQueryError makes.
