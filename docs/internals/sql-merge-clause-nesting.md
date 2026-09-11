# Sql merge clause nesting

Source: internal/planner/sql/parser.go — func scanMergeClauseUntil(l *lexer, stop ...TokenType) string {, moved 2026-09-11 (#1026)

scanMergeClauseUntil consumes input up to the next stop token that is at
DEPTH ZERO — outside every parenthesis and every CASE … END — and returns
the raw text it consumed.

All four of a MERGE's scans used to stop at the first stop token whatever
its nesting, and a CASE expression carries the very keywords they stop on
(#722):

	ON        stops at WHEN   broken by `ON CASE WHEN … END`
	AND       stops at THEN   broken by `AND CASE … THEN … END`
	THEN UPDATE SET  at WHEN  broken by `SET n = CASE WHEN … END`
	THEN INSERT …    at WHEN  broken by `VALUES (CASE WHEN … END)`

The issue names the first two. A fix that patched only those would leave
`ON` and `THEN INSERT` broken, which is why all four go through one
function: the nesting rule is a property of a MERGE clause, not of one
clause position.

The pattern is collectUntil's (dml_parser.go): the stop test is
`depth == 0 && stop`, never `stop` alone, and EOF breaks unconditionally so
an unbalanced `(` or a CASE with no END cannot spin at depth > 0 forever —
FuzzParseSQL found that shape once already.
