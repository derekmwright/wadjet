# Sql subquery row read limits

Source: internal/planner/sql/row_limit.go — func AppendRowLimit(sql string, info *SelectInfo, n int) string {, moved 2026-09-11 (#1026)

AppendRowLimit returns sql bounded to at most n rows, when a LIMIT can be
appended to it without changing what it means.

It exists because two of the four subquery constructs do not want a SET.
EXISTS asks whether there is A row and a scalar subquery is an ERROR past
ONE, so reading the whole result to answer either is work nobody asked for
— and on the DML door it was a WRONG REFUSAL: that door bounds what a
subquery may return (54000, `WADJET_IN_SET_MAX`), so
`DELETE FROM t WHERE EXISTS (SELECT 1 FROM big)` refused at the ten
thousandth row of a question one row answers, and a scalar subquery past
the bound reported 54000 where this engine's own rule is 21000. Bounding
the READ puts each construct back inside the rule it is judged by: IN keeps
the bound because IN really does need the set.

The append is declined — sql is returned unchanged — when the query already
bounds itself with LIMIT or OFFSET, or is a UNION, because in those spellings
a trailing LIMIT is not an append: it is either a syntax error or a second,
differently-scoped bound. Those keep whatever behaviour they had.

info must be the parse of sql. Callers that hold one (the correlated
evaluators parse once at compile time and rebuild per row) pass it; callers
that hold only text use WithRowLimit.
