# Having shared aggregate name test

Source: internal/planner/logical/builder.go — aggOutputNameIsShared, moved 2026-09-11 (#1026)

aggOutputNameIsShared reports whether an aggregate's output column name is
also answered by something else in the aggregate's own output batch — a
GROUP BY key published under that name, or a second aggregate output.

It is the test that decides whether a HAVING may REFERENCE that column. The
aggregate emits its keys and its outputs into ONE schema and
batch.RecordBatch.ColumnIndex returns the FIRST match, so a name two columns
answer to cannot say which one a predicate meant (#785).

The keys are compared the way the RESOLVER reads them, which is the question
this predicate is really asking: `exec.columnIndexFallback` tries the exact
spelling and then the BARE part of a qualified one, so a HAVING naming `a`
finds a key the aggregate emits as `x.a` just as surely as one it emits as
`a`. Comparing `cleanExpr(gb)` alone — whitespace only, the qualifier intact
— answered false for every QUALIFIED key, so `SELECT x.a AS b, SUM(x.b) AS a
FROM decpair x GROUP BY x.a HAVING SUM(x.b) > 0` reused the SELECT list's
aggregate, the rewritten predicate named `a`, and the filter compared the
GROUP KEY (#968; the same defect ADR-0026 §3a records for the unqualified
spelling).

Answering true where the operator would in fact have kept the qualifier
costs one extra aggregate computation under a `__having_N` slot and can
never be a wrong answer — which is why the bare test is the whole rule here
and not an approximation of `exec.PublishedGroupKeyNames`.
