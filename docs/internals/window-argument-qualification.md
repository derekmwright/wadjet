# Window argument qualification

Source: internal/planner/physical/output_rename_resolve.go — windowArgKeepsItsQualifier, moved 2026-09-11 (#1026)

```go
// windowArgKeepsItsQualifier reports whether a window function's ARGUMENT has
// to keep the table qualifier the query wrote, because dropping it would leave
// a name MORE THAN ONE arm of the window's input publishes.
//
// `cleanExpr` strips the qualifier unconditionally, which is right almost
// everywhere — the streams carry the column bare and `exec.Window`'s
// `columnIndexFallback` finds it — and is a coin toss where two arms of a join
// publish one alias. It landed on opposite sides of that toss on the two
// execution paths, because they name a join's duplicate columns differently:
//
//	SELECT x.id, x.w, y.w, SUM(y.w) OVER () AS s
//	FROM (SELECT id, a AS w FROM decpair) x
//	JOIN (SELECT id, a * 100 AS w FROM decpair) y ON x.id = y.id
//	-- PostgreSQL s = 5299.00 (Σ y.w)
//	-- single    s =   52.99  (Σ x.w) — its stream spells x's copy `w`
//	-- and the mirror, SUM(x.w) OVER (), is wrong on the DAG instead,
//	--    whose stream spells Y's copy `w` and x's under its source name
//
// So the qualifier is kept exactly where it is load-bearing, and the answer is
// today's bare name everywhere else. Two arms publishing one name is the whole
// of the trigger, and it is the same question `ownedJoinArm` asks one resolver
// over: which relation does this reference name, and does anything else answer
// to the same bare column.
```
