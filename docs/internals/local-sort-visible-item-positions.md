# Local sort visible item positions

Source: internal/planner/physical/sort_plan.go — sortKeyLocalSlotPos, moved 2026-09-11 (#1026)

```go
// sortKeyLocalSlotPos is the single-process pipeline's address for an ORDER BY
// key: sortKeySlotPos's ordinal answer first, and then the SELECT-list
// POSITION of the item the key NAMES.
//
// The name alone stopped being an address the moment two output columns could
// share one, which is #556/#557's position identity one consumer over.
// `WITH cte AS (...) SELECT a.id, b.id FROM cte a JOIN cte b ON a.a = b.a
// ORDER BY a.id, b.id` publishes two columns called `id`; the Sort's keys are
// built as `cleanExpr(ob.Column)`, which STRIPS the qualifier, so both keys
// became `id` and `columnIndexFallback` bound both of them to the FIRST one.
// The second key was never applied: PostgreSQL 17 answers
// `1,1 | 1,2 | 1,3 | 1,8` and the single-process path answered
// `1,8 | 1,3 | 1,2 | 1,1` — the right rows in the wrong sequence, which no
// multiset comparison can see (#905, the #629 family). The stage DAG is right
// on this shape already, because its sort keys keep the QUALIFIED spelling and
// its join stage publishes `a.id` and `b.id` under those names; the single
// path's Project output carries neither, so the position is the only address
// it has.
//
// The match is the one PostgreSQL makes: an ORDER BY term may name an output
// column, by its alias or by the spelling the SELECT list wrote. Exactly one
// visible item must match — two is ambiguous and keeps today's by-name
// resolution, which is also what the qualified-to-bare fallback is for.
//
// It is deliberately NOT wired into sortKeySlotPosStage. A position there
// addresses the PRODUCING STAGE's output, which is the select list only for a
// single narrowed relation (see that function), and the DAG does not need it.
```

## Amendment, 2026-09-12 (#1014)

The by-name half of this function is now `sortKeyWrittenSlotPos`, and it is
SHARED with the DAG: `sortKeySlotPosStage` calls it too, under its own
measured proof that the position addresses the producing stage's stream
(`producerPublishesSelectList`). The paragraph above saying it is "deliberately
NOT wired into sortKeySlotPosStage" described the state in which #1014 lived —
`ORDER BY b.amount` beside an output column also called `amount` sorted by the
other one on the distributed arms and by the right one in process. What the
two callers still do not share is the PROOF; what they now share is the
resolution.
