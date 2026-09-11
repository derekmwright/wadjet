# Having aggregate reuse identity

Source: internal/planner/logical/builder.go — BuildFromSelectWithCTEs, moved 2026-09-11 (#1026)

Reuse an identical aggregate the SELECT list already
computes, so HAVING references its output column instead
of adding a second copy under a synthetic name. Match on
the NORMALIZED fields rather than on rendered text: the
old key rebuilt "count()" from an AggExpr whose InputCol
the normalization above had already emptied, and compared
it against the AST's "count(*)", so `SELECT a, COUNT(*)
AS c ... HAVING COUNT(*) > 1` never matched — it counted
twice and leaked the second count as __having_N.

The reuse is DECLINED when that output column's name is not
the aggregate's alone. An aggregate may be ALIASED like a
group key — `SELECT g + 1 AS k, COUNT(*) AS "g + 1" …
GROUP BY g + 1` — and then the aggregate's output batch
carries TWO columns of that name, the key's and the count's.
Every by-name lookup answers with the FIRST, which is the
key, so the HAVING was evaluated against the key's values:
`COUNT(*) > 100` became `g + 1 > 100`, false in every group,
and the query returned ZERO rows for PostgreSQL's eight —
on all four arms, in silence (#785, ADR-0026 §3a).

The predicate reaches the aggregate through the slot it OWNS
instead: the branch below mints `__having_N`, a name nothing
else in the batch answers to, and the aggregate computes the
value a second time under it. The SELECT list is untouched —
a duplicate OUTPUT name is legal SQL that PostgreSQL answers,
and the consumers that publish it tell the two apart by
CLASS and POSITION rather than by name (#575).
