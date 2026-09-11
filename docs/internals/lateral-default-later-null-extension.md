# Lateral default later null extension

Source: internal/planner/logical/lateral_empty_input.go — lateralEmptyInputPlan, moved 2026-09-11 (#1026)

A RIGHT or FULL join further along the FROM clause MANUFACTURES
rows in which the lateral's columns are NULL. Neither half of this
repair can tell such a row from one the lateral itself produced:
the COALESCE would read a manufactured NULL as 0, and an ON moved
into the enclosing WHERE would DELETE the manufactured row instead
of leaving it alone. Both are wrong, and both were measured wrong
(`... JOIN LATERAL (…) s ON s.n > 1 RIGHT JOIN c ON …` lost the
unmatched right row; `… ON true RIGHT JOIN …` printed n=0 where
PostgreSQL prints NULL).

The repair's rewrites live in the ENCLOSING query — the SELECT
list, the WHERE — and so they see the whole FROM clause's result,
while what they are entitled to speak about is the LATERAL's own
output. While those two are the same relation the repair is sound;
a later RIGHT or FULL join is exactly what separates them.
Expressing it would need the default applied at the lateral's own
output, before the later join sees it, which is a plan-level change
rather than a SelectInfo rewrite.

So: decline, and leave the query exactly as written. That is what
this engine answered before the repair existed, and it is
PostgreSQL's answer for every one of these shapes but the ungrouped
empty-input row itself, which is pinned as the boundary.
