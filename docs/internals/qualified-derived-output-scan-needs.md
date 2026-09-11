# Qualified derived output scan needs

Source: internal/planner/logical/optimizer.go — sanitizeScanNeeds, moved 2026-09-11 (#1026)

The qualifier matches this scan and the column does NOT
exist in it. A derived table's alias BECOMES the scan's
TableAlias, so `x.w` over `(SELECT g*3 AS w FROM t) x` is
the Project's OUTPUT name arriving qualified — and keeping
it wrote a column the table does not have into the scan's
read set. Every "what does this stage emit" model reads
that list (physical.stageEmittedColumns and, through it,
emittedThroughPassThrough, gatherOutputSources,
stageStreamColumns), so the phantom made the reachability
check, the sort-key resolver and the window-key resolver
all believe in a column no file has: the DAG then either
skipped the materialization that would have created it or
failed at dispatch with `column "w" does not exist in the
input schema`, for queries PostgreSQL answers (#776, and
ADR-0026 §4b, which named this and stopped here).

It is the rule the NodeWindow arm of pushColumnNeeds
already applies to `__win_N` — a node's own output is not a
need of the node below it — reached from the SANITIZE side,
which is where a QUALIFIED spelling arrives.

Only when the schema is known: with no catalog at plan
time every name is kept, and full width is the safe
failure mode. A "__"-prefixed derived name keeps the
worker guard's semantics the bare branch below gives it.
