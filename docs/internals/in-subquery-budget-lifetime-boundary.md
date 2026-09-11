# In subquery budget lifetime boundary

Source: internal/engine/expr/compile.go — WithBudget, moved 2026-09-11 (#1026)

WithBudget charges an uncorrelated InSubquery's membership set to the
caller's memory tracker (ADR-0006, #528, #531), and hands the caller each
such node so it can Release the charge when the compiled tree's life ends.

It is an OPTION rather than a seventh CompileWith* function because the
options carry things a compile site already needs: swapping a call site to
an entry point that takes a budget and nothing else silently drops
WithSubqueryDeclTypes, and a scalar subquery then compares by the bytes of
its box again (#696). Every existing entry point takes opts; this composes
with them.

release is REQUIRED and the option refuses a nil one, because the failure it
prevents is worse than the bug it fixes: an InSubquery holds its membership
map for the life of the compiled tree, so charging without a teardown turns
an unaccounted map into a permanently-charged one, and a task that plans
several of them runs out of budget for work that has already finished.
InSubquery.Release is idempotent and safe on a node that never resolved.

What this does NOT do is bound the ALLOCATION. chargeMemory runs after
resolveSlow has built the map, so it makes the set visible to the budget and
turns a set that is over budget on its own into a query error; it does not
stop a subquery large enough to exhaust the machine from doing so. See
chargeMemory's doc.
