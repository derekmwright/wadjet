# Compiled predicate subquery environment

Source: internal/planner/physical/subquery_env.go — EnsureMemoryTracker / SubqueryEnv, moved 2026-09-11 (#1026)

SubqueryEnv returns what a door that COMPILES a predicate without PLANNING
it needs in order to answer a subquery inside that predicate the way the
query path answers one: a runner, the resolver for a subquery's own FROM
columns, and the compile options that carry a scalar subquery's declared
output type.

The DML doors are that door. ADR-0031 records the position — a DML
predicate is compiled, not planned — and the consequence was that every
planner-resident guarantee was absent there, including the ability to run a
subquery at all: `DELETE FROM t WHERE id IN (SELECT id FROM s)` was 0A000
(#688). Handing them the same three pieces the planner hands its own
compile sites keeps ONE answer to "what does this subquery mean" rather
than a second implementation of it.

ctx is stored as the planner's plan context, so a subquery the compiled
expression runs later — once for an uncorrelated one, per outer row for a
correlated one — respects the statement's cancellation and deadline.

The three pieces are exactly what `CompileWithScopeResolver` takes past the
scope: pass them with the outer scope the caller knows (for a DML statement
that is one relation, the target under its alias or its name) and a
correlated subquery compiles as correlated, which is what makes
`EXISTS (SELECT 1 FROM s WHERE s.id = t.id)` answerable rather than
refused.
EnsureMemoryTracker creates this planner's per-query memory tracker if it
does not have one yet, so SubqueryEnv's budget option is non-nil.

The tracker is built lazily, by getSpillManager, the first time something
asks to spill — and a door that COMPILES a predicate without PLANNING it
never asks. Without this the IN-subquery membership map that SubqueryEnv's
budget option exists to charge was the one allocation on the DML door that
nothing accounted for (ADR-0006, #531).
