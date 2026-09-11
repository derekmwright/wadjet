# Dml predicate compilation and subqueries

Source: wadjet/dml.go — func BuildDMLPredicate(target plansql.DMLTarget, schema []parquet.Column, sub *DMLSubqueryEnv) (DMLPredicate, error) {, moved 2026-09-11 (#1026)
Superseded: The subquery reads the catalog current manifest, not an explicitly captured statement snapshot. This statement publishes its markers only at the end; the prose below does not establish isolation from concurrent commits.

BuildDMLPredicate compiles a DML WHERE clause against a table's schema. An
empty clause compiles to nil — "every row".

The SCHEMA is a parameter, not an optional extra, because the DML doors do
not go through the planner and so had no name-resolution step at all:
`UPDATE t SET n = 1 WHERE nosuchcol = 1` compiled fine, evaluated to NULL on
every row and reported "UPDATE 0", where PostgreSQL raises 42703 (#678).
Every column the clause names is resolved here, before anything executes.

It is exported because MatchDMLRows is the other half of the contract and
is the one that must be used to RUN a predicate. (The HTTP door is no longer
a second caller: since #815 it reaches the executors through
DB.ExecuteParsed like everything else.)

A SUBQUERY IN A DML PREDICATE (#688).

`DELETE … WHERE id IN (SELECT …)`, `NOT IN (SELECT …)`, a scalar subquery
and a correlated `EXISTS` were all 0A000 here. The reason was structural:
this function is not a planner. It parsed, resolved the column names against
the target's schema, and called `expr.Compile` with a NIL runner and no
outer scope, so every planner-resident guarantee was absent on this door.

It is answered now, and the shape of the answer is what makes it not the
bounded repair ADR-0031 forbade. That one was `expr.CompileWithRunner` — a
runner and nothing else — which closes `IN`, `NOT IN` and the scalar
subquery and leaves CORRELATED `EXISTS` refused, because a compile site with
no outer scope cannot classify a subquery as correlated in the first place.
The scope is the missing half, and a DML statement has the simplest one
there is: exactly ONE relation, the target, under its alias when it has one
and its own name when it does not, with the columns of the schema this
function was already handed. Given that scope,
`expr.CompileWithScopeResolver` builds the same correlated evaluators the
query path builds, and `EXISTS (SELECT 1 FROM s WHERE s.id = t.id)` — the
shape #688's own body names first — answers.

THE PREDICATE IS STILL COMPILED AND NOT PLANNED, which is ADR-0031's
position and is unchanged: the door still walks its files and evaluates the
clause per row, and the structural DELETE-as-a-planned-SELECT design that
record blocks on a projectable row identity is still blocked and still
unnecessary here. What is planned is the SUBQUERY, through the ordinary
SELECT path.

TWO CONSEQUENCES ARE THE QUERY PATH'S, INHERITED RATHER THAN INVENTED.
An uncorrelated subquery is executed ONCE and memoized; a correlated one is
re-run per outer row with the outer values substituted as typed literals
(ADR-0021 §1e), so an outer value with no literal spelling is 0A000 there as
it is in a SELECT. And a subquery that cannot be RUN fails the statement
rather than deciding it (§1c) — which on a WRITE door is the difference
between refusing and deleting the wrong rows.

THE SNAPSHOT. The subquery runs against the manifest the catalog holds while
the statement is scanning, and a DML statement commits its markers at the
end (ADR-0030), so a subquery over the TARGET TABLE reads the pre-statement
state — which is what PostgreSQL does. `DELETE FROM t WHERE id IN (SELECT id
FROM t WHERE …)` is in the census with PostgreSQL's answer beside it.

THE EMPTY-PREDICATE BACKSTOP. A nil predicate is the widest answer this
function can give — every row of the table — so "the statement had no
WHERE" and "the parser dropped the statement's WHERE" must not look the
same here. They did, and the second one emptied tables: a DELETE with an
aliased table returned an empty WhereSQL and deleted everything (#686). The
check below is not about that spelling, which the parser now reads; it
makes the CLASS unreachable, so the next clause any parser path fails to
carry fails the STATEMENT instead of widening it (ADR-0019, correctness-fix
protocol item 8: loud beats plausible).
