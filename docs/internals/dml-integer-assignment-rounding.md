# Dml integer assignment rounding

Source: wadjet/dml.go — func assignIntegerValue(v any, col parquet.Column, srcFloat bool) (any, error) {, moved 2026-09-11 (#1026)

assignIntegerValue rounds, ranges and narrows a value into an integer
column.

A fractional value ROUNDS the way PostgreSQL's assignment cast rounds, and
which way that is depends on the SOURCE's PostgreSQL TYPE: a float8 rounds
half to EVEN (C's rint) and a numeric half AWAY FROM ZERO. Only a value
outside the column's range (NaN and the infinities included) is 22003.

This engine boxes both families as float64, so the BOX cannot decide it:
`SET n = f` over a FLOAT64 column and `SET n = 0 - 2.5` arrive here as the
same Go type and want opposite answers — 2 and -3. One rule served both, and
it was the numeric one, so `UPDATE fl SET n = f` over 2.5, -2.5, 0.5, 3.5,
1.5 stored 3, -3, 1, 4, 2 where PostgreSQL stores 2, -2, 0, 4, 2 — three of
five rows wrong, silently (#699).

Nor can the CARRIER decide it. Division, SQRT, POWER, EXP, LN, LOG and
EXTRACT over numeric operands are computed in float64 here and declared
FLOAT64 (ADR-0024's recorded divergence, and what the wire publishes), while
PostgreSQL computes and types them numeric. Reading the carrier stored
`5 / 2.0`, `SQRT(6.25)` and `POWER(2.5, 1)` as 2 beside `2.5 * 1` stored as
3 (#1353).

srcFloat is therefore "PostgreSQL's type is float8", read from the
DECLARATION: physical.DeclaredTypeOfNode, resolved once per SET clause, whose
answers carry PostgreSQL's category where it disagrees with the carrier — a
FLOAT64 that is numeric there (expr.DeclType.PGNumeric), a DECIMAL that is
float8 there (expr.DeclType.PGFloat8). The category is PostgreSQL's own
resolution:
numeric ⊕ integer is numeric, anything ⊕ float8 is float8; the functions
with a numeric overload (sqrt, exp, ln, log, log10, abs, ceil, floor, round,
trunc, sign) follow their argument, an integer resolving to float8; power
and mod are float8 and integer over two integers and numeric otherwise;
round/trunc(x, n) and log(b, x) are numeric; EXTRACT is numeric and
date_part float8; CASE, COALESCE, GREATEST and LEAST fold as
select_common_type does, and NULLIF returns its first argument promoted by the
`=` its comparison resolves to, so `NULLIF(2.5, f)` over a float8 `f` is
float8 although it declares its first argument's DECIMAL; every other
double-precision function is float8.

INSERT … SELECT reads the same fact for each select-list position from the
plan (expr.PGCategory, the collect sink's SchemaHintPGCategoryPos), carried
through every construct a value can reach the select list by: a derived
table, a CTE, a recursive CTE (its anchor's category, as its types are), a
scalar subquery (its own plan's), a join, an aggregate, a window, a set
operation, LATERAL, a VALUES list and unnest over numeric literals. A join
carries each arm's category for every column it emits, so a name two arms
publish at different categories — a float8 base column beside a derived
numeric of the same name — is read through its qualifier, never through the
other arm.

A MERGE whose source is a subquery reads the same fact: the source query's
declared output and its category per position (the pair INSERT … SELECT
reads) are the merged namespace's declarations for the source's columns
(physical.DeclaredTypeOfNodeIn loads the category into ColDecls.pgCat), so
`USING (SELECT id, y FROM s) src … SET n = src.y` over a float8 `y` rounds
half to even, as it does over `USING s`. A name such a source publishes twice
is 42702 wherever it is referenced — the ON clause, a SET or VALUES
expression, a WHEN condition — as in PostgreSQL. The layer also carries a
container column's element (ColDecls.Elems), so `SET n = af[1]` over a
float8[] column decides float8.

A scalar subquery in a MERGE action (UPDATE SET or INSERT VALUES) declares
the column its own plan declares, category included:
physical.DeclaredTypeOfNodeWith installs the Planner's memoized
scalarSubqueryColumnDecl as the ColDecls subquery resolvers, the same answer
annotateSubqueryColumnDecls stamps on a query plan. `SET n = (SELECT MAX(y)
FROM s)` over a float8 2.5 stores 2; a text-typed one is 42804. UPDATE SET
and INSERT … VALUES refuse a subquery before assignment (0A000).

The MERGE action's expression forms are one table (intround.MergeSetCells:
every form × float8 / numeric / integer / text source × UPDATE and INSERT
action × catalog and subquery source, into INTEGER and BIGINT, plus
parameters over pgwire), each cell PostgreSQL 17.11's stored rows or its
SQLSTATE. A statement PostgreSQL refuses writes nothing here either: an
aggregate is 42803 and a window function 42P20 on every door that evaluates
one row (UPDATE SET, INSERT VALUES, a MERGE action, a MERGE WHEN condition);
a subquery correlated to the target under WHEN NOT MATCHED is 42P01; text
arithmetic against a number is 42883. An expression over a MERGE subquery
source (anything but a reference to one of its columns) is 0A000 where
PostgreSQL answers (#1398), and PostgreSQL's own SQLSTATE where PostgreSQL
refuses.

A JSON field read — `j->>'k'` and `j->'k'`, which the parser lowers to
json_extract_scalar and json_extract — is TEXT to the assignment (PostgreSQL
types them text and json), so it is 42804 into a non-text column on every
write door, as INSERT … SELECT's plan already declared it; the registry
declares both dynamic, which the declaration walk leaves undecided.

An expression whose type the layer declines to decide keeps the NUMERIC
rule — a float64 box rounds half away from zero — which is what it had.
What the layer still declines on an assignment door: a DECIMAL computed
without a declared precision (its value is a numeric, whose rule this is)
and a VECTOR or a container with no element (refused before any rounding
matters).

The declaration picks the rule whatever box the value arrives in: a DECIMAL
text box under a float8 declaration (GREATEST over a numeric and a float8,
the numeric arm winning; NULLIF(2.5, f)) rounds half to even.

An explicit `CAST(x AS INTEGER)` is not an assignment and does not read this
declaration: the cast kernel sees only its compiled operand and the batch, so
a float-carried numeric (`CAST(5 / 2.0 AS INTEGER)`) rounds half to even, 2,
where PostgreSQL's numeric-to-integer cast answers 3. A numeric literal
operand is recognized there and rounds half away.

The range check reaches PORT (uint16) and PROTOCOL (uint8) too, because
nothing below this line re-checks either — convertValue does, but only for
literals — so an out-of-range computed value would truncate into a port no
real port can be.

The coverage table is internal/oracle/intround: the operator and function
grammar over every operand category, the CASE family and the plan
constructs, on VALUES, INSERT … SELECT, UPDATE and MERGE, the MERGE source
constructs (a derived table, a nested one, a CTE inside the source, UNION
ALL, a join, a VALUES list) and a container column's element, measured on
PostgreSQL 17.11.
