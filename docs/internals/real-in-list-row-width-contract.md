# Real in list row width contract

Source: internal/engine/expr/real_in_width.go — type realLitSet struct {, moved 2026-09-11 (#1026)
Superseded: realTypedOperand also recognizes REAL arithmetic and ABS; realListMember accepts quoted literals and explicit REAL casts, so the older operand/member list is incomplete.

The row-at-a-time half of the FLOAT32 IN-list width rule (#633, the
distributed sibling of #549).

PostgreSQL decides a `real IN (...)`'s comparison WIDTH from the list's
SYNTACTIC arity, and the two arities disagree (EXPLAIN VERBOSE, postgres:17):

	more than one element  ->  real = ANY('{...}'::real[])   -- NARROW to real
	single element         ->  real = 'x'::double precision  -- WIDEN to double

#549 taught the VECTORIZED kernel that rule
(kernel.ResolveInFilterKernelArity's TypeFloat32 arm). It did not reach this
path, and this path is the one the stage DAG uses: a worker compiles every
scan-pushed filter straight to the row evaluator (worker.compileFilterExprs
-> expr.FilterPredicate), where `In` compares BOXED values and a FLOAT32
column boxes as float64 (ColRef.Eval). Every member was therefore compared
at DOUBLE width, so `real IN (3.1, 7.1)` matched NOTHING on the DAG while
the single-process kernel matched both rows — a silent wrong answer on the
distributed path only, which the pg-oracle corpus cannot see because it runs
at SF0.01, where the coordinator takes the in-process fast path.

The binding below narrows for exactly the lists the kernel narrows for, so
the two paths answer one predicate:

  - the probed operand is REAL-TYPED (resolved from the batch, since the
    compiler has no schema) — a real column, an explicit cast to real, or
    unary ± over either, which is what PostgreSQL calls real;
  - the list holds MORE THAN ONE member (the syntactic count, NULL members
    included — PostgreSQL casts `{3.1,NULL}` to real[] as readily as
    `{3.1,7.1}`);
  - every member is a literal boxed as a NUMBER.

A member that is not a constant takes the binding away entirely, because
PostgreSQL stops building an array at all: `real IN (3.1, other_col)` plans
as `(r_val = '3.1'::double precision) OR (r_val = other_col)` — the widened
scalar rule, twice — so the fallthrough below is that answer, unchanged.

A QUOTED literal narrows too (#646). PostgreSQL casts `real IN
('3.1','7.1')` to real[] exactly as it casts the unquoted spelling, and a
MIXED list `real IN ('3.1', 7.1)` likewise — the array's element type is
resolved once for the whole list. It was excluded while the kernel's
float32InSet read a text constant through kernel.toFloat64 (zero for any
string): narrowing here alone would have split the two paths on a shape they
agreed — wrongly — about. Both read the float input grammar now.

The refusal follows the member's SPELLING, because PostgreSQL's message
does: a quoted literal names its own TEXT verbatim ("1e40" is out of range
for type real) and a numeric one names its DIGITS, since the cast that fails
there is numeric->real. Verified live for both.
