# Correctness-fix protocol: what a fix must prove before review

Written 2026-08-29 during the numeric-parity arc (ADR-0024), after the same
pattern repeated across ten fixes: the first pass was green on every gate in
the tree, and the adversarial review still found a silent wrong answer in
about half of them — twice a change that turned a *correct* answer into a
wrong one. Every one of those findings came from a method, not from luck.
This document makes the method the author's job, so the review checks that
it was done instead of doing it for the first time.

This is a working protocol, not an ADR: it says how to build the evidence,
not what the semantics are (ADR-0012 decides that).

## The method

1. **Build the PostgreSQL matrix first, from a live server.** `task
   pg-oracle:test` starts `postgres:17-alpine`; use it. For every shape the
   fix touches, write the SQL, run it on PG, and record the answer *before*
   changing wadjet. Include `EXPLAIN VERBOSE` when the question is which
   type PG resolved (`real = 3.1` → `'3.1'::double precision` was the whole
   of #631), and `\gdesc` / `pg_attribute.atttypmod` when it is a declared
   type. A rule remembered from documentation is a hypothesis; the server is
   the authority.

2. **Every gate must be able to fail.** Before trusting a test, break the
   fix (revert one line, stub the new function to a no-op) and confirm the
   test fails. A fixture whose values cannot distinguish the old rule from
   the new one — every float column non-negative, every DECIMAL pair where
   the wider column always wins, `GREATEST(a, b)` standing in for a
   container element — passes for the wrong reason. Choose values that
   differ under the two rules: non-representable literals, negative rows,
   NULL rows, the value at the exact boundary, both argument orders.

3. **Both execution paths, every shape.** The single-process fast path and
   the stage DAG are two engines (ADR-0018 §3). Run each shape through the
   two-path harness (`internal/coordinator/*_two_path_test.go`), including
   the shapes that force a shuffle above the operator (GROUP BY the
   expression, a join key, a window partition key) and the shapes that reach
   a *different* mechanism on the DAG (`expr.FilterPredicate` vs the kernel;
   the set-operation adapter vs `reconcileSetOpArmTypes`). "Same file on
   both paths" is not evidence; a query result on both paths is.

4. **Diff behaviour, not just tests.** For a change to a shared primitive
   (a comparison, a parser, a type fold), run a random corpus of ordinary
   inputs through the old and new code and assert *nothing but the intended
   class moved*. The #534 float change was accepted on the strength of 136
   ordinary literals being byte-identical and exactly ten NaN/Infinity
   answers changing.

5. **Independent oracle for arithmetic.** A property test over random
   operands compared to `math/big` — with the oracle's rounding and
   narrowing written from scratch, not imported from the implementation —
   at the boundaries: ±2^127, 10^38, the exact half-way rounding case, both
   signs, zero, powers of ten.

6. **Enumerate the callers.** When a function's contract changes (a
   saturating parser becomes checked; a bool becomes a status), grep every
   caller and classify each as "comparison" or "value-producing", and write
   the table into the commit body. #553 was one discarded field in one
   function; the review found the same discard in its sibling.

7. **Name what is deferred, in the code.** A `TODO(#N)` with a *pin test
   that fails when the deferral is lifted* is the honest form. A silent
   fallback to the old behaviour is how #555's FLOAT64 arithmetic survived
   three releases.

8. **Loud beats plausible.** If a value cannot be answered exactly, the
   error (with PostgreSQL's SQLSTATE) is the answer. A fix that replaces an
   obviously-wrong 0 with a plausible wrong number — #653's first round did
   exactly that — is a regression in kind, not an improvement.

9. **Don't record what the code does not do.** An ADR or a comment that
   describes intended behaviour as present behaviour is a false gate (#534's
   first pass wrote "CAST … is 22003" into two ADRs while CAST was still a
   no-op). Records name the sites covered and the sites not covered.

## What the reviewer then does

Refute, concretely. The review's only currency is a failing input: SQL, the
PostgreSQL answer, the wadjet answer per path. "This might be wrong" is not a
finding. The reviewer also reproduces every claim of "pre-existing" on the
parent commit before the author's filing is accepted as a residual rather
than a regression.

## Landing

Fixes are cherry-picked onto `main` serially, one branch at a time, with the
full battery (`./internal/...`, `./wadjet/`, TPC-H correctness and
optimization invariance, `task pg-oracle:test`) run on the *combined* tree.
The pre-push hook tests the working tree, not the pushed ref: nothing may
touch the tree until a background push has exited.

Issues are filed at the mechanism, not the symptom. Several symptoms of one
producer-emits-no-stage gap are one umbrella issue with a shape table (#656),
not six issues — they are fixed as one change and reviewed as one change.
