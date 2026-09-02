# ADR-0013: The correctness gates, and what they deliberately do not gate

Status: Accepted (2026-08-19; nondeterminism class 9 added 2026-08-22;
type-matrix gates and the per-issue ratchet amendment added 2026-08-23;
nondeterminism class 10 added 2026-08-29;
replication-floor amendment added 2026-09-02)

## Context

Between 2026-08-17 and 08-19 roughly thirty wrong-answer defects were found
and fixed. Almost none of them were caught by the gates that existed at the
start: a fixed 22-query TPC-H corpus, row counts, and a per-column numeric
signature. The recurring shape was a query that returned the **right number of
rows with wrong values**, or the right values in the wrong order — #312
inflated Q05's revenues 25× under an identical row count; MEDIAN returned SUM
on the stage DAG; STDDEV was computed over one parallel clone's rows.

Three gates now exist, and each answers a different question. Their value
depends on knowing which question, because a gate consulted for the wrong one
produces false confidence or a tail-chase. Equally important, some result
differences are **legal SQL nondeterminism** rather than defects; treating
those as bugs wastes the same time in the other direction.

## Decision

### The three gates

| Gate | Question it answers | Blind to |
|---|---|---|
| DuckDB fingerprint (`duckdb_compare_test.go`) | Is the answer RIGHT? | anything both arms get wrong identically AND DuckDB agrees with; semantics where DuckDB is not the authority (ADR-0012) |
| Two-path invariance (`two_path_invariance_test.go`) | Do the fast path and stage DAG AGREE? | both-arms-wrong |
| Differential fuzzer (`shape_fuzz_test.go`, `internal/oracle/shapegen`) | What shapes have we never tried? | whatever the generator cannot express |

A fingerprint is a row count plus two truncated SHA-256 digests at 6 and 4
significant float digits. It is **opaque by construction**: an answer can be
verified against it and cannot be read out of it. That is deliberate — a
readable expectation invites implementing toward the answer key, and invites a
failing gate being "fixed" by editing the expectation. The harness's
`ValueSig` (a per-column numeric sum) is readable and is therefore NOT used in
canonical files.

Provenance is enforced, not documented: every stored entry names the engine
that produced it and the loader fatals on an unstamped one. **A fingerprint
produced by wadjet is regression detection, not correctness** — it answers
"did I change?", never "am I right?" — and must live in a separate file that
says so.

### Deliberate nondeterminism: these are NOT defects

Chasing these has cost real time. Each is legal, expected, and handled by a
named mechanism.

1. **A query with no ORDER BY may return rows in any order.** Compared as a
   multiset. A positional comparison here would fail on legal behavior.
2. **Tied rows under a partial ORDER BY may come back in either order.**
   Q03 (`revenue DESC, o_orderdate`) and others do not specify a total order.
   Handled by making the order total in the correctness variant (append and
   project a tiebreaker) or by the fuzzer's tie-immune scheme: digest the row
   multiset AND the positional sequence of ORDER-BY key values, so tie order
   varies freely while a dropped or inverted ORDER BY still shows.
3. **Which members of a tie group survive a LIMIT is arbitrary.** Gated by
   count, or by comparing the LIMIT-stripped result.
4. **A float sort key is only as stable as its arithmetic.** Accumulation
   order varies with partitioning and worker count, so a query ordered by a
   float SUM (Q03, Q05) can legitimately reorder near-ties between runs. The
   dual-precision digest (match at 6 *or* 4 significant digits) is the
   mitigation. Whether 4 digits suffices at SF100 is measured, not assumed.
5. **Q11 uses the TPC-H spec fraction (0.0001/SF)**; cross-validated to 1 ulp
   against Trino. **Q15 = 1 row**, where Trino flakes 0/1 on its own float
   tie. Both documented in `baseline-sf100.json`'s `captured_on`.
6. **String ordering differs from PostgreSQL by design** — binary collation,
   per ADR-0012.
7. **`MODE` over a string column returns NULL** — a pre-existing engine gap,
   pinned, not a distribution defect.
8. **Concurrent load produces false failures.** Coordinator tests fail with
   `workers failed to register within 15s` and `context deadline exceeded`
   under heavy parallel load — never a wrong row. Test output from a busy
   machine is not evidence.
9. **A float aggregate's own VALUE moves in its last significant digits across
   parallel partial aggregation.** Class 4 covers a float *sort key*; this is
   the value itself, with no ordering involved, and it shows up in the
   harness's readable `ValueSig` because that prints 10 significant digits.
   Example: Q19's `sum(l_extendedprice*(1-l_discount))` over 600 M rows reads
   `c0:5.985878904e+08` in some runs and `…903e+08` in others — a relative
   delta of 1.7e-10 at the last printed digit (2026-08-22 SF100 windows 1 and
   3; in window 1 the *same binary* under a kill switch reproduced the other
   value in all four runs, which is what rules out a code-level change). The
   dual-precision fingerprint digest (6 or 4 significant digits) is unaffected.
   Investigate only if the divergence reaches the digest.
10. **A window function with no ORDER BY inside the OVER assigns each row an
    unspecified value.** `ROW_NUMBER() OVER (PARTITION BY k)` numbers rows in
    ARRIVAL order, and arrival order at the Window sink is the order several
    scan workers hand it their batches — so the same query, the same binary
    and the same data answer differently between runs, and the in-memory and
    the spilled paths answer differently from each other (the external path
    numbers the merged sorted runs). LAG, LEAD, FIRST_VALUE, LAST_VALUE and
    NTH_VALUE are unspecified the same way; SUM, COUNT, MIN, MAX and the
    other whole-partition aggregates are not, and neither is any window whose
    OVER carries a TOTAL order. What such a window still owes is a PROPERTY,
    and that is what a gate asserts: within every partition the numbers are a
    permutation of 1..n — none repeated, none skipped, none out of range —
    over the partitions the key really names, at their real sizes. #687 is
    the worked example: `TestWindowKeySpellingsAgreeUnderSpill` compared the
    two arms row for row and failed under `-race` with a different value each
    run (id=0 read 234, 701, 1101 against the spilled arm's 1), which looks
    exactly like a race on a shared counter. Reconstructing the arrival order
    from the answer — sort one partition's rows by the assigned number and
    read out the ids — decomposed every run into whole ROW GROUPS in internal
    order, 1101 being precisely "row group 0 arrived last". The permutation
    held on every run of both arms. A positional comparison of an unordered
    window is the defect; `wadjet.TestWindowUnorderedRowNumberIsAPermutation`
    is the replacement, run with the scan forced wide so the reordering
    actually happens rather than holding vacuously.

### Pins

A known divergence is recorded with `knownBug` / `knownBugArm` naming the
issue. The comparison still runs, and the subtest **fails when the arm starts
agreeing**, so an exemption cannot outlive its bug. A pin must also check
column SHAPE, not only values: a wrong 2-row answer once "agreed" by
coincidence because it carried the right two values.

### Amendment 2026-08-23: the type-matrix gates, and a per-issue ratchet

The three gates above share one blind spot: their corpora carry three storage
types (Int32, Float64, String — ClickBench adds Int64 and Date). Nineteen of
the engine's twenty-two types therefore had no differential coverage from any
of them, which is how `(*Vector).GetValue`'s TypeBytes arm aliasing the
column arena (#391) shipped through every one of them. `internal/oracle/
typematrix` adds a fixture covering all 22 types and five gates consume it:
`wadjet.TestTypeMatrixNoProcessKillers`, `TestTypeMatrixBatchReuse` (poisoned
pool vs clean pool — see `internal/engine/batch/poison.go`),
`TestTypeMatrixOptimizationInvariance` and its fuzz-generated counterpart
(#287's kill-switch differential over all 22 types), and
`coordinator.TestTypeMatrixTwoPath` (stage DAG vs single process, per type).
These follow the same Pin mechanism as the existing gates (§Pins).

**The ratchet these gates use is per ISSUE, not per pin.** The "this bug is
fixed, delete the pin" half of a Pin's ratchet is satisfied by ANY pin naming
that issue still diverging, not by every pin naming it. The trade this
accepts: an issue with many pins can have most of them silently start
agreeing while the ratchet stays green on the strength of one still
diverging — #396 (the stage DAG returns network/UUID columns in raw storage
form) has 37 pins, so 36 of them could start agreeing without the gate
noticing.

This is deliberate, not an oversight, for two reasons:

1. **Some of these defects are nondeterministic by construction.** #391
   answers with whatever the allocator wrote over a freed arena, so a given
   entry agrees by luck on some fraction of runs. A per-pin ratchet across
   many pins of the same nondeterministic issue would flap — some pin, on
   some run, agrees — for a reason that has nothing to do with the bug being
   fixed. `typematrix.Pin.GatedBy` names the OTHER gate that forces the
   divergence deterministically (poison-on-release forces #391's reuse on
   every invocation), exempting a pin from this arm's must-still-diverge
   check without exempting it from anything else — the "matches no corpus
   entry" check still applies, and a real divergence, when this arm does see
   one, is still logged.
2. **Not every intermittent defect has a forcing gate.** #402 (GROUP BY +
   ORDER BY + LIMIT returns a different top-N with partitioned aggregation
   off, under a total order) diverged on roughly 60-75% of individual
   invocations of the same seed and toggle, with no known trigger that forced
   it every time — unlike #391, nothing else in the suite could serve as its
   `GatedBy` gate. (#402 is fixed and its pin is deleted; it remains the
   worked example, and `tmFuzzIntermittentOptRetries` remains the mechanism
   for the next pin of its shape. The trigger IS known now, in hindsight:
   whether a clone lane consumed nothing, which is scheduling — nothing a
   gate can set. The point stands, and is sharper for it: a trigger can be
   perfectly understood and still be unforceable.) `wadjet.TestTypeMatrixFuzzOptimizationInvariance` retries
   such a pin's comparison a bounded number of times
   (`tmFuzzIntermittentOptRetries`) before concluding "no divergence this
   run": every attempt is a real comparison against the baseline, the loop
   stops and logs the instant one diverges, and only an unbroken run of
   agreements — astronomically unlikely at the chosen bound — reaches the
   ratchet's fail path. This tolerates the flake without becoming an
   exemption: the entry is compared every time, just more than once when
   needed.

**Mitigation for the 36-of-37 gap**: `tmRatchet.finish` (wadjet package) and
`tmdRatchet.finish` (coordinator package) log a summary line per issue on
every run — `"<kind> issue #N: k of m pins still diverge this run"` — whether
or not the ratchet fires. A real fix narrowing #396 from 37/37 to 1/37 is
visible in the test log across however many runs it takes to land, long
before the last pin agrees and the ratchet actually fails. No numeric
threshold is enforced on the count (e.g. failing below `m` pins still
diverging): the corpus is generated per type (`typematrix.Columns()`), so `m`
shifts by design when a type or query shape is added, and a threshold tied to
today's `m` would need updating for reasons unrelated to the bug it gates.
The summary line is the record; a human (or a future amendment, if the gap
proves costly in practice) decides whether a shrinking `k` warrants action
before it reaches 1.

### Scale

- **Semantics** are gated small (SF0.01, and a PostgreSQL arm at SF0.1/SF1).
  Divergences of meaning are visible at 25 rows.
- **Scale-dependent defects** (partial/final merges, spill, parallel
  partitions, NULL group splits) need wadjet at SF100, with an oracle that can
  answer at that scale — DuckDB or Trino, not PostgreSQL, whose row-store
  execution makes SF100 impractical as an oracle.
- SF100 correctness is a **committed fingerprint file**, so the gate has no
  EC2 dependency. Only the one-time truth generation runs in-region, because
  the bucket is ~280 GiB.

### Amendment 2026-09-02: a ratchet fires only on a sample that could have seen the bug

The 2026-08-23 amendment settled this for the type-matrix gates — a
nondeterministic pin must not be retired by a lucky run — and named two
mechanisms: `Pin.GatedBy`, which points at another gate that forces the
divergence deterministically, and bounded retries
(`tmFuzzIntermittentOptRetries`) where nothing can force it.

The spill sweep (`wadjet.TestTypeMatrixAnswersTheSameUnderEveryMemoryBudget`,
ADR-0027) is the third gate in this family and did not apply the rule to its
own pins. Its ratchet fired whenever every budgeted run of a pinned cell
agreed, on however many runs the sweep happened to make — and under `-short`
that is ONE. Its pinned defects are nondeterministic by construction: #788 is
wrong on 9 to 12 runs out of 12, so roughly one CI run in four saw the single
run agree and failed the gate on a tree where nothing about #788 had changed.
The file's own doctrine is that one passing run of a spilled arm proves
nothing; the ratchet was the one place it did not hold.

The mechanism here is a third of the same kind: a **replication floor**
(`spillMxRatchetMinRuns`). The ratchet fires only when the cell ran the full
replication count; below it the agreement is logged with its reason and the
full-replication arm — CI's Type-Matrix step runs this file without `-short` —
is the one that decides. The pin is still compared on every run and still
cannot outlive its bug.

The general rule, which the next gate should apply without being told:
**a ratchet is an assertion about EVIDENCE, so it may only fire on a sample
large enough for agreement to be evidence.** Forcing the divergence
(`GatedBy`), retrying to find it (bounded retries) and requiring enough
samples to have found it (a replication floor) are three answers to one
question. A gate that runs at reduced replication in some arm — `-short`,
`-race`, a smoke lane — has to decide which arm owns its ratchet, and say so.

## Consequences

- A gate failure is triaged against the list above BEFORE it is investigated
  as a defect.
- New corpus entries state their comparison mode; `ordered` is derived from
  the SQL, and that derivation is itself load-bearing — an earlier version
  classified `ORDER BY LENGTH(x)` as unordered and would have exempted the
  #320 shape from the gate built to catch it.
- Adding a kill switch to an optimization extends the invariance oracle for
  free and is part of the definition of done (#287).
- A pin whose issue has many entries relies on the per-issue summary line
  (§Amendment 2026-08-23), not on the ratchet itself, to surface a fix that
  narrows the count without silencing every pin — read the log, not just the
  pass/fail, when triaging one of these issues.

## Related

- ADR-0012 (semantics authority), ADR-0011 (performance measurement)
- `internal/oracle/fingerprint.go`, `benchmarks/tpch/oracle_semantics_test.go`
- `internal/oracle/typematrix`, `wadjet/type_matrix_test.go`,
  `wadjet/type_matrix_fuzz_test.go`, `wadjet/type_matrix_crash_test.go`,
  `internal/coordinator/type_matrix_distributed_test.go`,
  `internal/engine/batch/poison.go`,
  `docs/testing/gate-coverage-audit-2026-08-22.md`
