# Two level input amortization bound

Source: internal/engine/exec/two_level_hash.go — twoLevelAmortizeMultiple, moved 2026-09-11 (#1026)

twoLevelAmortizeMultiple is R*, expressed in units of twoLevelConvertAt —
the SECOND construction-time bound, and the one that covers the UNBOUNDED
final aggregates twoLevelBoundedMinGroups deliberately left alone.

A conversion is paid ONCE, in full, at the moment it fires, and is repaid
only by the rows that pass through the index AFTERWARDS: every later probe
costs less, and every later rehash is 256 cache-resident scatters instead
of one DRAM-wide one. The gate in convertsToTwoLevel tests when to convert
(at the doubling it displaces) but never whether there is anything left to
repay it — that quantity is not in the flat table's live/slot counters at
all. It is, however, exact in the coordinator: a DAG aggregate task reads a
known set of upstream partitions whose row counts the producing stage
already reported (StageOutput.PartitionRows).

The earliest a conversion can fire is twoLevelConvertAt live entries, so a
sink that will read fewer than R* rows IN TOTAL cannot have R* − convertAt
rows left after it. Requiring the whole input to be at least 8× the
conversion threshold is the same statement with the arithmetic done once.

DERIVATION — three measurements, two shapes:

  - SF100 TPC-H Q18 `final_aggregate-7` (24 tasks, one shuffle partition
    each, ~6.25 M rows and ~6.25 M near-unique groups per task, merge mode
    so rows ≈ groups): 4.14 s with the index off against 5.16 s (old count
    gate) and 5.79–6.52 s (load-factor gate) — a LOSS at R = 6.25 M, and
    the whole of the query's residual
    (docs/benchmarks/sf100-window2-analysis-2026-08-22.md §1.1, §8.2 #4;
    …-window3-… §2.6). Q20's `final_aggregate-9` is the same shape at
    ~2.3 M rows per task and moves the same way (w1: −7.7 % task-seconds
    with the index off).
  - BenchmarkAggIntCardinalitySweep holds rows fixed at 16.78 M
    (`rows = 16 << 20` for every arm — only `groups` varies) and is NOT
    two near-unique arms: the "4 M" arm is 16.78 M rows over 4.19 M
    groups (≈4 probes/group), and only the "16 M" arm is near-unique
    (groups == rows == 16.78 M, ≈1 probe/group). The 4.19 M-group arm
    measures the bucketed layout at +25/+31 % — a LOSS — and the
    16.78 M-group near-unique arm at −4.1/−11 % — a WIN. In ROW units,
    R* = 8 M is bracketed by exactly TWO measurements: the Q18
    production arm above (~6.25 M rows ≈ groups, measured loss) BELOW
    it, and this near-unique arm (16.78 M rows ≈ groups, measured win)
    ABOVE it. The 4.19 M-group arm's 16.78 M rows already exceed R*, so
    the pure-row rule deliberately classifies that shape adaptive
    (bucketed) too — a measured loss (+25/+31 %) that the rule does not
    cover. That is a known gap, called out here rather than folded into
    the bracket above.
  - The shapes where the structure earns its keep are the ones with MANY
    rows per group: ClickBench Q33 is ~100 M rows over ~6 M groups per
    sink, i.e. ~17 probes per group, and a scan-level aggregate always
    reads far more rows than it holds groups. R is the row count, not the
    group count, precisely so those keep the adaptive path: R ≥ R* is
    satisfied by any high-cardinality scan long before its group count
    matters.

8 × twoLevelConvertAt = 8 M sits inside the bracket. Expressing it as a
multiple of the threshold rather than as a second absolute number keeps
the two halves of the gate calibrated together — including under the
WADJET_TWO_LEVEL_AT override, which exists so CI corpora exercise the
bucketed path at group counts nowhere near a million.

The rule is MONOTONE: it can only take conversions away, never add one, so
no shape can become bucketed that was not bucketed before. And it fires
only where an EXACT bound exists — an estimate that reads low would pin a
genuinely huge aggregate flat, so estimates (the single-process planner's
InputRowHint / GroupNDVHint) are deliberately not accepted here.
