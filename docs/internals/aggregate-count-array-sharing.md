# Aggregate count array sharing

Source: internal/engine/exec/agg_accumulators.go — planCountArrays, moved 2026-09-11 (#1026)

planCountArrays decides, per aggregate, which aggregate's count[] it reads.
Result[i] == i means "owns its own"; result[i] == j < i means "shares j's".

Two aggregates may share a count array only when every row increments both
counts or neither — i.e. their count kernels run over an identical
predicate. That holds exactly when:

  - both are COUNT(*) (no input column: every row with a live group index
    counts), or
  - both read the SAME input column AND both count kernels are guaranteed
    to fire for that column's type.

The type guard matters: scatterFlatAggUpdate's SUM/AVG dispatch has no case
for e.g. Bool or String, so SUM over such a column silently increments
nothing while COUNT over it increments every non-null row. Restricting
sharing to the numeric set the SUM/AVG switches actually handle keeps the
two predicates identical.

MIN/MAX never participate — they have no count at all (aggNeedsCount).

NOT shared: aggregates over different columns, even when the data happens
to have no nulls in either. Null-ness is a per-batch property, so that
equality isn't provable at plan time. ClickBench Q33's three aggregates
(COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth)) fall in exactly that
bucket and each keep their own count.
