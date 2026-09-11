# Sink clone merge fence

Source: internal/engine/exec/pipeline.go — SinkSurvivesCloning, moved 2026-09-11 (#1026)

SinkSurvivesCloning reports whether the sink's state can be split across
morsel-parallel clones and merged back.

EXPORTED because CloneSink has TWO call sites, not one: this package's
Pipeline.runParallel and the worker's runBreakerConsumeParallel. Guarding
only the first left the whole defect below reachable on the stage DAG
whenever a fragment's breaker ran morsel-parallel — `SUM(DISTINCT a)`
answered 64.96 for 16.24 with four morsel workers, exactly four times over,
and `STRING_AGG(DISTINCT …)` emitted every value four times in a row.
TestCloneSinkCallersConsultTheCloneFence enumerates the call sites so a
third one cannot be added without consulting this.

A DISTINCT aggregate other than COUNT cannot (#703). Its accumulator holds a
SUM (or a mean, or a variance triple) that each clone has already folded its
own copy of a shared value into, and mergeSinkState adds those accumulators:
two clones that both saw 12.75 contribute it twice, so
`SELECT SUM(DISTINCT a) FROM revdup` answered 97.44, 129.92 and 194.88 for
16.24 across four runs of the same binary over the same bytes — the
multiplier being however many clones happened to receive rows. Unioning the
distinct SETS at the merge, which the code already does, cannot undo an
addition that already happened.

COUNT(DISTINCT) and APPROX_DISTINCT are unaffected and keep their
parallelism: their whole state IS the set, so the union is the merge. So is
every non-distinct aggregate.

This is #291's rule — "a DISTINCT aggregate has no mergeable partial form" —
applied to the in-process clone merge, where the stage DAG already applies it
by routing every distinct aggregate through the one-level RawInputAggregate
shape. The cost is morsel parallelism for queries that were WRONG before, and
only for those; the spilled path is unaffected because it re-aggregates from
raw rows.
