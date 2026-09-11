# Parallel limit offset claims

Source: internal/engine/exec/limit.go — Limit.Execute, moved 2026-09-11 (#1026)

Execute applies OFFSET and then Max to one batch.

Both counters are advanced with a single atomic Add whose RESULT is the
decision input, so the batch owns the interval [end-n, end) of the stream
and no other worker can be given the same positions. Claims partition the
stream however the workers interleave, which is what makes the row COUNT
exact under morsel parallelism: every batch contributes exactly its
interval's overlap with [Offset, ∞) and then with [0, Max).

The earlier shape — `seen := l.seen.Load()`, decide, `l.seen.Add(n)` — is
not that. Two workers reading the same `seen` both measured their batch
against it, and the two ways that goes are both wrong answers:

  - OVER-SKIP. The two-path fixture's `nation` is 25 rows in three files,
    so three batches of 9/9/7. With `OFFSET 20`, two workers reading
    `seen == 9` each found 9+9 and 9+7 to be within the offset and dropped
    their batch whole. Nothing was left, and
    `SELECT COUNT(*) FROM (SELECT n_nationkey FROM nation OFFSET 20) u`
    answered 0 where the answer is 5 — silently, on the coordinator's
    default route, roughly once in 1,600 executions of that shape
    (#567, #765).
  - UNDER-SKIP. Two workers in the partial-skip branch both trimmed
    `Offset - seen` rows from the same stale `seen` and both stored
    `l.Offset`, so fewer than Offset rows were skipped in total.

`remaining := l.Max - l.passed.Load()` had the identical shape and
over-DELIVERED: two workers each seeing the whole budget passed their
whole batch, so a `LIMIT 3` returned six rows (#845, the OFFSET twin —
same operator, same read-modify-write, opposite direction).

WHICH rows an unordered OFFSET or LIMIT drops is unspecified (ADR-0013
classes 1 and 3) and still is — the claim order varies with scheduling.
HOW MANY never was.
