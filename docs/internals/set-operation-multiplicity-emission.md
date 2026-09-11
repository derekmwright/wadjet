# Set operation multiplicity emission

Source: internal/engine/exec/set_op_emit.go — SetOpEmit, moved 2026-09-11 (#1026)

SetOpEmit turns grouped per-arm counts into an INTERSECT/EXCEPT answer
(#346). Its input is the drain of a counting hash aggregate: one row per
DISTINCT result row, carrying the result columns plus two count columns —
the row's multiplicity in arm A (leftCol) and in arm B (rightCol). The
operator emits k copies of each row per the operation's rule and drops the
count columns:

	INTERSECT      k = 1 if countA > 0 && countB > 0, else 0
	INTERSECT ALL  k = min(countA, countB)
	EXCEPT         k = 1 if countA > 0 && countB == 0, else 0
	EXCEPT ALL     k = max(0, countA − countB)

The distinct forms never copy a row: k ∈ {0,1} means the output is a
selection over the input, so the operator sets a selection vector and
re-slices the batch's columns (the count columns drop zero-copy, like
ColumnPrune). The ALL forms materialize, because k > 1 has no selection
representation; output size is the operation's true answer size for the
input batch, the same expansion contract a hash-join probe has.

NULL handling is inherited, which is the point: the upstream GROUP BY
already treats NULLs as equal (SQL's set-operation membership rule), so by
the time a row reaches this operator its counts are settled and its values
are opaque. The count columns themselves are SUMs of literal 0/1 tags over
≥1 row per group and therefore never NULL; a NULL count is read as 0
defensively.
