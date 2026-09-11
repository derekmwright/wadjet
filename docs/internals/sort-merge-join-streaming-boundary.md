# Sort merge join streaming boundary

Source: internal/engine/exec/sort_merge_join.go — SMJCounterpartAdoptions / SortMergeJoin, moved 2026-09-11 (#1026)

SortMergeJoin joins two large inputs by sorting both sides on the join keys
and streaming a two-cursor merge. Unlike HashJoin, neither side is held
resident: each side buffers under the shared tracker and self-spills sorted
columnar runs (Sort's external-merge machinery), so peak memory is
O(run buffer + one batch per merge cursor) regardless of input size.

The build side (right, as in HashJoin) arrives via Build; the probe side
(left) arrives via Consume. Both are pipeline breakers here — inherent to
sort-based joins over unsorted input. After Finalize, Next streams joined
batches: probe columns first, then build columns, with the same
duplicate-name qualification and OutputFilter semantics as HashJoinProbe.

v1 scope (docs/design/sort-merge-join.md): INNER equi-joins only, no
JoinFilter. Rows with a NULL in any join key are excluded at buffer time
(SQL equi-join semantics: NULL matches nothing — mirrors the hash paths,
where null keys produce no index entry). Not Cloneable: the breaker path
runs it single-consumer.
SMJCounterpartAdoptions counts key resolutions that only succeeded by
adopting the OTHER side's key name (swapped pair) — the sort-merge analog
of KeyAssignmentRepairs, and carrying the same warning: on a self-join the
swap can be wrong rather than corrective. Should stay 0 on
planner-produced plans. NOTE: unlike KeyAssignmentRepairs, no suite
asserts that today — this is observability, not a gate.
