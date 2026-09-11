# Global window two pass streaming

Source: internal/engine/exec/window_global.go — globalWindowStats, moved 2026-09-11 (#1026)

---- Global (empty PARTITION BY) window: two-pass streaming over runs ----

A window spec with no PARTITION BY makes the whole input one partition, so
the partition-at-a-time walker degenerates to full materialization — the
last remaining unbounded-memory path in Window. Instead, the sorted runs
on disk let us stream the single partition twice:

  pass 1  stream the merge once, collecting the partition-level scalars a
          row can depend on (row count n, whole-partition aggregates,
          first/last/nth values);
  pass 2  re-open the merge and stream output batch-at-a-time, computing
          each function incrementally with O(1) carried state.

Per-function streaming needs (mirroring computePartitionColumnar exactly —
the golden A/B tests enforce value-identity with the in-memory path):

  row_number, rank, dense_rank, percent_rank   prev-row peer compare
  sum/count/avg/min/max WITH order by           running state, peer-close
  sum/count/avg/min/max WITHOUT order by        pass-1 scalar
  first_value                                   pass-1 scalar
  nth_value, last_value (no order by)           pass-1 scalar
  nth_value, last_value (with order by)         peer-close
  ntile                                         counter state + n
  lag(k)                                        k-slot ring buffer
  lead(k)                                       k-row lookahead
  cume_dist                                     peer-group lookahead

"peer-close" is the frame: with an ORDER BY and no explicit ROWS/RANGE
clause, the frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW,
which ends at the end of the row's ORDER-BY PEER GROUP — so every row of a
group takes the same value and that value is known the moment the group
closes (backfillPeerFrame). Writing the running per-row value instead is
the same answer only when no two rows tie, which is what this path used to
do (#350).

Lead, cume_dist and the peer-close columns hold rows back; everything else
resolves the row the moment it arrives. Memory is bounded by max lead
offset plus the largest ORDER-BY peer group (an all-equal-keys input
degenerates to the full partition — documented bound, charged to the
tracker).

An EXPLICIT frame does not come here at all: it can reach forward or move
its lower end, which needs rows this pass has not produced or has already
dropped, so groupNeedsMaterializedFrame routes those to the
partition-at-a-time walker instead.
