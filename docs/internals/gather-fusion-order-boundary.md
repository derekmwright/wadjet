# Gather fusion order boundary

Source: internal/coordinator/dag_dispatch.go — canFuseGather, moved 2026-09-11 (#1026)

canFuseGather reports whether a gather stage can be absorbed into its
upstream stage. Returns the upstream stage ID when fusion is eligible.

Two upstream-shape cases are eligible:

 1. Aggregate (final_aggregate / merge_aggregate). The upstream emits
    unordered groups (or pre-sorted groups when SortKeys are set via
    fuseSortIntoPredecessor). Reject Ordering on the gather Exchange in
    this case: ordered gather is the coordinator-side sort-merge of
    pre-sorted streams; when the upstream agg already absorbs the sort
    and runs single-task, the gather Ordering is redundant — but we
    still require it to be unset to avoid silent semantic changes for
    any caller relying on it.

 2. Sort (sort / merge_sort). The upstream produces a single ordered
    stream. Ordering on the gather Exchange is permitted IFF the
    upstream SortKeys equal it (i.e., the legacy gather's coord-side
    re-sort would be redundant). Otherwise the keys differ and fusion
    would silently corrupt order.

Common requirements for both cases:
  - Exactly one upstream dependency.
  - Upstream is in the pending dispatch set (not a leaf scan or pre-
    computed input).
  - Distribution.Kind == DistSingleton when the upstream emits ordered
    output (sort, merge_sort, or sort-bearing aggregate). Multi-task
    ordered upstreams sort their partition independently — streaming N
    partial-sorted streams to gather concatenates them in arrival
    order, losing global order.

Amplification-safe: the gather sink publishes via NATS, not S3, so
per-task fan-out doesn't multiply S3 GETs the way scan/join fragment
fusion does.
