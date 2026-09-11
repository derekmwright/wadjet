# Partial group drain cursor

Source: internal/engine/exec/aggregate_partial_drain_cursor.go — partialGroupCursor, moved 2026-09-11 (#1026)

partialGroupCursor is a streaming partialRunSource backed by a HashAggregate's
SoA hash state. It builds a sort-key arena + sorted index up-front (memory
proportional to keys, not to full partial-group records) and emits one
*partialGroup per Peek/Advance by reading from the flat accumulator arrays
at the next sorted index.

Memory characteristics for N groups, ngc group cols, na aggs, k bytes/key:
  - Pre-existing SoA arrays — already live; we don't re-allocate them
  - Key arena       = N*k bytes  (typ. 16 B/group → 320 MB at N=20M)
  - Key offsets     = (N+1)*4    (~80 MB at N=20M)
  - Sort index      = N*4        (~80 MB at N=20M)
  - Reusable head   = O(ngc + na), single struct overwritten per Advance

Compare with the prior []*partialGroup materialization: each group allocated
a *partialGroup (24 B) + a fresh []byte SortKey + a fresh []any KeyVals + a
fresh []kernel.Accumulator Accs, totaling ~150–200 B per group, or ~3–4 GB
at N=20M. The cursor cuts that to ~480 MB at SF100 Q17 scale.

Lifetime: the cursor borrows references to the HashAggregate's SoA arrays
and group-state slices. The caller must not mutate those arrays for the
cursor's lifetime. finalizeViaPartialMerge transfers ownership by clearing
the aggregate's references after construction so the cursor is the sole
owner; spillPartialState consumes the cursor synchronously before its
resetGroupStateAfterSpill call frees the references.
