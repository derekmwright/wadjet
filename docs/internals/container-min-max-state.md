# Container min max state

Source: internal/engine/exec/agg_container_minmax.go — containerMinMaxState, moved 2026-09-11 (#1026)

MIN() and MAX() over ARRAY, ROW, MAP and VECTOR.

The six MIN/MAX resolvers in kernel/agg.go fill an Accumulator slot —
MinI64/MinF64/MinDec/MinStr — and a container fits none of them: its value
is a whole nested structure, not a scalar. So the resolvers answered nil,
the row updater was never called, HasMin/HasMax stayed false, and
MIN(arr_col) finalized to NULL on every input. Silently (#426).

It is a gap rather than a position. PostgreSQL orders arrays —
min(anyarray)/max(anyarray) exist and use array_smaller/array_larger over
the same lexicographic array_cmp — and #415 gave all four containers that
total order here (kernel.CompareValuesAt), which is already what ORDER BY,
a sort-merge join on a container key, and PARTITION BY use. Declining only
in the aggregate makes one operator disagree with the rest of the engine
about whether two containers are comparable. ROW/MAP/VECTOR have no
PostgreSQL equivalent to follow, but they have a defined total order here
too (ADR-0012 records MAP's and VECTOR's as WADJET-DEFINED), so answering
is the consistent choice.

The state is the shape MIN_BY/MAX_BY already needed: a RETAINED value
rather than a scalar slot. It differs in keeping that value as a one-row
VECTOR instead of a boxed `any`, because the comparison is
kernel.CompareValuesAt, which reads vectors — and because the copy is then
batch.AppendFrom, the engine's own nested-aware deep copy, rather than a
GetValue box that would have to be re-parsed to compare.
