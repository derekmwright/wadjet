# Worker empty fragment output contract

Source: internal/worker/executor_fragment.go — emptyScalarAgg := false, moved 2026-09-11 (#1026)

Empty source files: legitimate "upstream produced nothing" case (e.g.,
an aggregate or semi-join filtered every row). Emit zero rows and no
result files; downstream stages handle the empty-input shape via their
own short-circuits; we short-circuit here before any S3 I/O happens.

Exception: OpGatherSink. The coordinator's gather receiver counts
terminal markers, not messages — skipping finalize would leave it
hanging until the 10-minute gather timeout. Open + finalize the sink
(its Finalize publishes a terminal even with no batches consumed) so
the receiver unblocks immediately on empty fragments.

Exception: eager-fed aliases (Task.EagerInputs). Their InputFiles is
empty BY CONSTRUCTION — the file set streams in as producer-task
manifests — so "no frozen files" carries no emptiness signal at all.
Short-circuiting here silently dropped the entire input (0 rows,
task success) when eager dispatch first went end-to-end.
Exception: an UNGROUPED aggregate fragment that owes SQL its identity
row. An ungrouped aggregate returns exactly one row for any input,
including none — COUNT()=0, SUM/MIN/MAX/AVG=NULL — and a downstream
consumer (scalar-subquery substitution, #292) blocks on that row
existing. Fall through with an empty source so the aggregate
finalizes and the row is written.

Two shapes qualify, and the difference is whose type the row wears.

 1. COUNT-family, at ANY stage: their output type (int64) is
    input-independent, so a partial identity row is typed the same as
    every sibling's and merges cleanly.

 2. Anything else, ONLY on the ungrouped final (EmitEmptyIdentity) and
    ONLY when the planner declared every aggregate's output type
    (AggSpec.OutputType). MIN/MAX types follow the input column, which
    cannot be read here — zero input files, no schema — so without the
    declaration the row would guess. The final is also where guessing
    costs the least even if the declaration were wrong: its input being
    empty means there are no sibling partials to merge against, and the
    planner makes it a Singleton, so the identity row it emits is the
    one row of the answer, not one of N (#329).

Everything else keeps the empty-output short-circuit: a partial or
merge_aggregate that produces nothing is absorbed by the final above
it, which emits the row instead — and one mistyped partial poisons the
merge for every sibling (a float64-typed NULL min among string-typed
partials broke the skew-parity left join before this gate).

Exception: a RIGHT or FULL join whose PROBE partition is empty. Its
build rows are all unmatched by construction, and they are the rows the
join exists to preserve — one shuffle partition holding build rows and
no probe rows is the ordinary case, not a degenerate one. Falling
through builds the hash table, probes nothing, and lets the flush emit
them NULL-padded on the probe side, using the declared ProbeSchema for
their names (#352).
