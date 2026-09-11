# Worker morsel dispenser view safety

Source: internal/worker/morsel_dispenser.go — type morselDispenser struct {, moved 2026-09-11 (#1026)

morselDispenser replaces the parallel fragment paths' raw batch channel:
a single producer goroutine pulls from the fragment source, admits each
decoded batch against a byte budget, optionally splits it into
~DefaultBatchSize zero-copy views, and feeds k consumers.

Splitting is what makes row-group-sized batches safe AND parallel: k
consumers work different slices of the same admitted parent instead of
each holding a private 280 MB batch, per-morsel backpressure checks
actually run every ~2048 rows instead of once per row group, and derived
batches (join-probe output pools size off ActiveLen) stay morsel-sized.

View safety rests on audited facts about the fragment op chains (see
morsel-execution.md §4.1 v1.5): linear-path operators (exec.Filter,
exec.Project, HashJoinProbe, DynamicFilterEmitOp) never write input column
storage, never append to or replace in.Columns, and mutate only the batch's
own Sel FIELD (pointer reassignment to operator-private scratch). Views
therefore share the parent's column vectors and get a private Sel slice.
The Sel slices are three-index subslices of one parent-sized array, so
even an op that compacted in place into in.Sel (the KernelFilter family —
not built for fragments today) would write only its own view's region.

split must be false when the downstream sink RETAINS consumed batches and
charges them by MemBytes (exec.Sort stores the batch and charges
b.MemBytes(), which is Sel-blind — each retained view would charge the
full parent). HashAggregate copies rows out during Consume, so views are
safe there.
