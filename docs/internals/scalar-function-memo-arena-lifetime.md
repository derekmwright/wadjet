# Scalar function memo arena lifetime

Source: internal/engine/expr/expr_scalar_fns.go — FuncCall.tryEvalMemoized, moved 2026-09-11 (#1026)

tryEvalMemoized runs the per-row fallback with a per-batch input memo.
Returns false when the call shape doesn't qualify (caller falls through
to the plain per-row loop).

Memo ownership (load-bearing for the zero-copy keys below): the map is a
local of this call. One evaluation of one batch, by one goroutine, owns
it exclusively and it dies at return — parallel pipeline clones share
the *FuncCall but never the memo. Keys are therefore zero-copy views
into the input column's arena (map assign stores the string header, it
does not copy the bytes), which is sound on two invariants:

  - the input batch outlives this call — it is the caller's live batch;
  - nothing mutates the input column while the call runs. The output
    vector is a separate pooled batch's column (project.go / plan.go
    both write into out.Columns[j] of a freshly-obtained batch); an
    output aliasing the input would already corrupt the pre-existing
    zero-copy probe and the sequential offset writes, so no-alias is a
    precondition of this path, not a new one.

Values live under the same rule and can also be views into the input
(replaceAll returns its argument when nothing matches, and a whole-match
single-group replacement returns a substring); they are copied into the
output arena on the way out.
