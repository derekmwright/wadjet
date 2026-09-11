# Scan page row predicate boundary

Source: internal/engine/scan/row_filter.go — type RowPred struct {, moved 2026-09-11 (#1026)

Scan-level row filtering (Level 3 of the pushdown ladder, completed):
eligible `col <op> literal` conjuncts are evaluated by the scan itself,
straight off column pages, before any value is materialized into a
vector. Two structural wins over the decode-then-filter pipeline:

  - Dictionary-encoded pages evaluate the predicate ONCE per dictionary
    (a few thousand entries) and map the resulting mask over the
    indices — no value gather, no per-row typed compare.
  - Filter-only columns (referenced by the filter and nothing else)
    are never materialized at all; the scan reads their pages here and
    the projected read schema excludes them.
  - RUN-GRANULARITY evaluation (WADJET_RLE_RUN_PREDS): a dictionary
    page's index stream is RLE — columns like ClickBench's EventDate
    are ONE run per row group, CounterID three — so the mask is applied
    once per run over a SPAN of rows rather than once per row, and the
    indices are never expanded to an int32 array at all.

Semantics parity with the expression layer: NULL rows never match a
comparison; the planner only pushes conjuncts whose literal/column type
pair compares exactly (integral literals on int-class columns, numeric
on float, string on byte-array) — anything else stays in the residual
exec filter.
