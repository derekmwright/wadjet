# Scan dictionary run row alignment

Source: internal/engine/scan/row_filter.go — func dictRunPathEligible(page *pqt.PageData, n int) bool {, moved 2026-09-11 (#1026)

dictRunPathEligible reports whether a run of k dictionary indices covers
exactly k consecutive ROWS on this page, which is the whole premise of
the run path.

That holds only when the page has no nulls: values are stored dense over
non-null rows, so with nulls present a run of k values spans k *non-null*
rows and locating it means walking the definition levels row by row —
the very work the run path exists to skip. Such pages take the expand
path unchanged, keeping "a NULL never matches a comparison" implemented
in exactly one place.

"No nulls" has to be established from the levels, not asserted by the
file. Three cases:

  - no definition levels at all: a REQUIRED column, nothing to check;
  - NullsFromLevels: the reader counted the nulls off these levels
    (v1 pages always; v2 pages with no level data);
  - otherwise a v2 header's num_nulls claim, which is verified here with
    a compare-only pass. That pass costs far less than the RLE expansion
    plus per-row mask work it buys, and the caller runs it only after
    the census has already accepted the page.
