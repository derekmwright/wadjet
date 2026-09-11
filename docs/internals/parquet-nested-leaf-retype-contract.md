# Parquet nested leaf retype contract

Source: internal/storage/parquet/nested_retype.go — func applyNestedLeafRetype(dst, want *Column, n *SchemaNode) {, moved 2026-09-11 (#1026)

applyNestedLeafRetype writes the catalog's declaration over one leaf's, where
the substitution is admissible. It is the ONE place a nested leaf's declared
type changes, so the two walks above — the Column tree the caller carries
onward, and the per-leaf array the nested decode reads — cannot disagree.

Two admissible substitutions, and they are different in kind:

  - A TYPE the file cannot annotate (the nine of ADR-0018 §8) is restored
    from the catalog. Type identity ONLY, exactly as overlayDeclaredLeaf
    copies only the type: none of the nine has a precision, scale or
    dimension to carry, and copying those fields is how a declaration would
    reach the decode and allocation paths.
  - A DECIMAL's (p, s) is adopted when the file declares a different one.
    Here the parameters ARE the substitution: the type identity already
    matches and the disagreement is entirely in the two numbers, half of
    which is half of every value in the column. Round 0's B1 was exactly
    this pairing being declined — so a `DECIMAL(15,4)` leaf inside a ROW
    under a `DECIMAL(15,2)` catalog column answered 1275.00 for 12.75 while
    the flat column beside it, reconciled by retypeFromCatalog's top-level
    arm, answered 12.75. The decode moves the carrier
    (readLeafColumn / scan.rescaleDecimalChunk) once this has said what the
    leaf means.
