# Container expression declared shape

Source: internal/engine/expr/container_shape.go — func containerVector(e Expr, b *batch.RecordBatch) *batch.Vector {, moved 2026-09-11 (#1026)

A container expression's SHAPE — is it a MAP or an ARRAY, and what is its
element declared as — is a property of the container's declaration, resolved
from its parent's the way ADR-0022 resolves a ROW field. This file answers
that question for an arbitrary container-valued expression rather than for a
bare column reference alone, which is what #669 and #635 are:

  - `element_at(element_at(aa, 1), 1)` over an ARRAY of ARRAY of DECIMAL was
    unclassified, so `GREATEST(<that>, '2')` answered "2" by byte order where
    the server compares 12.75 numerically, and a predicate over it lost every
    row (#669 item 1).
  - `element_at(COALESCE(m, m), 'a')` routed to POSITIONAL array indexing and
    answered NULL, because only a `*ColRef` and a fixed-RetMap function were
    recognised as a MAP (#635). A MAP column materializes as
    ARRAY(ROW("key","value")), which is byte-for-byte the shape map_entries()
    produces, so the runtime VALUE cannot decide this and the declaration
    must.

The vector is the declaration at this layer: batch.RecordBatch carries the
parquet schema into Vector.Child (ARRAY element, MAP entry ROW) and
Vector.Children (ROW fields), and a compiled expression has no other handle
on a nested declared type.

nil means "no declared shape here", which is the honest answer for a value
whose producer declares none — a RetDynamic function, a UDF that names no
container type. That case is genuinely ambiguous rather than merely
unresolved: a MAP and an ARRAY of two-field ROWs are one runtime shape, and
PostgreSQL has no MAP at all to arbitrate. `fnElementAt` still keys a Go map
directly, which is the shape json_extract and map_from_entries produce.
