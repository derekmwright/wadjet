# Parquet container overlay alignment

Source: internal/storage/parquet/file_reader.go — func overlayDeclaredContainer(ic, dc *Column, n *SchemaNode) {, moved 2026-09-11 (#1026)
Superseded: Container alignment requires equal child counts and exact names in the same order, not a name-only match in arbitrary order.

overlayDeclaredContainer walks a ROW, ARRAY or MAP and overlays what is
underneath it.

The container's own structure is NOT taken from the blob — parquet's LIST,
MAP and STRUCT annotations express it and the tree already carries it. What
parquet cannot express is the same thing one level down that it cannot
express at the top: an IPv6, a UUID or a BYTES leaf is an unannotated
BYTE_ARRAY wherever it sits, so the reader read one back as TypeString and
the row assembler boxed sixteen intact bytes as a Go string, which
Vector.SetValue then handed to net.ParseIP and dropped — the value read
back as "" (#589). The blob is the only place the declared type survives,
at every depth.

Alignment is re-derived from the shape nodeToColumn itself read, so the
inferred column and the tree cannot drift apart here:

  - ARRAY: one repeated child holding one element node;
  - MAP: one repeated child holding the key and the value, which the
    inferred column presents as the synthesized "entry" ROW's two fields —
    so the ROW arm aligns them against that same repeated group;
  - ROW: fields positionally aligned with the group's children (that is how
    nodeToColumn built them), each matched to a DECLARED field by exact
    name.

Any disagreement — the blob calling a container something the tree does
not, a field the blob does not name, a field name it repeats — leaves that
subtree exactly as the tree described it.
