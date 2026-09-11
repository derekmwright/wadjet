# Distributed recursive column declaration

Source: internal/distributed/messages.go — type ColumnSpec struct {, moved 2026-09-11 (#1026)

ColumnSpec is one column of a plan-declared schema on the wire: the name
and the parquet.TypeID as an int, matching AggSpec.OutputType's encoding.

Precision, Scale and Dimension are the parameters a bare TypeID does not
carry — DECIMAL's two, VECTOR's one. A schema declared for a SCAN needs
them (OpSpec.ColumnTypes, #423): the reader allocates a VECTOR's storage
from its dimension and renders a DECIMAL from its scale, so a spec that
dropped them would declare a type the worker cannot build. They are
omitempty and zero for every other type, so the join-side declarations
(BuildSchema / ProbeSchema) encode exactly as they did before.

ElementType and Fields carry a CONTAINER's shape, and they exist for the
same reason the three scalars above do: a bare TypeID that says ROW says
nothing about the ROW's fields, so a declaration that stopped at the top
level could not tell the worker what an IPv6 inside that ROW is. Nine of
this engine's types have no parquet annotation, so for a file written before
the wadjet.schema footer key existed the catalog is the ONLY place a nested
leaf's type survives — and it could not cross the wire (#608). ARRAY and MAP
use ElementType, ROW uses Fields, exactly as parquet.Column does. Both are
omitempty, so a flat declaration encodes byte-for-byte as it did before and
a worker that ignores them behaves exactly as it did.
