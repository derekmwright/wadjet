# Batch shape only row box

Source: internal/engine/batch/vector.go — type ShapeOnlyLen int, moved 2026-09-11 (#1026)
Superseded: length() over text has counted characters since #856 and cannot use a byte-length-only shape; octet_length/bit_length retain byte-count semantics.

ShapeOnlyLen is what the boxing boundary hands back for a row of a
SHAPE-ONLY column: the value is NOT AVAILABLE, and this is its byte length.

It exists because a shape-only column has to survive a ROW-SHAPED detour.
The scan decodes lengths and no bytes when the planner proves every use of
the column reads its shape (COUNT, LENGTH, IS NULL, empty-string), and the
vector paths carry that faithfully — copyShapeRange propagates the mark
rather than moving bytes that do not exist. The row paths could not: a
grouped aggregate under memory pressure buffers its input through
RecordBatch.ToRows, whose per-row box comes from Vector.GetValue, and the
only thing GetValue could produce for such a row was the panic that says a
value was read (#791).

So the box is neither the value nor a rendering of it. It is a REFUSAL that
carries the length: LengthAt's answer, and nothing else. Written back
through SetValue it reconstructs a shape-only column with the same
per-row lengths, so what comes out of the detour is what went in — and a
consumer that then wants the bytes raises the same guard it always did,
at the same place, saying the same thing.

A type of its own rather than an int: an int would be indistinguishable
from a value at every `switch v := x.(type)` in the tree, which is exactly
how a lossy encoding gets written by accident (#632, ADR-0023 item 6 — an
encoder must never write bytes its own reader refuses, and a renderer is
not a value).
