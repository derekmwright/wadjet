# Batch nested column payload

Source: internal/engine/batch/container_codec.go — const containerNilChild = 0xFF // type byte for a nil child (legal only at 0 elements), moved 2026-09-11 (#1026)

Container-column payload codec.

ARRAY, ROW, MAP and VECTOR are the four column shapes whose bytes are not
a flat run of fixed-width or length-prefixed values: they carry element
offsets, a child vector, ROW field order, or a per-column dimension, and
none of that survives a schema that records only a name and a type byte.
That is #397 — every distributed plan that had to move one of the four
across a stage boundary failed at the WSHF encoder ("unsupported shuffle
type: ARRAY"), while the single-process engine answered the same query.

The layout is the exec columnar spill codec's nested framing
(internal/engine/exec/join_spill.go writeNestedVector / readNestedVector)
re-homed here, because the WSHF format has TWO independent decoders — the
worker's (internal/worker/shuffle_format.go) and the coordinator's
(internal/coordinator/shuffle_reader.go) — and a third hand-written copy
of a recursive nested layout is exactly the shape that drifts. Both call
this. batch is the layer that already owns the nested Vector shape
(NewVectorLike, AppendFrom, CopyValueFrom), and it is below both callers,
so there is no new dependency edge.

The one deliberate difference from the spill codec is the BYTES leaf,
which uses WSHF's null-skipping form (totalLen, concatenated data, n END
offsets) instead of a straight copy of the offsets array: a null slot's
offset pair can be a descending, malformed pair (see BytesColumn.Value),
which a straight copy would carry into the reader.

Row-level nulls of the COLUMN ITSELF are NOT in this payload — WSHF
writes every column's null bitmap ahead of its data regardless of type.
Nulls of a CHILD vector ARE, because nothing else records them. A NULL
container and an EMPTY container therefore stay distinct: both encode
Offsets[i] == Offsets[i+1], and only the null one carries the column
bitmap bit.
