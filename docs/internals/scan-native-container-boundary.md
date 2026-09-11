# Scan native container boundary

Source: internal/engine/scan/columnar.go — func HasUnsupportedColumnarTypes(schema []pqt.Column) bool {, moved 2026-09-11 (#1026)

HasUnsupportedColumnarTypes returns true if any column uses a type the
native columnar reader cannot handle (Array, Map).
TypeDecimal is supported by the native reader.

TypeRow is supported only when every field is a DIRECT PRIMITIVE LEAF.
readRowGroupNative's leafByPath is keyed on a leaf's FULL path, and the
ROW arm can only ever build a two-element one — {column, field}. That is
the whole of what it can address: a field that is itself a ROW, ARRAY or
MAP is a GROUP in the file, its leaves live one or more levels below that
path, and the lookup missed — taking the "column absent from the file →
all nulls" branch and answering `{a:"x", inner:{x:7}}` as `inner:<nil>`
with no error (#448, the same silent-wrong-answer class as #425 one level
down).

Refusing the shape here is the fix rather than teaching the ROW arm to
recurse, because the recursion is not one level of lookup: assembling a
container needs the def/rep level walk the row reader's record assembler
already does (nested_assembly.go), and the native reader has no level
machinery at all — which is why ARRAY and MAP are refused outright. A ROW
of ROW alone could be addressed by path, but ROW of ARRAY and ROW of MAP
could not, so the predicate would still have to exist and the reader would
gain a second partial assembler to keep in agreement with the first
(ADR-0018 §3). The row reader has been correct for these shapes since the
#409 nested-assembly rewrite; this routes to it.

Nothing loses a read path by this. The planner decides between the eager
native scan and the row fallback on parquet.Schema.HasNestedColumns, which
refuses a table with ANY ROW column one layer earlier (plan.go Init,
buildRGUnits, readBatchDirect), and the top-N late-materialization rewrite
refuses one on the full table schema (topn_late_mat.go) — so
ReadRowGroupNativeShaped, the late-mat decode, never sees a ROW at all.
The worker's cachedFileStreamSource is the path that does, and it tests
this predicate before opening a row-group iterator and falls back to
ReadFileBatchesShard, which reads through the row reader.
