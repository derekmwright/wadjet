# Parquet absent required leaf

Source: internal/storage/parquet/file_writer.go — func (nw *NativeWriter) appendAbsentLeaf(lb *leafBuffer, col Column, defLevel, repLevel int32) {, moved 2026-09-11 (#1026)

appendAbsentLeaf records that this leaf has no value at this position, or
refuses when the position cannot say so.

A definition level says how many of a leaf's optional ancestors are present;
the leaf's own maxDefLevel is the level at which the VALUE itself is
present. A REQUIRED leaf has no level below that to spend on absence, so
appending one for a nil wrote the PRESENT level and advanced the count with
nothing behind it: every later value in that column shifted by one. For a
required BOOLEAN, `[nil, true]` read back as `[true, false]` — the bit
padding hid the mismatch — and for a required INT64, `[nil, 42]` produced a
file the decoder could not finish, two values declared over eight data bytes
(#887).

The test is the LEVEL, not the column's Nullable flag, and that is what makes
it right at depth: a required field of a PRESENT optional struct has
defLevel == maxDefLevel here and is refused, while the same field under an
ABSENT optional ancestor never reaches this function at all (its subtree goes
through emitNullForSubtree at the ancestor's own lower level), which is
exactly the case that must stay legal.

The SQLSTATE is PostgreSQL's 23502 not_null_violation, the one
ingest.validateRow already raises for a missing non-nullable column.
