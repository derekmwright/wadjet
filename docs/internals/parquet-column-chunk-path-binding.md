# Parquet column chunk path binding

Source: internal/storage/parquet/footer.go — func ValidateColumnChunkPaths(md *FileMetaData) error {, moved 2026-09-11 (#1026)

ValidateColumnChunkPaths binds each row group's column chunks to the schema
leaves by FULL path, refusing a footer whose column metadata contradicts the
schema it belongs to.

The reader resolves a leaf to its chunk by SLICE POSITION: FileReader.ColumnPages
reads rg.Columns[leafIdx] for schema leaf leafIdx, and every read path (the
row reader, the native scan) resolves a column NAME to that leafIdx first.
That is correct only while rg.Columns[j].PathInSchema names schema leaf j —
which the format requires (a row group lists one column chunk per leaf, in
schema-leaf order) and every writer honours. Nothing checked it. Swapping two
ColumnChunk entries in the footer therefore handed each leaf its neighbour's
chunk; for two columns of the same physical type the decode met no mismatch
and ReadRows returned the values under the wrong names, nil error (#927).
ValidateChunkLayout cannot catch it — it sorts extents by byte offset, so a
swap that keeps every byte range valid passes. The binding is checked here, at
open, by full path: a position whose chunk names a different leaf than the
schema puts there — a swap, a duplicate, or a foreign path — is refused by
name. A row group SHORT a chunk keeps its existing per-column refusal
("carries no chunk for it", column_completeness.go), which names the absent
column; every position this file DOES carry is validated here, so a middle
drop that shifts the survivors is caught as a contradicting path.
