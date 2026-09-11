# Parquet cidr utf8 overlay

Source: internal/storage/parquet/file_reader.go — var declaredOverlayUTF8Types = map[TypeID]bool{, moved 2026-09-11 (#1026)

declaredOverlayUTF8Types is the one exception to "an annotated leaf is
immune", and it holds exactly one type.

CIDR has no parquet annotation of its own either, so buildLeafSchemaElement
writes it as UTF8 STRING — the annotation describes the STORAGE truthfully
and loses only the name. Restoring the name over a UTF8 leaf changes
nothing about how the page is decoded (BYTE_ARRAY either way) and nothing
about how a value renders (Vector.GetValue returns the same text for both
STRING and CIDR), which is what makes this safe where STRING→IPv6 is not:
IPv6's storage contract is exactly 16 bytes and GetValue renders anything
else as "".

It is not cosmetic. The engine dispatches on the TYPE, and CIDR and STRING
do not behave identically everywhere: with CIDR reverting to STRING, the
stage DAG and the single-process engine started answering
`SELECT MIN(c_cidr), MAX(c_cidr)` differently — the DAG correctly, the
single-process arm with NULLs — because one saw a STRING column and the
other the catalog's CIDR (TestTypeMatrixTwoPath/minmax_c_cidr; the
single-process NULL is a separate defect, and #392's MIN_BY switch is
another). Restoring the declared name is what keeps the two paths reading
the same column as the same type.
