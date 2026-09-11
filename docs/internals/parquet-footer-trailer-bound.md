# Parquet footer trailer bound

Source: internal/storage/parquet/file_writer.go — func footerTrailerLength(n int64) (uint32, error) {, moved 2026-09-11 (#1026)

footerTrailerLength is the value a file's four-byte trailer carries for a
footer of n bytes — and the ONLY way to obtain one, so the narrowing cannot
happen anywhere else.

The trailer is a fixed four-byte unsigned length, so a footer past
math.MaxUint32 has no honest position to point at. writeFooter used to
encode the metadata, WRITE IT, and only then narrow the length with a bare
uint32() conversion: 2^32 bytes of footer became a trailer of 0 and 2^32+4
became 4, so Close appended PAR1 over a location pointing into the data and
returned nil. Readers seek there and interpret whatever they find as
metadata. It is reachable from row-group metadata alone — one RowGroup plus
one ColumnChunk per column accumulates per flush and is retained to Close —
not only from huge values (#974).

Two bounds, in the order that names the failure most precisely:

  - The FORMAT's width. Structural, not policy: nothing can carry it.
  - This package's own READ ceiling, footerMaxSize. ADR-0018 §2's corollary
    binds the writer to the reader's ceilings, and a 64 MiB footer is a file
    wadjet itself refuses to open — writing one produces an artifact that is
    unreadable here and merely pathological elsewhere. Refusing at Close
    says so while the caller still has the rows.
