# Parquet cidr stat bound representations

Source: internal/storage/parquet/reader.go — type CidrInetBound struct {, moved 2026-09-11 (#1026)

CidrInetBound is a CIDR row-group MinValue/MaxValue RowGroupStats has
CONFIRMED is orderable in PostgreSQL's inet order (#523). It is a distinct
type, not a plain string, specifically so a consumer's comparison cannot
mix the two by accident: kernel.StatsDomainValue's CIDR literal converts
to this same type, and a generic string comparator that special-cases it
(see scan.compareValuesOK) refuses to compare one against an ordinary
string — which is what an UNCONFIRMED file's untouched TEXT bound still
is. That refusal is what keeps kernel.StatsDomainValue's conversion
unconditional (every valid CIDR literal converts) safe even for a row
group whose file this reader cannot confirm is CIDR at all, or is CIDR but
pre-#523: the type system, not a per-file heuristic, is what stops the
comparison.

It carries BOTH representations because its two consumers need different
ones and neither can be derived from the other without loss:

  - Key is the comparison domain — kernel.CidrSortKey's encoding,
    duplicated in this package as CidrStatsSortKey. It is a BINARY string
    (a family byte, the masked address bytes, the mask length, the full
    address bytes), so it is not valid UTF-8 and must never reach a JSON
    or text encoder.
  - Text is the winning row's address text exactly as the file stores it,
    which is what a CATALOG stat has to hold: catalog.FileColumnStats is
    JSON-tagged and persisted in NATS KV, and encoding/json rewrites every
    byte a Key holds above 0x7F as U+FFFD, irreversibly. Both
    extractColumnStats sites (storage/ingest, storage/compaction) unbox to
    this before the stats leave for the catalog.

Text is empty on the LITERAL side (kernel.StatsDomainValue has a predicate
constant, not a row), which is sound because a literal-side bound is only
ever compared, never persisted.
