# Parquet catalog leaf retyping

Source: internal/storage/parquet/reader.go — func retypeFromCatalog(readCols, catalog []Column, root *SchemaNode, leaves []*SchemaNode) ([]Column, error) {, moved 2026-09-11 (#1026)
Superseded: Nested annotation restoration now exists in nested_retype.go, and DECIMAL declaration reconciliation can update (p,s); the historical claims that nested leaves stay lossy and matching decimals need no action are stale.

retypeFromCatalog replaces each read column's type with the catalog's,
where the catalog names it, both sides are leaves, and the two types are
carried by the same physical bytes.

Leaves only, deliberately: a nested column's read plan is built from the
FILE's shape (the assembly plan is built from the file's schema tree), and
substituting a catalog Column whose children were resolved differently
would look up leaves that do not exist. A lossy leaf INSIDE a container
stays lossy — that is the same annotation gap, one level down, and it
needs the annotations, not a substitution.

Same physical bytes, non-negotiably: this substitution exists so that a
column the file cannot ANNOTATE (IPv4, IPv6, MAC, PORT, PROTOCOL,
DURATION, BYTES, UUID) is decoded as what it is, and every one of those
eight has the same physical type as the type the file recovered for it.
A catalog type of a DIFFERENT width is not a lost annotation, it is
catalog/file drift, and honouring it means decoding the file's bytes as
a wider element: unpackAllPresent would ask Values.Int64() for one int64
per INT32 in the page, an unsafe.Slice twice as long as its backing array
— megabytes of adjacent heap returned as query results.

The comparison is against the FILE LEAF's RECOVERED TYPE — physical type
plus the logical/converted annotations that TypeIDFromSchemaNode reads —
not against a physical type alone and not against what our writer would
have chosen. Each of those weaker questions admits pairings that decode to
nonsense:

  - Our writer's mapping on both sides compares what WE would have written,
    so a pyarrow DECIMAL(9,2) (physically INT32) sitting under a catalog
    INT64 compared INT64 to INT64 and passed, then read eight bytes per
    four-byte value.
  - The physical type alone cannot tell a DECIMAL from the INT32, INT64,
    BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY it is stored in — the format allows
    all four, and only the annotation says which one this is. Asking
    "is DECIMAL readable from this physical?" therefore answered yes for
    every leaf in the file, so a catalog DECIMAL(18,2) over a STRING column
    was admitted and read ("hello","world") back as two integers made of
    the letters. Same width, different meaning: the decode does not fault,
    it just answers something else.

The question that is actually being asked is whether the values the file's
own type decodes to can be STORED as the catalog's type without converting
them, and StorageClassOf is exactly that relation. DECIMAL and VECTOR are
classes of their own, so a catalog DECIMAL is admissible only over a leaf
the annotations already recovered AS a decimal — at which point there is
nothing to substitute and the loop has already skipped it. The eight
inexpressible types share a class with the plain INT32/INT64/BYTE_ARRAY
their annotation-free leaves recover as, which is the whole mechanism.
CoercibleTo names the only pairings admitted ACROSS classes, and those are
decoded as the file's type and converted afterwards.

Drift is an ERROR rather than a silent skip. Skipping would answer the
query from the file's own type, which is a different answer from the one
the catalog promised, arrived at without saying so; the caller cannot tell
that from a correct read. A named error says which column, what the
catalog claims and what the file actually holds — which is the whole
diagnosis.
