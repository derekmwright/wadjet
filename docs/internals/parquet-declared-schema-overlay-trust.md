# Parquet declared schema overlay trust

Source: internal/storage/parquet/file_reader.go — func overlayDeclaredSchema(inferred Schema, nodes []*SchemaNode, kv []KeyValue) Schema {, moved 2026-09-11 (#1026)

overlayDeclaredSchema restores the declared TYPE IDENTITY of every leaf in
the schema — top-level columns and the leaves inside ROW, ARRAY and MAP
alike — from the footer's declared-schema blob. Nothing else is taken:
name, nullability, precision, scale, dimension and nested structure all
stay as the parquet tree described them, because those the tree CAN express
and the tree is what the page decoders are driven by.

A file written before this key existed, or by any other producer, has no
blob and keeps the inferred schema — the behaviour this replaces.

The blob is UNTRUSTED INPUT: it is bytes in a file, and a reader that lets
it choose how pages are interpreted has handed a file the power to make the
engine misread its own data. Five conditions must all hold before a single
leaf is touched, and any failure leaves that leaf — or that subtree, or the
whole schema — exactly as the tree described it:

 1. the blob is under maxDeclaredSchemaBytes and decodes as JSON;
 2. it describes the same number of top-level columns, with the same names
    in the same order (inside a container: the same structural shape, and
    ROW fields matched by exact name — see overlayDeclaredContainer);
 3. the leaf carries NO LogicalType and NO ConvertedType — the file itself
    said nothing about what the column means, which is the only situation
    the blob is here to fix — or it is annotated UTF8 text, the single
    exception below;
 4. the declared type is one of declaredOverlayTypes (the eight types
    parquet cannot annotate) on an unannotated leaf, or CIDR on a UTF8 one
    (declaredOverlayUTF8Types: same storage, same rendering, name only);
 5. the physical parquet type of that declared type is the physical type
    the leaf ACTUALLY has in the file.

Together those make a stale or mismatched blob inert rather than a source
of misread pages, and they bound the blast radius of a hostile one to
relabelling an unannotated INT32/INT64/BYTE_ARRAY column as another type
with the identical storage.
