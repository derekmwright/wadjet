# Parquet map storage shape and order

Source: internal/storage/parquet/file_writer.go — func mapFromStorageShapeEntries(val any, keyName, valName string) (map[string]any, bool) {, moved 2026-09-11 (#1026)
Superseded: The original block combines sortedMapKeys documentation with mapFromStorageShapeEntries, which is the declaration immediately below it.

sortedMapKeys returns m's keys in byte order.

Go map iteration is randomized, so ranging over the map wrote the same
MAP value's entries in a different order on every call, and the file was
therefore not a function of its input: two writes of identical rows
produced different bytes. Nothing that compares files can work against
that — no golden file, no content hash, no byte-for-byte check that a
rewrite changed nothing. (Row-group min/max survive it, being
commutative; the bytes and the entry order do not.)

The order is also observable downstream: it is the order the entries are
laid out in, and the vector side turns exactly that into the order
GetValue hands back. batch.mapEntryRows sorts on the same rule, because
this writer and that vector are the two ways the same map reaches disk
and they have to agree.
mapFromStorageShapeEntries converts a MAP's storage-shape value — []any of
{keyName: k, valName: v} entry maps, the shape batch.Vector.GetValue's
TypeMap arm produces (and batch.mapEntryRows builds from a native map on
the way in) — back into the native map[string]any this writer expects.
Returns ok=false for anything else, so the caller's existing
malformed-input handling is unchanged.

MAP keys are always Go strings at this boundary (mapKeyValue's own
comment: "Row-level keys are always strings"), so a non-string key entry
is exactly as malformed as any other shape val could have been.
