# Catalog drop metadata order

Source: internal/storage/catalog/catalog.go — func (c *Catalog) DropTable(ctx context.Context, name string) error {, moved 2026-09-11 (#1026)

DropTable removes a table from the catalog.

Metadata only: the table's name and manifest KV keys go away here, which
is what makes it immediately invisible to every NEW query (GetTable,
GetManifest, and ListTables all answer from this same metadata, and #483
keys the manifest cache by KV revision so a stale in-process copy can't
serve a resurrected name's old files either). The table's DATA FILES are
deliberately NOT deleted here — see FlushDroppedTableFiles for why, when,
and under what guard they go.

Tombstone-then-grace-delete, not a prefix delete under tables/<name>/,
and not "leave it forever" either (#494 asked for a decision between
those). A live prefix delete is the wrong shape regardless of timing: a
CREATE TABLE of the same name during the grace window gets an entirely
new, unrelated set of files at that same prefix (chunk/compacted names
are per-file random, not derived from the table name), and a prefix
delete run after the fact cannot tell that incarnation's files from the
dropped one's — it would eat the new table's data. Recording the exact
paths this incarnation OWNED (engine-written only — see the snapshot
below), once, right here, and checking each one against every CURRENT
manifest before ever deleting it (FlushDroppedTableFiles) has no such
blast radius. It doesn't reach
RGMetaKey/SketchesKey blobs under stats/<name>/ — those are named by
table+column, not by a birthday-collision-prone short ID, so they sit
outside #494's collision hazard; leaking them is a separate, lower-
severity storage-hygiene gap.

Ordering matters twice. The metadata put that removes the name from
meta.Tables goes FIRST — it is the write that constitutes the drop, and
putting it first is what makes a failed DROP a clean no-op rather than a
table that is listed but unreadable (see the comment at the put). And
the pending-drop record is appended only AFTER that put succeeds: a
failed DROP must leave the table exactly as recoverable as it was before
the call — nothing scheduled for physical deletion — not half-gone with
its files already timed for reclaim.
