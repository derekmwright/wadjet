# Catalog live reference observation

Source: internal/storage/catalog/catalog.go — func (c *Catalog) liveCatalogState(ctx context.Context) (paths map[string]bool, names map[string]bool, err error) {, moved 2026-09-11 (#1026)

liveCatalogState observes the catalog as it stands RIGHT NOW: the set of
every file path referenced by ANY table's manifest, and the set of table
names that exist. Both halves are FlushDroppedTableFiles's guard.

The path set is the load-bearing one: a path recorded in pendingDrops can
ALSO be live at flush time — the same table name re-created and the very
same object paths re-registered into it (#278's documented idempotent
re-registration workflow lets a harness/bench loader do exactly that, and
iceberg.CatalogIntegration.RefreshTable does it on every metadata
refresh: drop, recreate, re-register the same warehouse files) — and a
path referenced by any CURRENT manifest must never be deleted just
because some OTHER, already-gone incarnation once also owned it.

The name set closes the window the path set alone cannot: CreateTable
publishes a table's name and its (empty) manifest BEFORE any AddFiles
call registers a single path into it, so there is an interval in which a
re-created table is live and its manifest is still empty. Nothing is
protected by path during that interval. A dropped name that has come back
since this flush started is therefore treated as "the world changed under
us" and its whole pending entry is left alone — see FlushDroppedTableFiles.

A GetManifest error for a table ListTables just returned is treated as
"this sweep cannot prove anything is safe" rather than "that table has
no files": the caller declines to delete against a possibly-incomplete
picture.
