# Catalog manifest revision memo

Source: internal/storage/catalog/catalog.go — func (c *Catalog) GetManifest(_ context.Context, tableName string) (*PartitionManifest, error) {, moved 2026-09-11 (#1026)

GetManifest returns the partition manifest for a table.

Freshness is decided by the manifest key's KV REVISION, on every call.
The cache only ever skips re-decoding a revision this process already
decoded; it is a decode memo, never a staleness window.

It used to be one, and that was #483. A 2-second wall-clock TTL,
invalidated only by writes made through the same *Catalog value, is
sound only while a process holds exactly one of them. Standalone holds
three over the same KV — the coordinator's, the pgwire DB's, and a fresh
one per worker pipeline task — and pgwire routes SELECT through the
coordinator's catalog while INSERT/UPDATE/DELETE and DDL go through the
DB's. Every write therefore invalidated a cache no reader was consulting,
and reads answered from a manifest up to two seconds old. Statements
issued back to back (a psql script, a SQLancer round, any client driving
a session) all land inside that window: writes looked lost, and
DROP TABLE + CREATE TABLE of the same name answered out of the previous
incarnation's files — silently when the two schemas were
encoding-compatible, and as a decode-time type refusal when they were
not. A revision is the catalog's own notion of "which version is this",
so validating against it cannot drift from what the catalog holds; a
clock can.

The returned manifest is SHARED with every other caller holding this
revision. Treat it as immutable — mutators inside this package take
loadManifest instead.
