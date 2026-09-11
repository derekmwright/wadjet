# Worker required base scan schema

Source: internal/worker/executor_fragment.go — func applyDeclaredScanSchema(src exec.Source, what, alias string, files []string, declared []distributed.ColumnSpec) error {, moved 2026-09-11 (#1026)

applyDeclaredScanSchema hands a source the catalog's declared column types,
and REFUSES the read when a base-table parquet input arrives without them.

The declaration is not an optimization. A parquet file cannot express nine
of this engine's types (IPv4, IPv6, MAC, UUID, BYTES, PORT, PROTOCOL,
DURATION have no logical annotation; CIDR is written as plain UTF8), and
files written before v0.18.0 do not carry wadjet's own footer key either —
so a scan that types its columns from the FILE answers 167772165 where the
catalog says 10.0.0.5 (#396/#423). Worse, and the reason this refuses
rather than warns: parquet.Reader.SchemaAs(nil) short-circuits to the
file's own schema, so the catalog never enters the comparison and a file
whose stored type CONTRADICTS the catalog decodes silently as whatever it
happens to hold. A stale or foreign file — the shape a chunk-name collision
(#494) produces on its own — returned the string 'hello' for a column the
catalog calls BIGINT, while the single-process reader refused it by name
("schema declares INT64 but the file stores STRING").

Stage output is the legitimate empty case and is left alone: a .wshf
payload carries its own types, and there is nothing for a declaration to
add. That is the whole of the distinction, and readsBaseTableParquet is
where it is drawn.

WADJET_DECLARED_SCHEMA_STRICT=0 restores the pre-#503 behavior — the file's
own types win — for the same reason WADJET_FASTPATH_STRICT=0 exists: this
refusal turns a class of query that USED to answer into a hard failure, and
a plumbing path nobody has found yet (a stage that forgets to carry
ScanSchema) then takes a deployment down rather than answering as it did
last release. It is a way out, not a supported mode: what it restores is a
read that can answer 167772165 for 10.0.0.5, so it logs at Warn every time
it is used.
