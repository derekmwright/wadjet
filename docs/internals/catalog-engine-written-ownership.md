# Catalog engine written ownership

Source: internal/storage/catalog/catalog.go — EngineWritten bool `json:"engine_written,omitempty"`, moved 2026-09-11 (#1026)
Superseded: CommitDML and CommitCompaction also stamp their newly written outputs; ownership is no longer set in exactly two places.

EngineWritten marks an object WADJET ITSELF wrote: ingest's
chunk_<uuid>, compaction's compacted_<uuid>, delete-marker GC's
rewrite_<uuid>. It is the ownership marker DropTable's physical
reclaim keys off — only a marked entry is ever scheduled for
deletion (#494), so reclaim can only ever delete bytes this engine
created.

Set in exactly two places, both of which mint the path themselves:
AddNewFiles (ingest, compaction) and SwapFileForGC's rewrite output.
AddFiles — the REGISTRATION path — deliberately leaves it alone,
because its callers point the catalog at objects somebody else
staged: cmd/tpch-bench (--data-prefix "tables/"), cmd/clickbench-
bench (--s3-prefix "tables/hits/"), internal/harness's s3_catalog,
and iceberg.CatalogIntegration all register pre-existing operator
data, and a bench bucket's reference dataset is not wadjet's to
delete on a DROP.

Absent means NOT owned, which is the safe default in both
directions that matter: `omitempty` keeps it out of every manifest
that has no engine-written files, and every manifest written before
this field existed decodes with it false — so no pre-existing
object can be reclaimed by a newer binary. Unmarked entries leak on
DROP by design; see docs/adr/0020-drop-table-reclaim-is-opt-in.md.
