# Parquet decoded footer cache

Source: internal/storage/parquet/footer_cache.go — const defaultFooterCacheBytes = 128 << 20, moved 2026-09-11 (#1026)

Process-level cache of DECODED parquet footers.

Motivation (2026-08-17 pruned-scan profile): every query decoded every
file's footer TWICE — once in the planner's buildRGUnits to enumerate and
prune row groups, and again in fileSlot.ensureLoaded when the file's bytes
(or fd) were opened for reading. At ClickBench shape (100 parts × 105
columns) that is 21,000 Thrift ColumnMetaData records decoded per query:
~9.1 ms wall and ~31 ms CPU per query on the fast-query tier, and the top
mallocgc source in that profile. Footers are also re-decoded from scratch
on every subsequent query over the same files.

SAFETY MODEL — read this before touching anything here.

 1. Key. The cache is keyed by an opaque identity string the CALLER builds
    (see physical.footerCacheIdentity), of the shape
    "<storeID>/<bucket>/<key>#<size>@<createdAtUnixNano>". Each component
    answers a distinct way the same name could describe different bytes:

    storeID — objstore.StoreID: a per-instance ID for MemStore, the
    absolute root for FileStore, the endpoint for S3. Two unrelated stores
    that happen to share bucket and object names cannot collide. This is
    the realistic in-process hazard: separate tests in one binary write
    different content — different schemas, even — to identical names such
    as bucket "test" / "tables/items/chunk_001.parquet". A store that
    declines to identify itself yields "" and is never cached.

    bucket + key — the manifest's own notion of file identity. Wadjet's
    writers never rewrite a data object in place: ingest names chunks with
    a fresh UUIDv7 (storage/ingest/ingest.go:309), compaction and
    delete-marker GC write a new UUIDv7-stamped key and swap the manifest
    atomically (storage/compaction/compactor.go:57, 459, 897;
    catalog.SwapFileForGC). Every one of those names is a full 128-bit
    identifier, not a truncated prefix (#494) — a fresh key never
    recreates an old one within the birthday bound this cache's own key
    format has to worry about. The base-table NVMe cache and the
    decoded-chunk cache (engine/scan/decoded_cache.go:88) already stand on
    exactly this premise.

    size + createdAt — the guards for the residual case that premise does
    NOT cover: a path recreated out of band. That is a real, observed
    event, not a hypothetical — compactor.go:512-525 carries a
    "recreated-object guard" for precisely it (re-ingest, or benchmark
    datagen with deterministic chunk names). A recreated object gets a
    fresh manifest entry with a fresh CreatedAt, so the key changes and
    the stale footer is simply never consulted. catalog.FileEntry carries
    no ETag and obtaining one would cost a Head per file per query — the
    round trip storage/catalog/rgmeta.go exists to eliminate — so
    (size, createdAt) is the strongest free discriminator available at
    plan time.

    An empty identity disables caching for that call — callers fail closed.

 2. Shared immutability. A cached entry is handed to many concurrent
    FileReaders, so its contents must be read-only after construction.
    Verified 2026-08-17 by exhaustive grep over internal/: the ONLY writes
    to FileMetaData / RowGroup / ColumnChunk / ColumnMetaData / Statistics
    fields are in the Thrift decoder (thrift.go decodeFileMetaData and its
    helpers, lines ~304-970), and the ONLY writes to SchemaNode fields are
    in BuildSchemaTree / computeLevels (schema_tree.go lines ~40-118) —
    both construction-time. Consumers only read: FileReader.ColumnPages
    copies scalar fields out of *ColumnMetaData into a ColumnPageReader
    (page_reader.go NewColumnPageReader / NewColumnPageReaderAt) and never
    retains or mutates it; scan/{columnar_native,sel_decode,decode_ahead,
    dict_prune,row_filter}.go read Leaves()/RowGroupMeta() only.
    Belt-and-braces: the two slices handed out by value (leaves and
    Schema.Columns) are capacity-clipped on insert, so a future consumer's
    stray append reallocates instead of writing into shared backing store.

 3. No singleflight. Concurrent decoders of the same identity both decode
    and the first insert wins. Deliberate: a shared wait would let one slow
    or hung object read block unrelated queries' planner goroutines, and
    straggler amplification is a failure mode this engine has paid for
    before. Duplicate decodes cost CPU, never correctness.

Escape hatch: WADJET_FOOTER_CACHE=0 disables the cache process-wide (pure
cache of immutable data — no plan or row-set change, so no optswitch kill
switch; this exists to rule the cache in or out during an incident).
WADJET_FOOTER_CACHE_BYTES overrides the byte cap (default 128 MiB).
