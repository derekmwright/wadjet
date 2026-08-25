package parquet

import (
	"container/list"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Process-level cache of DECODED parquet footers.
//
// Motivation (2026-08-17 pruned-scan profile): every query decoded every
// file's footer TWICE — once in the planner's buildRGUnits to enumerate and
// prune row groups, and again in fileSlot.ensureLoaded when the file's bytes
// (or fd) were opened for reading. At ClickBench shape (100 parts × 105
// columns) that is 21,000 Thrift ColumnMetaData records decoded per query:
// ~9.1 ms wall and ~31 ms CPU per query on the fast-query tier, and the top
// mallocgc source in that profile. Footers are also re-decoded from scratch
// on every subsequent query over the same files.
//
// SAFETY MODEL — read this before touching anything here.
//
//  1. Key. The cache is keyed by an opaque identity string the CALLER builds
//     (see physical.footerCacheIdentity), of the shape
//     "<storeID>/<bucket>/<key>#<size>@<createdAtUnixNano>". Each component
//     answers a distinct way the same name could describe different bytes:
//
//     storeID — objstore.StoreID: a per-instance ID for MemStore, the
//     absolute root for FileStore, the endpoint for S3. Two unrelated stores
//     that happen to share bucket and object names cannot collide. This is
//     the realistic in-process hazard: separate tests in one binary write
//     different content — different schemas, even — to identical names such
//     as bucket "test" / "tables/items/chunk_001.parquet". A store that
//     declines to identify itself yields "" and is never cached.
//
//     bucket + key — the manifest's own notion of file identity. Wadjet's
//     writers never rewrite a data object in place: ingest names chunks with
//     a fresh UUIDv7 (storage/ingest/ingest.go:309), compaction and
//     delete-marker GC write a new UUIDv7-stamped key and swap the manifest
//     atomically (storage/compaction/compactor.go:57, 459, 897;
//     catalog.SwapFileForGC). Every one of those names is a full 128-bit
//     identifier, not a truncated prefix (#494) — a fresh key never
//     recreates an old one within the birthday bound this cache's own key
//     format has to worry about. The base-table NVMe cache and the
//     decoded-chunk cache (engine/scan/decoded_cache.go:88) already stand on
//     exactly this premise.
//
//     size + createdAt — the guards for the residual case that premise does
//     NOT cover: a path recreated out of band. That is a real, observed
//     event, not a hypothetical — compactor.go:512-525 carries a
//     "recreated-object guard" for precisely it (re-ingest, or benchmark
//     datagen with deterministic chunk names). A recreated object gets a
//     fresh manifest entry with a fresh CreatedAt, so the key changes and
//     the stale footer is simply never consulted. catalog.FileEntry carries
//     no ETag and obtaining one would cost a Head per file per query — the
//     round trip storage/catalog/rgmeta.go exists to eliminate — so
//     (size, createdAt) is the strongest free discriminator available at
//     plan time.
//
//     An empty identity disables caching for that call — callers fail closed.
//
//  2. Shared immutability. A cached entry is handed to many concurrent
//     FileReaders, so its contents must be read-only after construction.
//     Verified 2026-08-17 by exhaustive grep over internal/: the ONLY writes
//     to FileMetaData / RowGroup / ColumnChunk / ColumnMetaData / Statistics
//     fields are in the Thrift decoder (thrift.go decodeFileMetaData and its
//     helpers, lines ~304-970), and the ONLY writes to SchemaNode fields are
//     in BuildSchemaTree / computeLevels (schema_tree.go lines ~40-118) —
//     both construction-time. Consumers only read: FileReader.ColumnPages
//     copies scalar fields out of *ColumnMetaData into a ColumnPageReader
//     (page_reader.go NewColumnPageReader / NewColumnPageReaderAt) and never
//     retains or mutates it; scan/{columnar_native,sel_decode,decode_ahead,
//     dict_prune,row_filter}.go read Leaves()/RowGroupMeta() only.
//     Belt-and-braces: the two slices handed out by value (leaves and
//     Schema.Columns) are capacity-clipped on insert, so a future consumer's
//     stray append reallocates instead of writing into shared backing store.
//
//  3. No singleflight. Concurrent decoders of the same identity both decode
//     and the first insert wins. Deliberate: a shared wait would let one slow
//     or hung object read block unrelated queries' planner goroutines, and
//     straggler amplification is a failure mode this engine has paid for
//     before. Duplicate decodes cost CPU, never correctness.
//
// Escape hatch: WADJET_FOOTER_CACHE=0 disables the cache process-wide (pure
// cache of immutable data — no plan or row-set change, so no optswitch kill
// switch; this exists to rule the cache in or out during an incident).
// WADJET_FOOTER_CACHE_BYTES overrides the byte cap (default 128 MiB).

// defaultFooterCacheBytes bounds resident decoded-footer heap. Measured by
// TestFooterEntrySizeEstimate: a 105-column part decodes to ~78 KB of
// metadata, a 16-column part to ~12 KB. 128 MiB therefore covers ~1700
// ClickBench-shaped parts or ~11k TPC-H-shaped parts — several thousand
// either way — and the cache only ever holds what has actually been scanned.
// Override with WADJET_FOOTER_CACHE_BYTES.
const defaultFooterCacheBytes = 128 << 20

var (
	footerCacheOn     atomic.Bool
	globalFooterCache = newFooterCache(footerCacheCapFromEnv())
)

func init() {
	footerCacheOn.Store(os.Getenv("WADJET_FOOTER_CACHE") != "0")
}

func footerCacheCapFromEnv() int64 {
	if v := os.Getenv("WADJET_FOOTER_CACHE_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return defaultFooterCacheBytes
}

// footerEntry is one decoded footer. Every field is immutable once the entry
// is inserted (see safety note 2 above) — readers share it without copying.
type footerEntry struct {
	meta       *FileMetaData
	schemaRoot *SchemaNode
	leaves     []*SchemaNode
	schema     Schema
	bytes      int64
}

type footerCache struct {
	capBytes int64
	maxEntry int64 // entries above this are never admitted

	mu      sync.Mutex
	entries map[string]*list.Element // identity -> element holding *footerCacheItem
	lru     *list.List               // front = most recently used
	bytes   int64

	hits, misses, inserts atomic.Int64
	evictions, rejected   atomic.Int64
}

type footerCacheItem struct {
	id    string
	entry *footerEntry
}

func newFooterCache(capBytes int64) *footerCache {
	if capBytes <= 0 {
		capBytes = defaultFooterCacheBytes
	}
	return &footerCache{
		capBytes: capBytes,
		maxEntry: capBytes / 8,
		entries:  make(map[string]*list.Element),
		lru:      list.New(),
	}
}

// get returns the cached entry for id and moves it to the LRU front, or nil.
func (c *footerCache) get(id string) *footerEntry {
	c.mu.Lock()
	el, ok := c.entries[id]
	if !ok {
		c.mu.Unlock()
		c.misses.Add(1)
		return nil
	}
	c.lru.MoveToFront(el)
	e := el.Value.(*footerCacheItem).entry
	c.mu.Unlock()
	c.hits.Add(1)
	return e
}

// put inserts e under id, evicting LRU-first until the byte cap holds. A
// racing insert of the same id keeps the incumbent — entries for one identity
// are interchangeable by construction.
func (c *footerCache) put(id string, e *footerEntry) {
	if e.bytes <= 0 || e.bytes > c.maxEntry {
		c.rejected.Add(1)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, present := c.entries[id]; present {
		return
	}
	c.entries[id] = c.lru.PushFront(&footerCacheItem{id: id, entry: e})
	c.bytes += e.bytes
	c.inserts.Add(1)
	for c.bytes > c.capBytes {
		tail := c.lru.Back()
		if tail == nil {
			break
		}
		it := tail.Value.(*footerCacheItem)
		c.lru.Remove(tail)
		delete(c.entries, it.id)
		c.bytes -= it.entry.bytes
		c.evictions.Add(1)
	}
}

// FooterCacheStats is a point-in-time snapshot of the process footer cache.
type FooterCacheStats struct {
	Hits, Misses, Inserts int64
	Evictions, Rejected   int64
	SizeBytes, CapBytes   int64
	Entries               int
}

// FooterCacheStatsSnapshot returns the process footer cache counters.
func FooterCacheStatsSnapshot() FooterCacheStats {
	c := globalFooterCache
	c.mu.Lock()
	entries, size := len(c.entries), c.bytes
	c.mu.Unlock()
	return FooterCacheStats{
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Inserts:   c.inserts.Load(),
		Evictions: c.evictions.Load(),
		Rejected:  c.rejected.Load(),
		SizeBytes: size,
		CapBytes:  c.capBytes,
		Entries:   entries,
	}
}

// ResetFooterCache drops every cached footer and zeroes the counters. For
// tests and benchmarks only — it exists so a test that deliberately reuses an
// object name for different content can start from a clean cache.
func ResetFooterCache() {
	c := globalFooterCache
	c.mu.Lock()
	c.entries = make(map[string]*list.Element)
	c.lru = list.New()
	c.bytes = 0
	c.mu.Unlock()
	c.hits.Store(0)
	c.misses.Store(0)
	c.inserts.Store(0)
	c.evictions.Store(0)
	c.rejected.Store(0)
}

// FooterCacheEnabled reports whether the process footer cache is active.
func FooterCacheEnabled() bool { return footerCacheOn.Load() }

// SetFooterCacheEnabled turns the process footer cache on or off and returns
// the previous setting. The supported production control is the
// WADJET_FOOTER_CACHE=0 environment variable read at startup; this exists so
// tests can run the same query on both sides of the switch in one binary.
func SetFooterCacheEnabled(on bool) bool {
	return footerCacheOn.Swap(on)
}

// decodeFooter performs the full footer decode: Thrift FileMetaData, schema
// tree, and Wadjet schema projection — everything a FileReader needs that
// does not depend on the backing bytes.
//
// validateHeader mirrors the uncached opener the caller stands in for:
// OpenFileReaderMetadata / OpenFileReaderAt check the leading PAR1 magic,
// OpenFileReaderFromBytes does not. Keeping that per-opener means a cached
// path never rejects a file its uncached twin would have opened. The decoded
// entry is identical either way — header validation contributes nothing to
// it — so an entry populated through either opener serves both.
func decodeFooter(r io.ReaderAt, size int64, validateHeader bool) (*footerEntry, error) {
	meta, err := ReadFileMetaData(r, size)
	if err != nil {
		return nil, err
	}
	if validateHeader {
		if err := ValidateHeader(r); err != nil {
			return nil, err
		}
	}
	root, leaves := BuildSchemaTree(meta.Schema)
	// readerSchema, not the bare schemaFromTree: every OTHER FileReader
	// constructor (OpenFileReader, OpenFileReaderMetadata, OpenFileReaderAt,
	// OpenFileReaderFromBytes) restores the DECLARED type from
	// DeclaredSchemaKey before handing out a Schema, and this cached path
	// had not — nine types (IPv4, IPv6, MAC, UUID, Bytes, Port, Protocol,
	// Duration, CIDR) came back as their bare parquet inference instead of
	// their declared identity for any reader built through the footer
	// cache. Harmless as long as nothing keyed a DECISION on the declared
	// type here; #523 is the first thing that does (a CIDR column's
	// row-group stats are trusted for pruning only when RowGroupStats can
	// confirm the column IS CIDR), so a cached reader silently withheld
	// pruning it should have engaged. Every consumer of this cache gets the
	// fix for free, not just the CIDR case that found it.
	schema := readerSchema(root, leaves, meta.KeyValueMetadata)
	// Clip the two slices handed out by value so a consumer's append can
	// never write into storage shared with other readers (safety note 2).
	leaves = leaves[:len(leaves):len(leaves)]
	schema.Columns = schema.Columns[:len(schema.Columns):len(schema.Columns)]
	e := &footerEntry{meta: meta, schemaRoot: root, leaves: leaves, schema: schema}
	e.bytes = estimateFooterBytes(e)
	return e, nil
}

// footerFor returns the decoded footer for identity, decoding through r on a
// miss. identity == "" (or a disabled cache) means decode without caching.
func footerFor(r io.ReaderAt, size int64, identity string, validateHeader bool) (*footerEntry, error) {
	cacheable := identity != "" && footerCacheOn.Load()
	if cacheable {
		if e := globalFooterCache.get(identity); e != nil {
			return e, nil
		}
	}
	e, err := decodeFooter(r, size, validateHeader)
	if err != nil {
		return nil, err
	}
	if cacheable {
		globalFooterCache.put(identity, e)
	}
	return e, nil
}

// LookupFooter returns a metadata-only FileReader for identity WITHOUT any
// I/O, or nil when the footer is not cached. It lets a planner skip opening
// the object at all when it only needs row-group metadata for pruning.
//
// The manifest, not this cache, is the authority on whether a file exists: a
// hit here means "this object's footer was decoded earlier in this process",
// and a since-deleted object will surface its error when the scan reads data.
func LookupFooter(identity string) *FileReader {
	if identity == "" || !footerCacheOn.Load() {
		return nil
	}
	e := globalFooterCache.get(identity)
	if e == nil {
		return nil
	}
	return e.newReader(nil, nil, 0)
}

// newReader wraps the shared decoded footer in a fresh FileReader. Only the
// backing (whole-file bytes, staged source, or neither) is per-reader; the
// metadata is shared and read-only.
func (e *footerEntry) newReader(data []byte, src io.ReaderAt, size int64) *FileReader {
	return &FileReader{
		data:       data,
		src:        src,
		size:       size,
		meta:       e.meta,
		schemaRoot: e.schemaRoot,
		leaves:     e.leaves,
		schema:     e.schema,
	}
}

// OpenFileReaderMetadataCached is OpenFileReaderMetadata through the process
// footer cache. On a hit r is never read.
func OpenFileReaderMetadataCached(r io.ReaderAt, size int64, identity string) (*FileReader, error) {
	e, err := footerFor(r, size, identity, true)
	if err != nil {
		return nil, err
	}
	return e.newReader(nil, nil, 0), nil
}

// OpenFileReaderAtCached is OpenFileReaderAt (staged pread mode) through the
// process footer cache. On a hit the footer bytes are never read from r; r is
// still used for every column-chunk read, so it must outlive the reader.
func OpenFileReaderAtCached(r io.ReaderAt, size int64, identity string) (*FileReader, error) {
	e, err := footerFor(r, size, identity, true)
	if err != nil {
		return nil, err
	}
	return e.newReader(nil, r, size), nil
}

// OpenFileReaderFromBytesCached is OpenFileReaderFromBytes through the
// process footer cache. Zero-copy over data, as its uncached twin.
func OpenFileReaderFromBytesCached(data []byte, identity string) (*FileReader, error) {
	e, err := footerFor(newBytesReaderAt(data), int64(len(data)), identity, false)
	if err != nil {
		return nil, err
	}
	return e.newReader(data, nil, int64(len(data))), nil
}

// estimateFooterBytes approximates an entry's retained heap. Struct bodies are
// exact (compile-time sizes); slice and string payloads are summed. It is a
// budgeting input, not an allocator ledger — within roughly a factor of two is
// all the byte cap needs.
func estimateFooterBytes(e *footerEntry) int64 {
	m := e.meta
	if m == nil {
		return 0
	}
	n := int64(unsafe.Sizeof(*m))
	n += int64(len(m.CreatedBy))
	n += int64(len(m.ColumnOrders)) * int64(unsafe.Sizeof(ColumnOrder{}))
	n += int64(len(m.Schema)) * int64(unsafe.Sizeof(SchemaElement{}))
	for i := range m.Schema {
		se := &m.Schema[i]
		n += int64(len(se.Name))
		if se.Type != nil {
			n += int64(unsafe.Sizeof(PhysicalType(0)))
		}
		if se.ConvertedType != nil {
			n += int64(unsafe.Sizeof(ConvertedType(0)))
		}
		if se.LogicalType != nil {
			n += int64(unsafe.Sizeof(LogicalType{}))
		}
	}
	for i := range m.KeyValueMetadata {
		kv := &m.KeyValueMetadata[i]
		n += int64(unsafe.Sizeof(*kv)) + int64(len(kv.Key)+len(kv.Value))
	}
	for i := range m.RowGroups {
		rg := &m.RowGroups[i]
		n += int64(unsafe.Sizeof(*rg))
		n += int64(len(rg.SortingColumns)) * int64(unsafe.Sizeof(SortingColumn{}))
		n += int64(len(rg.Columns)) * int64(unsafe.Sizeof(ColumnChunk{}))
		for j := range rg.Columns {
			cc := &rg.Columns[j]
			n += int64(len(cc.FilePath))
			cm := cc.MetaData
			if cm == nil {
				continue
			}
			n += int64(unsafe.Sizeof(*cm))
			n += int64(len(cm.Encodings)) * int64(unsafe.Sizeof(Encoding(0)))
			n += int64(len(cm.PathInSchema)) * int64(unsafe.Sizeof(""))
			for _, p := range cm.PathInSchema {
				n += int64(len(p))
			}
			n += int64(len(cm.EncodingStats)) * int64(unsafe.Sizeof(PageEncodingStats{}))
			if s := cm.Statistics; s != nil {
				n += int64(unsafe.Sizeof(*s))
				n += int64(len(s.Min) + len(s.Max) + len(s.MinValue) + len(s.MaxValue))
			}
		}
	}
	n += schemaNodeBytes(e.schemaRoot)
	n += int64(len(e.leaves)) * int64(unsafe.Sizeof((*SchemaNode)(nil)))
	for i := range e.schema.Columns {
		n += columnBytes(&e.schema.Columns[i])
	}
	return n
}

func schemaNodeBytes(n *SchemaNode) int64 {
	if n == nil {
		return 0
	}
	total := int64(unsafe.Sizeof(*n)) + int64(len(n.Name))
	total += int64(len(n.Path)) * int64(unsafe.Sizeof(""))
	for _, p := range n.Path {
		total += int64(len(p))
	}
	total += int64(len(n.Children)) * int64(unsafe.Sizeof((*SchemaNode)(nil)))
	for _, c := range n.Children {
		total += schemaNodeBytes(c)
	}
	return total
}

func columnBytes(c *Column) int64 {
	total := int64(unsafe.Sizeof(*c)) + int64(len(c.Name))
	if c.ElementType != nil {
		total += columnBytes(c.ElementType)
	}
	for i := range c.Fields {
		total += columnBytes(&c.Fields[i])
	}
	return total
}
