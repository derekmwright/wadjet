// Package catalog manages table schema and partition metadata.
// Metadata is stored in a MetaKV (NATS KV in production, MemKV in tests).
// Data files remain in object storage (S3/MinIO).
//
// All KV keys are prefixed with a cluster ID to support federation:
//
//	<clusterID>.meta              → CatalogMeta JSON
//	<clusterID>.table.<name>      → TableMeta JSON
//	<clusterID>.manifest.<name>   → PartitionManifest JSON
package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/storage/partition"
)

const (
	defaultClusterID = "local"
)

// manifestCacheEntry is a decoded manifest tagged with the KV revision it
// was decoded from. The revision — never wall-clock time — is what makes
// the entry usable; see Catalog.GetManifest.
type manifestCacheEntry struct {
	manifest *PartitionManifest
	rev      uint64
}

// Catalog manages table metadata via KV and data via object storage.
type Catalog struct {
	kv        MetaKV
	store     objstore.Store
	bucket    string
	clusterID string

	// manifestCache memoizes the DECODED manifest per table, keyed by the
	// KV revision it was decoded from. It is a decode memo, not a
	// staleness window: GetManifest re-checks the revision on every call.
	manifestMu    sync.Mutex
	manifestCache map[string]manifestCacheEntry

	// aggStatsCache memoizes AggregateColumnStats per table, keyed by the
	// manifest's KV revision. The aggregate is a pure function of the
	// manifest, but computing it reads one
	// sketches blob PER FILE — on an object-store-backed catalog that was
	// 600 serial S3 GETs (~40s) of PLANNING on every query touching
	// lineitem at SF10 (2026-07-05 finding: the dominant cost of the
	// cold-S3 standalone suite, dwarfing the scan itself). Entries
	// invalidate when the manifest's KV revision moves.
	aggStatsMu    sync.Mutex
	aggStatsCache map[string]aggStatsCacheEntry

	// rgMetaCache memoizes the decoded per-table RG-metadata blob (see
	// rgmeta.go), keyed by the same manifest revision as aggStatsCache.
	// Without it every scan re-fetches and re-decodes the blob; with it
	// the blob costs one store GET per (table, manifest revision) per
	// process.
	rgMetaMu    sync.Mutex
	rgMetaCache map[string]rgMetaCacheEntry

	// pendingDrops holds dropped tables' data files awaiting physical
	// deletion once DefaultDropTableGrace elapses, subject to
	// FlushDroppedTableFiles's live-manifest guard. See DropTable and
	// FlushDroppedTableFiles. Nothing in this package flushes it on a
	// timer, and production wiring of that flush is opt-in — see
	// compaction.BackgroundConfig.ReclaimDroppedTables and
	// EnableDropReclaim.
	//
	// dropReclaimWired gates recording entirely: with no flusher wired,
	// nothing will ever consume this list, so nothing is put on it.
	// pendingDropPaths is the running total of len(pd.paths) across
	// pendingDrops — what the cap is actually denominated in, since one
	// entry can hold a single file or a hundred thousand.
	dropMu           sync.Mutex
	pendingDrops     []pendingTableDrop
	pendingDropPaths int
	dropReclaimWired bool
}

// aggStatsCacheEntry is a memoized AggregateColumnStats result, valid for
// exactly the manifest revision it was computed from. The stats map is
// shared with callers — treat it as immutable.
type aggStatsCacheEntry struct {
	rev   uint64
	stats map[string]TableColumnStats
}

// CatalogMeta is the top-level catalog metadata.
type CatalogMeta struct {
	Version   int       `json:"version"`
	Tables    []string  `json:"tables"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TableMeta contains metadata for a single table.
type TableMeta struct {
	Name          string         `json:"name"`
	Schema        parquet.Schema `json:"schema"`
	PartitionKeys []string       `json:"partition_keys"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
	Version       int            `json:"version"`
}

// PartitionManifest tracks all partitions and their files for a table.
type PartitionManifest struct {
	Table         string           `json:"table"`
	Partitions    []PartitionEntry `json:"partitions"`
	DeleteMarkers []DeleteMarker   `json:"delete_markers,omitempty"` // merge-on-read deletes
	UpdatedAt     time.Time        `json:"updated_at"`
	// RGMetaKey points to the table's row-group-metadata blob in the
	// object store (see rgmeta.go), written by AnalyzeTable. Scans use
	// it to enumerate and prune row groups without reading any parquet
	// footers. Files added after the blob was written simply aren't in
	// it and fall back to footer reads — the key stays valid across
	// ingest. Empty until the table is first analyzed.
	RGMetaKey string `json:"rg_meta_key,omitempty"`
}

// DeleteMarker records rows to skip during scan (merge-on-read).
// Each marker identifies deleted rows within a specific data file.
type DeleteMarker struct {
	FilePath   string    `json:"file_path"`   // path of the data file containing deleted rows
	RowIndices []int64   `json:"row_indices"` // 0-based row indices to skip
	CreatedAt  time.Time `json:"created_at"`  // when this marker was created
}

// PartitionEntry describes a single partition.
type PartitionEntry struct {
	Path   string            `json:"path"`
	Values map[string]string `json:"values"`
	Files  []FileEntry       `json:"files"`
}

// FileColumnStats contains per-column min/max/null statistics for a single file.
// Extracted from Parquet row group metadata at write time.
type FileColumnStats struct {
	MinValue  any   `json:"min_value,omitempty"`
	MaxValue  any   `json:"max_value,omitempty"`
	NullCount int64 `json:"null_count"`

	// HLL is a HyperLogLog sketch over the column's distinct values in this
	// file. Persisted as 1 byte version + 16384 register bytes (~16 KB).
	// Empty if HLL was not collected at write time (e.g., legacy files,
	// or for columns where NDV isn't useful — strings of comments). Read
	// at plan time via AggregateColumnStats which merges sketches across
	// files to estimate table-level NDV.
	HLL []byte `json:"hll,omitempty"`

	// Sample is a reservoir-sampled snapshot of the column's values in
	// this file, encoded by EncodeSample (typically ~256 values, ~2 KB).
	// AggregateColumnStats merges samples across files (weighted by
	// file row count) and builds an equi-depth Histogram at query time.
	// Used by stats.estimatePredSelectivity for range/equality filters.
	Sample []byte `json:"sample,omitempty"`
}

// FileEntry describes a single Parquet file within a partition.
type FileEntry struct {
	Path        string                     `json:"path"`
	SizeBytes   int64                      `json:"size_bytes"`
	NumRows     int64                      `json:"num_rows"`
	CreatedAt   time.Time                  `json:"created_at"`
	ColumnStats map[string]FileColumnStats `json:"column_stats,omitempty"`
	// SketchesKey points to a bundled per-column HLL+Sample blob in the
	// object store (see sketches.go). Externalizing the sketches keeps
	// the manifest small enough to fit in the NATS KV per-message
	// payload cap (~1 MB) — without it, SF100 lineitem manifests
	// (63 files × 16 cols × ~18 KB per sketch) blew past the limit and
	// ANALYZE failed to persist the manifest. Empty string when no
	// sketches are externalized (legacy inline path still supported via
	// FileColumnStats.HLL / .Sample).
	SketchesKey string `json:"sketches_key,omitempty"`
	// EngineWritten marks an object WADJET ITSELF wrote: ingest's
	// chunk_<uuid>, compaction's compacted_<uuid>, delete-marker GC's
	// rewrite_<uuid>. It is the ownership marker DropTable's physical
	// reclaim keys off — only a marked entry is ever scheduled for
	// deletion (#494), so reclaim can only ever delete bytes this engine
	// created.
	//
	// Set in exactly two places, both of which mint the path themselves:
	// AddNewFiles (ingest, compaction) and SwapFileForGC's rewrite output.
	// AddFiles — the REGISTRATION path — deliberately leaves it alone,
	// because its callers point the catalog at objects somebody else
	// staged: cmd/tpch-bench (--data-prefix "tables/"), cmd/clickbench-
	// bench (--s3-prefix "tables/hits/"), internal/harness's s3_catalog,
	// and iceberg.CatalogIntegration all register pre-existing operator
	// data, and a bench bucket's reference dataset is not wadjet's to
	// delete on a DROP.
	//
	// Absent means NOT owned, which is the safe default in both
	// directions that matter: `omitempty` keeps it out of every manifest
	// that has no engine-written files, and every manifest written before
	// this field existed decodes with it false — so no pre-existing
	// object can be reclaimed by a newer binary. Unmarked entries leak on
	// DROP by design; see docs/adr/0020-drop-table-reclaim-is-opt-in.md.
	EngineWritten bool `json:"engine_written,omitempty"`
}

// TableColumnStats holds aggregated per-column statistics across all files.
// Used by the optimizer for selectivity estimation.
type TableColumnStats struct {
	MinValue  any
	MaxValue  any
	NullCount int64
	TotalRows int64
	// NDV is the merged HLL estimate of distinct values across all files,
	// or 0 when no file had an HLL sketch. When >0 it's preferred over the
	// min/max-range heuristic in the optimizer's NDV estimator.
	NDV int64
	// Histogram is the equi-depth histogram built from merging per-file
	// reservoir samples, scaled to TotalRows. Nil when no file had a
	// Sample. Used by stats.estimatePredSelectivity for range/equality
	// filters — replaces the hardcoded 0.33/0.1 fractions with
	// data-driven selectivity.
	Histogram *Histogram
}

// New creates a new Catalog backed by the given KV store and object store.
func New(kv MetaKV, store objstore.Store, bucket string) *Catalog {
	return &Catalog{kv: kv, store: store, bucket: bucket, clusterID: defaultClusterID}
}

// NewWithCluster creates a Catalog with a specific cluster identity.
func NewWithCluster(kv MetaKV, store objstore.Store, bucket string, clusterID string) *Catalog {
	if clusterID == "" {
		clusterID = defaultClusterID
	}
	return &Catalog{kv: kv, store: store, bucket: bucket, clusterID: clusterID}
}

// NewWithStore creates a Catalog using an in-memory KV (for tests/embedded use).
func NewWithStore(store objstore.Store, bucket string) *Catalog {
	return &Catalog{kv: NewMemKV(), store: store, bucket: bucket, clusterID: defaultClusterID}
}

// Init initializes the catalog. Creates the S3 bucket and seed metadata if needed.
func (c *Catalog) Init(ctx context.Context) error {
	if err := c.store.MakeBucket(ctx, c.bucket); err != nil {
		return fmt.Errorf("creating bucket: %w", err)
	}

	// Check if catalog meta already exists in KV
	_, _, err := c.kv.Get(c.key("meta"))
	if err == nil {
		return nil // already initialized
	}
	if err != ErrKeyNotFound {
		return fmt.Errorf("checking catalog: %w", err)
	}

	meta := CatalogMeta{
		Version:   1,
		Tables:    []string{},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	return c.putJSON(c.key("meta"), meta)
}

// ClusterID returns this catalog's cluster identifier.
func (c *Catalog) ClusterID() string {
	return c.clusterID
}

// ListTables returns the names of all tables in the local catalog.
func (c *Catalog) ListTables(_ context.Context) ([]string, error) {
	meta, err := c.getMeta()
	if err != nil {
		return nil, err
	}
	return meta.Tables, nil
}

// CreateTable creates a new table with the given schema and partition keys.
func (c *Catalog) CreateTable(_ context.Context, name string, schema parquet.Schema, partitionKeys []string) error {
	if err := checkDistinctColumnNames(schema); err != nil {
		return fmt.Errorf("creating table %q: %w", name, err)
	}
	for _, pk := range partitionKeys {
		if !schema.HasColumn(pk) {
			return fmt.Errorf("partition key %q not found in schema", pk)
		}
	}

	meta, err := c.getMeta()
	if err != nil {
		return err
	}

	for _, t := range meta.Tables {
		if t == name {
			return fmt.Errorf("table %q already exists", name)
		}
	}

	now := time.Now().UTC()
	tableMeta := TableMeta{
		Name:          name,
		Schema:        schema,
		PartitionKeys: partitionKeys,
		CreatedAt:     now,
		UpdatedAt:     now,
		Version:       1,
	}

	if err := c.putJSON(c.key("table."+name), tableMeta); err != nil {
		return err
	}

	manifest := PartitionManifest{
		Table:      name,
		Partitions: []PartitionEntry{},
		UpdatedAt:  now,
	}
	if err := c.putJSON(c.key("manifest."+name), manifest); err != nil {
		return err
	}

	meta.Tables = append(meta.Tables, name)
	meta.UpdatedAt = now
	return c.putJSON(c.key("meta"), meta)
}

// checkDistinctColumnNames refuses a schema whose column names collide under
// the parquet package's identity rule.
//
// CreateTable validated the partition keys and nothing else, and the embedded
// API reaches it directly, so a schema of [V INT32, v INT64] was accepted and
// stored. Nothing downstream can then answer "what type is v": the reader
// maps a file column to a catalog column by FoldName, so which of the two
// entries decided the answer came down to the order they were listed in. The
// refusal belongs here, where the schema is still the caller's to fix, rather
// than at the read of a table that should never have existed.
func checkDistinctColumnNames(schema parquet.Schema) error {
	seen := make(map[string]string, len(schema.Columns))
	for _, col := range schema.Columns {
		k := parquet.FoldName(col.Name)
		if prev, dup := seen[k]; dup {
			return fmt.Errorf(
				"schema columns %q and %q both answer to the name %q: the schema names "+
					"one column twice under this package's identity rule",
				prev, col.Name, k)
		}
		seen[k] = col.Name
	}
	return nil
}

// ErrTableNotFound marks a GetTable miss: the catalog was reachable and the
// table is definitely absent. Callers distinguish it (errors.Is) from a
// transport failure, where the table's existence is unknown — the planner
// rejects a query on the former (42P01) and stays conservative on the latter.
var ErrTableNotFound = errors.New("not found")

// isFoldedTableName reports whether a name is its OWN folded form — no ASCII
// upper-case letter. PostgreSQL folds only A-Z in a UTF8 database, so this is
// the same test batch.IsFoldedIdent applies to a column reference.
func isFoldedTableName(name string) bool {
	for i := 0; i < len(name); i++ {
		if name[i] >= 'A' && name[i] <= 'Z' {
			return false
		}
	}
	return true
}

// ResolveTableName is the READ-side spelling of a table name: the catalog's
// own, for a reference that named it in a different case.
//
// It is `batch.ResolveColumnIndex`'s rule one level up (ADR-0012): byte-exact
// first, and only on a miss a UNIQUE ASCII-case-insensitive match among the
// registered tables. Two matches resolve to NOTHING and the caller reports the
// miss, exactly as two columns do — picking one would be a silent wrong table.
//
// It exists because an unquoted reference FOLDS at the lexer (#731) while
// wadjet's table names come from parquet and ingest, where a user-chosen
// mixed-case name is ordinary. Without it `FROM MyTab` is 42P01 against a
// table this engine itself created, which is PostgreSQL's rule but breaks
// every catalog written before the fold.
//
// READ ONLY. Every write door — CreateTable, the DML paths, DropTable — keys
// byte-exact, because creating or writing `MyTab` when `mytab` exists must
// land on the name the caller wrote and never on its case-twin.
func (c *Catalog) ResolveTableName(name string) string {
	if name == "" {
		return name
	}
	meta, err := c.getMeta()
	if err != nil {
		return name
	}
	for _, t := range meta.Tables {
		if t == name {
			return name
		}
	}
	if !isFoldedTableName(name) {
		// A reference carrying an UPPER-CASE letter can only have been written
		// between double quotes — an unquoted one was folded at the lexer
		// (#731) — and a delimited name is byte-exact. So it gets no
		// concession, which is the same boundary ADR-0012 draws for a
		// delimited COLUMN reference. `FROM "MYTAB"` is 42P01 here exactly as
		// in PostgreSQL; `FROM MyTab` reaches `MyTab`, because by this point
		// it is the folded `mytab`.
		return name
	}
	found := ""
	for _, t := range meta.Tables {
		if !strings.EqualFold(t, name) {
			continue
		}
		if found != "" {
			return name // two matches: no unique answer, report the miss
		}
		found = t
	}
	if found != "" {
		return found
	}
	return name
}

// AmbiguousTableNames returns the registered tables a reference matches
// case-insensitively when there are TWO OR MORE of them and none is a
// byte-exact match — the case ResolveTableName declines to answer. Nil
// otherwise, including for an ordinary miss.
//
// The caller turns it into a refusal that names the candidates. Without it the
// refusal is the plain "does not exist", which is true (no unique relation has
// that name) but tells the user nothing about the two tables that do.
func (c *Catalog) AmbiguousTableNames(name string) []string {
	if name == "" {
		return nil
	}
	meta, err := c.getMeta()
	if err != nil {
		return nil
	}
	if !isFoldedTableName(name) {
		return nil // a delimited reference takes no concession, so none is declined
	}
	var matches []string
	for _, t := range meta.Tables {
		if t == name {
			return nil
		}
		if strings.EqualFold(t, name) {
			matches = append(matches, t)
		}
	}
	if len(matches) < 2 {
		return nil
	}
	sort.Strings(matches)
	return matches
}

// GetTable returns the metadata for a table.
func (c *Catalog) GetTable(_ context.Context, name string) (*TableMeta, error) {
	var meta TableMeta
	if err := c.getJSON(c.key("table."+name), &meta); err != nil {
		if err == ErrKeyNotFound {
			// 42P01 at the PRODUCER, not at each door. Every caller that
			// reports a miss to a client wants the same class, and #719 was
			// four DML doors that each wrapped this sentinel with %w and sent
			// a client a stateless error — while the SELECT door, which goes
			// through the planner's own check, sent 42P01. Attaching it here
			// means a door added later inherits the class instead of having
			// to remember it. The sentinel is unchanged, so errors.Is still
			// distinguishes "definitely absent" from "catalog unreachable".
			//
			// sqlerr.Wrap, not a bespoke type: internal/sqlerr imports only
			// errors and fmt, so there is no cycle to work around — the
			// earlier claim that there was one is false, and this package now
			// imports it directly.
			return nil, sqlerr.Wrap("42P01",
				fmt.Errorf("table %q %w", name, ErrTableNotFound))
		}
		return nil, err
	}
	return &meta, nil
}

// GetManifest returns the partition manifest for a table.
//
// Freshness is decided by the manifest key's KV REVISION, on every call.
// The cache only ever skips re-decoding a revision this process already
// decoded; it is a decode memo, never a staleness window.
//
// It used to be one, and that was #483. A 2-second wall-clock TTL,
// invalidated only by writes made through the same *Catalog value, is
// sound only while a process holds exactly one of them. Standalone holds
// three over the same KV — the coordinator's, the pgwire DB's, and a fresh
// one per worker pipeline task — and pgwire routes SELECT through the
// coordinator's catalog while INSERT/UPDATE/DELETE and DDL go through the
// DB's. Every write therefore invalidated a cache no reader was consulting,
// and reads answered from a manifest up to two seconds old. Statements
// issued back to back (a psql script, a SQLancer round, any client driving
// a session) all land inside that window: writes looked lost, and
// DROP TABLE + CREATE TABLE of the same name answered out of the previous
// incarnation's files — silently when the two schemas were
// encoding-compatible, and as a decode-time type refusal when they were
// not. A revision is the catalog's own notion of "which version is this",
// so validating against it cannot drift from what the catalog holds; a
// clock can.
//
// The returned manifest is SHARED with every other caller holding this
// revision. Treat it as immutable — mutators inside this package take
// loadManifest instead.
func (c *Catalog) GetManifest(_ context.Context, tableName string) (*PartitionManifest, error) {
	manifest, _, err := c.manifestWithRevision(tableName)
	return manifest, err
}

// GetManifestWithRevision is GetManifest with the KV revision the manifest
// came from, so a caller that must pin a CONSISTENT view of a table can
// hand both halves to AggregateColumnStatsFrom instead of letting it read
// the manifest a second time.
//
// Without it, a statement that read the manifest and then asked for column
// statistics got TWO reads of the same key, and — because a writer can
// commit between them — a stats map describing rows the pinned manifest
// does not contain. Measured with a NATS-equivalent KV: a pinned 2-file
// manifest of 200 rows alongside stats reporting TotalRows=300, and the
// tear then pinned for the whole statement (#540).
func (c *Catalog) GetManifestWithRevision(_ context.Context, tableName string) (*PartitionManifest, uint64, error) {
	return c.manifestWithRevision(tableName)
}

// manifestWithRevision returns the manifest together with the KV revision
// it came from. Derived caches (aggregate column stats, RG metadata) key
// on that revision so they expire exactly when the manifest does, rather
// than on a content proxy that a same-nanosecond rewrite could repeat.
func (c *Catalog) manifestWithRevision(tableName string) (*PartitionManifest, uint64, error) {
	key := c.key("manifest." + tableName)

	// Revision-only probe when the store offers one: a hit then costs a
	// map lookup instead of copying and decoding the whole manifest.
	if rr, ok := c.kv.(RevisionReader); ok {
		switch rev, err := rr.Revision(key); {
		case err == ErrKeyNotFound:
			return nil, 0, fmt.Errorf("manifest for table %q not found", tableName)
		case err == nil:
			if entry, hit := c.cachedManifest(tableName, rev); hit {
				return entry, rev, nil
			}
		}
	}

	data, rev, err := c.kv.Get(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return nil, 0, fmt.Errorf("manifest for table %q not found", tableName)
		}
		return nil, 0, err
	}
	if entry, hit := c.cachedManifest(tableName, rev); hit {
		return entry, rev, nil
	}

	var manifest PartitionManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, 0, fmt.Errorf("decoding manifest for table %q: %w", tableName, err)
	}

	c.manifestMu.Lock()
	if c.manifestCache == nil {
		c.manifestCache = make(map[string]manifestCacheEntry)
	}
	c.manifestCache[tableName] = manifestCacheEntry{manifest: &manifest, rev: rev}
	c.manifestMu.Unlock()

	return &manifest, rev, nil
}

// cachedManifest returns the memoized manifest for a table when it was
// decoded from exactly the revision asked for.
func (c *Catalog) cachedManifest(tableName string, rev uint64) (*PartitionManifest, bool) {
	c.manifestMu.Lock()
	defer c.manifestMu.Unlock()
	entry, ok := c.manifestCache[tableName]
	// rev 0 means the KV could not report a revision — never serve the memo on
	// it, or the cache becomes an unbounded staleness window (worse than the
	// old TTL). Both current KVs allocate revisions from 1.
	if !ok || rev == 0 || entry.rev != rev {
		return nil, false
	}
	return entry.manifest, true
}

// loadManifest reads and decodes a table's manifest without consulting or
// filling the cache. Callers that MUTATE what they read take this: the
// cached manifest is shared with every concurrent reader in the process,
// so mutating it in place is both a wrong answer and a data race.
func (c *Catalog) loadManifest(tableName string) (*PartitionManifest, error) {
	var manifest PartitionManifest
	if err := c.getJSON(c.key("manifest."+tableName), &manifest); err != nil {
		if err == ErrKeyNotFound {
			return nil, fmt.Errorf("manifest for table %q not found", tableName)
		}
		return nil, err
	}
	return &manifest, nil
}

// casBackoff sleeps with jittered exponential backoff after a CAS conflict.
// Base delay doubles each attempt (1ms, 2ms, 4ms, ...) with ±50% jitter.
func casBackoff(attempt int) {
	base := time.Millisecond << uint(attempt)
	if base > 128*time.Millisecond {
		base = 128 * time.Millisecond
	}
	jitter := time.Duration(rand.Int64N(int64(base)))
	time.Sleep(base/2 + jitter)
}

// mergeFileEntries folds incoming file entries into existing ones keyed by
// Path: an entry whose path is already present REPLACES it (last writer
// wins, so re-adding refreshes stale size/row metadata); new paths append
// in order. Makes AddFiles idempotent — re-running discovery over a
// populated catalog previously APPENDED duplicate entries, tripling
// lineitem to 189 files and silently multiplying every aggregate while
// row-count gates stayed green (#278).
//
// This replace-on-collision contract is right for AddFiles's callers —
// harness and Iceberg catalog discovery, which legitimately re-register a
// path an earlier run (or the source catalog) already knows about. A writer
// that mints a brand-new path per call wants the opposite contract: see
// mergeNewFileEntries and AddNewFiles.
func mergeFileEntries(existing, incoming []FileEntry) []FileEntry {
	out := append([]FileEntry(nil), existing...)
	byPath := make(map[string]int, len(out))
	for i, f := range out {
		byPath[f.Path] = i
	}
	for _, f := range incoming {
		if i, ok := byPath[f.Path]; ok {
			out[i] = f
			continue
		}
		byPath[f.Path] = len(out)
		out = append(out, f)
	}
	return out
}

// mergeNewFileEntries is mergeFileEntries's strict counterpart, for callers
// whose every incoming Path is freshly minted and must not already be in
// the partition (#494). Ingest's chunk_<uuid>, compaction's
// compacted_<uuid>, and delete-marker GC's rewrite_<uuid> all mint a full
// UUIDv7 per file specifically so a collision here is astronomically
// unlikely — which is exactly why one is refused with an error instead of
// silently replacing the existing entry the way mergeFileEntries does: a
// hit means a real bug (an ID-generation defect, or two writers racing on
// the same output path), not the ordinary idempotent re-registration
// mergeFileEntries exists for. Silently replacing here is how #494's
// row-loss happened: the pre-existing entry — and, in the wild, quite
// possibly its still-referenced bytes in the object store — disappears
// with no error anywhere.
func mergeNewFileEntries(existing, incoming []FileEntry) ([]FileEntry, error) {
	out := append([]FileEntry(nil), existing...)
	byPath := make(map[string]int, len(out))
	for i, f := range out {
		byPath[f.Path] = i
	}
	for _, f := range incoming {
		if _, ok := byPath[f.Path]; ok {
			return nil, fmt.Errorf("file path %q already exists in the manifest: refusing to silently replace it (#494)", f.Path)
		}
		byPath[f.Path] = len(out)
		out = append(out, f)
	}
	return out, nil
}

// addFiles is the shared CAS-retry loop behind AddFiles and AddNewFiles;
// merge decides how a Path collision within one partition's file list is
// resolved.
func (c *Catalog) addFiles(tableName string, partValues map[string]string, partPath string, files []FileEntry,
	merge func(existing, incoming []FileEntry) ([]FileEntry, error)) error {
	c.invalidateManifestCache(tableName)
	key := c.key("manifest." + tableName)

	// Retry loop for CAS conflicts (concurrent ingest flushes).
	const maxRetries = 10
	for attempt := 0; attempt < maxRetries; attempt++ {
		data, rev, err := c.kv.Get(key)
		if err != nil {
			return err
		}

		var manifest PartitionManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return fmt.Errorf("unmarshaling manifest: %w", err)
		}

		found := false
		for i, p := range manifest.Partitions {
			if p.Path == partPath {
				merged, mErr := merge(p.Files, files)
				if mErr != nil {
					return mErr
				}
				manifest.Partitions[i].Files = merged
				found = true
				break
			}
		}
		if !found {
			merged, mErr := merge(nil, files)
			if mErr != nil {
				return mErr
			}
			manifest.Partitions = append(manifest.Partitions, PartitionEntry{
				Path:   partPath,
				Values: partValues,
				Files:  merged,
			})
		}

		manifest.UpdatedAt = time.Now().UTC()
		updated, err := json.Marshal(manifest)
		if err != nil {
			return fmt.Errorf("marshaling manifest: %w", err)
		}

		_, err = c.kv.Update(key, updated, rev)
		if err == ErrRevisionMismatch {
			casBackoff(attempt)
			continue
		}
		return err
	}
	return fmt.Errorf("manifest update failed after %d CAS retries (table %q)", maxRetries, tableName)
}

// AddFiles adds file entries to the manifest for a given partition.
// Uses compare-and-swap to prevent concurrent flushes from losing updates.
// Idempotent per file path (mergeFileEntries): duplicate adds replace
// rather than append. For a writer minting brand-new paths, prefer
// AddNewFiles, which refuses a collision instead of masking it.
func (c *Catalog) AddFiles(_ context.Context, tableName string, partValues map[string]string, partPath string, files []FileEntry) error {
	return c.addFiles(tableName, partValues, partPath, files, func(existing, incoming []FileEntry) ([]FileEntry, error) {
		return mergeFileEntries(existing, incoming), nil
	})
}

// AddNewFiles adds newly-created file entries to the manifest for a given
// partition — the production write path for ingest, compaction, and
// delete-marker GC, none of which ever legitimately re-register a path
// (#494). Uses the same CAS retry as AddFiles, but a Path collision with an
// existing entry is refused with an error rather than silently replaced;
// see mergeNewFileEntries.
//
// This is also where the ownership marker is stamped: every entry landing
// through here names an object wadjet itself just wrote, which is exactly
// the condition FileEntry.EngineWritten records and DropTable's physical
// reclaim requires. The caller's slice is copied rather than mutated —
// stamping in place would edit a FileEntry the caller still holds (and, for
// a caller that reuses a backing array, entries it has already handed
// elsewhere).
func (c *Catalog) AddNewFiles(_ context.Context, tableName string, partValues map[string]string, partPath string, files []FileEntry) error {
	owned := make([]FileEntry, len(files))
	copy(owned, files)
	for i := range owned {
		owned[i].EngineWritten = true
	}
	return c.addFiles(tableName, partValues, partPath, owned, mergeNewFileEntries)
}

// DeletedRowsByFile indexes a manifest's delete markers by file path, as the
// set of row positions WITHIN that file which no longer exist.
//
// Every reader of a table owes this filter. The scanner applies it (its own
// deleteMarkers map is the same thing, built at Init) and so the SELECT path
// has always been right; the DML match scans did not, so an UPDATE matched
// rows in files its own earlier UPDATEs had already superseded, re-emitted
// them, and DOUBLED the row on every re-update — 1, 2, 4 (#674). A merge-on-
// read table has exactly one definition of which rows exist, and it is this.
func DeletedRowsByFile(markers []DeleteMarker) map[string]map[int64]bool {
	if len(markers) == 0 {
		return nil
	}
	out := make(map[string]map[int64]bool, len(markers))
	for _, dm := range markers {
		set := out[dm.FilePath]
		if set == nil {
			set = make(map[int64]bool, len(dm.RowIndices))
			out[dm.FilePath] = set
		}
		for _, idx := range dm.RowIndices {
			set[idx] = true
		}
	}
	return out
}

// AddDeleteMarkers adds delete markers to a table's manifest using CAS.
// Merges new markers with existing ones for the same file.
//
// It does NOT check that the files its markers name are still in the manifest,
// and it is not the entry point a DML statement uses. `CommitDML` is: it
// validates every marker against the manifest it is committing into, and lands
// the statement's new files in the same CAS (#691). This one stays as the
// low-level primitive for callers that mint markers against a manifest they
// are holding right now — the GC and compaction tests, and any embedder
// managing markers directly.
func (c *Catalog) AddDeleteMarkers(_ context.Context, tableName string, markers []DeleteMarker) error {
	c.invalidateManifestCache(tableName)
	key := c.key("manifest." + tableName)
	const maxRetries = 10

	for retry := 0; retry < maxRetries; retry++ {
		raw, rev, err := c.kv.Get(key)
		if err != nil {
			return fmt.Errorf("reading manifest for %q: %w", tableName, err)
		}

		var manifest PartitionManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			return fmt.Errorf("decoding manifest: %w", err)
		}

		// One merge rule, shared with CommitDML: the rule decides which rows a
		// reader skips, so two copies of it are two definitions of which rows
		// a table has.
		manifest.DeleteMarkers = mergeDeleteMarkers(manifest.DeleteMarkers, markers)
		manifest.UpdatedAt = time.Now().UTC()

		updated, err := json.Marshal(manifest)
		if err != nil {
			return fmt.Errorf("marshaling manifest: %w", err)
		}

		_, err = c.kv.Update(key, updated, rev)
		if err == ErrRevisionMismatch {
			casBackoff(retry)
			continue
		}
		return err
	}
	return fmt.Errorf("delete marker update failed after %d CAS retries (table %q)", maxRetries, tableName)
}

// RemoveFiles removes data files and their delete markers from the manifest.
// Used after compaction to clean up rewritten files.
func (c *Catalog) RemoveFiles(_ context.Context, tableName string, filePaths []string) error {
	c.invalidateManifestCache(tableName)
	key := c.key("manifest." + tableName)
	const maxRetries = 10
	removeSet := make(map[string]bool, len(filePaths))
	for _, p := range filePaths {
		removeSet[p] = true
	}

	for retry := 0; retry < maxRetries; retry++ {
		raw, rev, err := c.kv.Get(key)
		if err != nil {
			return fmt.Errorf("reading manifest for %q: %w", tableName, err)
		}

		var manifest PartitionManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			return fmt.Errorf("decoding manifest: %w", err)
		}

		// Remove files from partitions
		for i := range manifest.Partitions {
			filtered := manifest.Partitions[i].Files[:0]
			for _, f := range manifest.Partitions[i].Files {
				if !removeSet[f.Path] {
					filtered = append(filtered, f)
				}
			}
			manifest.Partitions[i].Files = filtered
		}

		// Remove delete markers for removed files
		filtered := manifest.DeleteMarkers[:0]
		for _, dm := range manifest.DeleteMarkers {
			if !removeSet[dm.FilePath] {
				filtered = append(filtered, dm)
			}
		}
		manifest.DeleteMarkers = filtered
		manifest.UpdatedAt = time.Now().UTC()

		updated, err := json.Marshal(manifest)
		if err != nil {
			return fmt.Errorf("marshaling manifest: %w", err)
		}

		_, err = c.kv.Update(key, updated, rev)
		if err == ErrRevisionMismatch {
			casBackoff(retry)
			continue
		}
		return err
	}
	return fmt.Errorf("file removal failed after %d CAS retries (table %q)", maxRetries, tableName)
}

// GCDeleteMarkers identifies delete markers older than minAge. Returns file
// paths that need a forced rewrite (marker aged, file still exists) and orphan
// paths (marker aged, file already gone — orphan markers are removed from the
// manifest). Rewrite markers are left in the manifest so ForceCompactFile can
// apply them during the file rewrite; SwapFileForGC removes only the applied
// markers atomically.
//
// Zero-value CreatedAt markers (pre-existing before the GC feature) are
// backfilled with the current time so they become eligible for GC in the next
// cycle rather than being immortal.
func (c *Catalog) GCDeleteMarkers(_ context.Context, tableName string, minAge time.Duration) (rewriteTargets map[string][]int64, orphanPaths []string, err error) {
	c.invalidateManifestCache(tableName)
	key := c.key("manifest." + tableName)
	const maxRetries = 10
	cutoff := time.Now().Add(-minAge)

	for retry := 0; retry < maxRetries; retry++ {
		raw, rev, err := c.kv.Get(key)
		if err != nil {
			return nil, nil, fmt.Errorf("reading manifest for %q: %w", tableName, err)
		}

		var manifest PartitionManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			return nil, nil, fmt.Errorf("decoding manifest: %w", err)
		}

		// Build set of files that exist in the manifest
		fileSet := make(map[string]bool)
		for _, p := range manifest.Partitions {
			for _, f := range p.Files {
				fileSet[f.Path] = true
			}
		}

		// Partition markers into: keep (fresh), rewrite (aged + file exists),
		// orphan (aged + file gone). Backfill zero-value CreatedAt so legacy
		// markers become GC-eligible next cycle.
		rewriteTargets = nil
		orphanPaths = nil
		orphanSeen := make(map[string]bool)
		backfilled := false

		now := time.Now().UTC()
		for i := range manifest.DeleteMarkers {
			dm := &manifest.DeleteMarkers[i]
			if dm.CreatedAt.IsZero() {
				dm.CreatedAt = now
				backfilled = true
				continue // freshly backfilled — skip this cycle
			}
			if dm.CreatedAt.After(cutoff) {
				continue // fresh marker
			}
			if fileSet[dm.FilePath] {
				if rewriteTargets == nil {
					rewriteTargets = make(map[string][]int64)
				}
				rewriteTargets[dm.FilePath] = append(rewriteTargets[dm.FilePath], dm.RowIndices...)
			} else {
				if !orphanSeen[dm.FilePath] {
					orphanPaths = append(orphanPaths, dm.FilePath)
					orphanSeen[dm.FilePath] = true
				}
			}
		}

		needsUpdate := backfilled || len(orphanPaths) > 0

		if len(rewriteTargets) == 0 && len(orphanPaths) == 0 && !backfilled {
			return nil, nil, nil
		}

		// Remove orphan markers; keep rewrite markers for ForceCompactFile.
		if len(orphanPaths) > 0 {
			var keepMarkers []DeleteMarker
			for _, dm := range manifest.DeleteMarkers {
				if !orphanSeen[dm.FilePath] {
					keepMarkers = append(keepMarkers, dm)
				}
			}
			manifest.DeleteMarkers = keepMarkers
		}

		if needsUpdate {
			manifest.UpdatedAt = now

			updated, err := json.Marshal(manifest)
			if err != nil {
				return nil, nil, fmt.Errorf("marshaling manifest: %w", err)
			}

			_, err = c.kv.Update(key, updated, rev)
			if err == ErrRevisionMismatch {
				casBackoff(retry)
				continue
			}
			if err != nil {
				return nil, nil, err
			}
		}
		return rewriteTargets, orphanPaths, nil
	}
	return nil, nil, fmt.Errorf("GC delete markers failed after %d CAS retries (table %q)", maxRetries, tableName)
}

// SwapFileForGC atomically replaces an old file with a rewritten file in the
// manifest. In a single CAS operation it: (1) removes the old file from the
// partition, (2) adds the new file entry, and (3) removes only the specific
// delete marker row indices that were applied during the rewrite. Any row
// indices added concurrently (by a DELETE after GC started) are preserved.
//
// NOTE: Surviving concurrent markers still reference the old file path after
// the swap. These become dangling markers since the old file no longer exists.
// This is by design — the next GC sweep detects them as orphans and cleans
// them up. The deleted rows they reference will be visible in query results
// for at most one GC cycle (~5 min default). Remapping marker paths and row
// indices inside the CAS loop was rejected due to complexity and increased
// CAS conflict surface (see security review, 2026-04-05).
//
// If newFile is nil, the old file is simply removed (all rows were deleted).
func (c *Catalog) SwapFileForGC(_ context.Context, tableName string, oldPath string, newFile *FileEntry, partValues map[string]string, partPath string, appliedIndices map[int64]bool) error {
	c.invalidateManifestCache(tableName)
	key := c.key("manifest." + tableName)
	const maxRetries = 10

	// The rewrite output is an object the GC sweep itself just wrote, so it
	// carries the same ownership marker AddNewFiles stamps — this is the
	// third of the three engine write paths (#494). Copied, not mutated in
	// place: newFile belongs to the caller.
	if newFile != nil {
		owned := *newFile
		owned.EngineWritten = true
		newFile = &owned
	}

	for retry := 0; retry < maxRetries; retry++ {
		raw, rev, err := c.kv.Get(key)
		if err != nil {
			return fmt.Errorf("reading manifest for %q: %w", tableName, err)
		}

		var manifest PartitionManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			return fmt.Errorf("decoding manifest: %w", err)
		}

		// Remove old file from partition and add new file
		for i := range manifest.Partitions {
			if manifest.Partitions[i].Path != partPath {
				continue
			}
			// newFile's path is a freshly minted rewrite_<uuid> (#494) — it
			// must not already name a file this partition still carries.
			// Checked before the in-place filter below mutates the slice.
			if newFile != nil {
				for _, f := range manifest.Partitions[i].Files {
					if f.Path != oldPath && f.Path == newFile.Path {
						return fmt.Errorf("file path %q already exists in partition %q: refusing to add a duplicate GC-rewrite output (#494)", newFile.Path, partPath)
					}
				}
			}
			filtered := manifest.Partitions[i].Files[:0]
			for _, f := range manifest.Partitions[i].Files {
				if f.Path != oldPath {
					filtered = append(filtered, f)
				}
			}
			if newFile != nil {
				filtered = append(filtered, *newFile)
			}
			manifest.Partitions[i].Files = filtered
			break
		}

		// Remove only the applied row indices from delete markers.
		// If concurrent DELETEs added new indices, those survive.
		var updatedMarkers []DeleteMarker
		for _, dm := range manifest.DeleteMarkers {
			if dm.FilePath != oldPath {
				updatedMarkers = append(updatedMarkers, dm)
				continue
			}
			// Keep only indices that were NOT applied
			var remaining []int64
			for _, idx := range dm.RowIndices {
				if !appliedIndices[idx] {
					remaining = append(remaining, idx)
				}
			}
			if len(remaining) > 0 {
				updatedMarkers = append(updatedMarkers, DeleteMarker{
					FilePath:   dm.FilePath,
					RowIndices: remaining,
					CreatedAt:  dm.CreatedAt,
				})
			}
		}
		manifest.DeleteMarkers = updatedMarkers
		manifest.UpdatedAt = time.Now().UTC()

		updated, err := json.Marshal(manifest)
		if err != nil {
			return fmt.Errorf("marshaling manifest: %w", err)
		}

		_, err = c.kv.Update(key, updated, rev)
		if err == ErrRevisionMismatch {
			casBackoff(retry)
			continue
		}
		return err
	}
	return fmt.Errorf("file swap for GC failed after %d CAS retries (table %q)", maxRetries, tableName)
}

// UDFDef mirrors expr.UDFDef for persistence without import cycles.
type UDFDef struct {
	Name   string   `json:"name"`
	Params []string `json:"params"`
	Body   string   `json:"body"`
	Owner  string   `json:"owner"`
	Locked bool     `json:"locked"`
}

// SaveUDFs persists user-defined function definitions to the catalog KV.
func (c *Catalog) SaveUDFs(defs []UDFDef) error {
	data, err := json.Marshal(defs)
	if err != nil {
		return fmt.Errorf("marshaling UDFs: %w", err)
	}
	_, err = c.kv.Put(c.key("udfs"), data)
	return err
}

// LoadUDFs reads persisted UDF definitions from the catalog KV.
// Returns nil (not error) if no UDFs have been saved.
func (c *Catalog) LoadUDFs() ([]UDFDef, error) {
	data, _, err := c.kv.Get(c.key("udfs"))
	if err == ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var defs []UDFDef
	if err := json.Unmarshal(data, &defs); err != nil {
		return nil, fmt.Errorf("unmarshaling UDFs: %w", err)
	}
	return defs, nil
}

// DropTable removes a table from the catalog.
//
// Metadata only: the table's name and manifest KV keys go away here, which
// is what makes it immediately invisible to every NEW query (GetTable,
// GetManifest, and ListTables all answer from this same metadata, and #483
// keys the manifest cache by KV revision so a stale in-process copy can't
// serve a resurrected name's old files either). The table's DATA FILES are
// deliberately NOT deleted here — see FlushDroppedTableFiles for why, when,
// and under what guard they go.
//
// Tombstone-then-grace-delete, not a prefix delete under tables/<name>/,
// and not "leave it forever" either (#494 asked for a decision between
// those). A live prefix delete is the wrong shape regardless of timing: a
// CREATE TABLE of the same name during the grace window gets an entirely
// new, unrelated set of files at that same prefix (chunk/compacted names
// are per-file random, not derived from the table name), and a prefix
// delete run after the fact cannot tell that incarnation's files from the
// dropped one's — it would eat the new table's data. Recording the exact
// paths this incarnation OWNED (engine-written only — see the snapshot
// below), once, right here, and checking each one against every CURRENT
// manifest before ever deleting it (FlushDroppedTableFiles) has no such
// blast radius. It doesn't reach
// RGMetaKey/SketchesKey blobs under stats/<name>/ — those are named by
// table+column, not by a birthday-collision-prone short ID, so they sit
// outside #494's collision hazard; leaking them is a separate, lower-
// severity storage-hygiene gap.
//
// Ordering matters twice. The metadata put that removes the name from
// meta.Tables goes FIRST — it is the write that constitutes the drop, and
// putting it first is what makes a failed DROP a clean no-op rather than a
// table that is listed but unreadable (see the comment at the put). And
// the pending-drop record is appended only AFTER that put succeeds: a
// failed DROP must leave the table exactly as recoverable as it was before
// the call — nothing scheduled for physical deletion — not half-gone with
// its files already timed for reclaim.
func (c *Catalog) DropTable(ctx context.Context, name string) error {
	meta, err := c.getMeta()
	if err != nil {
		return err
	}

	found := false
	tables := make([]string, 0, len(meta.Tables))
	for _, t := range meta.Tables {
		if t == name {
			found = true
			continue
		}
		tables = append(tables, t)
	}
	if !found {
		return fmt.Errorf("table %q not found", name)
	}

	// Snapshot the data files this incarnation OWNS before its manifest
	// disappears. CreateTable always writes an (initially empty) manifest
	// alongside the table entry, so this should never miss for a table
	// meta.Tables still lists; a miss just means nothing to schedule.
	//
	// Ownership, not mere reference, is the test (#494 review): only an
	// entry wadjet itself wrote — FileEntry.EngineWritten, stamped by
	// AddNewFiles and SwapFileForGC — is ever scheduled for physical
	// deletion. A manifest is full of paths the engine merely POINTS at:
	// cmd/tpch-bench registers its dataset under --data-prefix "tables/",
	// cmd/clickbench-bench under --s3-prefix "tables/hits/",
	// internal/harness's s3_catalog and Iceberg discovery do the same —
	// all through AddFiles, all naming objects an operator staged in a
	// bucket that is not ours to empty. Those entries take exactly the
	// "tables/<name>/..." shape the prefix check below permits, so without
	// this marker a single DROP plus one grace period would delete the
	// shared reference datasets out of the SF10/SF100/ClickBench buckets.
	// Unmarked entries leak instead; that is the documented trade.
	var dropPaths []string
	if manifest, mErr := c.GetManifest(ctx, name); mErr == nil {
		for _, part := range manifest.Partitions {
			for _, f := range part.Files {
				if !f.EngineWritten {
					continue
				}
				dropPaths = append(dropPaths, f.Path)
			}
		}
	}

	// The meta put goes FIRST, and it is the whole DROP. It is the write
	// that makes the table invisible — ListTables, GetTable and the
	// planner all resolve through meta.Tables — so once it lands the DROP
	// has happened, and everything after it is cleanup of metadata nothing
	// reaches any more.
	//
	// The reverse order (delete the per-table keys, then commit meta) put
	// the failure window in the worst possible place: a failed put left
	// the table LISTED in meta with its table./manifest. keys already
	// gone. Every read of it then failed, and — because
	// liveCatalogState treats a GetManifest error for a listed table as
	// "prove nothing, delete nothing" — one such failure bricked reclaim
	// catalog-wide, permanently, for every other table. Committing meta
	// first makes a failed DROP a no-op: nothing else has been touched
	// yet, and the caller sees the error with the table fully intact.
	meta.Tables = tables
	meta.UpdatedAt = time.Now().UTC()
	if err := c.putJSON(c.key("meta"), meta); err != nil {
		return err
	}

	// Cleanup, past the point of no return. Re-read meta first: the name
	// is free the instant the put above lands, so a concurrent CreateTable
	// can legitimately claim it and write its own table./manifest. keys
	// before we get here — and deleting THOSE would brick a live table
	// (listed, with no manifest) instead of the one we dropped. If the
	// name is back, its new incarnation has already overwritten both keys,
	// so there is nothing of ours left to clean anyway.
	if fresh, mErr := c.getMeta(); mErr == nil {
		for _, t := range fresh.Tables {
			if t == name {
				slog.Default().Warn("DROP TABLE: the name was re-created before this drop finished cleaning up; leaving the new incarnation's metadata alone",
					"table", name)
				c.invalidateManifestCache(name)
				if len(dropPaths) > 0 {
					c.recordPendingDrop(name, dropPaths)
				}
				return nil
			}
		}
	}
	c.invalidateManifestCache(name)
	_ = c.kv.Delete(c.key("table." + name))
	_ = c.kv.Delete(c.key("manifest." + name))

	if len(dropPaths) > 0 {
		c.recordPendingDrop(name, dropPaths)
	}
	return nil
}

// maxPendingDropPaths bounds pendingDrops so a process that drops many
// tables faster than they are reclaimed doesn't grow this list, and the
// process's memory, without bound.
//
// Denominated in PATHS, not entries. Entries are the wrong unit by three
// or four orders of magnitude: one dropped table can hold a single chunk
// or an SF100 lineitem's worth of files, so a cap of N entries bounds
// memory only if you assume every table is the same size. Paths are what
// the memory actually is.
const maxPendingDropPaths = 100_000

// EnableDropReclaim declares that something in this process will call
// FlushDroppedTableFiles, and is what allows DropTable to record anything
// at all.
//
// Reclaim is opt-in (compaction.BackgroundConfig.ReclaimDroppedTables,
// default off) and a *Catalog is not unique per process — an embedded
// wadjet.DB and a standalone pgwire DB each hold their own. On a catalog
// nobody sweeps, a pending-drop list is pure cost: nothing will ever
// consume it, so every DROP would grow it until the cap started evicting.
// Recording only where a flusher exists makes the default configuration
// (reclaim off) cost exactly nothing, and makes "which catalogs reclaim"
// a structural fact rather than a comment.
//
// Call it before the DROPs whose files should be reclaimed —
// compaction.NewBackgroundCompactor does, at construction, when
// ReclaimDroppedTables is set. Idempotent; there is no disable, since a
// flusher that stops running just leaves entries pending.
func (c *Catalog) EnableDropReclaim() {
	c.dropMu.Lock()
	defer c.dropMu.Unlock()
	c.dropReclaimWired = true
}

// PendingDropCount reports how many dropped-table entries are currently
// queued in pendingDrops, awaiting FlushDroppedTableFiles. Exported so a
// regression test outside this package (internal/iceberg's #494 repros)
// can pin layer 0 — ownership marking — directly: zero here means nothing
// was ever scheduled, which a test that only checks what a later flush
// deletes cannot distinguish from "scheduled, then caught by a later
// guard".
func (c *Catalog) PendingDropCount() int {
	c.dropMu.Lock()
	defer c.dropMu.Unlock()
	return len(c.pendingDrops)
}

// recordPendingDrop appends a dropped table's file snapshot to
// pendingDrops, evicting the OLDEST entries first while the list would
// otherwise exceed maxPendingDropPaths. Eviction leaks the evicted
// table's files — they are removed from pendingDrops without ever being
// scheduled for physical deletion — rather than deleting anything outside
// FlushDroppedTableFiles's guard: a leak is a storage-hygiene problem an
// operator can clean up later; an incorrect delete is unrecoverable data
// loss (#494).
func (c *Catalog) recordPendingDrop(table string, paths []string) {
	c.dropMu.Lock()
	defer c.dropMu.Unlock()
	if !c.dropReclaimWired {
		return // nothing will ever flush this list: don't build one
	}
	for c.pendingDropPaths+len(paths) > maxPendingDropPaths && len(c.pendingDrops) > 0 {
		evicted := c.pendingDrops[0]
		// Zero the evicted slot before re-slicing: the backing array
		// outlives the re-slice, and an un-zeroed element keeps its whole
		// []string alive — which is the memory this cap exists to bound.
		c.pendingDrops[0] = pendingTableDrop{}
		c.pendingDrops = c.pendingDrops[1:]
		c.pendingDropPaths -= len(evicted.paths)
		slog.Default().Warn("pending drop-reclaim list at capacity: evicting the oldest dropped table's file record without deleting its files",
			"evicted_table", evicted.table, "evicted_files", len(evicted.paths), "cap_paths", maxPendingDropPaths)
	}
	// A single snapshot larger than the whole cap is still recorded, after
	// the drain above has emptied the list: refusing it would leak a real
	// table's files to bound memory we are already holding anyway (that
	// manifest was just read into this process), and the overshoot is
	// exactly one table's path list.
	c.pendingDrops = append(c.pendingDrops, pendingTableDrop{
		table: table,
		paths: paths,
		at:    time.Now(),
	})
	c.pendingDropPaths += len(paths)
}

// pendingTableDrop is one dropped table's data files awaiting physical
// deletion once their grace period elapses and FlushDroppedTableFiles's
// live-manifest guard clears them. See DropTable and FlushDroppedTableFiles.
type pendingTableDrop struct {
	table string
	paths []string
	at    time.Time
}

// DefaultDropTableGrace bounds how long a dropped table's data files stay
// physically present after DropTable returns, mirroring
// compaction.DefaultDeleteGrace's reasoning exactly: a query dispatched
// against the table's last manifest resolved its file list at dispatch
// time and keeps reading those exact paths until it finishes, so deleting
// the bytes the instant the manifest disappears races every such query. No
// NEW query can be racing — the table is already gone from meta.Tables —
// so the grace only has to outlive work already in flight.
//
// Nothing ENFORCES that it does, and an operator enabling reclaim has to
// know it: wadjet's --query-timeout defaults to 0 (unlimited), so a long
// analytical query can outlive any grace. The rule is to keep the query
// timeout at or below the drop grace, or raise the grace above the
// longest query allowed. The failure mode if you don't is a query failing
// on a missing object, not a wrong answer — but it is still a failure the
// operator chose. See docs/adr/0020-drop-table-reclaim-is-opt-in.md.
const DefaultDropTableGrace = 30 * time.Minute

// liveCatalogState observes the catalog as it stands RIGHT NOW: the set of
// every file path referenced by ANY table's manifest, and the set of table
// names that exist. Both halves are FlushDroppedTableFiles's guard.
//
// The path set is the load-bearing one: a path recorded in pendingDrops can
// ALSO be live at flush time — the same table name re-created and the very
// same object paths re-registered into it (#278's documented idempotent
// re-registration workflow lets a harness/bench loader do exactly that, and
// iceberg.CatalogIntegration.RefreshTable does it on every metadata
// refresh: drop, recreate, re-register the same warehouse files) — and a
// path referenced by any CURRENT manifest must never be deleted just
// because some OTHER, already-gone incarnation once also owned it.
//
// The name set closes the window the path set alone cannot: CreateTable
// publishes a table's name and its (empty) manifest BEFORE any AddFiles
// call registers a single path into it, so there is an interval in which a
// re-created table is live and its manifest is still empty. Nothing is
// protected by path during that interval. A dropped name that has come back
// since this flush started is therefore treated as "the world changed under
// us" and its whole pending entry is left alone — see FlushDroppedTableFiles.
//
// A GetManifest error for a table ListTables just returned is treated as
// "this sweep cannot prove anything is safe" rather than "that table has
// no files": the caller declines to delete against a possibly-incomplete
// picture.
func (c *Catalog) liveCatalogState(ctx context.Context) (paths map[string]bool, names map[string]bool, err error) {
	tables, err := c.ListTables(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("listing tables: %w", err)
	}
	paths = make(map[string]bool)
	names = make(map[string]bool, len(tables))
	for _, name := range tables {
		names[name] = true
		manifest, mErr := c.GetManifest(ctx, name)
		if mErr != nil {
			return nil, nil, fmt.Errorf("reading manifest for %q: %w", name, mErr)
		}
		for _, part := range manifest.Partitions {
			for _, f := range part.Files {
				paths[f.Path] = true
			}
		}
	}
	return paths, names, nil
}

// FlushDroppedTableFiles physically deletes the data files of tables
// DropTable removed at least grace ago (zero or negative flushes
// everything pending, for tests). Three independent safety layers stand
// between a pending path and the Delete call below; the first alone bounds
// the blast radius to bytes wadjet wrote, and either of the next two alone
// blocks the #494 review's reproduced data loss:
//
//  0. Ownership (DropTable, upstream of this list at all): a path is only
//     ever in pendingDrops if its FileEntry was EngineWritten — stamped by
//     AddNewFiles and SwapFileForGC, never by the AddFiles registration
//     path. Nothing an operator staged and merely registered can reach
//     this function, whatever shape its path takes.
//  1. The live-manifest guard, RE-OBSERVED per pending entry immediately
//     before that entry's deletes (liveCatalogState, and only when
//     something is actually DUE): a path referenced by
//     ANY current table's manifest is never deleted, no matter how long
//     its OLD incarnation has been gone. This is the load-bearing layer —
//     it is what makes drop-then-re-register-the-same-files (#278's
//     workflow) and Iceberg's RefreshTable (drop+recreate over the same
//     warehouse files, every refresh) safe. Building the set ONCE up front
//     and deleting against it was the review's second reproduced data
//     loss: a re-registration landing after the set was built and before
//     the Delete fired was invisible to it. Re-observation narrows that
//     window from "the whole flush" to "one entry's delete batch"; it does
//     not close it (see the residual note below).
//  2. Defense in depth: a path is only ever a delete candidate if it
//     falls under its OWN table's partition.TablePrefix(name) —
//     "tables/<name>/..." — and only via this catalog's own configured
//     store and bucket. This is a CONVENTION, not an impossibility:
//     iceberg/reader.go's resolvePath strips the scheme AND the bucket
//     off an absolute data-file URI, so a warehouse at
//     s3://somebucket/tables/events/... resolves into exactly the
//     guarded shape. It is a cheap second opinion on paths that are
//     already owned, not the thing standing between an Iceberg warehouse
//     and a delete — layer 0 is (everything Iceberg registers goes
//     through AddFiles, so none of it is ever marked).
//
// On top of those, this mirrors compaction.Compactor's own
// deleteFromStore/FlushDeferredDeletes recreated-object guard: a path
// whose object was modified after the drop was recorded is skipped,
// since something has legitimately written there since.
//
// RESIDUAL, stated plainly: pendingDrops is in-process, and the
// re-observation is a read; nothing serializes it against a write. dropMu
// guards only pendingDrops itself, not the Head/Delete calls below, so
// this is NOT scoped to a DIFFERENT *Catalog instance — a second
// goroutine calling AddFiles on THIS SAME *Catalog while the delete loop
// is mid-entry is just as invisible, and was reproduced directly against
// one instance. The window is one pending entry's WHOLE delete batch
// (every Head+Delete pair over that entry's paths), not a single call.
// cmd/wadjet's standalone mode has no in-process AddFiles caller sharing
// a *Catalog with its BackgroundCompactor (its pgwire server opens a
// separate wadjet.DB), so this is unreachable through that binary today;
// an embedder calling db.Catalog().AddFiles beside its own
// BackgroundCompactor reaches it. Layer 0 — ownership — is the layer that
// does not depend on timing at all, which is why it, not this one, is
// what bounds the blast radius. See
// docs/adr/0020-drop-table-reclaim-is-opt-in.md.
//
// Not called from within this package on any timer, and — unlike
// compaction's own deferred-delete flush — not called unconditionally by
// the production background sweep either: see
// compaction.BackgroundConfig.ReclaimDroppedTables (opt-in, default off).
// Not every process that can DROP a table runs that sweep against the
// same *Catalog (an embedded wadjet.DB and a standalone pgwire DB each
// hold their own), so leaving this off by default means "not reclaimed
// yet" rather than "reclaimed here but not there" is the honest default
// everywhere; a leaked object is an ops cleanup problem, where an
// incorrectly deleted one is data loss. Like the compactor's own
// pendingDeletes, this list is process-local — a crash before the grace
// elapses leaves the files in place rather than losing track of them
// destructively, the same trade compaction already makes. Returns the
// number of files deleted.
func (c *Catalog) FlushDroppedTableFiles(ctx context.Context, grace time.Duration) int {
	c.dropMu.Lock()
	nothingPending := len(c.pendingDrops) == 0
	c.dropMu.Unlock()
	if nothingPending {
		return 0
	}

	// Every branch below that declines to delete something is an operator
	// signal, and a bare `continue` makes reclaim unobservable: "the
	// sweep ran, nothing happened" is indistinguishable from "the sweep
	// protected a live table's files from a re-registration race". Each
	// class is logged, and counted for the one summary line at the end.
	// Mirrors compaction.Compactor.FlushDeferredDeletes.
	log := slog.Default().With("component", "drop_reclaim")
	var (
		skipLive     int // protected by a live manifest — the interesting one
		skipReborn   int // the table name came back mid-flush
		skipPrefix   int // outside its own table's prefix
		skipModified int // object written since the drop was recorded
		failedDelete int
		requeued     int
	)

	// What is DUE decides whether this round reads the catalog at all.
	// During the grace window after a DROP — six sweeps at the 5m/30m
	// defaults — there is something pending but nothing due, and observing
	// the catalog then is a ListTables plus a GetManifest per table for a
	// round that cannot delete anything.
	cutoff := time.Now().Add(-grace)
	c.dropMu.Lock()
	var due, keep []pendingTableDrop
	keptPaths := 0
	for _, pd := range c.pendingDrops {
		if pd.at.Before(cutoff) {
			due = append(due, pd)
		} else {
			keep = append(keep, pd)
			keptPaths += len(pd.paths)
		}
	}
	c.pendingDrops = keep
	c.pendingDropPaths = keptPaths
	c.dropMu.Unlock()
	if len(due) == 0 {
		return 0
	}

	// First observation.
	livePaths, liveNames, err := c.liveCatalogState(ctx)
	if err != nil {
		// Can't prove any path is safe to delete this round: put the due
		// entries back and try again next sweep instead of deleting blind.
		log.Warn("reclaim round skipped: cannot read the live catalog state",
			"error", err, "entries_requeued", len(due))
		for _, pd := range due {
			c.requeuePendingDrop(pd)
		}
		return 0
	}

	// reborn collects table names that were absent from the FIRST
	// observation and have appeared in a later one — a re-creation that
	// landed while this very flush was running. That is the signal the
	// path set cannot carry on its own, because CreateTable publishes a
	// name and an empty manifest before AddFiles registers anything into
	// it. A name in here means "the world changed under us for this
	// table": leave its whole entry alone. A name that was live in the
	// first observation is NOT reborn — the recreated table's files are
	// protected precisely, by path, so an earlier incarnation's dead
	// files are still reclaimable.
	reborn := make(map[string]bool)

	deleted := 0
	for _, pd := range due {
		// Second (and third, and fourth...) observation: re-read the
		// catalog immediately before THIS entry's delete batch. Once per
		// entry, not once per path — the whole point is to be current at
		// the moment of deletion, and re-reading per path would multiply
		// the KV traffic by a table's file count for no additional
		// narrowing. GetManifest revalidates a cached manifest against
		// its KV revision (#483), so repeated observations inside one
		// flush are a revision probe each, not a full manifest download.
		rePaths, reNames, reErr := c.liveCatalogState(ctx)
		if reErr != nil {
			// Same rule as the first observation, applied to one entry:
			// prove nothing, delete nothing. Put it back so a transient
			// KV error doesn't leak a whole table's files.
			log.Warn("reclaim deferred: cannot re-read the live catalog state before deleting",
				"table", pd.table, "files", len(pd.paths), "error", reErr)
			c.requeuePendingDrop(pd)
			requeued++
			continue
		}
		// The protected set only ever GROWS within a flush: a path seen
		// live at any observation this round is off limits for the rest
		// of it, even if a later read no longer shows it.
		for p := range rePaths {
			livePaths[p] = true
		}
		for n := range reNames {
			if !liveNames[n] {
				reborn[n] = true
			}
		}
		if reborn[pd.table] {
			// re-created mid-flush: leak this entry rather than race it
			log.Warn("reclaim skipped: the dropped table's name was re-created while this sweep was running",
				"table", pd.table, "files", len(pd.paths), "dropped_at", pd.at)
			skipReborn++
			continue
		}

		tablePrefix := partition.TablePrefix(pd.table) + "/"
		for _, p := range pd.paths {
			if livePaths[p] {
				// Still referenced by a live manifest: never delete. This
				// is the guard earning its keep — some table re-registered
				// a path a dropped incarnation also owned — so it is a
				// signal, not routine.
				log.Warn("reclaim skipped: path is still referenced by a live table's manifest",
					"table", pd.table, "path", p)
				skipLive++
				continue
			}
			if !strings.HasPrefix(p, tablePrefix) {
				log.Warn("reclaim skipped: path falls outside its own table's prefix",
					"table", pd.table, "path", p, "prefix", tablePrefix)
				skipPrefix++
				continue
			}
			if info, hErr := c.store.Head(ctx, c.bucket, p); hErr == nil && info.LastModified.After(pd.at) {
				log.Warn("reclaim skipped: object was written after the drop was recorded",
					"table", pd.table, "path", p, "dropped_at", pd.at, "object_modified", info.LastModified)
				skipModified++
				continue
			}
			if dErr := c.store.Delete(ctx, c.bucket, p); dErr != nil {
				log.Warn("reclaim failed to delete a dropped table's file",
					"table", pd.table, "path", p, "error", dErr)
				failedDelete++
				continue
			}
			deleted++
		}
	}

	skipped := skipLive + skipReborn + skipPrefix + skipModified
	switch {
	case skipped > 0 || failedDelete > 0 || requeued > 0:
		// A round that protected something, or failed to delete
		// something, is worth a line even when nothing was reclaimed.
		log.Warn("dropped-table reclaim finished with skips",
			"deleted", deleted, "skipped_live", skipLive, "skipped_recreated_table", skipReborn,
			"skipped_prefix", skipPrefix, "skipped_modified", skipModified,
			"delete_errors", failedDelete, "entries_requeued", requeued)
	case deleted > 0:
		log.Info("reclaimed dropped tables' files", "deleted", deleted, "tables", len(due))
	}
	return deleted
}

// requeuePendingDrop puts an entry back on the pending list after a flush
// declined to act on it for a reason that may not hold next time — a KV
// read that failed, not a guard that fired. A guard firing is a decision
// (leak it); a failed read is an absence of information, and dropping the
// entry on the floor for that would turn one transient KV error into a
// whole table's files leaked forever.
//
// Inserted at the FRONT, not appended to the tail: a requeued entry was
// already due (recordPendingDrop's eviction and this flush's due/keep
// split both work in insertion order, treating index 0 as oldest), so
// appending it would put a genuinely older entry behind every entry
// recorded after this failed round — including ones recordPendingDrop
// adds while this round is still running. Eviction under
// maxPendingDropPaths always drops pendingDrops[0], so that ordering
// mistake would evict a real table's files ahead of a KV hiccup's stale
// requeue, which is backwards: the requeue's staleness is exactly why it
// should be first in line to retry, and eviction should still take the
// oldest genuine drop first, not whichever one happened to fail a read.
func (c *Catalog) requeuePendingDrop(pd pendingTableDrop) {
	c.dropMu.Lock()
	defer c.dropMu.Unlock()
	c.pendingDrops = append([]pendingTableDrop{pd}, c.pendingDrops...)
	c.pendingDropPaths += len(pd.paths)
}

// AggregateColumnStats computes table-level column statistics by merging
// per-file stats across all partitions. Returns nil for columns without stats.
func (c *Catalog) AggregateColumnStats(ctx context.Context, tableName string) (map[string]TableColumnStats, error) {
	manifest, rev, err := c.manifestWithRevision(tableName)
	if err != nil {
		return nil, err
	}
	return c.AggregateColumnStatsFrom(ctx, tableName, manifest, rev)
}

// AggregateColumnStatsFrom is AggregateColumnStats over a manifest the
// caller ALREADY HOLDS, with the revision it came from.
//
// The revision is not decorative: it keys the memo, and passing the pair is
// what makes the statistics and the manifest ONE consistent view. The
// internal fetch this replaces was the first statement of the body, so a
// caller that had just read the manifest read it again and could receive a
// different one — stats describing files the pinned manifest does not list
// (an AddFiles landed between the two reads) or omitting files it does
// (RemoveFiles, compaction). The direction is fixed, because
// annotateScanColumns reads the manifest first and the stats second, so the
// stats are always the newer half.
//
// Today's two consumers are cost-model only, so a torn view is a worse plan
// rather than a wrong answer. That is a property of the current code and
// not an invariant: the natural next optimizer feature — proving a
// predicate unsatisfiable from ScanColStats.MinValue/MaxValue — turns it
// into dropped rows on the day it lands (#540).
func (c *Catalog) AggregateColumnStatsFrom(_ context.Context, tableName string, manifest *PartitionManifest, rev uint64) (map[string]TableColumnStats, error) {
	if manifest == nil {
		return nil, fmt.Errorf("aggregate column stats for table %q: nil manifest", tableName)
	}

	// Memoized result for this manifest revision? The stats map is shared —
	// callers must not mutate it (both planner call sites only read).
	c.aggStatsMu.Lock()
	if e, ok := c.aggStatsCache[tableName]; ok && e.rev == rev {
		c.aggStatsMu.Unlock()
		return e.stats, nil
	}
	c.aggStatsMu.Unlock()

	// Prefetch all externalized sketch blobs concurrently. Loads are
	// best-effort (a failed blob degrades that file to inline/heuristic
	// stats, matching loadFileSketches' contract) and merged strictly in
	// file order below so results stay deterministic.
	prefetched := c.prefetchFileSketches(manifest)

	agg := make(map[string]TableColumnStats)
	// HLL sketches are merged outside the TableColumnStats struct because
	// merging is incremental (byte-wise max) and the merged sketch can be
	// estimated at the end. Track per-column merged HLL here.
	mergedHLLs := make(map[string]*HLL)
	// Per-column sample byte slices, fed into MergeSamples at the end.
	samples := make(map[string][][]byte)
	var totalRows int64
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			totalRows += f.NumRows
			// Externalized sketches: use the prefetched per-file bundle,
			// merge each column's HLL + collect each column's sample
			// bytes. SF100 catalogs use this path (manifests too big to
			// hold sketches inline).
			if f.SketchesKey != "" {
				if entries := prefetched[f.SketchesKey]; entries != nil {
					for _, e := range entries {
						if len(e.HLL) > 0 {
							if h := HLLFromBytes(e.HLL); h != nil {
								if existing := mergedHLLs[e.Column]; existing != nil {
									existing.Merge(h)
								} else {
									mergedHLLs[e.Column] = h
								}
							}
						}
						if len(e.Sample) > 0 {
							samples[e.Column] = append(samples[e.Column], e.Sample)
						}
					}
				}
			}
			for col, cs := range f.ColumnStats {
				cur := agg[col]
				cur.NullCount += cs.NullCount
				cur.TotalRows += f.NumRows
				if cs.MinValue != nil {
					if cur.MinValue == nil || compareStatValues(cs.MinValue, cur.MinValue) < 0 {
						cur.MinValue = cs.MinValue
					}
				}
				if cs.MaxValue != nil {
					if cur.MaxValue == nil || compareStatValues(cs.MaxValue, cur.MaxValue) > 0 {
						cur.MaxValue = cs.MaxValue
					}
				}
				// Inline legacy path: pre-externalization catalogs may
				// still carry HLL/Sample bytes inside FileColumnStats.
				// New ANALYZE clears these and writes externally, but the
				// fallback keeps existing data usable.
				if len(cs.HLL) > 0 {
					if h := HLLFromBytes(cs.HLL); h != nil {
						if existing := mergedHLLs[col]; existing != nil {
							existing.Merge(h)
						} else {
							mergedHLLs[col] = h
						}
					}
				}
				if len(cs.Sample) > 0 {
					samples[col] = append(samples[col], cs.Sample)
				}
				agg[col] = cur
			}
		}
	}

	// Fill TotalRows + NDV + Histogram for columns that didn't appear in all files
	for col, cs := range agg {
		if cs.TotalRows < totalRows {
			cs.TotalRows = totalRows
		}
		if h := mergedHLLs[col]; h != nil {
			cs.NDV = h.Estimate()
		}
		if sb := samples[col]; len(sb) > 0 {
			merged, _, typeCode := MergeSamples(sb)
			if len(merged) > 0 {
				cs.Histogram = HistogramFromMergedSample(merged, cs.TotalRows, typeCode, HistDefaultBuckets)
			}
		}
		agg[col] = cs
	}

	if len(agg) == 0 {
		agg = nil
	}
	c.aggStatsMu.Lock()
	if c.aggStatsCache == nil {
		c.aggStatsCache = make(map[string]aggStatsCacheEntry)
	}
	c.aggStatsCache[tableName] = aggStatsCacheEntry{rev: rev, stats: agg}
	c.aggStatsMu.Unlock()
	return agg, nil
}

// sketchPrefetchConcurrency bounds concurrent sketch-blob fetches during
// AggregateColumnStats. Blobs are small (tens to hundreds of KB), so this
// is a round-trip bound, not a bandwidth one.
const sketchPrefetchConcurrency = 16

// prefetchFileSketches loads every externalized sketches blob referenced
// by the manifest concurrently and returns them keyed by SketchesKey.
// Failed loads are simply absent (best-effort, matching loadFileSketches).
func (c *Catalog) prefetchFileSketches(manifest *PartitionManifest) map[string][]FileSketchesEntry {
	var keys []string
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			if f.SketchesKey != "" {
				keys = append(keys, f.SketchesKey)
			}
		}
	}
	if len(keys) == 0 {
		return nil
	}
	results := make([][]FileSketchesEntry, len(keys))
	var idx int64
	var wg sync.WaitGroup
	workers := sketchPrefetchConcurrency
	if workers > len(keys) {
		workers = len(keys)
	}
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for {
				i := int(atomic.AddInt64(&idx, 1) - 1)
				if i >= len(keys) {
					return
				}
				entries, _ := c.loadFileSketches(context.Background(), keys[i])
				results[i] = entries
			}
		}()
	}
	wg.Wait()
	out := make(map[string][]FileSketchesEntry, len(keys))
	for i, k := range keys {
		if results[i] != nil {
			out[k] = results[i]
		}
	}
	return out
}

// compareStatValues compares two statistic values for ordering, for
// AggregateColumnStats' cross-FILE min/max merge.
//
// A pair whose types neither arm below recognizes is a BUG, not an unusual
// value, and must not answer "equal": doing so silently keeps whichever
// bound happened to arrive first and discards every other file's, which is
// the identical defect class parquet.CompareNative's own doc comment
// describes for the row-group-level merge one layer down — and the one
// that let a CIDR column's bound fall through here unnoticed, because
// nothing named the type CidrInetBound actually arrives as (#579 review).
// Reaching the panic means a boxed stat-value type gained a new case
// somewhere upstream (RowGroupStats today, something else tomorrow)
// without a matching arm here; AggregateColumnStats runs at plan/ANALYZE
// time, never per row, so this is a planning-time failure the coordinator's
// query-boundary recover (coordinator.go's ExecuteSQL, #511) turns into an
// XX000 for that one statement — not a process-wide crash, and not a wrong
// answer nobody notices.
func compareStatValues(a, b any) int {
	// JSON unmarshals numbers as float64
	af, aOk := toStatFloat(a)
	bf, bOk := toStatFloat(b)
	if aOk && bOk {
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}
	as, aOk := a.(string)
	bs, bOk := b.(string)
	if aOk && bOk {
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	}
	// A confirmed CIDR bound (#523) — compared by Key, PostgreSQL's inet
	// order, never by Text: the two disagree, which is the whole reason the
	// box exists (parquet.CidrInetBound's own doc comment).
	if ab, ok := a.(parquet.CidrInetBound); ok {
		if bb, ok := b.(parquet.CidrInetBound); ok {
			switch {
			case ab.Key < bb.Key:
				return -1
			case ab.Key > bb.Key:
				return 1
			}
			return 0
		}
	}
	panic(fmt.Sprintf("compareStatValues: no ordering arm for stat-value pair (%T, %T)", a, b))
}

func toStatFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case int:
		return float64(val), true
	default:
		return 0, false
	}
}

// --- Federation ---

// RemoteClusterInfo describes a remote cluster's catalog.
type RemoteClusterInfo struct {
	ClusterID string
	Tables    []string
}

// ListClusters discovers all clusters that have registered in the shared KV.
// Returns cluster IDs and their table lists.
func (c *Catalog) ListClusters() ([]RemoteClusterInfo, error) {
	keys, err := c.kv.List("")
	if err != nil {
		return nil, fmt.Errorf("listing KV keys: %w", err)
	}

	// Find all keys matching "<clusterID>.meta"
	seen := make(map[string]bool)
	var clusters []RemoteClusterInfo
	for _, k := range keys {
		if !strings.HasSuffix(k, ".meta") {
			continue
		}
		cid := strings.TrimSuffix(k, ".meta")
		if cid == "" || seen[cid] {
			continue
		}
		seen[cid] = true

		var meta CatalogMeta
		if err := c.getJSON(k, &meta); err != nil {
			continue // skip clusters we can't read
		}
		clusters = append(clusters, RemoteClusterInfo{
			ClusterID: cid,
			Tables:    meta.Tables,
		})
	}
	return clusters, nil
}

// GetRemoteTable reads table metadata from a remote cluster's catalog.
func (c *Catalog) GetRemoteTable(clusterID, tableName string) (*TableMeta, error) {
	key := clusterID + ".table." + tableName
	var meta TableMeta
	if err := c.getJSON(key, &meta); err != nil {
		if err == ErrKeyNotFound {
			return nil, fmt.Errorf("table %q not found in cluster %q", tableName, clusterID)
		}
		return nil, err
	}
	return &meta, nil
}

// GetRemoteManifest reads the partition manifest from a remote cluster's catalog.
func (c *Catalog) GetRemoteManifest(clusterID, tableName string) (*PartitionManifest, error) {
	key := clusterID + ".manifest." + tableName
	var manifest PartitionManifest
	if err := c.getJSON(key, &manifest); err != nil {
		if err == ErrKeyNotFound {
			return nil, fmt.Errorf("manifest for table %q not found in cluster %q", tableName, clusterID)
		}
		return nil, err
	}
	return &manifest, nil
}

// --- Data access (S3) ---

// Store returns the underlying object store (for data file access).
func (c *Catalog) Store() objstore.Store {
	return c.store
}

// Bucket returns the bucket name.
func (c *Catalog) Bucket() string {
	return c.bucket
}

// ReadFile reads a file from the catalog's bucket. Convenience helper.
func (c *Catalog) ReadFile(ctx context.Context, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	return c.store.Get(ctx, c.bucket, key)
}

// KV returns the underlying MetaKV store.
func (c *Catalog) KV() MetaKV {
	return c.kv
}

// --- internal helpers ---

// invalidateManifestCache drops a table's memoized manifest and everything
// derived from it. Correctness no longer rests on it — every entry carries
// the KV revision it was decoded from, so a write another instance never
// hears about still cannot be served (#483). This is the local writer's
// courtesy: it frees the superseded decode instead of waiting for the next
// reader to notice the revision moved.
func (c *Catalog) invalidateManifestCache(tableName string) {
	c.manifestMu.Lock()
	delete(c.manifestCache, tableName)
	c.manifestMu.Unlock()
	// The aggregated column stats are derived from the manifest — every
	// manifest invalidation must drop them too, so a writer that forgets
	// to bump UpdatedAt still can't serve stale stats from this process.
	c.aggStatsMu.Lock()
	delete(c.aggStatsCache, tableName)
	c.aggStatsMu.Unlock()
	c.rgMetaMu.Lock()
	delete(c.rgMetaCache, tableName)
	c.rgMetaMu.Unlock()
}

// key returns a cluster-prefixed KV key.
func (c *Catalog) key(suffix string) string {
	return c.clusterID + "." + suffix
}

func (c *Catalog) getMeta() (*CatalogMeta, error) {
	var meta CatalogMeta
	if err := c.getJSON(c.key("meta"), &meta); err != nil {
		return nil, fmt.Errorf("reading catalog meta: %w", err)
	}
	return &meta, nil
}

func (c *Catalog) getJSON(key string, v any) error {
	data, _, err := c.kv.Get(key)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// IsKVEmpty returns true if no <clusterID>.meta key exists. Used by the
// coordinator to decide whether startup should restore from a snapshot.
func (c *Catalog) IsKVEmpty(_ context.Context) (bool, error) {
	_, _, err := c.kv.Get(c.key("meta"))
	if err == ErrKeyNotFound {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

func (c *Catalog) putJSON(key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshaling JSON: %w", err)
	}
	_, err = c.kv.Put(key, data)
	return err
}
