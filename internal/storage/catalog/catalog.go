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
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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

// GetTable returns the metadata for a table.
func (c *Catalog) GetTable(_ context.Context, name string) (*TableMeta, error) {
	var meta TableMeta
	if err := c.getJSON(c.key("table."+name), &meta); err != nil {
		if err == ErrKeyNotFound {
			return nil, fmt.Errorf("table %q %w", name, ErrTableNotFound)
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

// AddFiles adds file entries to the manifest for a given partition.
// Uses compare-and-swap to prevent concurrent flushes from losing updates.
// Idempotent per file path (mergeFileEntries): duplicate adds replace
// rather than append.
func (c *Catalog) AddFiles(_ context.Context, tableName string, partValues map[string]string, partPath string, files []FileEntry) error {
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
				manifest.Partitions[i].Files = mergeFileEntries(p.Files, files)
				found = true
				break
			}
		}
		if !found {
			manifest.Partitions = append(manifest.Partitions, PartitionEntry{
				Path:   partPath,
				Values: partValues,
				Files:  mergeFileEntries(nil, files),
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

// AddDeleteMarkers adds delete markers to a table's manifest using CAS.
// Merges new markers with existing ones for the same file.
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

		// Merge markers: combine row indices for same file path
		existing := make(map[string]map[int64]bool)
		for _, dm := range manifest.DeleteMarkers {
			if existing[dm.FilePath] == nil {
				existing[dm.FilePath] = make(map[int64]bool)
			}
			for _, idx := range dm.RowIndices {
				existing[dm.FilePath][idx] = true
			}
		}
		for _, dm := range markers {
			if existing[dm.FilePath] == nil {
				existing[dm.FilePath] = make(map[int64]bool)
			}
			for _, idx := range dm.RowIndices {
				existing[dm.FilePath][idx] = true
			}
		}

		// Rebuild merged markers, preserving earliest CreatedAt per file
		existingTimes := make(map[string]time.Time)
		for _, dm := range manifest.DeleteMarkers {
			if !dm.CreatedAt.IsZero() {
				if t, ok := existingTimes[dm.FilePath]; !ok || dm.CreatedAt.Before(t) {
					existingTimes[dm.FilePath] = dm.CreatedAt
				}
			}
		}
		for _, dm := range markers {
			if !dm.CreatedAt.IsZero() {
				if t, ok := existingTimes[dm.FilePath]; !ok || dm.CreatedAt.Before(t) {
					existingTimes[dm.FilePath] = dm.CreatedAt
				}
			}
		}

		now := time.Now().UTC()
		manifest.DeleteMarkers = nil
		for filePath, indices := range existing {
			rows := make([]int64, 0, len(indices))
			for idx := range indices {
				rows = append(rows, idx)
			}
			createdAt := now
			if t, ok := existingTimes[filePath]; ok {
				createdAt = t
			}
			manifest.DeleteMarkers = append(manifest.DeleteMarkers, DeleteMarker{
				FilePath:   filePath,
				RowIndices: rows,
				CreatedAt:  createdAt,
			})
		}
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
func (c *Catalog) DropTable(_ context.Context, name string) error {
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

	c.invalidateManifestCache(name)
	_ = c.kv.Delete(c.key("table." + name))
	_ = c.kv.Delete(c.key("manifest." + name))

	meta.Tables = tables
	meta.UpdatedAt = time.Now().UTC()
	return c.putJSON(c.key("meta"), meta)
}

// AggregateColumnStats computes table-level column statistics by merging
// per-file stats across all partitions. Returns nil for columns without stats.
func (c *Catalog) AggregateColumnStats(_ context.Context, tableName string) (map[string]TableColumnStats, error) {
	manifest, rev, err := c.manifestWithRevision(tableName)
	if err != nil {
		return nil, err
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

// compareStatValues compares two statistic values for ordering.
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
	return 0
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
