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
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

const (
	defaultClusterID = "local"
)

// Catalog manages table metadata via KV and data via object storage.
type Catalog struct {
	kv        MetaKV
	store     objstore.Store
	bucket    string
	clusterID string
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
	Table        string           `json:"table"`
	Partitions   []PartitionEntry `json:"partitions"`
	DeleteMarkers []DeleteMarker  `json:"delete_markers,omitempty"` // merge-on-read deletes
	UpdatedAt    time.Time        `json:"updated_at"`
}

// DeleteMarker records rows to skip during scan (merge-on-read).
// Each marker identifies deleted rows within a specific data file.
type DeleteMarker struct {
	FilePath   string  `json:"file_path"`   // path of the data file containing deleted rows
	RowIndices []int64 `json:"row_indices"` // 0-based row indices to skip
}

// PartitionEntry describes a single partition.
type PartitionEntry struct {
	Path   string            `json:"path"`
	Values map[string]string `json:"values"`
	Files  []FileEntry       `json:"files"`
}

// FileEntry describes a single Parquet file within a partition.
type FileEntry struct {
	Path      string    `json:"path"`
	SizeBytes int64     `json:"size_bytes"`
	NumRows   int64     `json:"num_rows"`
	CreatedAt time.Time `json:"created_at"`
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

// GetTable returns the metadata for a table.
func (c *Catalog) GetTable(_ context.Context, name string) (*TableMeta, error) {
	var meta TableMeta
	if err := c.getJSON(c.key("table."+name), &meta); err != nil {
		if err == ErrKeyNotFound {
			return nil, fmt.Errorf("table %q not found", name)
		}
		return nil, err
	}
	return &meta, nil
}

// GetManifest returns the partition manifest for a table.
func (c *Catalog) GetManifest(_ context.Context, tableName string) (*PartitionManifest, error) {
	var manifest PartitionManifest
	if err := c.getJSON(c.key("manifest."+tableName), &manifest); err != nil {
		if err == ErrKeyNotFound {
			return nil, fmt.Errorf("manifest for table %q not found", tableName)
		}
		return nil, err
	}
	return &manifest, nil
}

// AddFiles adds file entries to the manifest for a given partition.
// Uses compare-and-swap to prevent concurrent flushes from losing updates.
func (c *Catalog) AddFiles(_ context.Context, tableName string, partValues map[string]string, partPath string, files []FileEntry) error {
	key := c.key("manifest." + tableName)

	// Retry loop for CAS conflicts (concurrent ingest flushes).
	const maxRetries = 5
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
				manifest.Partitions[i].Files = append(manifest.Partitions[i].Files, files...)
				found = true
				break
			}
		}
		if !found {
			manifest.Partitions = append(manifest.Partitions, PartitionEntry{
				Path:   partPath,
				Values: partValues,
				Files:  files,
			})
		}

		manifest.UpdatedAt = time.Now().UTC()
		updated, err := json.Marshal(manifest)
		if err != nil {
			return fmt.Errorf("marshaling manifest: %w", err)
		}

		_, err = c.kv.Update(key, updated, rev)
		if err == ErrRevisionMismatch {
			continue // retry with fresh read
		}
		return err
	}
	return fmt.Errorf("manifest update failed after %d CAS retries (table %q)", maxRetries, tableName)
}

// AddDeleteMarkers adds delete markers to a table's manifest using CAS.
// Merges new markers with existing ones for the same file.
func (c *Catalog) AddDeleteMarkers(_ context.Context, tableName string, markers []DeleteMarker) error {
	key := c.key("manifest." + tableName)
	const maxRetries = 5

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

		// Rebuild merged markers
		manifest.DeleteMarkers = nil
		for filePath, indices := range existing {
			rows := make([]int64, 0, len(indices))
			for idx := range indices {
				rows = append(rows, idx)
			}
			manifest.DeleteMarkers = append(manifest.DeleteMarkers, DeleteMarker{
				FilePath:   filePath,
				RowIndices: rows,
			})
		}
		manifest.UpdatedAt = time.Now().UTC()

		updated, err := json.Marshal(manifest)
		if err != nil {
			return fmt.Errorf("marshaling manifest: %w", err)
		}

		_, err = c.kv.Update(key, updated, rev)
		if err == ErrRevisionMismatch {
			continue
		}
		return err
	}
	return fmt.Errorf("delete marker update failed after %d CAS retries (table %q)", maxRetries, tableName)
}

// RemoveFiles removes data files and their delete markers from the manifest.
// Used after compaction to clean up rewritten files.
func (c *Catalog) RemoveFiles(_ context.Context, tableName string, filePaths []string) error {
	key := c.key("manifest." + tableName)
	const maxRetries = 5
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
			continue
		}
		return err
	}
	return fmt.Errorf("file removal failed after %d CAS retries (table %q)", maxRetries, tableName)
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

	_ = c.kv.Delete(c.key("table." + name))
	_ = c.kv.Delete(c.key("manifest." + name))

	meta.Tables = tables
	meta.UpdatedAt = time.Now().UTC()
	return c.putJSON(c.key("meta"), meta)
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

func (c *Catalog) putJSON(key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshaling JSON: %w", err)
	}
	_, err = c.kv.Put(key, data)
	return err
}
