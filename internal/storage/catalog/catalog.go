// Package catalog manages table schema and partition metadata in object storage.
package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

const (
	catalogPath  = "_catalog/catalog.json"
	schemasDir   = "_catalog/tables/"
	schemaFile   = "schema.json"
	manifestFile = "partitions/manifest.json"
)

// Catalog manages table metadata in object storage.
type Catalog struct {
	store  objstore.Store
	bucket string
}

// CatalogMeta is the top-level catalog metadata.
type CatalogMeta struct {
	Version   int       `json:"version"`
	Tables    []string  `json:"tables"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	etag      string
}

// TableMeta contains metadata for a single table.
type TableMeta struct {
	Name          string          `json:"name"`
	Schema        parquet.Schema  `json:"schema"`
	PartitionKeys []string        `json:"partition_keys"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	Version       int             `json:"version"`
	etag          string
}

// PartitionManifest tracks all partitions and their files for a table.
type PartitionManifest struct {
	Table      string              `json:"table"`
	Partitions []PartitionEntry    `json:"partitions"`
	UpdatedAt  time.Time           `json:"updated_at"`
	etag       string
}

// PartitionEntry describes a single partition.
type PartitionEntry struct {
	Path     string            `json:"path"`
	Values   map[string]string `json:"values"`
	Files    []FileEntry       `json:"files"`
}

// FileEntry describes a single Parquet file within a partition.
type FileEntry struct {
	Path      string    `json:"path"`
	SizeBytes int64     `json:"size_bytes"`
	NumRows   int64     `json:"num_rows"`
	CreatedAt time.Time `json:"created_at"`
}

// New creates a new Catalog backed by the given object store and bucket.
func New(store objstore.Store, bucket string) *Catalog {
	return &Catalog{store: store, bucket: bucket}
}

// Init initializes the catalog in storage if it doesn't exist.
func (c *Catalog) Init(ctx context.Context) error {
	if err := c.store.MakeBucket(ctx, c.bucket); err != nil {
		return fmt.Errorf("creating bucket: %w", err)
	}

	_, err := c.store.Head(ctx, c.bucket, catalogPath)
	if err == nil {
		return nil // catalog already exists
	}
	if err != objstore.ErrNotFound {
		return fmt.Errorf("checking catalog: %w", err)
	}

	meta := CatalogMeta{
		Version:   1,
		Tables:    []string{},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	return c.writeCatalogMeta(ctx, meta, "")
}

// ListTables returns the names of all tables in the catalog.
func (c *Catalog) ListTables(ctx context.Context) ([]string, error) {
	meta, err := c.readCatalogMeta(ctx)
	if err != nil {
		return nil, err
	}
	return meta.Tables, nil
}

// CreateTable creates a new table with the given schema and partition keys.
func (c *Catalog) CreateTable(ctx context.Context, name string, schema parquet.Schema, partitionKeys []string) error {
	// Validate partition keys exist in schema
	for _, pk := range partitionKeys {
		if !schema.HasColumn(pk) {
			return fmt.Errorf("partition key %q not found in schema", pk)
		}
	}

	meta, err := c.readCatalogMeta(ctx)
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

	if err := c.writeTableMeta(ctx, tableMeta, ""); err != nil {
		return err
	}

	manifest := PartitionManifest{
		Table:      name,
		Partitions: []PartitionEntry{},
		UpdatedAt:  now,
	}
	if err := c.writeManifest(ctx, name, manifest, ""); err != nil {
		return err
	}

	meta.Tables = append(meta.Tables, name)
	meta.UpdatedAt = now
	return c.writeCatalogMeta(ctx, *meta, meta.etag)
}

// GetTable returns the metadata for a table.
func (c *Catalog) GetTable(ctx context.Context, name string) (*TableMeta, error) {
	return c.readTableMeta(ctx, name)
}

// GetManifest returns the partition manifest for a table.
func (c *Catalog) GetManifest(ctx context.Context, tableName string) (*PartitionManifest, error) {
	return c.readManifest(ctx, tableName)
}

// AddFiles adds file entries to the manifest for a given partition.
func (c *Catalog) AddFiles(ctx context.Context, tableName string, partValues map[string]string, partPath string, files []FileEntry) error {
	manifest, err := c.readManifest(ctx, tableName)
	if err != nil {
		return err
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
	return c.writeManifest(ctx, tableName, *manifest, manifest.etag)
}

// DropTable removes a table from the catalog.
func (c *Catalog) DropTable(ctx context.Context, name string) error {
	meta, err := c.readCatalogMeta(ctx)
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

	// Remove table metadata files
	tablePath := schemasDir + name + "/"
	objects, err := c.store.List(ctx, c.bucket, objstore.ListOptions{Prefix: tablePath})
	if err != nil {
		return fmt.Errorf("listing table files: %w", err)
	}
	for _, obj := range objects {
		_ = c.store.Delete(ctx, c.bucket, obj.Key)
	}

	meta.Tables = tables
	meta.UpdatedAt = time.Now().UTC()
	return c.writeCatalogMeta(ctx, *meta, meta.etag)
}

func (c *Catalog) readCatalogMeta(ctx context.Context) (*CatalogMeta, error) {
	rc, info, err := c.store.Get(ctx, c.bucket, catalogPath)
	if err != nil {
		return nil, fmt.Errorf("reading catalog: %w", err)
	}
	defer rc.Close()

	var meta CatalogMeta
	if err := json.NewDecoder(rc).Decode(&meta); err != nil {
		return nil, fmt.Errorf("decoding catalog: %w", err)
	}
	meta.etag = info.ETag
	return &meta, nil
}

func (c *Catalog) writeCatalogMeta(ctx context.Context, meta CatalogMeta, expectedETag string) error {
	return c.writeJSON(ctx, catalogPath, meta, expectedETag)
}

func (c *Catalog) readTableMeta(ctx context.Context, name string) (*TableMeta, error) {
	path := schemasDir + name + "/" + schemaFile
	rc, info, err := c.store.Get(ctx, c.bucket, path)
	if err != nil {
		if err == objstore.ErrNotFound {
			return nil, fmt.Errorf("table %q not found", name)
		}
		return nil, fmt.Errorf("reading table meta: %w", err)
	}
	defer rc.Close()

	var meta TableMeta
	if err := json.NewDecoder(rc).Decode(&meta); err != nil {
		return nil, fmt.Errorf("decoding table meta: %w", err)
	}
	meta.etag = info.ETag
	return &meta, nil
}

func (c *Catalog) writeTableMeta(ctx context.Context, meta TableMeta, expectedETag string) error {
	path := schemasDir + meta.Name + "/" + schemaFile
	return c.writeJSON(ctx, path, meta, expectedETag)
}

func (c *Catalog) readManifest(ctx context.Context, tableName string) (*PartitionManifest, error) {
	path := schemasDir + tableName + "/" + manifestFile
	rc, info, err := c.store.Get(ctx, c.bucket, path)
	if err != nil {
		if err == objstore.ErrNotFound {
			return nil, fmt.Errorf("manifest for table %q not found", tableName)
		}
		return nil, fmt.Errorf("reading manifest: %w", err)
	}
	defer rc.Close()

	var manifest PartitionManifest
	if err := json.NewDecoder(rc).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decoding manifest: %w", err)
	}
	manifest.etag = info.ETag
	return &manifest, nil
}

func (c *Catalog) writeManifest(ctx context.Context, tableName string, manifest PartitionManifest, expectedETag string) error {
	path := schemasDir + tableName + "/" + manifestFile
	return c.writeJSON(ctx, path, manifest, expectedETag)
}

func (c *Catalog) writeJSON(ctx context.Context, path string, v any, expectedETag string) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling JSON: %w", err)
	}
	r := bytes.NewReader(data)

	if expectedETag != "" {
		_, err = c.store.PutIfMatch(ctx, c.bucket, path, r, int64(len(data)), "application/json", expectedETag)
	} else {
		_, err = c.store.Put(ctx, c.bucket, path, r, int64(len(data)), "application/json")
	}
	if err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}

// Store returns the underlying object store.
func (c *Catalog) Store() objstore.Store {
	return c.store
}

// Bucket returns the bucket name.
func (c *Catalog) Bucket() string {
	return c.bucket
}

// ReadFile reads a file from the catalog's bucket. Convenience helper for other packages.
func (c *Catalog) ReadFile(ctx context.Context, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	return c.store.Get(ctx, c.bucket, key)
}
