package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TableReader reads Iceberg table metadata from an object store and
// resolves the current snapshot to a list of Parquet data files.
type TableReader struct {
	store  objstore.Store
	bucket string
}

// NewTableReader creates a reader for Iceberg tables stored in the given object store.
func NewTableReader(store objstore.Store, bucket string) *TableReader {
	return &TableReader{store: store, bucket: bucket}
}

// TableInfo contains resolved information about an Iceberg table.
type TableInfo struct {
	Metadata      *TableMetadata
	Schema        parquet.Schema
	DataFiles     []DataFileEntry
	PartitionKeys []string
	Location      string
}

// readBytes fetches an object from the store and returns its contents as bytes.
func (r *TableReader) readBytes(ctx context.Context, key string) ([]byte, error) {
	rc, _, err := r.store.Get(ctx, r.bucket, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// ReadTable reads and resolves an Iceberg table from its metadata file path.
// The metadataPath should point to the table metadata JSON file
// (e.g., "warehouse/db/table/metadata/v1.metadata.json").
func (r *TableReader) ReadTable(ctx context.Context, metadataPath string) (*TableInfo, error) {
	// Read table metadata
	metaData, err := r.readBytes(ctx, metadataPath)
	if err != nil {
		return nil, fmt.Errorf("reading Iceberg metadata %q: %w", metadataPath, err)
	}

	meta, err := ParseTableMetadata(metaData)
	if err != nil {
		return nil, err
	}

	snap := meta.CurrentSnapshot()
	if snap == nil {
		// Table exists but has no data
		schema := meta.ActiveSchema()
		return &TableInfo{
			Metadata: meta,
			Schema:   schema.ToParquetSchema(),
			Location: meta.Location,
		}, nil
	}

	// Resolve manifest list → manifests → data files
	dataFiles, err := r.resolveDataFiles(ctx, meta, snap)
	if err != nil {
		return nil, err
	}

	// Extract partition keys from partition spec
	schema := meta.ActiveSchema()
	partSpec := meta.ActivePartitionSpec()
	var partitionKeys []string
	for _, pf := range partSpec {
		// Find source column name by field ID
		for _, f := range schema.Fields {
			if f.ID == pf.SourceID && pf.Transform == "identity" {
				partitionKeys = append(partitionKeys, f.Name)
			}
		}
	}

	return &TableInfo{
		Metadata:      meta,
		Schema:        schema.ToParquetSchema(),
		DataFiles:     dataFiles,
		PartitionKeys: partitionKeys,
		Location:      meta.Location,
	}, nil
}

func (r *TableReader) resolveDataFiles(ctx context.Context, meta *TableMetadata, snap *Snapshot) ([]DataFileEntry, error) {
	if snap.ManifestList == "" {
		return nil, nil
	}

	// Read manifest list
	manifestListPath := r.resolvePath(meta.Location, snap.ManifestList)
	mlData, err := r.readBytes(ctx, manifestListPath)
	if err != nil {
		return nil, fmt.Errorf("reading manifest list: %w", err)
	}

	ml, err := ParseManifestList(mlData)
	if err != nil {
		return nil, err
	}

	// Read each manifest and collect data files
	var allFiles []DataFileEntry
	for _, entry := range ml.Entries {
		if entry.Content != 0 {
			continue // skip delete manifests
		}

		manifestPath := r.resolvePath(meta.Location, entry.ManifestPath)
		mData, err := r.readBytes(ctx, manifestPath)
		if err != nil {
			return nil, fmt.Errorf("reading manifest %q: %w", manifestPath, err)
		}

		mf, err := ParseManifest(mData)
		if err != nil {
			return nil, err
		}

		allFiles = append(allFiles, mf.DataFiles()...)
	}

	return allFiles, nil
}

// resolvePath resolves a file path relative to the table location.
// Handles both absolute paths and relative paths.
func (r *TableReader) resolvePath(location, filePath string) string {
	if strings.HasPrefix(filePath, "s3://") || strings.HasPrefix(filePath, "gs://") ||
		strings.HasPrefix(filePath, "s3a://") || strings.HasPrefix(filePath, "/") {
		// Strip scheme prefix to get object store key
		for _, prefix := range []string{"s3://", "gs://", "s3a://"} {
			if strings.HasPrefix(filePath, prefix) {
				filePath = strings.TrimPrefix(filePath, prefix)
				// Remove bucket name from path if present
				if idx := strings.Index(filePath, "/"); idx >= 0 {
					filePath = filePath[idx+1:]
				}
				return filePath
			}
		}
		return strings.TrimPrefix(filePath, "/")
	}
	return path.Join(location, filePath)
}

// DiscoverTable tries to find and read an Iceberg table by looking for
// the metadata directory in common locations.
func (r *TableReader) DiscoverTable(ctx context.Context, tablePath string) (*TableInfo, error) {
	// Try version-hint.text first
	hintPath := path.Join(tablePath, "metadata", "version-hint.text")
	hintData, err := r.readBytes(ctx, hintPath)
	if err == nil {
		version := strings.TrimSpace(string(hintData))
		metaPath := path.Join(tablePath, "metadata", fmt.Sprintf("v%s.metadata.json", version))
		info, err := r.ReadTable(ctx, metaPath)
		if err == nil {
			return info, nil
		}
	}

	// Try common metadata file names
	for _, name := range []string{"v1.metadata.json", "v2.metadata.json", "metadata.json"} {
		metaPath := path.Join(tablePath, "metadata", name)
		info, err := r.ReadTable(ctx, metaPath)
		if err == nil {
			return info, nil
		}
	}

	return nil, fmt.Errorf("no Iceberg metadata found at %q", tablePath)
}

// SnapshotToJSON serializes a table info summary to JSON for debugging.
func (info *TableInfo) SnapshotToJSON() ([]byte, error) {
	summary := map[string]any{
		"location":       info.Location,
		"columns":        len(info.Schema.Columns),
		"partition_keys": info.PartitionKeys,
		"data_files":     len(info.DataFiles),
	}
	if info.Metadata != nil {
		summary["format_version"] = info.Metadata.FormatVersion
		summary["table_uuid"] = info.Metadata.TableUUID
	}
	return json.MarshalIndent(summary, "", "  ")
}
