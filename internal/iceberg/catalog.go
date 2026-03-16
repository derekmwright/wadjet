package iceberg

import (
	"context"
	"fmt"
	"time"

	"github.com/derekmwright/caelum/internal/storage/catalog"
	"github.com/derekmwright/caelum/internal/storage/objstore"
)

// CatalogIntegration bridges Iceberg table metadata into Caelum's native catalog.
type CatalogIntegration struct {
	reader  *TableReader
	catalog *catalog.Catalog
}

// NewCatalogIntegration creates a new integration bridge.
func NewCatalogIntegration(cat *catalog.Catalog) *CatalogIntegration {
	return &CatalogIntegration{
		reader:  NewTableReader(cat.Store(), cat.Bucket()),
		catalog: cat,
	}
}

// NewCatalogIntegrationWithStore creates an integration bridge with a custom store/bucket
// (e.g., when the Iceberg table is in a different bucket than the catalog).
func NewCatalogIntegrationWithStore(cat *catalog.Catalog, store objstore.Store, bucket string) *CatalogIntegration {
	return &CatalogIntegration{
		reader:  NewTableReader(store, bucket),
		catalog: cat,
	}
}

// RegisterTable reads an Iceberg table from the given metadata path and
// registers it in the Caelum catalog with the given name. Data files from
// the Iceberg manifest are registered as partition entries in the catalog.
func (ci *CatalogIntegration) RegisterTable(ctx context.Context, name, metadataPath string) (*TableInfo, error) {
	info, err := ci.reader.ReadTable(ctx, metadataPath)
	if err != nil {
		return nil, fmt.Errorf("reading Iceberg table: %w", err)
	}

	// Register the table schema in the catalog
	if err := ci.catalog.CreateTable(ctx, name, info.Schema, info.PartitionKeys); err != nil {
		return nil, fmt.Errorf("creating catalog table %q: %w", name, err)
	}

	// Register data files as partition entries
	if err := ci.registerDataFiles(ctx, name, info); err != nil {
		// Best-effort cleanup
		_ = ci.catalog.DropTable(ctx, name)
		return nil, fmt.Errorf("registering data files for %q: %w", name, err)
	}

	return info, nil
}

// DiscoverAndRegister discovers an Iceberg table at the given path and
// registers it in the catalog.
func (ci *CatalogIntegration) DiscoverAndRegister(ctx context.Context, name, tablePath string) (*TableInfo, error) {
	info, err := ci.reader.DiscoverTable(ctx, tablePath)
	if err != nil {
		return nil, fmt.Errorf("discovering Iceberg table at %q: %w", tablePath, err)
	}

	if err := ci.catalog.CreateTable(ctx, name, info.Schema, info.PartitionKeys); err != nil {
		return nil, fmt.Errorf("creating catalog table %q: %w", name, err)
	}

	if err := ci.registerDataFiles(ctx, name, info); err != nil {
		_ = ci.catalog.DropTable(ctx, name)
		return nil, fmt.Errorf("registering data files for %q: %w", name, err)
	}

	return info, nil
}

// RefreshTable updates an existing catalog table from the latest Iceberg
// metadata. It drops and re-creates the table to pick up schema changes.
func (ci *CatalogIntegration) RefreshTable(ctx context.Context, name, metadataPath string) (*TableInfo, error) {
	info, err := ci.reader.ReadTable(ctx, metadataPath)
	if err != nil {
		return nil, fmt.Errorf("reading Iceberg table: %w", err)
	}

	// Drop existing (ignore error if not found)
	_ = ci.catalog.DropTable(ctx, name)

	if err := ci.catalog.CreateTable(ctx, name, info.Schema, info.PartitionKeys); err != nil {
		return nil, fmt.Errorf("creating catalog table %q: %w", name, err)
	}

	if err := ci.registerDataFiles(ctx, name, info); err != nil {
		_ = ci.catalog.DropTable(ctx, name)
		return nil, err
	}

	return info, nil
}

// registerDataFiles adds all Iceberg data files to the catalog manifest.
func (ci *CatalogIntegration) registerDataFiles(ctx context.Context, tableName string, info *TableInfo) error {
	if len(info.DataFiles) == 0 {
		return nil
	}

	// Group files by partition values
	type partGroup struct {
		values map[string]string
		path   string
		files  []catalog.FileEntry
	}

	groups := make(map[string]*partGroup)
	for _, df := range info.DataFiles {
		// Build partition key from partition values
		partKey := "__default__"
		partValues := make(map[string]string)
		if df.Partition != nil && len(df.Partition) > 0 {
			partKey = ""
			for k, v := range df.Partition {
				partValues[k] = fmt.Sprintf("%v", v)
				if partKey != "" {
					partKey += "/"
				}
				partKey += fmt.Sprintf("%s=%v", k, v)
			}
		}

		g, ok := groups[partKey]
		if !ok {
			g = &partGroup{
				values: partValues,
				path:   partKey,
			}
			groups[partKey] = g
		}

		// Resolve the file path
		filePath := ci.reader.resolvePath(info.Location, df.FilePath)
		g.files = append(g.files, catalog.FileEntry{
			Path:      filePath,
			SizeBytes: df.FileSizeBytes,
			NumRows:   df.RecordCount,
			CreatedAt: time.Now().UTC(),
		})
	}

	for _, g := range groups {
		if err := ci.catalog.AddFiles(ctx, tableName, g.values, g.path, g.files); err != nil {
			return err
		}
	}

	return nil
}
