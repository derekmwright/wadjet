// Package iceberg provides read-only support for Apache Iceberg table metadata.
// It parses Iceberg metadata JSON files to resolve snapshots, manifests, and
// data file paths for querying existing Iceberg tables.
package iceberg

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/derekmwright/caelum/internal/storage/parquet"
)

// TableMetadata represents an Iceberg table metadata file (v1 or v2).
type TableMetadata struct {
	FormatVersion int              `json:"format-version"`
	TableUUID     string           `json:"table-uuid"`
	Location      string           `json:"location"`
	Schema        Schema           `json:"schema"`
	Schemas       []Schema         `json:"schemas"`
	CurrentSchema int              `json:"current-schema-id"`
	PartitionSpec []PartitionField `json:"partition-spec"`
	PartitionSpecs []PartitionSpec `json:"partition-specs"`
	DefaultSpecID  int             `json:"default-spec-id"`
	Properties    map[string]string `json:"properties"`
	Snapshots     []Snapshot       `json:"snapshots"`
	CurrentSnapshotID *int64       `json:"current-snapshot-id"`
	SortOrders    []SortOrder      `json:"sort-orders"`
}

// Schema represents an Iceberg table schema.
type Schema struct {
	SchemaID int     `json:"schema-id"`
	Type     string  `json:"type"`
	Fields   []Field `json:"fields"`
}

// Field represents a column in an Iceberg schema.
type Field struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     string `json:"type"` // "boolean", "int", "long", "float", "double", "string", "date", "timestamp", "timestamptz", "binary", "uuid", "decimal(p,s)", "fixed[l]"
}

// PartitionSpec groups partition fields under a spec ID.
type PartitionSpec struct {
	SpecID int              `json:"spec-id"`
	Fields []PartitionField `json:"fields"`
}

// PartitionField describes how a source column is partitioned.
type PartitionField struct {
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id"`
	Name      string `json:"name"`
	Transform string `json:"transform"` // identity, bucket[N], truncate[N], year, month, day, hour, void
}

// Snapshot represents an Iceberg table snapshot.
type Snapshot struct {
	SnapshotID   int64             `json:"snapshot-id"`
	ParentID     *int64            `json:"parent-snapshot-id"`
	TimestampMs  int64             `json:"timestamp-ms"`
	ManifestList string            `json:"manifest-list"`
	Summary      map[string]string `json:"summary"`
	SchemaID     *int              `json:"schema-id"`
}

// SortOrder defines how data files are sorted.
type SortOrder struct {
	OrderID int         `json:"order-id"`
	Fields  []SortField `json:"fields"`
}

// SortField defines a sort column and direction.
type SortField struct {
	Transform  string `json:"transform"`
	SourceID   int    `json:"source-id"`
	Direction  string `json:"direction"`
	NullOrder  string `json:"null-order"`
}

// ManifestList is a list of manifest file entries.
type ManifestList struct {
	Entries []ManifestEntry
}

// ManifestEntry points to a manifest file.
type ManifestEntry struct {
	ManifestPath   string `json:"manifest_path"`
	ManifestLength int64  `json:"manifest_length"`
	PartitionSpecID int   `json:"partition_spec_id"`
	AddedSnapshotID int64 `json:"added_snapshot_id"`
	Content         int   `json:"content"` // 0 = data, 1 = deletes
}

// ManifestFile contains data file entries.
type ManifestFile struct {
	Entries []DataFileEntry
}

// DataFileEntry describes a single data file in an Iceberg table.
type DataFileEntry struct {
	FilePath    string            `json:"file_path"`
	FileFormat  string            `json:"file_format"` // PARQUET, ORC, AVRO
	RecordCount int64             `json:"record_count"`
	FileSizeBytes int64           `json:"file_size_in_bytes"`
	Partition   map[string]any    `json:"partition"`
	ColumnSizes map[int]int64     `json:"column_sizes"`
	ValueCounts map[int]int64     `json:"value_counts"`
	NullCounts  map[int]int64     `json:"null_value_counts"`
	LowerBounds map[int]string    `json:"lower_bounds"`
	UpperBounds map[int]string    `json:"upper_bounds"`
}

// ParseTableMetadata parses an Iceberg table metadata JSON file.
func ParseTableMetadata(data []byte) (*TableMetadata, error) {
	var meta TableMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("parsing Iceberg metadata: %w", err)
	}

	if meta.FormatVersion != 1 && meta.FormatVersion != 2 {
		return nil, fmt.Errorf("unsupported Iceberg format version: %d", meta.FormatVersion)
	}

	return &meta, nil
}

// CurrentSnapshot returns the current snapshot, or nil if none exists.
func (m *TableMetadata) CurrentSnapshot() *Snapshot {
	if m.CurrentSnapshotID == nil {
		return nil
	}
	for i := range m.Snapshots {
		if m.Snapshots[i].SnapshotID == *m.CurrentSnapshotID {
			return &m.Snapshots[i]
		}
	}
	return nil
}

// ActiveSchema returns the current schema for the table.
func (m *TableMetadata) ActiveSchema() *Schema {
	// v2: use current-schema-id to find the schema
	if m.FormatVersion >= 2 && len(m.Schemas) > 0 {
		for i := range m.Schemas {
			if m.Schemas[i].SchemaID == m.CurrentSchema {
				return &m.Schemas[i]
			}
		}
		return &m.Schemas[len(m.Schemas)-1]
	}
	// v1: use the single schema field
	return &m.Schema
}

// ActivePartitionSpec returns the current partition spec.
func (m *TableMetadata) ActivePartitionSpec() []PartitionField {
	if len(m.PartitionSpecs) > 0 {
		for _, spec := range m.PartitionSpecs {
			if spec.SpecID == m.DefaultSpecID {
				return spec.Fields
			}
		}
	}
	return m.PartitionSpec
}

// ToParquetSchema converts an Iceberg schema to Caelum's parquet schema.
func (s *Schema) ToParquetSchema() parquet.Schema {
	cols := make([]parquet.Column, len(s.Fields))
	for i, f := range s.Fields {
		cols[i] = parquet.Column{
			Name:     f.Name,
			Type:     icebergTypeToCaelum(f.Type),
			Nullable: !f.Required,
		}
	}
	return parquet.Schema{Columns: cols}
}

// icebergTypeToCaelum maps Iceberg type strings to Caelum TypeIDs.
func icebergTypeToCaelum(iceType string) parquet.TypeID {
	lower := strings.ToLower(iceType)
	switch {
	case lower == "boolean":
		return parquet.TypeBool
	case lower == "int":
		return parquet.TypeInt32
	case lower == "long":
		return parquet.TypeInt64
	case lower == "float":
		return parquet.TypeFloat32
	case lower == "double":
		return parquet.TypeFloat64
	case lower == "string":
		return parquet.TypeString
	case lower == "date":
		return parquet.TypeDate
	case lower == "timestamp", lower == "timestamptz":
		return parquet.TypeTimestamp
	case lower == "binary":
		return parquet.TypeBytes
	case lower == "uuid":
		return parquet.TypeUUID
	case strings.HasPrefix(lower, "decimal"):
		return parquet.TypeFloat64 // approximate
	case strings.HasPrefix(lower, "fixed"):
		return parquet.TypeBytes
	default:
		return parquet.TypeString // fallback
	}
}

// ParseManifestList parses a manifest list JSON (simplified — real Iceberg
// uses Avro, but many implementations also support JSON manifest lists).
func ParseManifestList(data []byte) (*ManifestList, error) {
	var entries []ManifestEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parsing manifest list: %w", err)
	}
	return &ManifestList{Entries: entries}, nil
}

// ParseManifest parses a manifest file JSON (simplified).
func ParseManifest(data []byte) (*ManifestFile, error) {
	var entries []DataFileEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parsing manifest: %w", err)
	}
	return &ManifestFile{Entries: entries}, nil
}

// DataFiles returns all data file paths from a manifest, filtered to
// only include Parquet files.
func (mf *ManifestFile) DataFiles() []DataFileEntry {
	var result []DataFileEntry
	for _, e := range mf.Entries {
		if e.FileFormat == "" || strings.ToUpper(e.FileFormat) == "PARQUET" {
			result = append(result, e)
		}
	}
	return result
}
