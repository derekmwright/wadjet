package iceberg

import (
	"testing"
)

func TestParseTableMetadataV1(t *testing.T) {
	raw := `{
		"format-version": 1,
		"table-uuid": "abc-123",
		"location": "s3://bucket/warehouse/db/t1",
		"schema": {
			"schema-id": 0,
			"type": "struct",
			"fields": [
				{"id": 1, "name": "id", "required": true, "type": "long"},
				{"id": 2, "name": "name", "required": false, "type": "string"},
				{"id": 3, "name": "ts", "required": false, "type": "timestamptz"}
			]
		},
		"partition-spec": [
			{"source-id": 1, "field-id": 1000, "name": "id_identity", "transform": "identity"}
		],
		"snapshots": [
			{
				"snapshot-id": 100,
				"timestamp-ms": 1700000000000,
				"manifest-list": "metadata/snap-100-manifest-list.json",
				"summary": {"operation": "append"}
			}
		],
		"current-snapshot-id": 100,
		"sort-orders": []
	}`

	meta, err := ParseTableMetadata([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}

	if meta.FormatVersion != 1 {
		t.Errorf("format version = %d, want 1", meta.FormatVersion)
	}
	if meta.TableUUID != "abc-123" {
		t.Errorf("table uuid = %q, want %q", meta.TableUUID, "abc-123")
	}
	if meta.Location != "s3://bucket/warehouse/db/t1" {
		t.Errorf("location = %q", meta.Location)
	}

	schema := meta.ActiveSchema()
	if len(schema.Fields) != 3 {
		t.Fatalf("schema fields = %d, want 3", len(schema.Fields))
	}
	if schema.Fields[0].Name != "id" || schema.Fields[0].Type != "long" {
		t.Errorf("field 0 = %+v", schema.Fields[0])
	}

	snap := meta.CurrentSnapshot()
	if snap == nil {
		t.Fatal("current snapshot is nil")
	}
	if snap.SnapshotID != 100 {
		t.Errorf("snapshot id = %d, want 100", snap.SnapshotID)
	}

	partSpec := meta.ActivePartitionSpec()
	if len(partSpec) != 1 {
		t.Fatalf("partition spec = %d, want 1", len(partSpec))
	}
	if partSpec[0].Transform != "identity" {
		t.Errorf("partition transform = %q", partSpec[0].Transform)
	}
}

func TestParseTableMetadataV2(t *testing.T) {
	raw := `{
		"format-version": 2,
		"table-uuid": "def-456",
		"location": "s3://bucket/warehouse/db/t2",
		"schema": {},
		"schemas": [
			{
				"schema-id": 0,
				"type": "struct",
				"fields": [
					{"id": 1, "name": "old_col", "required": false, "type": "string"}
				]
			},
			{
				"schema-id": 1,
				"type": "struct",
				"fields": [
					{"id": 1, "name": "id", "required": true, "type": "long"},
					{"id": 2, "name": "value", "required": false, "type": "double"}
				]
			}
		],
		"current-schema-id": 1,
		"partition-specs": [
			{"spec-id": 0, "fields": []},
			{"spec-id": 1, "fields": [
				{"source-id": 1, "field-id": 1000, "name": "id_bucket", "transform": "bucket[16]"}
			]}
		],
		"default-spec-id": 1,
		"snapshots": [],
		"sort-orders": []
	}`

	meta, err := ParseTableMetadata([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}

	schema := meta.ActiveSchema()
	if schema.SchemaID != 1 {
		t.Errorf("active schema id = %d, want 1", schema.SchemaID)
	}
	if len(schema.Fields) != 2 {
		t.Fatalf("schema fields = %d, want 2", len(schema.Fields))
	}
	if schema.Fields[1].Name != "value" {
		t.Errorf("field 1 name = %q, want %q", schema.Fields[1].Name, "value")
	}

	partSpec := meta.ActivePartitionSpec()
	if len(partSpec) != 1 {
		t.Fatalf("partition spec fields = %d, want 1", len(partSpec))
	}
	if partSpec[0].Transform != "bucket[16]" {
		t.Errorf("partition transform = %q", partSpec[0].Transform)
	}

	if meta.CurrentSnapshot() != nil {
		t.Error("expected nil current snapshot for empty snapshots")
	}
}

func TestParseTableMetadataUnsupportedVersion(t *testing.T) {
	raw := `{"format-version": 99}`
	_, err := ParseTableMetadata([]byte(raw))
	if err == nil {
		t.Fatal("expected error for unsupported version")
	}
}

func TestParseTableMetadataInvalidJSON(t *testing.T) {
	_, err := ParseTableMetadata([]byte(`{bad json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestToParquetSchema(t *testing.T) {
	schema := Schema{
		Fields: []Field{
			{ID: 1, Name: "id", Required: true, Type: "long"},
			{ID: 2, Name: "name", Required: false, Type: "string"},
			{ID: 3, Name: "active", Required: true, Type: "boolean"},
			{ID: 4, Name: "score", Required: false, Type: "double"},
			{ID: 5, Name: "created", Required: false, Type: "timestamptz"},
			{ID: 6, Name: "data", Required: false, Type: "binary"},
			{ID: 7, Name: "price", Required: false, Type: "decimal(10,2)"},
			{ID: 8, Name: "uuid_col", Required: false, Type: "uuid"},
			{ID: 9, Name: "day", Required: false, Type: "date"},
			{ID: 10, Name: "tag", Required: false, Type: "fixed[16]"},
			{ID: 11, Name: "count", Required: false, Type: "int"},
			{ID: 12, Name: "weight", Required: false, Type: "float"},
		},
	}

	ps := schema.ToParquetSchema()
	if len(ps.Columns) != 12 {
		t.Fatalf("columns = %d, want 12", len(ps.Columns))
	}

	// Check nullable mapping
	if ps.Columns[0].Nullable != false {
		t.Error("required field should not be nullable")
	}
	if ps.Columns[1].Nullable != true {
		t.Error("non-required field should be nullable")
	}
}

func TestParseManifestList(t *testing.T) {
	raw := `[
		{"manifest_path": "metadata/m1.json", "manifest_length": 1024, "partition_spec_id": 0, "added_snapshot_id": 100, "content": 0},
		{"manifest_path": "metadata/m2.json", "manifest_length": 2048, "partition_spec_id": 0, "added_snapshot_id": 100, "content": 1}
	]`

	ml, err := ParseManifestList([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(ml.Entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(ml.Entries))
	}
	if ml.Entries[0].ManifestPath != "metadata/m1.json" {
		t.Errorf("manifest path = %q", ml.Entries[0].ManifestPath)
	}
	if ml.Entries[1].Content != 1 {
		t.Errorf("content = %d, want 1 (deletes)", ml.Entries[1].Content)
	}
}

func TestParseManifest(t *testing.T) {
	raw := `[
		{"file_path": "data/part-00000.parquet", "file_format": "PARQUET", "record_count": 1000, "file_size_in_bytes": 50000},
		{"file_path": "data/part-00001.parquet", "file_format": "PARQUET", "record_count": 500, "file_size_in_bytes": 25000},
		{"file_path": "data/part-00002.orc", "file_format": "ORC", "record_count": 200, "file_size_in_bytes": 10000}
	]`

	mf, err := ParseManifest([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(mf.Entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(mf.Entries))
	}

	// DataFiles should filter to only Parquet
	dataFiles := mf.DataFiles()
	if len(dataFiles) != 2 {
		t.Fatalf("parquet data files = %d, want 2", len(dataFiles))
	}
	if dataFiles[0].RecordCount != 1000 {
		t.Errorf("record count = %d, want 1000", dataFiles[0].RecordCount)
	}
}

func TestDataFilesEmptyFormat(t *testing.T) {
	// Empty format should be treated as Parquet
	mf := &ManifestFile{
		Entries: []DataFileEntry{
			{FilePath: "data/file.parquet", FileFormat: "", RecordCount: 100},
		},
	}
	files := mf.DataFiles()
	if len(files) != 1 {
		t.Errorf("empty format should be treated as parquet, got %d files", len(files))
	}
}

func TestSnapshotToJSON(t *testing.T) {
	fv := 2
	s := &Schema{Fields: []Field{{ID: 1, Name: "a"}, {ID: 2, Name: "b"}}}
	info := &TableInfo{
		Metadata: &TableMetadata{
			FormatVersion: fv,
			TableUUID:     "test-uuid",
		},
		Schema:        s.ToParquetSchema(),
		DataFiles:     []DataFileEntry{{FilePath: "f1.parquet"}, {FilePath: "f2.parquet"}},
		PartitionKeys: []string{"a"},
		Location:      "s3://bucket/table",
	}

	data, err := info.SnapshotToJSON()
	if err != nil {
		t.Fatal(err)
	}
	js := string(data)
	if len(js) == 0 {
		t.Fatal("empty JSON output")
	}
}
