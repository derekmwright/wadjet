package catalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func testSchema() parquet.Schema {
	return parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64, Nullable: false},
			{Name: "name", Type: parquet.TypeString, Nullable: true},
			{Name: "region", Type: parquet.TypeString, Nullable: false},
		},
	}
}

func setupCatalog(t *testing.T) (*Catalog, context.Context) {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	cat := NewWithStore(store, "test-bucket")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	return cat, ctx
}

func TestInit(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cat := NewWithStore(store, "test-bucket")

	if err := cat.Init(ctx); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Bucket should exist.
	exists, err := store.BucketExists(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("BucketExists: %v", err)
	}
	if !exists {
		t.Fatal("expected bucket to exist after Init")
	}

	// Catalog meta should be readable from KV.
	_, _, err = cat.KV().Get("local.meta")
	if err != nil {
		t.Fatalf("catalog meta not found in KV after Init: %v", err)
	}

	// Calling Init again should be idempotent (no error).
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("second Init failed: %v", err)
	}
}

func TestCreateTableAndGetTable(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()

	if err := cat.CreateTable(ctx, "users", schema, []string{"region"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	got, err := cat.GetTable(ctx, "users")
	if err != nil {
		t.Fatalf("GetTable: %v", err)
	}

	if got.Name != "users" {
		t.Errorf("Name = %q, want %q", got.Name, "users")
	}
	if got.Version != 1 {
		t.Errorf("Version = %d, want 1", got.Version)
	}
	if len(got.Schema.Columns) != len(schema.Columns) {
		t.Fatalf("got %d columns, want %d", len(got.Schema.Columns), len(schema.Columns))
	}
	for i, col := range got.Schema.Columns {
		if col.Name != schema.Columns[i].Name {
			t.Errorf("column %d name = %q, want %q", i, col.Name, schema.Columns[i].Name)
		}
		if col.Type != schema.Columns[i].Type {
			t.Errorf("column %d type = %v, want %v", i, col.Type, schema.Columns[i].Type)
		}
	}
	if len(got.PartitionKeys) != 1 || got.PartitionKeys[0] != "region" {
		t.Errorf("PartitionKeys = %v, want [region]", got.PartitionKeys)
	}
	if got.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
}

func TestListTables(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()

	// Empty catalog should have no tables.
	tables, err := cat.ListTables(ctx)
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(tables) != 0 {
		t.Fatalf("expected 0 tables, got %d", len(tables))
	}

	// Create two tables and verify they appear.
	if err := cat.CreateTable(ctx, "alpha", schema, nil); err != nil {
		t.Fatalf("CreateTable alpha: %v", err)
	}
	if err := cat.CreateTable(ctx, "beta", schema, nil); err != nil {
		t.Fatalf("CreateTable beta: %v", err)
	}

	tables, err = cat.ListTables(ctx)
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(tables))
	}

	found := map[string]bool{}
	for _, name := range tables {
		found[name] = true
	}
	if !found["alpha"] || !found["beta"] {
		t.Errorf("expected tables alpha and beta, got %v", tables)
	}
}

func TestAddFilesAndGetManifest(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()

	if err := cat.CreateTable(ctx, "events", schema, []string{"region"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	files := []FileEntry{
		{Path: "data/region=us/part-0001.parquet", SizeBytes: 1024, NumRows: 100, CreatedAt: time.Now().UTC()},
		{Path: "data/region=us/part-0002.parquet", SizeBytes: 2048, NumRows: 200, CreatedAt: time.Now().UTC()},
	}
	partValues := map[string]string{"region": "us"}

	if err := cat.AddFiles(ctx, "events", partValues, "data/region=us", files); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}

	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}

	if manifest.Table != "events" {
		t.Errorf("manifest.Table = %q, want %q", manifest.Table, "events")
	}
	if len(manifest.Partitions) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(manifest.Partitions))
	}

	part := manifest.Partitions[0]
	if part.Path != "data/region=us" {
		t.Errorf("partition path = %q, want %q", part.Path, "data/region=us")
	}
	if part.Values["region"] != "us" {
		t.Errorf("partition value region = %q, want %q", part.Values["region"], "us")
	}
	if len(part.Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(part.Files))
	}

	// Add more files to the same partition.
	extraFiles := []FileEntry{
		{Path: "data/region=us/part-0003.parquet", SizeBytes: 512, NumRows: 50, CreatedAt: time.Now().UTC()},
	}
	if err := cat.AddFiles(ctx, "events", partValues, "data/region=us", extraFiles); err != nil {
		t.Fatalf("AddFiles (append): %v", err)
	}

	manifest, err = cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatalf("GetManifest after append: %v", err)
	}
	if len(manifest.Partitions) != 1 {
		t.Fatalf("expected 1 partition after append, got %d", len(manifest.Partitions))
	}
	if len(manifest.Partitions[0].Files) != 3 {
		t.Fatalf("expected 3 files after append, got %d", len(manifest.Partitions[0].Files))
	}

	// #278 regression: re-adding already-registered paths must be
	// idempotent (replace, not append) — re-discovery over a populated
	// catalog previously duplicated every file entry, silently
	// multiplying scan inputs while row-count gates stayed green.
	redisc := []FileEntry{
		{Path: "data/region=us/part-0001.parquet", SizeBytes: 4096, NumRows: 100, CreatedAt: time.Now().UTC()},
		{Path: "data/region=us/part-0002.parquet", SizeBytes: 2048, NumRows: 200, CreatedAt: time.Now().UTC()},
		{Path: "data/region=us/part-0003.parquet", SizeBytes: 512, NumRows: 50, CreatedAt: time.Now().UTC()},
	}
	if err := cat.AddFiles(ctx, "events", partValues, "data/region=us", redisc); err != nil {
		t.Fatalf("AddFiles (re-discovery): %v", err)
	}
	manifest, err = cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatalf("GetManifest after re-discovery: %v", err)
	}
	if got := len(manifest.Partitions[0].Files); got != 3 {
		t.Fatalf("re-discovery duplicated entries: got %d files, want 3", got)
	}
	// Replacement refreshes metadata (last writer wins).
	for _, f := range manifest.Partitions[0].Files {
		if f.Path == "data/region=us/part-0001.parquet" && f.SizeBytes != 4096 {
			t.Fatalf("re-added entry did not refresh metadata: size=%d, want 4096", f.SizeBytes)
		}
	}

	// Add files to a different partition.
	euFiles := []FileEntry{
		{Path: "data/region=eu/part-0001.parquet", SizeBytes: 768, NumRows: 80, CreatedAt: time.Now().UTC()},
	}
	if err := cat.AddFiles(ctx, "events", map[string]string{"region": "eu"}, "data/region=eu", euFiles); err != nil {
		t.Fatalf("AddFiles (new partition): %v", err)
	}

	manifest, err = cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatalf("GetManifest after new partition: %v", err)
	}
	if len(manifest.Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(manifest.Partitions))
	}
}

func TestDropTable(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()

	if err := cat.CreateTable(ctx, "logs", schema, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := cat.CreateTable(ctx, "metrics", schema, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	if err := cat.DropTable(ctx, "logs"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}

	tables, err := cat.ListTables(ctx)
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 table after drop, got %d", len(tables))
	}
	if tables[0] != "metrics" {
		t.Errorf("remaining table = %q, want %q", tables[0], "metrics")
	}

	// GetTable on dropped table should fail.
	_, err = cat.GetTable(ctx, "logs")
	if err == nil {
		t.Fatal("expected error for dropped table, got nil")
	}

	// KV keys should be cleaned up.
	_, _, err = cat.KV().Get("local.table.logs")
	if err != ErrKeyNotFound {
		t.Error("expected table KV key to be deleted after drop")
	}
	_, _, err = cat.KV().Get("local.manifest.logs")
	if err != ErrKeyNotFound {
		t.Error("expected manifest KV key to be deleted after drop")
	}
}

func TestGetTableNotFound(t *testing.T) {
	cat, ctx := setupCatalog(t)

	_, err := cat.GetTable(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent table, got nil")
	}
}

func TestCreateTableDuplicate(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()

	if err := cat.CreateTable(ctx, "dup", schema, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	err := cat.CreateTable(ctx, "dup", schema, nil)
	if err == nil {
		t.Fatal("expected error when creating duplicate table, got nil")
	}
}

// TestCreateTableRefusesFoldCollidingColumns: CreateTable validated the
// partition keys and nothing else, and the embedded API reaches it directly,
// so a schema of [V INT32, v INT64] was accepted and stored. Nothing
// downstream can then answer "what type is v" — the parquet reader maps a file
// column to a catalog column by FoldName, so which of the two entries decided
// the answer came down to the order they were listed in.
func TestCreateTableRefusesFoldCollidingColumns(t *testing.T) {
	for _, tc := range []struct {
		name    string
		columns []parquet.Column
		wantErr bool
	}{
		{"case_only", []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "V", Type: parquet.TypeInt32},
			{Name: "v", Type: parquet.TypeInt64},
		}, true},
		{"exact_duplicate", []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "id", Type: parquet.TypeString},
		}, true},
		{"mixed_case", []parquet.Column{
			{Name: "Amount", Type: parquet.TypeInt64},
			{Name: "AMOUNT", Type: parquet.TypeInt64},
		}, true},
		// Distinct names that merely SHARE a prefix are fine, and so is a
		// single column whose spelling is not lower case: folding is the
		// identity rule, not a naming policy.
		{"distinct", []parquet.Column{
			{Name: "Amount", Type: parquet.TypeInt64},
			{Name: "amount_usd", Type: parquet.TypeInt64},
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat, ctx := setupCatalog(t)
			err := cat.CreateTable(ctx, "t_"+tc.name, parquet.Schema{Columns: tc.columns}, nil)
			if tc.wantErr {
				if err == nil {
					t.Fatal("CreateTable stored a schema that names one column twice — " +
						"the column's type is then decided by list order")
				}
				for _, want := range []string{"t_" + tc.name, "twice"} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not mention %q", err, want)
					}
				}
				// And nothing was stored.
				if _, err := cat.GetTable(ctx, "t_"+tc.name); err == nil {
					t.Error("the refused table exists in the catalog")
				}
				return
			}
			if err != nil {
				t.Fatalf("CreateTable refused a schema with distinct names: %v", err)
			}
		})
	}
}

func TestDropTableNotFound(t *testing.T) {
	cat, ctx := setupCatalog(t)

	err := cat.DropTable(ctx, "ghost")
	if err == nil {
		t.Fatal("expected error when dropping non-existent table, got nil")
	}
}

func TestCreateTableInvalidPartitionKey(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()

	err := cat.CreateTable(ctx, "bad", schema, []string{"nonexistent_col"})
	if err == nil {
		t.Fatal("expected error for invalid partition key, got nil")
	}
}

func TestClusterID(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewWithCluster(NewMemKV(), store, "test", "afb-east")
	if cat.ClusterID() != "afb-east" {
		t.Fatalf("expected cluster ID 'afb-east', got %q", cat.ClusterID())
	}
}

func TestFederatedCatalog(t *testing.T) {
	ctx := context.Background()
	schema := testSchema()

	// Shared KV simulates NATS KV replicated across clusters
	sharedKV := NewMemKV()
	store := objstore.NewMemStore()
	_ = store.MakeBucket(ctx, "test")

	// Create two clusters sharing the same KV
	central := NewWithCluster(sharedKV, store, "test", "central")
	if err := central.Init(ctx); err != nil {
		t.Fatal(err)
	}
	edge := NewWithCluster(sharedKV, store, "test", "afb-east")
	if err := edge.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Central creates a table
	if err := central.CreateTable(ctx, "users", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Edge creates a different table
	if err := edge.CreateTable(ctx, "sensor_data", schema, []string{"region"}); err != nil {
		t.Fatal(err)
	}

	// Each catalog only sees its own tables via ListTables
	centralTables, _ := central.ListTables(ctx)
	edgeTables, _ := edge.ListTables(ctx)
	if len(centralTables) != 1 || centralTables[0] != "users" {
		t.Fatalf("central should see [users], got %v", centralTables)
	}
	if len(edgeTables) != 1 || edgeTables[0] != "sensor_data" {
		t.Fatalf("edge should see [sensor_data], got %v", edgeTables)
	}

	// ListClusters sees both clusters
	clusters, err := central.ListClusters()
	if err != nil {
		t.Fatal(err)
	}
	if len(clusters) != 2 {
		t.Fatalf("expected 2 clusters, got %d", len(clusters))
	}

	clusterMap := make(map[string][]string)
	for _, c := range clusters {
		clusterMap[c.ClusterID] = c.Tables
	}
	if tables, ok := clusterMap["central"]; !ok || len(tables) != 1 || tables[0] != "users" {
		t.Errorf("central cluster tables: %v", clusterMap["central"])
	}
	if tables, ok := clusterMap["afb-east"]; !ok || len(tables) != 1 || tables[0] != "sensor_data" {
		t.Errorf("afb-east cluster tables: %v", clusterMap["afb-east"])
	}

	// Central can read edge's table metadata via GetRemoteTable
	remoteMeta, err := central.GetRemoteTable("afb-east", "sensor_data")
	if err != nil {
		t.Fatal(err)
	}
	if remoteMeta.Name != "sensor_data" {
		t.Errorf("expected table name 'sensor_data', got %q", remoteMeta.Name)
	}
	if len(remoteMeta.PartitionKeys) != 1 || remoteMeta.PartitionKeys[0] != "region" {
		t.Errorf("expected partition keys [region], got %v", remoteMeta.PartitionKeys)
	}

	// Central can read edge's manifest via GetRemoteManifest
	remoteManifest, err := central.GetRemoteManifest("afb-east", "sensor_data")
	if err != nil {
		t.Fatal(err)
	}
	if remoteManifest.Table != "sensor_data" {
		t.Errorf("expected manifest table 'sensor_data', got %q", remoteManifest.Table)
	}
}

func TestAddDeleteMarkers(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	files := []FileEntry{
		{Path: "data/part-0001.parquet", SizeBytes: 1024, NumRows: 100, CreatedAt: time.Now().UTC()},
		{Path: "data/part-0002.parquet", SizeBytes: 2048, NumRows: 200, CreatedAt: time.Now().UTC()},
	}
	if err := cat.AddFiles(ctx, "events", nil, "", files); err != nil {
		t.Fatal(err)
	}

	// Add delete markers
	markers := []DeleteMarker{
		{FilePath: "data/part-0001.parquet", RowIndices: []int64{0, 5, 10}},
		{FilePath: "data/part-0002.parquet", RowIndices: []int64{3}},
	}
	if err := cat.AddDeleteMarkers(ctx, "events", markers); err != nil {
		t.Fatalf("AddDeleteMarkers: %v", err)
	}

	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}

	if len(manifest.DeleteMarkers) != 2 {
		t.Fatalf("expected 2 delete markers, got %d", len(manifest.DeleteMarkers))
	}

	markerMap := make(map[string][]int64)
	for _, dm := range manifest.DeleteMarkers {
		markerMap[dm.FilePath] = dm.RowIndices
	}

	if len(markerMap["data/part-0001.parquet"]) != 3 {
		t.Errorf("expected 3 deleted indices for part-0001, got %d", len(markerMap["data/part-0001.parquet"]))
	}
	if len(markerMap["data/part-0002.parquet"]) != 1 {
		t.Errorf("expected 1 deleted index for part-0002, got %d", len(markerMap["data/part-0002.parquet"]))
	}
}

func TestAddDeleteMarkers_Merge(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	files := []FileEntry{{Path: "data/part-0001.parquet", SizeBytes: 1024, NumRows: 100, CreatedAt: time.Now().UTC()}}
	if err := cat.AddFiles(ctx, "events", nil, "", files); err != nil {
		t.Fatal(err)
	}

	// First batch of deletes
	if err := cat.AddDeleteMarkers(ctx, "events", []DeleteMarker{
		{FilePath: "data/part-0001.parquet", RowIndices: []int64{0, 5}},
	}); err != nil {
		t.Fatal(err)
	}

	// Second batch — should merge, not overwrite
	if err := cat.AddDeleteMarkers(ctx, "events", []DeleteMarker{
		{FilePath: "data/part-0001.parquet", RowIndices: []int64{5, 10}},
	}); err != nil {
		t.Fatal(err)
	}

	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}

	if len(manifest.DeleteMarkers) != 1 {
		t.Fatalf("expected 1 marker entry (merged), got %d", len(manifest.DeleteMarkers))
	}

	// Should have {0, 5, 10} — deduped
	indices := make(map[int64]bool)
	for _, idx := range manifest.DeleteMarkers[0].RowIndices {
		indices[idx] = true
	}
	if len(indices) != 3 {
		t.Fatalf("expected 3 unique indices after merge, got %d", len(indices))
	}
	for _, expected := range []int64{0, 5, 10} {
		if !indices[expected] {
			t.Errorf("expected index %d in merged markers", expected)
		}
	}
}

func TestAggregateColumnStats(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()

	if err := cat.CreateTable(ctx, "metrics", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Add files with column stats
	files := []FileEntry{
		{
			Path: "data/chunk_0001.parquet", SizeBytes: 1024, NumRows: 100,
			CreatedAt: time.Now().UTC(),
			ColumnStats: map[string]FileColumnStats{
				"id":   {MinValue: int64(1), MaxValue: int64(100), NullCount: 0},
				"name": {MinValue: "alice", MaxValue: "zara", NullCount: 5},
			},
		},
		{
			Path: "data/chunk_0002.parquet", SizeBytes: 2048, NumRows: 200,
			CreatedAt: time.Now().UTC(),
			ColumnStats: map[string]FileColumnStats{
				"id":   {MinValue: int64(50), MaxValue: int64(300), NullCount: 0},
				"name": {MinValue: "bob", MaxValue: "yvonne", NullCount: 10},
			},
		},
	}
	if err := cat.AddFiles(ctx, "metrics", nil, "data/", files); err != nil {
		t.Fatal(err)
	}

	agg, err := cat.AggregateColumnStats(ctx, "metrics")
	if err != nil {
		t.Fatal(err)
	}
	if agg == nil {
		t.Fatal("expected non-nil aggregated stats")
	}

	// id: min=1, max=300, nulls=0
	// Note: JSON roundtrip converts int64 → float64
	idStats := agg["id"]
	idMin, _ := toStatFloat(idStats.MinValue)
	idMax, _ := toStatFloat(idStats.MaxValue)
	if idMin != 1 {
		t.Errorf("id MinValue: got %v, want 1", idStats.MinValue)
	}
	if idMax != 300 {
		t.Errorf("id MaxValue: got %v, want 300", idStats.MaxValue)
	}
	if idStats.NullCount != 0 {
		t.Errorf("id NullCount: got %d, want 0", idStats.NullCount)
	}

	// name: min=alice, max=zara, nulls=15
	nameStats := agg["name"]
	if nameStats.MinValue != "alice" {
		t.Errorf("name MinValue: got %v, want alice", nameStats.MinValue)
	}
	if nameStats.MaxValue != "zara" {
		t.Errorf("name MaxValue: got %v, want zara", nameStats.MaxValue)
	}
	if nameStats.NullCount != 15 {
		t.Errorf("name NullCount: got %d, want 15", nameStats.NullCount)
	}
	if nameStats.TotalRows != 300 {
		t.Errorf("name TotalRows: got %d, want 300", nameStats.TotalRows)
	}
}

func TestAggregateColumnStats_NoStats(t *testing.T) {
	cat, ctx := setupCatalog(t)
	if err := cat.CreateTable(ctx, "empty", testSchema(), nil); err != nil {
		t.Fatal(err)
	}

	// Files without column stats
	files := []FileEntry{
		{Path: "data/chunk.parquet", SizeBytes: 100, NumRows: 10, CreatedAt: time.Now().UTC()},
	}
	if err := cat.AddFiles(ctx, "empty", nil, "data/", files); err != nil {
		t.Fatal(err)
	}

	agg, err := cat.AggregateColumnStats(ctx, "empty")
	if err != nil {
		t.Fatal(err)
	}
	if agg != nil {
		t.Errorf("expected nil stats for files without column stats, got %v", agg)
	}
}

func TestRemoveFiles_CleansDeleteMarkers(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	files := []FileEntry{
		{Path: "data/part-0001.parquet", SizeBytes: 1024, NumRows: 100, CreatedAt: time.Now().UTC()},
		{Path: "data/part-0002.parquet", SizeBytes: 2048, NumRows: 200, CreatedAt: time.Now().UTC()},
	}
	if err := cat.AddFiles(ctx, "events", nil, "", files); err != nil {
		t.Fatal(err)
	}

	// Add delete markers to both files
	if err := cat.AddDeleteMarkers(ctx, "events", []DeleteMarker{
		{FilePath: "data/part-0001.parquet", RowIndices: []int64{0}},
		{FilePath: "data/part-0002.parquet", RowIndices: []int64{1}},
	}); err != nil {
		t.Fatal(err)
	}

	// Remove one file — its markers should also be removed
	if err := cat.RemoveFiles(ctx, "events", []string{"data/part-0001.parquet"}); err != nil {
		t.Fatal(err)
	}

	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}

	if len(manifest.DeleteMarkers) != 1 {
		t.Fatalf("expected 1 delete marker after removal, got %d", len(manifest.DeleteMarkers))
	}
	if manifest.DeleteMarkers[0].FilePath != "data/part-0002.parquet" {
		t.Errorf("expected remaining marker for part-0002, got %q", manifest.DeleteMarkers[0].FilePath)
	}
}

// TestAddNewFilesRefusesPathCollision is a #494 regression: a writer that
// mints a fresh path per file (ingest, compaction, delete-marker GC) must
// never have a Path collision silently REPLACE the existing manifest entry
// the way AddFiles/mergeFileEntries deliberately does for re-discovery
// (#278) — that silent replace is how a birthday collision on a short
// chunk ID used to lose a whole file's rows with no error anywhere.
// AddNewFiles must refuse the collision, and the original entry must
// survive untouched.
func TestAddNewFilesRefusesPathCollision(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	original := FileEntry{Path: "tables/events/chunk_aaaa.parquet", SizeBytes: 1000, NumRows: 100, CreatedAt: time.Now().UTC()}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{original}); err != nil {
		t.Fatalf("AddNewFiles (first write): %v", err)
	}

	colliding := FileEntry{Path: "tables/events/chunk_aaaa.parquet", SizeBytes: 999999, NumRows: 999, CreatedAt: time.Now().UTC()}
	err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{colliding})
	if err == nil {
		t.Fatal("expected AddNewFiles to refuse a colliding path, got nil error")
	}

	manifest, gErr := cat.GetManifest(ctx, "events")
	if gErr != nil {
		t.Fatal(gErr)
	}
	if len(manifest.Partitions) != 1 || len(manifest.Partitions[0].Files) != 1 {
		t.Fatalf("expected the original single entry to survive a refused collision, got %+v", manifest.Partitions)
	}
	got := manifest.Partitions[0].Files[0]
	if got.SizeBytes != original.SizeBytes || got.NumRows != original.NumRows {
		t.Fatalf("refused collision must not modify the existing entry: got %+v, want %+v", got, original)
	}
}

// TestAddNewFilesAllowsDistinctPaths sanity-checks the common case: distinct
// freshly minted paths add cleanly, same as AddFiles.
func TestAddNewFilesAllowsDistinctPaths(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	files := []FileEntry{
		{Path: "tables/events/chunk_aaaa.parquet", SizeBytes: 100, NumRows: 10, CreatedAt: time.Now().UTC()},
		{Path: "tables/events/chunk_bbbb.parquet", SizeBytes: 200, NumRows: 20, CreatedAt: time.Now().UTC()},
	}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", files); err != nil {
		t.Fatalf("AddNewFiles: %v", err)
	}

	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Partitions) != 1 || len(manifest.Partitions[0].Files) != 2 {
		t.Fatalf("expected 2 files across 1 partition, got %+v", manifest.Partitions)
	}
}

// TestSwapFileForGCRefusesDuplicatePath is a #494 regression for the other
// writer of freshly minted paths that bypasses AddFiles entirely:
// ForceCompactFile's delete-marker rewrite swaps its new file in via
// SwapFileForGC. A rewrite output landing on a path some OTHER file in the
// same partition already holds must be refused, not silently accepted
// alongside (or over) it.
func TestSwapFileForGCRefusesDuplicatePath(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	partPath := "tables/events"
	a := FileEntry{Path: "tables/events/chunk_a.parquet", SizeBytes: 100, NumRows: 10, CreatedAt: time.Now().UTC()}
	b := FileEntry{Path: "tables/events/chunk_b.parquet", SizeBytes: 200, NumRows: 20, CreatedAt: time.Now().UTC()}
	if err := cat.AddFiles(ctx, "events", nil, partPath, []FileEntry{a, b}); err != nil {
		t.Fatal(err)
	}

	// Rewrite of `a` collides with the already-present `b`.
	newFile := &FileEntry{Path: b.Path, SizeBytes: 1, NumRows: 1, CreatedAt: time.Now().UTC()}
	err := cat.SwapFileForGC(ctx, "events", a.Path, newFile, nil, partPath, map[int64]bool{0: true})
	if err == nil {
		t.Fatal("expected SwapFileForGC to refuse a duplicate path, got nil error")
	}

	manifest, gErr := cat.GetManifest(ctx, "events")
	if gErr != nil {
		t.Fatal(gErr)
	}
	if len(manifest.Partitions) != 1 || len(manifest.Partitions[0].Files) != 2 {
		t.Fatalf("refused swap must leave both original files in place, got %+v", manifest.Partitions)
	}
}

// TestDropTableDefersPhysicalDeletion is a #494 regression: DropTable must
// not physically delete a table's data files itself (a query dispatched
// against the pre-drop manifest may still be reading them), but it must
// not leak them forever either — FlushDroppedTableFiles deletes them once
// their grace period has elapsed.
func TestDropTableDefersPhysicalDeletion(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	path := "tables/events/chunk_aaaa.parquet"
	if _, err := cat.Store().Put(ctx, cat.Bucket(), path, bytes.NewReader([]byte("data")), 4, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{
		{Path: path, SizeBytes: 4, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}

	// Not yet due: a long grace must leave the bytes in place.
	if n := cat.FlushDroppedTableFiles(ctx, time.Hour); n != 0 {
		t.Fatalf("expected 0 files flushed before grace elapses, got %d", n)
	}
	if _, _, err := cat.Store().Get(ctx, cat.Bucket(), path); err != nil {
		t.Fatalf("dropped table's file must survive until its grace elapses: %v", err)
	}

	// Due: a zero/expired grace flushes it.
	time.Sleep(2 * time.Millisecond)
	if n := cat.FlushDroppedTableFiles(ctx, 0); n != 1 {
		t.Fatalf("expected 1 file flushed once due, got %d", n)
	}
	if _, _, err := cat.Store().Get(ctx, cat.Bucket(), path); err == nil {
		t.Error("expected dropped table's file to be deleted from the object store after flush")
	}
}

// TestDropTableFlushSkipsRecreatedObject mirrors compaction's own
// recreated-object guard (deleteFromStore/FlushDeferredDeletes): if the
// object at a to-be-deleted path was modified AFTER the drop was recorded,
// something else legitimately wrote there since, and FlushDroppedTableFiles
// must not delete it out from under that write.
func TestDropTableFlushSkipsRecreatedObject(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	path := "tables/events/chunk_aaaa.parquet"
	if _, err := cat.Store().Put(ctx, cat.Bucket(), path, bytes.NewReader([]byte("old")), 3, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{
		{Path: path, SizeBytes: 3, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}

	// Something rewrites the same path after the drop was recorded.
	time.Sleep(2 * time.Millisecond)
	if _, err := cat.Store().Put(ctx, cat.Bucket(), path, bytes.NewReader([]byte("new-data")), 8, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}

	if n := cat.FlushDroppedTableFiles(ctx, 0); n != 0 {
		t.Fatalf("expected the recreated object to be skipped, got %d files flushed", n)
	}
	rc, _, err := cat.Store().Get(ctx, cat.Bucket(), path)
	if err != nil {
		t.Fatalf("recreated object must survive the flush: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "new-data" {
		t.Fatalf("recreated object content = %q, want %q", got, "new-data")
	}
}

// TestFlushDroppedTableFilesSkipsPathsLiveInARecreatedTable is the
// catalog-level version of the #494 review's reproduced data loss: drop a
// table, recreate a table of the SAME name, and re-register the very same
// object path into it (#278's documented idempotent re-registration
// workflow — a harness/bench loader, or Iceberg's RefreshTable, doing
// exactly that). The live-manifest guard must see the path is referenced
// by the recreated table's CURRENT manifest and refuse to delete it, even
// though a pendingDrops entry for the earlier incarnation is due.
func TestFlushDroppedTableFilesSkipsPathsLiveInARecreatedTable(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	path := "tables/events/chunk_aaaa.parquet"
	if _, err := cat.Store().Put(ctx, cat.Bucket(), path, bytes.NewReader([]byte("data")), 4, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{
		{Path: path, SizeBytes: 4, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}

	// Recreate + re-register the SAME path.
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{
		{Path: path, SizeBytes: 4, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Millisecond)
	if n := cat.FlushDroppedTableFiles(ctx, 0); n != 0 {
		t.Fatalf("expected the live-manifest guard to skip a path re-registered into a recreated table, got %d flushed", n)
	}
	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Partitions) != 1 || len(manifest.Partitions[0].Files) != 1 {
		t.Fatalf("recreated table's manifest should still reference 1 file, got %+v", manifest.Partitions)
	}
	if _, _, err := cat.Store().Get(ctx, cat.Bucket(), path); err != nil {
		t.Errorf("DATA LOSS: recreated table's live file was deleted: %v", err)
	}
}

// TestFlushDroppedTableFilesNeverDeletesOutsideItsOwnTablePrefix is a
// defense-in-depth #494 regression, independent of the live-manifest
// guard: a path a dropped table's manifest held that does NOT take the
// "tables/<name>/..." shape — exactly what an Iceberg-registered table's
// warehouse files look like, or anything registered against a foreign
// store/bucket via iceberg.NewCatalogIntegrationWithStore — must never be
// deleted by FlushDroppedTableFiles, even once its own table is long gone
// and nothing protects it in liveCatalogState.
func TestFlushDroppedTableFilesNeverDeletesOutsideItsOwnTablePrefix(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	foreignPath := "warehouse/events/data/year=2024/part-00000.parquet"
	if _, err := cat.Store().Put(ctx, cat.Bucket(), foreignPath, bytes.NewReader([]byte("PAR1")), 4, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{
		{Path: foreignPath, SizeBytes: 4, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Millisecond)
	if n := cat.FlushDroppedTableFiles(ctx, 0); n != 0 {
		t.Fatalf("expected the foreign-shaped path to be skipped, got %d files flushed", n)
	}
	if _, _, err := cat.Store().Get(ctx, cat.Bucket(), foreignPath); err != nil {
		t.Errorf("a path outside tables/<name>/ must never be deleted: %v", err)
	}
}

// failingKV wraps a real MetaKV and can be told to fail the NEXT Put call —
// used to simulate a DropTable whose final metadata commit fails partway
// through, without needing a real KV outage.
type failingKV struct {
	MetaKV
	failNextPut bool
}

func (f *failingKV) Put(key string, value []byte) (uint64, error) {
	if f.failNextPut {
		f.failNextPut = false
		return 0, errors.New("injected: meta put failed")
	}
	return f.MetaKV.Put(key, value)
}

// TestDropTableKeepsFilesOnFailedMetaPut is a #494 regression for the
// pendingDrops append ordering: DropTable must schedule nothing for
// physical deletion unless its final metadata put actually succeeds. A
// DROP that fails partway through must leave the table's files exactly as
// recoverable as they were before the call — not scheduled for reclaim.
func TestDropTableKeepsFilesOnFailedMetaPut(t *testing.T) {
	store := objstore.NewMemStore()
	kv := &failingKV{MetaKV: NewMemKV()}
	cat := New(kv, store, "test-bucket")
	ctx := context.Background()
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	schema := testSchema()
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	path := "tables/events/chunk_aaaa.parquet"
	if _, err := cat.Store().Put(ctx, cat.Bucket(), path, bytes.NewReader([]byte("data")), 4, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{
		{Path: path, SizeBytes: 4, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	kv.failNextPut = true
	if err := cat.DropTable(ctx, "events"); err == nil {
		t.Fatal("expected DropTable to surface the injected meta-put failure")
	}

	if n := cat.FlushDroppedTableFiles(ctx, 0); n != 0 {
		t.Fatalf("a failed DROP must not schedule any files for deletion, got %d flushed", n)
	}
	if _, _, err := cat.Store().Get(ctx, cat.Bucket(), path); err != nil {
		t.Fatalf("file must survive a failed DROP: %v", err)
	}
}

// TestPendingDropsCapEvictsOldestWithoutDeleting is a #494 regression for
// the reclaim-is-opt-in default: if nothing ever calls
// FlushDroppedTableFiles (compaction.BackgroundConfig.ReclaimDroppedTables
// defaults off), pendingDrops must not grow without bound. Past
// maxPendingTableDrops the OLDEST entry is evicted — leaking its files —
// rather than ever risking their deletion outside FlushDroppedTableFiles's
// guard.
func TestPendingDropsCapEvictsOldestWithoutDeleting(t *testing.T) {
	cat, ctx := setupCatalog(t)

	path0 := "tables/t0/chunk_a.parquet"
	if _, err := cat.Store().Put(ctx, cat.Bucket(), path0, bytes.NewReader([]byte("x")), 1, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	cat.recordPendingDrop("t0", []string{path0})

	for i := 1; i <= maxPendingTableDrops; i++ {
		cat.recordPendingDrop(fmt.Sprintf("t%d", i), []string{fmt.Sprintf("tables/t%d/chunk_a.parquet", i)})
	}

	cat.dropMu.Lock()
	n := len(cat.pendingDrops)
	oldest := cat.pendingDrops[0].table
	cat.dropMu.Unlock()
	if n != maxPendingTableDrops {
		t.Fatalf("pendingDrops = %d entries, want capped at %d", n, maxPendingTableDrops)
	}
	if oldest == "t0" {
		t.Fatal("expected t0 (the oldest entry) to have been evicted")
	}
	// t0's file must be LEAKED (still present) — it fell out of
	// pendingDrops without ever reaching FlushDroppedTableFiles.
	if _, _, err := cat.Store().Get(ctx, cat.Bucket(), path0); err != nil {
		t.Fatalf("evicted drop's file must be leaked, not deleted: %v", err)
	}
}
