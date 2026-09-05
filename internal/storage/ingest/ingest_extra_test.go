package ingest

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestNew_DefaultConfigOverrides(t *testing.T) {
	cat, _ := setupCatalog(t)
	// Pass zero-value config -- all fields should get defaults
	ing := New(cat, testTable, testSchema, []string{"year"}, Config{})

	if ing.config.MaxBufferSize != DefaultConfig().MaxBufferSize {
		t.Errorf("MaxBufferSize = %d, want %d", ing.config.MaxBufferSize, DefaultConfig().MaxBufferSize)
	}
	if ing.config.MaxBufferRows != DefaultConfig().MaxBufferRows {
		t.Errorf("MaxBufferRows = %d, want %d", ing.config.MaxBufferRows, DefaultConfig().MaxBufferRows)
	}
	if ing.config.FlushInterval != DefaultConfig().FlushInterval {
		t.Errorf("FlushInterval = %v, want %v", ing.config.FlushInterval, DefaultConfig().FlushInterval)
	}
	if ing.config.RowGroupSize != DefaultConfig().RowGroupSize {
		t.Errorf("RowGroupSize = %d, want %d", ing.config.RowGroupSize, DefaultConfig().RowGroupSize)
	}
}

func TestNew_CustomConfig(t *testing.T) {
	cat, _ := setupCatalog(t)
	cfg := Config{
		MaxBufferSize: 1024,
		MaxBufferRows: 100,
		FlushInterval: 5 * time.Second,
		RowGroupSize:  50,
	}
	ing := New(cat, testTable, testSchema, []string{"year"}, cfg)

	if ing.config.MaxBufferSize != 1024 {
		t.Errorf("MaxBufferSize = %d, want 1024", ing.config.MaxBufferSize)
	}
	if ing.config.MaxBufferRows != 100 {
		t.Errorf("MaxBufferRows = %d, want 100", ing.config.MaxBufferRows)
	}
}

func TestStartAndStop(t *testing.T) {
	cat, _ := setupCatalog(t)
	cfg := Config{
		FlushInterval: 50 * time.Millisecond,
		MaxBufferSize: 1024 * 1024,
		MaxBufferRows: 1000,
		RowGroupSize:  100,
	}
	ing := New(cat, testTable, testSchema, []string{"year"}, cfg)

	ctx := context.Background()
	rows := []map[string]any{
		{"id": int64(1), "name": "test", "year": "2025"},
	}

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}

	ing.Start()
	// Allow one tick of the flush timer
	time.Sleep(100 * time.Millisecond)

	if err := ing.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestIngest_MissingPartitionKey(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	// Schema where year is nullable so validateRow passes, but it's a partition key
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "year", Type: parquet.TypeString, Nullable: true},
	}}
	_ = cat.CreateTable(ctx, "parttest", schema, []string{"year"})

	ing := New(cat, "parttest", schema, []string{"year"}, DefaultConfig())

	// Row missing the partition key "year" -- validateRow passes (nullable),
	// but partition logic requires the key
	err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "name": "alice"},
	})
	if err == nil {
		t.Fatal("expected error for missing partition key")
	}
	if !strings.Contains(err.Error(), "missing partition key") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFlushAll_EmptyBuffers(t *testing.T) {
	cat, _ := setupCatalog(t)
	ing := New(cat, testTable, testSchema, []string{"year"}, DefaultConfig())
	ctx := context.Background()

	// FlushAll with no data should be a no-op
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll with empty buffers should succeed: %v", err)
	}
}

func TestFlushAll_MultiplePartitions(t *testing.T) {
	cat, store := setupCatalog(t)
	ing := New(cat, testTable, testSchema, []string{"year"}, DefaultConfig())
	ctx := context.Background()

	rows := []map[string]any{
		{"id": int64(1), "name": "a", "year": "2023"},
		{"id": int64(2), "name": "b", "year": "2024"},
		{"id": int64(3), "name": "c", "year": "2025"},
	}

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Should have 3 partitions
	objects, err := store.List(ctx, testBucket, objstore.ListOptions{Prefix: "tables/" + testTable + "/"})
	if err != nil {
		t.Fatal(err)
	}

	parquetFiles := 0
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, ".parquet") {
			parquetFiles++
		}
	}
	if parquetFiles != 3 {
		t.Fatalf("expected 3 parquet files (one per partition), got %d", parquetFiles)
	}
}

func TestIngest_AutoFlushByRowCount(t *testing.T) {
	cat, store := setupCatalog(t)
	cfg := Config{
		MaxBufferRows: 5,                  // very small threshold
		MaxBufferSize: 1024 * 1024 * 1024, // large so row count triggers first
		FlushInterval: 1 * time.Hour,
		RowGroupSize:  100,
	}
	ing := New(cat, testTable, testSchema, []string{"year"}, cfg)
	ctx := context.Background()

	// Ingest more rows than MaxBufferRows
	rows := make([]map[string]any, 10)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i), "name": "x", "year": "2025"}
	}

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}

	// Should have auto-flushed
	objects, err := store.List(ctx, testBucket, objstore.ListOptions{Prefix: "tables/" + testTable + "/"})
	if err != nil {
		t.Fatal(err)
	}

	parquetFiles := 0
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, ".parquet") {
			parquetFiles++
		}
	}
	if parquetFiles == 0 {
		t.Fatal("expected auto-flush to have created at least one parquet file")
	}
}

func TestIngest_AutoFlushBySize(t *testing.T) {
	cat, store := setupCatalog(t)
	cfg := Config{
		MaxBufferSize: 1, // 1 byte threshold -- will trigger immediately
		MaxBufferRows: 1000000,
		FlushInterval: 1 * time.Hour,
		RowGroupSize:  100,
	}
	ing := New(cat, testTable, testSchema, []string{"year"}, cfg)
	ctx := context.Background()

	rows := []map[string]any{
		{"id": int64(1), "name": "test-data-that-is-bigger-than-1-byte", "year": "2025"},
	}

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}

	objects, err := store.List(ctx, testBucket, objstore.ListOptions{Prefix: "tables/" + testTable + "/"})
	if err != nil {
		t.Fatal(err)
	}

	parquetFiles := 0
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, ".parquet") {
			parquetFiles++
		}
	}
	if parquetFiles == 0 {
		t.Fatal("expected auto-flush by size to have created at least one parquet file")
	}
}

func TestEstimateRowSize(t *testing.T) {
	row := map[string]any{
		"id":   int64(1),
		"name": "alice",
		"data": []byte{1, 2, 3, 4, 5},
	}
	size := estimateRowSize(row)
	if size <= 0 {
		t.Fatal("expected positive size estimate")
	}

	// Larger string should produce larger estimate
	rowBig := map[string]any{
		"name": strings.Repeat("x", 1000),
	}
	sizeBig := estimateRowSize(rowBig)
	if sizeBig <= size {
		t.Fatalf("larger row should have larger size estimate: %d vs %d", sizeBig, size)
	}
}

func TestEstimateRowSize_Empty(t *testing.T) {
	row := map[string]any{}
	size := estimateRowSize(row)
	if size < 64 {
		t.Fatalf("empty row should have at least map overhead of 64, got %d", size)
	}
}

func TestEstimateRowSize_NumericTypes(t *testing.T) {
	row := map[string]any{
		"a": int64(1),
		"b": float64(3.14),
		"c": true,
	}
	size := estimateRowSize(row)
	if size <= 0 {
		t.Fatal("expected positive size estimate")
	}
}

func TestCheckType_Bool(t *testing.T) {
	col := parquet.Column{Name: "flag", Type: parquet.TypeBool}

	if err := checkType(col, true); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, "not-bool"); err == nil {
		t.Fatal("expected error for string in bool column")
	}
}

func TestCheckType_Int32(t *testing.T) {
	col := parquet.Column{Name: "val", Type: parquet.TypeInt32}

	validValues := []any{int(1), int8(1), int16(1), int32(1), int64(1), uint8(1), uint16(1), uint32(1)}
	for _, v := range validValues {
		if err := checkType(col, v); err != nil {
			t.Errorf("checkType(Int32, %T(%v)): unexpected error: %v", v, v, err)
		}
	}

	if err := checkType(col, "not-int"); err == nil {
		t.Fatal("expected error for string in int32 column")
	}
	// A WHOLE float is stored by the leaf, so this door admits it too: the
	// two boundaries are one rule now, and the writer's answer for float64(1)
	// is 1. A float with a fraction, a NaN or a magnitude no int32 holds is
	// still refused here, which is what changed (the old rule refused all four
	// at the door and the writer TRUNCATED the first two).
	if err := checkType(col, float64(1.0)); err != nil {
		t.Errorf("checkType(Int32, float64(1.0)): %v — the leaf stores 1", err)
	}
	for _, bad := range []any{float64(1.5), math.NaN(), float64(1e10)} {
		if err := checkType(col, bad); err == nil {
			t.Errorf("checkType(Int32, %v): accepted; no int32 holds it exactly", bad)
		}
	}
}

func TestCheckType_Int64(t *testing.T) {
	col := parquet.Column{Name: "val", Type: parquet.TypeInt64}

	validValues := []any{int(1), int8(1), int16(1), int32(1), int64(1), uint8(1), uint16(1), uint32(1), uint64(1)}
	for _, v := range validValues {
		if err := checkType(col, v); err != nil {
			t.Errorf("checkType(Int64, %T(%v)): unexpected error: %v", v, v, err)
		}
	}

	if err := checkType(col, "not-int"); err == nil {
		t.Fatal("expected error for string in int64 column")
	}
}

func TestCheckType_Float32(t *testing.T) {
	col := parquet.Column{Name: "val", Type: parquet.TypeFloat32}

	validValues := []any{float32(1.0), float64(1.0), int(1), int32(1), int64(1)}
	for _, v := range validValues {
		if err := checkType(col, v); err != nil {
			t.Errorf("checkType(Float32, %T(%v)): unexpected error: %v", v, v, err)
		}
	}

	if err := checkType(col, "not-float"); err == nil {
		t.Fatal("expected error for string in float32 column")
	}
}

func TestCheckType_Float64(t *testing.T) {
	col := parquet.Column{Name: "val", Type: parquet.TypeFloat64}

	validValues := []any{float32(1.0), float64(1.0), int(1), int32(1), int64(1)}
	for _, v := range validValues {
		if err := checkType(col, v); err != nil {
			t.Errorf("checkType(Float64, %T(%v)): unexpected error: %v", v, v, err)
		}
	}

	if err := checkType(col, "not-float"); err == nil {
		t.Fatal("expected error for string in float64 column")
	}
}

func TestCheckType_String(t *testing.T) {
	col := parquet.Column{Name: "s", Type: parquet.TypeString}

	if err := checkType(col, "hello"); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, 123); err == nil {
		t.Fatal("expected error for int in string column")
	}
}

func TestCheckType_Bytes(t *testing.T) {
	col := parquet.Column{Name: "data", Type: parquet.TypeBytes}

	if err := checkType(col, []byte{1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, "string-is-ok"); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, 123); err == nil {
		t.Fatal("expected error for int in bytes column")
	}
}

func TestCheckType_Timestamp(t *testing.T) {
	col := parquet.Column{Name: "ts", Type: parquet.TypeTimestamp}

	if err := checkType(col, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, int64(1234567890)); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, 123); err == nil {
		t.Fatal("expected error for int in timestamp column")
	}
}

func TestCheckType_Date(t *testing.T) {
	col := parquet.Column{Name: "dt", Type: parquet.TypeDate}

	if err := checkType(col, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, int32(100)); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, int64(100)); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, "2026-01-01"); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, float64(1.0)); err == nil {
		t.Fatal("expected error for float in date column")
	}
}

func TestCheckType_Duration(t *testing.T) {
	col := parquet.Column{Name: "dur", Type: parquet.TypeDuration}

	if err := checkType(col, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, int64(1000)); err != nil {
		t.Fatal(err)
	}
	// "1h" is NOT a DURATION literal here and never was: the type is
	// nanoseconds stored as an int64, and ParseDurationNanos accepts a plain
	// integer count and nothing else, deliberately (#673 — accepting Go's
	// "1h30m" would invent a grammar the SQL literal path does not have). This
	// door used to admit it and the WRITER then refused it at the flush; both
	// now answer the same way.
	if err := checkType(col, "3600000000000"); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, "1h"); err == nil {
		t.Fatal("expected error for a Go duration spelling in a duration column")
	}
	if err := checkType(col, 123); err == nil {
		t.Fatal("expected error for int in duration column")
	}
}

func TestCheckType_Port(t *testing.T) {
	col := parquet.Column{Name: "port", Type: parquet.TypePort}

	if err := checkType(col, int32(443)); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, int(80)); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, "not-int"); err == nil {
		t.Fatal("expected error for string in port column")
	}
}

func TestCheckType_Protocol(t *testing.T) {
	col := parquet.Column{Name: "proto", Type: parquet.TypeProtocol}

	if err := checkType(col, int32(6)); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, "not-int"); err == nil {
		t.Fatal("expected error for string in protocol column")
	}
}

func TestCheckType_IPv4(t *testing.T) {
	col := parquet.Column{Name: "ip", Type: parquet.TypeIPv4}

	if err := checkType(col, "192.168.1.1"); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, 12345); err == nil {
		t.Fatal("expected error for int in IPv4 column")
	}
}

func TestCheckType_UUID(t *testing.T) {
	col := parquet.Column{Name: "id", Type: parquet.TypeUUID}

	if err := checkType(col, "550e8400-e29b-41d4-a716-446655440000"); err != nil {
		t.Fatal(err)
	}
	if err := checkType(col, 12345); err == nil {
		t.Fatal("expected error for int in UUID column")
	}
}

func TestIngest_NoPartitionKeys(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}}
	_ = cat.CreateTable(ctx, "nopart", schema, nil)

	ing := New(cat, "nopart", schema, nil, DefaultConfig())

	rows := []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
	}

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	manifest, err := cat.GetManifest(ctx, "nopart")
	if err != nil {
		t.Fatal(err)
	}

	totalFiles := 0
	for _, p := range manifest.Partitions {
		totalFiles += len(p.Files)
	}
	if totalFiles != 1 {
		t.Fatalf("expected 1 file, got %d", totalFiles)
	}
}

func TestIngest_MultipleFlushes(t *testing.T) {
	cat, store := setupCatalog(t)
	ing := New(cat, testTable, testSchema, []string{"year"}, DefaultConfig())
	ctx := context.Background()

	// First batch
	rows1 := []map[string]any{
		{"id": int64(1), "name": "a", "year": "2025"},
	}
	if err := ing.Ingest(ctx, rows1); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Second batch to same partition
	rows2 := []map[string]any{
		{"id": int64(2), "name": "b", "year": "2025"},
	}
	if err := ing.Ingest(ctx, rows2); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Should have 2 files
	objects, err := store.List(ctx, testBucket, objstore.ListOptions{Prefix: "tables/" + testTable + "/"})
	if err != nil {
		t.Fatal(err)
	}

	parquetFiles := 0
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, ".parquet") {
			parquetFiles++
		}
	}
	if parquetFiles != 2 {
		t.Fatalf("expected 2 parquet files after 2 flushes, got %d", parquetFiles)
	}
}

func TestFlushAll_AlreadyFlushed(t *testing.T) {
	cat, _ := setupCatalog(t)
	ing := New(cat, testTable, testSchema, []string{"year"}, DefaultConfig())
	ctx := context.Background()

	rows := []map[string]any{
		{"id": int64(1), "name": "a", "year": "2025"},
	}
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Second FlushAll should be a no-op (buffers cleared)
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRow_NullableColumnWithNil(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "label", Type: parquet.TypeString, Nullable: true},
		{Name: "year", Type: parquet.TypeString, Nullable: false},
	}}
	_ = cat.CreateTable(ctx, "nullable_test", schema, []string{"year"})

	ing := New(cat, "nullable_test", schema, []string{"year"}, DefaultConfig())

	// Null value in nullable column should be OK
	err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "label": nil, "year": "2025"},
	})
	if err != nil {
		t.Fatalf("unexpected error for nil in nullable column: %v", err)
	}
}
